package snapshot

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

const archiveMagic = "CSBA"

var ErrInvalidArchive = errors.New("snapshot: invalid backup archive")

// Marshal encodes a verified portable backup using the same checksummed
// snapshot format used by Store. The returned bytes can be written to external
// storage and later passed to Unmarshal or raftnode.RestoreBackup.
func Marshal(meta Meta, data []byte) ([]byte, error) {
	if meta.Index == 0 {
		return nil, errors.New("snapshot: backup index must be >= 1")
	}
	if uint64(len(meta.Extra)) > 1<<31 {
		return nil, errors.New("snapshot: backup metadata too large")
	}
	if uint64(len(meta.Extra))+uint64(len(data)) > uint64(int(^uint(0)>>1))-headerSize-trailerSize {
		return nil, errors.New("snapshot: backup too large")
	}
	// A distinct archive magic prevents an exported bundle being mistaken for
	// a live snapshot filename by tools that inspect raw snapshot files.
	out := make([]byte, headerSize+len(meta.Extra)+len(data)+trailerSize)
	copy(out[:4], archiveMagic)
	out[4] = version
	binary.LittleEndian.PutUint64(out[8:16], meta.Index)
	binary.LittleEndian.PutUint64(out[16:24], meta.Term)
	binary.LittleEndian.PutUint32(out[24:28], uint32(len(meta.Extra)))
	binary.LittleEndian.PutUint32(out[28:32], crc32.Checksum(out[:28], castagnoli))
	copy(out[headerSize:], meta.Extra)
	copy(out[headerSize+len(meta.Extra):], data)
	body := out[headerSize : len(out)-trailerSize]
	tr := out[len(out)-trailerSize:]
	binary.LittleEndian.PutUint64(tr[:8], uint64(len(body)))
	binary.LittleEndian.PutUint32(tr[8:12], crc32.Checksum(body, castagnoli))
	copy(tr[12:], endMagic)
	return out, nil
}

// Unmarshal validates and decodes a portable backup. No metadata or state data
// is returned unless the entire body and framing checksum verify.
func Unmarshal(data []byte) (Snapshot, error) {
	bad := func(reason string) (Snapshot, error) {
		return Snapshot{}, fmt.Errorf("%w: %s", ErrInvalidArchive, reason)
	}
	if len(data) < headerSize+trailerSize {
		return bad("truncated header or trailer")
	}
	h := data[:headerSize]
	if string(h[:4]) != archiveMagic {
		return bad("bad magic")
	}
	if h[4] != version {
		return bad(fmt.Sprintf("unsupported version %d", h[4]))
	}
	if crc32.Checksum(h[:28], castagnoli) != binary.LittleEndian.Uint32(h[28:32]) {
		return bad("header checksum mismatch")
	}
	index := binary.LittleEndian.Uint64(h[8:16])
	if index == 0 {
		return bad("zero snapshot index")
	}
	extraLen := uint64(binary.LittleEndian.Uint32(h[24:28]))
	bodyLen := uint64(len(data) - headerSize - trailerSize)
	if extraLen > bodyLen {
		return bad("metadata exceeds body")
	}
	tr := data[len(data)-trailerSize:]
	if string(tr[12:]) != endMagic || binary.LittleEndian.Uint64(tr[:8]) != bodyLen {
		return bad("invalid trailer")
	}
	body := data[headerSize : len(data)-trailerSize]
	if crc32.Checksum(body, castagnoli) != binary.LittleEndian.Uint32(tr[8:12]) {
		return bad("body checksum mismatch")
	}
	meta := Meta{Index: index, Term: binary.LittleEndian.Uint64(h[16:24]), Extra: append([]byte(nil), body[:extraLen]...)}
	return Snapshot{Meta: meta, Data: append([]byte(nil), body[extraLen:]...)}, nil
}
