// Package atomicfile durably replaces a small file all-or-nothing: after a
// crash at any point, readers see either the complete old contents or the
// complete new contents, never a mix and never a torn file.
//
// Recipe (each step is load-bearing; the crash tests break each one):
//
//  1. write a temp file, 2. fsync it, 3. rename over the target,
//  4. fsync the directory so the rename itself survives power loss.
//
// Without step 2 a crash can leave the new name pointing at empty data; without
// step 4 Write can return success and the old contents come back after a crash.
//
// Files are versioned and checksummed so bit rot is detected instead of served:
//
//	magic "CABF" | version u8 | 3 zero bytes | length u64 | payload | crc32c(all preceding bytes) u32
//
// The whole payload is held in memory, so this is for small state such as
// Raft's term and vote, not for snapshots (see package snapshot).
package atomicfile

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path"

	"github.com/souviks22/continuum-go/internal/vfs"
)

const (
	magic       = "CABF"
	version     = 1
	headerSize  = 16
	trailerSize = 4
	// MaxSize bounds a payload so a corrupt file cannot trigger a huge allocation.
	MaxSize = 1 << 30
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// ErrCorrupt means the file exists but failed validation.
var ErrCorrupt = errors.New("atomicfile: corrupt")

// Write durably and atomically replaces name with payload. If it returns an
// error the durable state is unknown (old or new); callers that cannot tolerate
// that must treat the error as fatal, as with a failed fsync.
func Write(fsys vfs.FS, name string, payload []byte) error {
	if len(payload) > MaxSize {
		return fmt.Errorf("atomicfile: payload of %d bytes exceeds MaxSize", len(payload))
	}
	tmp := name + ".tmp"
	// A temp file left by a crashed earlier attempt is garbage by definition.
	if err := fsys.Remove(tmp); err != nil && !errors.Is(err, vfs.ErrNotExist) {
		return err
	}
	f, err := fsys.Create(tmp)
	if err != nil {
		return err
	}
	abort := func(err error) error {
		f.Close()
		fsys.Remove(tmp) // best effort; Open/Write tolerate a leftover
		return err
	}
	if _, err := f.Write(encode(payload)); err != nil {
		return abort(err)
	}
	if err := f.Sync(); err != nil {
		return abort(err)
	}
	if err := f.Close(); err != nil {
		fsys.Remove(tmp)
		return err
	}
	if err := fsys.Rename(tmp, name); err != nil {
		fsys.Remove(tmp)
		return err
	}
	return fsys.SyncDir(path.Dir(name))
}

func encode(payload []byte) []byte {
	b := make([]byte, 0, headerSize+len(payload)+trailerSize)
	b = append(b, magic...)
	b = append(b, version, 0, 0, 0)
	b = binary.LittleEndian.AppendUint64(b, uint64(len(payload)))
	b = append(b, payload...)
	return binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, castagnoli))
}

// Read returns the payload of name. A missing file returns an error matching
// vfs.ErrNotExist; a damaged one returns ErrCorrupt.
func Read(fsys vfs.FS, name string) ([]byte, error) {
	f, err := fsys.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	size, err := f.Size()
	if err != nil {
		return nil, err
	}
	if size < headerSize+trailerSize || size > MaxSize+headerSize+trailerSize {
		return nil, fmt.Errorf("%w: %s has implausible size %d", ErrCorrupt, name, size)
	}
	buf := make([]byte, size)
	if _, err := io.ReadFull(f, buf); err != nil {
		return nil, err
	}
	switch {
	case string(buf[:4]) != magic:
		return nil, fmt.Errorf("%w: %s: bad magic", ErrCorrupt, name)
	case buf[4] != version:
		return nil, fmt.Errorf("%w: %s: unsupported version %d", ErrCorrupt, name, buf[4])
	case binary.LittleEndian.Uint64(buf[8:16]) != uint64(size-headerSize-trailerSize):
		return nil, fmt.Errorf("%w: %s: length does not match file size", ErrCorrupt, name)
	case crc32.Checksum(buf[:size-trailerSize], castagnoli) != binary.LittleEndian.Uint32(buf[size-trailerSize:]):
		return nil, fmt.Errorf("%w: %s: checksum mismatch", ErrCorrupt, name)
	}
	return buf[headerSize : size-trailerSize], nil
}
