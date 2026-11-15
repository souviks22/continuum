// Package snapshot stores state-machine snapshots durably, one file per
// snapshot, named by the log index they cover.
//
// Contract with the caller (Raft): when Writer.Commit returns nil the snapshot
// is durable, and only then may log entries at or below its index be discarded.
// A crash before Commit returns leaves no visible snapshot, never a partial one.
//
// File format v1 (little-endian):
//
//	header (32): "CSNP" | version u8 | 3 zero bytes | index u64 | term u64 | extraLen u32 | crc32c(first 28 bytes) u32
//	extra (extraLen bytes), then data
//	trailer (16): bodyLen u64 | crc32c(extra+data) u32 | "CSNE"
//
// The length and checksum live in a trailer so a snapshot can be written as a
// stream of unknown size. The trailer also makes a truncated file detectable
// without reading the body.
package snapshot

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/souviks22/continuum/internal/vfs"
)

const (
	magic       = "CSNP"
	endMagic    = "CSNE"
	version     = 1
	headerSize  = 32
	trailerSize = 16
	filePrefix  = "snap-"
	fileSuffix  = ".snap"
	tmpSuffix   = ".tmp"
	indexDigits = 20
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// ErrCorrupt means a snapshot file failed validation.
var ErrCorrupt = errors.New("snapshot: corrupt")

func corrupt(file, format string, a ...any) error {
	return fmt.Errorf("%w: %s: %s", ErrCorrupt, file, fmt.Sprintf(format, a...))
}

// Meta describes a snapshot.
type Meta struct {
	Index uint64 // last log index the snapshot covers
	Term  uint64 // term of that entry
	// Extra is opaque to this package. The Raft layer stores the serialized
	// cluster configuration here so a restored node knows its membership.
	Extra []byte
}

// Store manages the snapshot files in one directory.
type Store struct {
	fs  vfs.FS
	dir string
}

// Open prepares dir, deleting temp files left by interrupted writes.
func Open(fsys vfs.FS, dir string) (*Store, error) {
	if err := fsys.MkdirAll(dir); err != nil {
		return nil, err
	}
	s := &Store{fs: fsys, dir: dir}
	names, err := fsys.List(dir)
	if err != nil {
		return nil, err
	}
	for _, n := range names {
		if strings.HasSuffix(n, fileSuffix+tmpSuffix) {
			if err := fsys.Remove(path.Join(dir, n)); err != nil && !errors.Is(err, vfs.ErrNotExist) {
				return nil, err
			}
		}
	}
	return s, nil
}

func (s *Store) path(index uint64) string {
	return path.Join(s.dir, fmt.Sprintf("%s%0*d%s", filePrefix, indexDigits, index, fileSuffix))
}

func parseName(name string) (uint64, bool) {
	if !strings.HasPrefix(name, filePrefix) || !strings.HasSuffix(name, fileSuffix) {
		return 0, false
	}
	digits := strings.TrimSuffix(strings.TrimPrefix(name, filePrefix), fileSuffix)
	if len(digits) != indexDigits {
		return 0, false
	}
	n, err := strconv.ParseUint(digits, 10, 64)
	return n, err == nil
}

// -- writing ----------------------------------------------------------------

// Writer streams one snapshot to a temp file. Nothing is visible to readers
// until Commit succeeds.
type Writer struct {
	s     *Store
	tmp   string
	final string
	f     vfs.File
	bw    *bufio.Writer
	h     hash.Hash32
	body  uint64 // extra + data bytes so far
	done  bool
}

// Create starts a snapshot for meta.Index. Writing a snapshot at an index that
// already has one replaces it on Commit.
func (s *Store) Create(meta Meta) (*Writer, error) {
	if meta.Index == 0 {
		return nil, errors.New("snapshot: index must be >= 1")
	}
	if uint64(len(meta.Extra)) > 1<<31 {
		return nil, errors.New("snapshot: Extra too large")
	}
	final := s.path(meta.Index)
	tmp := final + tmpSuffix
	if err := s.fs.Remove(tmp); err != nil && !errors.Is(err, vfs.ErrNotExist) {
		return nil, err
	}
	f, err := s.fs.Create(tmp)
	if err != nil {
		return nil, err
	}
	w := &Writer{s: s, tmp: tmp, final: final, f: f, bw: bufio.NewWriterSize(f, 256<<10), h: crc32.New(castagnoli)}

	hdr := make([]byte, headerSize)
	copy(hdr, magic)
	hdr[4] = version
	binary.LittleEndian.PutUint64(hdr[8:16], meta.Index)
	binary.LittleEndian.PutUint64(hdr[16:24], meta.Term)
	binary.LittleEndian.PutUint32(hdr[24:28], uint32(len(meta.Extra)))
	binary.LittleEndian.PutUint32(hdr[28:32], crc32.Checksum(hdr[:28], castagnoli))
	if _, err := w.bw.Write(hdr); err != nil {
		w.cleanup()
		return nil, err
	}
	if _, err := w.Write(meta.Extra); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *Writer) cleanup() {
	w.done = true
	w.f.Close()
	w.s.fs.Remove(w.tmp)
}

// Write appends snapshot data.
func (w *Writer) Write(p []byte) (int, error) {
	if w.done {
		return 0, errors.New("snapshot: writer finished")
	}
	n, err := w.bw.Write(p)
	w.h.Write(p[:n])
	w.body += uint64(n)
	if err != nil {
		w.cleanup()
	}
	return n, err
}

// Commit makes the snapshot durable and visible, atomically. On error the
// snapshot does not exist (or the previous one at this index is unchanged).
func (w *Writer) Commit() error {
	if w.done {
		return errors.New("snapshot: writer finished")
	}
	var tr [trailerSize]byte
	binary.LittleEndian.PutUint64(tr[0:8], w.body)
	binary.LittleEndian.PutUint32(tr[8:12], w.h.Sum32())
	copy(tr[12:], endMagic)
	if _, err := w.bw.Write(tr[:]); err != nil {
		w.cleanup()
		return err
	}
	if err := w.bw.Flush(); err != nil {
		w.cleanup()
		return err
	}
	if err := w.f.Sync(); err != nil {
		w.cleanup()
		return err
	}
	if err := w.f.Close(); err != nil {
		w.done = true
		w.s.fs.Remove(w.tmp)
		return err
	}
	w.done = true
	if err := w.s.fs.Rename(w.tmp, w.final); err != nil {
		w.s.fs.Remove(w.tmp)
		return err
	}
	return w.s.fs.SyncDir(w.s.dir) // the rename must survive power loss
}

// Abort discards an unfinished snapshot.
func (w *Writer) Abort() {
	if !w.done {
		w.cleanup()
	}
}

// -- reading ----------------------------------------------------------------

// Reader streams a snapshot's data, verifying integrity as it goes. Data and
// Meta.Extra are trustworthy only once Read has returned io.EOF without error:
// the checksum covers the whole body and is checked at the end.
type Reader struct {
	Meta Meta

	f      vfs.File
	file   string
	left   int64
	h      hash.Hash32
	want   uint32
	result error // set once the end is reached
}

// Open opens the snapshot at index and validates its structure (header,
// trailer, lengths). The body checksum is verified as the data is read.
func (s *Store) Open(index uint64) (*Reader, error) {
	file := s.path(index)
	f, err := s.fs.Open(file)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*Reader, error) {
		f.Close()
		return nil, err
	}
	size, err := f.Size()
	if err != nil {
		return fail(err)
	}
	if size < headerSize+trailerSize {
		return fail(corrupt(file, "file too short (%d bytes)", size))
	}
	hdr := make([]byte, headerSize)
	if _, err := io.ReadFull(f, hdr); err != nil {
		return fail(err)
	}
	switch {
	case string(hdr[:4]) != magic:
		return fail(corrupt(file, "bad magic"))
	case crc32.Checksum(hdr[:28], castagnoli) != binary.LittleEndian.Uint32(hdr[28:32]):
		return fail(corrupt(file, "header checksum mismatch"))
	case hdr[4] != version:
		return fail(corrupt(file, "unsupported version %d", hdr[4]))
	}
	meta := Meta{Index: binary.LittleEndian.Uint64(hdr[8:16]), Term: binary.LittleEndian.Uint64(hdr[16:24])}
	extraLen := int64(binary.LittleEndian.Uint32(hdr[24:28]))
	if meta.Index != index {
		return fail(corrupt(file, "header says index %d", meta.Index))
	}
	bodyLen := size - headerSize - trailerSize
	if extraLen > bodyLen {
		return fail(corrupt(file, "extra length %d exceeds body %d", extraLen, bodyLen))
	}

	tr := make([]byte, trailerSize)
	if _, err := f.Seek(size-trailerSize, io.SeekStart); err != nil {
		return fail(err)
	}
	if _, err := io.ReadFull(f, tr); err != nil {
		return fail(err)
	}
	if string(tr[12:]) != endMagic {
		return fail(corrupt(file, "bad trailer (truncated or overwritten)"))
	}
	if binary.LittleEndian.Uint64(tr[0:8]) != uint64(bodyLen) {
		return fail(corrupt(file, "trailer length %d does not match file size", binary.LittleEndian.Uint64(tr[0:8])))
	}

	if _, err := f.Seek(headerSize, io.SeekStart); err != nil {
		return fail(err)
	}
	h := crc32.New(castagnoli)
	meta.Extra = make([]byte, extraLen)
	if _, err := io.ReadFull(f, meta.Extra); err != nil {
		return fail(err)
	}
	h.Write(meta.Extra)
	return &Reader{Meta: meta, f: f, file: file, left: bodyLen - extraLen, h: h, want: binary.LittleEndian.Uint32(tr[8:12])}, nil
}

// Read implements io.Reader.
func (r *Reader) Read(p []byte) (int, error) {
	if r.result != nil {
		return 0, r.result
	}
	if r.left == 0 {
		if r.h.Sum32() != r.want {
			r.result = corrupt(r.file, "body checksum mismatch")
		} else {
			r.result = io.EOF
		}
		return 0, r.result
	}
	if int64(len(p)) > r.left {
		p = p[:r.left]
	}
	n, err := r.f.Read(p)
	r.h.Write(p[:n])
	r.left -= int64(n)
	if err != nil && err != io.EOF {
		r.result = err
		return n, err
	}
	if n == 0 && err == io.EOF { // file shrank underneath us
		r.result = corrupt(r.file, "unexpected end of file")
		return 0, r.result
	}
	return n, nil
}

// Close releases the file.
func (r *Reader) Close() error { return r.f.Close() }

// Snapshot is a fully loaded, verified snapshot.
type Snapshot struct {
	Meta Meta
	Data []byte
}

// Load reads and fully verifies the snapshot at index.
func (s *Store) Load(index uint64) (Snapshot, error) {
	r, err := s.Open(index)
	if err != nil {
		return Snapshot{}, err
	}
	defer r.Close()
	data, err := io.ReadAll(r)
	if err != nil {
		return Snapshot{}, err
	}
	return Snapshot{Meta: r.Meta, Data: data}, nil
}

// Entry is the result of checking one snapshot file.
type Entry struct {
	Index uint64
	Meta  Meta  // valid only if Err is nil
	Err   error // nil if the snapshot verified end to end
}

// Scan verifies every snapshot file and returns them ordered by index.
// Corrupt files are reported, not hidden and not deleted: whether an older
// snapshot is usable depends on how much log remains, which only the caller
// knows. Cost is one full read of each snapshot.
func (s *Store) Scan() ([]Entry, error) {
	names, err := s.fs.List(s.dir)
	if err != nil {
		return nil, err
	}
	var out []Entry
	for _, n := range names {
		idx, ok := parseName(n)
		if !ok {
			continue
		}
		e := Entry{Index: idx}
		r, err := s.Open(idx)
		if err == nil {
			_, err = io.Copy(io.Discard, r)
			r.Close()
			e.Meta = r.Meta
		}
		e.Err = err
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Index < out[j].Index })
	return out, nil
}

// Prune keeps the newest keepValid snapshots that verify and deletes every
// older file, including corrupt ones. Snapshots newer than the oldest kept one
// are never touched, even if corrupt: they are evidence. keepValid must be >= 1.
func (s *Store) Prune(keepValid int) error {
	if keepValid < 1 {
		return errors.New("snapshot: keepValid must be >= 1")
	}
	entries, err := s.Scan()
	if err != nil {
		return err
	}
	var cutoff uint64
	kept := 0
	for i := len(entries) - 1; i >= 0; i-- {
		if entries[i].Err == nil {
			kept++
			if kept == keepValid {
				cutoff = entries[i].Index
				break
			}
		}
	}
	if kept < keepValid {
		return nil // fewer valid snapshots than we are asked to keep: delete nothing
	}
	removed := false
	for _, e := range entries {
		if e.Index < cutoff {
			if err := s.fs.Remove(s.path(e.Index)); err != nil {
				return err
			}
			removed = true
		}
	}
	if removed {
		return s.fs.SyncDir(s.dir)
	}
	return nil
}
