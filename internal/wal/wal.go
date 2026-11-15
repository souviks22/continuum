// Package wal is a segmented, checksummed, append-only log of opaque records.
//
// Each record carries its own sequence number (for Raft, the log index), so
// recovery never depends on physical position. Records are stored in segment
// files named by their first sequence number; whole-segment deletion makes
// prefix compaction O(1) instead of a rewrite.
//
// # Durability contract
//
// Nothing is durable until Sync returns. Append only buffers. A crash may lose
// any suffix of unsynced records, never a synced one, and never leaves a gap.
// Once any I/O call fails the Log is poisoned: every later call returns
// ErrFailed. After a failed fsync the kernel may have dropped the dirty pages
// while reporting the error only once, so continuing would silently ack lost
// data. The caller must stop and reopen, which re-validates everything.
//
// # On-disk format (little-endian, version 1)
//
//	segment header (20 bytes): "CWAL" | version u8 | 3 zero bytes | firstSeq u64 | crc32c(first 16 bytes) u32
//	record header  (20 bytes): length u32 | seq u64 | crc32c(length,seq) u32 | crc32c(payload) u32
//	followed by length payload bytes
//
// The header has its own checksum so a flipped length cannot masquerade as a
// torn tail and silently discard acknowledged records.
//
// # Recovery
//
// Open validates every segment. Non-final segments must be perfect (they were
// fsynced before the next segment was created); any damage is ErrCorrupt. In
// the final segment, a damaged tail is truncated and reported through
// TruncatedOnOpen only if nothing valid follows it:
//
//   - an incomplete header, or a valid header with an incomplete payload;
//   - a record with a bad header or payload checksum, when no intact record
//     exists after it anywhere in the segment (a torn write can leave a record
//     partly real and partly zero, with more zero-fill after it).
//
// A bad record with an intact record after it is ErrCorrupt. Known limitation:
// media corruption of the very last record is indistinguishable from a torn
// write, so it is truncated and reported rather than rejected. A replicated
// caller should treat a non-zero TruncatedOnOpen as informational and rely on
// its quorum for the lost suffix.
package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/souviks22/continuum/internal/vfs"
)

const (
	segMagic      = "CWAL"
	segVersion    = 1
	segHeaderSize = 20
	recHeaderSize = 20
	segSuffix     = ".wal"
	seqDigits     = 20
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

var (
	// ErrCorrupt wraps every integrity failure that recovery refuses to repair.
	ErrCorrupt = errors.New("wal: corrupt")
	// ErrFailed is returned after an I/O error poisoned the log.
	ErrFailed = errors.New("wal: log failed; close and reopen to recover")
	// ErrClosed is returned after Close.
	ErrClosed = errors.New("wal: closed")
	// ErrRecordTooLarge is returned by Append for payloads over MaxRecordSize.
	ErrRecordTooLarge = errors.New("wal: record too large")
	// ErrSeqOutOfRange is returned for a sequence number outside the log.
	ErrSeqOutOfRange = errors.New("wal: sequence number out of range")

	errStop = errors.New("stop scan")
)

// CorruptError describes where and why a segment failed validation.
type CorruptError struct {
	Path   string
	Offset int64
	Reason string
}

func (e *CorruptError) Error() string {
	return fmt.Sprintf("wal: corrupt %s at offset %d: %s", e.Path, e.Offset, e.Reason)
}

// Is makes errors.Is(err, ErrCorrupt) true.
func (e *CorruptError) Is(target error) bool { return target == ErrCorrupt }

func corrupt(p string, off int64, format string, a ...any) error {
	return &CorruptError{Path: p, Offset: off, Reason: fmt.Sprintf(format, a...)}
}

// Options configures a Log. Zero values select defaults.
type Options struct {
	// FirstSeq is the sequence number of the first record of an empty log
	// (default 1). A log whose segments all exist ignores it.
	FirstSeq uint64
	// SegmentSize is the soft size limit before rotating (default 64 MiB). A
	// single record larger than this still lands in one segment.
	SegmentSize int64
	// MaxRecordSize bounds one payload (default 16 MiB). It also bounds the
	// allocation recovery makes for a length field, so it must not shrink
	// below the largest record ever written.
	MaxRecordSize int
	// FlushThreshold is how many buffered bytes trigger a write(2) before Sync
	// (default 256 KiB).
	FlushThreshold int
}

func (o Options) withDefaults() (Options, error) {
	if o.FirstSeq == 0 {
		o.FirstSeq = 1
	}
	if o.SegmentSize == 0 {
		o.SegmentSize = 64 << 20
	}
	if o.MaxRecordSize == 0 {
		o.MaxRecordSize = 16 << 20
	}
	if o.FlushThreshold == 0 {
		o.FlushThreshold = 256 << 10
	}
	if o.SegmentSize < segHeaderSize+recHeaderSize || o.MaxRecordSize < 0 || o.FlushThreshold < 0 {
		return o, fmt.Errorf("wal: invalid options %+v", o)
	}
	return o, nil
}

type segment struct {
	firstSeq uint64
	lastSeq  uint64 // firstSeq-1 while empty
	path     string
	size     int64 // logical size, including bytes still in the write buffer
}

func (s *segment) empty() bool { return s.lastSeq < s.firstSeq }

// Log is a segmented write-ahead log. It is safe for concurrent use; a mutex
// serializes all operations, so a Sync in progress blocks Appends.
type Log struct {
	mu   sync.Mutex
	fs   vfs.FS
	dir  string
	opts Options

	segs    []*segment // ordered; the last is active
	active  vfs.File   // handle on the last segment, positioned at its flushed end
	buf     []byte     // encoded records not yet written to active
	nextSeq uint64
	dirty   bool // bytes written or buffered since the last fsync

	truncatedOnOpen int64
	failed          error
	closed          bool
}

// Open recovers the log in dir (creating it if empty), repairing a torn tail.
func Open(fsys vfs.FS, dir string, opts Options) (*Log, error) {
	opts, err := opts.withDefaults()
	if err != nil {
		return nil, err
	}
	if err := fsys.MkdirAll(dir); err != nil {
		return nil, err
	}
	names, err := fsys.List(dir)
	if err != nil {
		return nil, err
	}
	var firsts []uint64
	for _, n := range names {
		if first, ok := parseSegName(n); ok {
			firsts = append(firsts, first)
		}
	}
	sort.Slice(firsts, func(i, j int) bool { return firsts[i] < firsts[j] })

	l := &Log{fs: fsys, dir: dir, opts: opts}
	if len(firsts) == 0 {
		if err := l.startSegment(opts.FirstSeq); err != nil {
			return nil, err
		}
		l.nextSeq = opts.FirstSeq
		return l, nil
	}

	var prevLast uint64
	for i, first := range firsts {
		isLast := i == len(firsts)-1
		if i > 0 && first != prevLast+1 {
			l.abandon()
			return nil, corrupt(l.segPath(first), 0, "segment starts at %d but the previous one ends at %d (missing or overlapping segment)", first, prevLast)
		}
		seg, f, dropped, err := l.recoverSegment(first, isLast)
		if err != nil {
			l.abandon()
			return nil, err
		}
		l.segs = append(l.segs, seg)
		l.truncatedOnOpen += dropped
		prevLast = seg.lastSeq
		if isLast {
			l.active = f
		}
	}
	l.nextSeq = prevLast + 1
	return l, nil
}

// abandon releases handles after a failed Open.
func (l *Log) abandon() {
	if l.active != nil {
		l.active.Close()
	}
}

func (l *Log) segPath(first uint64) string {
	return path.Join(l.dir, fmt.Sprintf("%0*d%s", seqDigits, first, segSuffix))
}

func parseSegName(name string) (uint64, bool) {
	if !strings.HasSuffix(name, segSuffix) {
		return 0, false
	}
	digits := strings.TrimSuffix(name, segSuffix)
	if len(digits) != seqDigits {
		return 0, false
	}
	n, err := strconv.ParseUint(digits, 10, 64)
	return n, err == nil
}

func encodeSegHeader(first uint64) []byte {
	b := make([]byte, segHeaderSize)
	copy(b, segMagic)
	b[4] = segVersion
	binary.LittleEndian.PutUint64(b[8:16], first)
	binary.LittleEndian.PutUint32(b[16:20], crc32.Checksum(b[:16], castagnoli))
	return b
}

func checkSegHeader(b []byte, first uint64) string {
	switch {
	case string(b[:4]) != segMagic:
		return "bad magic"
	case crc32.Checksum(b[:16], castagnoli) != binary.LittleEndian.Uint32(b[16:20]):
		return "bad segment header checksum"
	case b[4] != segVersion:
		return fmt.Sprintf("unsupported segment version %d", b[4])
	case binary.LittleEndian.Uint64(b[8:16]) != first:
		return fmt.Sprintf("header says first seq %d, file name says %d", binary.LittleEndian.Uint64(b[8:16]), first)
	}
	return ""
}

func appendRecord(dst []byte, seq uint64, payload []byte) []byte {
	var h [recHeaderSize]byte
	binary.LittleEndian.PutUint32(h[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint64(h[4:12], seq)
	binary.LittleEndian.PutUint32(h[12:16], crc32.Checksum(h[:12], castagnoli))
	binary.LittleEndian.PutUint32(h[16:20], crc32.Checksum(payload, castagnoli))
	dst = append(dst, h[:]...)
	return append(dst, payload...)
}

// startSegment creates, initializes and makes durable a new empty segment, and
// makes it the active one.
func (l *Log) startSegment(first uint64) error {
	p := l.segPath(first)
	f, err := l.fs.Create(p)
	if err != nil {
		return err
	}
	if _, err := f.Write(encodeSegHeader(first)); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := l.fs.SyncDir(l.dir); err != nil { // the new name must survive a crash
		f.Close()
		return err
	}
	l.segs = append(l.segs, &segment{firstSeq: first, lastSeq: first - 1, path: p, size: segHeaderSize})
	l.active = f
	return nil
}

// recoverSegment validates one segment. For the final segment it also repairs a
// torn tail and returns the handle, positioned for appending.
func (l *Log) recoverSegment(first uint64, isLast bool) (*segment, vfs.File, int64, error) {
	p := l.segPath(first)
	f, err := l.fs.Open(p)
	if err != nil {
		return nil, nil, 0, err
	}
	fail := func(err error) (*segment, vfs.File, int64, error) {
		f.Close()
		return nil, nil, 0, err
	}
	size, err := f.Size()
	if err != nil {
		return fail(err)
	}

	reason := "short segment header"
	if size >= segHeaderSize {
		hdr := make([]byte, segHeaderSize)
		if _, err := io.ReadFull(f, hdr); err != nil {
			return fail(err)
		}
		reason = checkSegHeader(hdr, first)
	}
	if reason != "" {
		if isLast && size <= segHeaderSize {
			// Crash during segment creation: no record can have been
			// acknowledged here. Rebuild the header in place.
			if err := f.Truncate(0); err != nil {
				return fail(err)
			}
			if _, err := f.Seek(0, io.SeekStart); err != nil {
				return fail(err)
			}
			if _, err := f.Write(encodeSegHeader(first)); err != nil {
				return fail(err)
			}
			if err := f.Sync(); err != nil {
				return fail(err)
			}
			return &segment{firstSeq: first, lastSeq: first - 1, path: p, size: segHeaderSize}, f, size, nil
		}
		return fail(corrupt(p, 0, "%s", reason))
	}

	res, err := scanSegment(f, p, first, l.opts.MaxRecordSize, isLast, nil)
	if err != nil {
		return fail(err)
	}
	var dropped int64
	if res.torn {
		dropped = size - res.end
		if err := f.Truncate(res.end); err != nil {
			return fail(err)
		}
		if err := f.Sync(); err != nil {
			return fail(err)
		}
	}
	if _, err := f.Seek(res.end, io.SeekStart); err != nil {
		return fail(err)
	}
	seg := &segment{firstSeq: first, lastSeq: res.last, path: p, size: res.end}
	if !isLast {
		f.Close()
		return seg, nil, 0, nil
	}
	return seg, f, dropped, nil
}

type scanResult struct {
	end  int64  // offset just past the last valid record
	last uint64 // seq of the last valid record; first-1 if none
	torn bool   // a torn tail begins at end
}

// scanSegment validates the records of one segment from its first record. With
// tolerateTorn (recovery of the final segment) a damaged tail is reported via
// scanResult.torn; otherwise any damage is an error. visit, if non-nil, sees
// each valid record; payload is valid only during the call. A visit error stops
// the scan and is returned as is.
func scanSegment(f vfs.File, p string, first uint64, maxRec int, tolerateTorn bool,
	visit func(seq uint64, off int64, payload []byte) error) (scanResult, error) {

	size, err := f.Size()
	if err != nil {
		return scanResult{}, err
	}
	if _, err := f.Seek(segHeaderSize, io.SeekStart); err != nil {
		return scanResult{}, err
	}
	r := bufio.NewReaderSize(f, 1<<16)
	off := int64(segHeaderSize)
	want := first
	done := func(torn bool) (scanResult, error) {
		return scanResult{end: off, last: want - 1, torn: torn}, nil
	}
	var hdr [recHeaderSize]byte
	var payload []byte

	for {
		remaining := size - off
		if remaining == 0 {
			return done(false)
		}
		if remaining < recHeaderSize {
			if tolerateTorn {
				return done(true)
			}
			return scanResult{}, corrupt(p, off, "truncated record header (%d bytes)", remaining)
		}
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return scanResult{}, fmt.Errorf("wal: read %s: %w", p, err)
		}
		length := binary.LittleEndian.Uint32(hdr[0:4])
		seq := binary.LittleEndian.Uint64(hdr[4:12])
		hcrc := binary.LittleEndian.Uint32(hdr[12:16])
		pcrc := binary.LittleEndian.Uint32(hdr[16:20])

		if crc32.Checksum(hdr[:12], castagnoli) != hcrc {
			if !tolerateTorn {
				return scanResult{}, corrupt(p, off, "record header checksum mismatch")
			}
			rest := make([]byte, remaining-recHeaderSize)
			if _, err := io.ReadFull(r, rest); err != nil {
				return scanResult{}, fmt.Errorf("wal: read %s: %w", p, err)
			}
			tail := append(append(make([]byte, 0, remaining), hdr[:]...), rest...)
			if validRecordAfter(tail, want+1, maxRec) {
				return scanResult{}, corrupt(p, off, "bad record header with intact records after it")
			}
			return done(true)
		}
		if seq != want {
			return scanResult{}, corrupt(p, off, "sequence break: found %d, expected %d", seq, want)
		}
		if int64(length) > int64(maxRec) {
			return scanResult{}, corrupt(p, off, "record length %d exceeds MaxRecordSize %d", length, maxRec)
		}
		if int64(length) > remaining-recHeaderSize {
			if tolerateTorn {
				return done(true) // header intact, payload cut short
			}
			return scanResult{}, corrupt(p, off, "truncated record payload")
		}
		if cap(payload) < int(length) {
			payload = make([]byte, length)
		}
		body := payload[:length]
		if _, err := io.ReadFull(r, body); err != nil {
			return scanResult{}, fmt.Errorf("wal: read %s: %w", p, err)
		}
		if crc32.Checksum(body, castagnoli) != pcrc {
			if !tolerateTorn {
				return scanResult{}, corrupt(p, off, "payload checksum mismatch")
			}
			// A torn write can leave a record whose payload only partly landed
			// (real bytes, then zeros) with more zero-fill after it, so the
			// record need not end at EOF. What distinguishes damage in the
			// middle of the log is an intact record after it.
			rest := make([]byte, size-(off+recHeaderSize+int64(length)))
			if _, err := io.ReadFull(r, rest); err != nil {
				return scanResult{}, fmt.Errorf("wal: read %s: %w", p, err)
			}
			tail := make([]byte, 0, size-off)
			tail = append(append(append(tail, hdr[:]...), body...), rest...)
			if validRecordAfter(tail, want+1, maxRec) {
				return scanResult{}, corrupt(p, off, "payload checksum mismatch with intact records after it")
			}
			return done(true)
		}
		if visit != nil {
			if err := visit(seq, off, body); err != nil {
				return scanResult{}, err
			}
		}
		off += recHeaderSize + int64(length)
		want++
	}
}

// validRecordAfter reports whether tail (which starts at a damaged record)
// contains, at any later offset, a fully intact record. If so, the damage is
// in the middle of the log, not at its tail.
func validRecordAfter(tail []byte, minSeq uint64, maxRec int) bool {
	for i := 1; i+recHeaderSize <= len(tail); i++ {
		h := tail[i : i+recHeaderSize]
		if crc32.Checksum(h[:12], castagnoli) != binary.LittleEndian.Uint32(h[12:16]) {
			continue
		}
		length := int64(binary.LittleEndian.Uint32(h[0:4]))
		if binary.LittleEndian.Uint64(h[4:12]) < minSeq || length > int64(maxRec) {
			continue
		}
		end := int64(i) + recHeaderSize + length
		if end > int64(len(tail)) {
			continue
		}
		if crc32.Checksum(tail[i+recHeaderSize:end], castagnoli) == binary.LittleEndian.Uint32(h[16:20]) {
			return true
		}
	}
	return false
}

// -- state ------------------------------------------------------------------

func (l *Log) usable() error {
	if l.closed {
		return ErrClosed
	}
	if l.failed != nil {
		return fmt.Errorf("%w: %w", ErrFailed, l.failed)
	}
	return nil
}

// fail poisons the log with err and returns it.
func (l *Log) fail(err error) error {
	if l.failed == nil {
		l.failed = err
	}
	return err
}

// FirstSeq returns the sequence number of the oldest retained record. It may be
// below a prior DiscardBefore point (compaction is by whole segment). For an
// empty log it equals LastSeq()+1.
func (l *Log) FirstSeq() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.segs[0].firstSeq
}

// LastSeq returns the newest sequence number, or FirstSeq()-1 if empty.
func (l *Log) LastSeq() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.nextSeq - 1
}

// TruncatedOnOpen reports how many bytes recovery discarded as a torn tail.
func (l *Log) TruncatedOnOpen() int64 { return l.truncatedOnOpen }

// SegmentCount returns the number of segment files.
func (l *Log) SegmentCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.segs)
}

// -- writing ----------------------------------------------------------------

func (l *Log) flush() error {
	if len(l.buf) == 0 {
		return nil
	}
	if _, err := l.active.Write(l.buf); err != nil {
		return err
	}
	l.buf = l.buf[:0]
	return nil
}

// Append buffers a record and returns its sequence number. The record is not
// durable until Sync.
func (l *Log) Append(payload []byte) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.usable(); err != nil {
		return 0, err
	}
	if len(payload) > l.opts.MaxRecordSize {
		return 0, fmt.Errorf("%w: %d bytes, max %d", ErrRecordTooLarge, len(payload), l.opts.MaxRecordSize)
	}
	seg := l.segs[len(l.segs)-1]
	recSize := int64(recHeaderSize + len(payload))
	if !seg.empty() && seg.size+recSize > l.opts.SegmentSize {
		if err := l.rotate(); err != nil {
			return 0, l.fail(err)
		}
		seg = l.segs[len(l.segs)-1]
	}
	seq := l.nextSeq
	l.buf = appendRecord(l.buf, seq, payload)
	seg.size += recSize
	seg.lastSeq = seq
	l.nextSeq++
	l.dirty = true
	if len(l.buf) >= l.opts.FlushThreshold {
		if err := l.flush(); err != nil {
			return 0, l.fail(err)
		}
	}
	return seq, nil
}

// rotate seals the active segment durably, then starts a new one. Sealing first
// is what lets recovery demand perfection from every non-final segment.
func (l *Log) rotate() error {
	if err := l.flush(); err != nil {
		return err
	}
	if err := l.active.Sync(); err != nil {
		return err
	}
	l.dirty = false
	if err := l.active.Close(); err != nil {
		return err
	}
	l.active = nil
	return l.startSegment(l.nextSeq)
}

// Sync makes every appended record durable.
func (l *Log) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.usable(); err != nil {
		return err
	}
	if err := l.flush(); err != nil {
		return l.fail(err)
	}
	if !l.dirty {
		return nil
	}
	if err := l.active.Sync(); err != nil {
		return l.fail(err)
	}
	l.dirty = false
	return nil
}

// TruncateTo discards every record with sequence number > seq, durably, and
// continues appending at seq+1. seq must be in [FirstSeq()-1, LastSeq()].
//
// Crash order: later segments are deleted newest-first before the containing
// segment is cut, so a crash at any point leaves a contiguous log that is a
// prefix of the original, never a gap.
func (l *Log) TruncateTo(seq uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.usable(); err != nil {
		return err
	}
	first := l.segs[0].firstSeq
	if seq+1 < first || seq >= l.nextSeq {
		return fmt.Errorf("%w: TruncateTo(%d) on log [%d, %d]", ErrSeqOutOfRange, seq, first, l.nextSeq-1)
	}
	if seq == l.nextSeq-1 {
		return nil
	}
	if err := l.flush(); err != nil {
		return l.fail(err)
	}
	target := seq + 1 // first record to remove
	k := sort.Search(len(l.segs), func(i int) bool { return l.segs[i].firstSeq > target }) - 1

	if k != len(l.segs)-1 {
		if err := l.active.Close(); err != nil {
			return l.fail(err)
		}
		l.active = nil
		for i := len(l.segs) - 1; i > k; i-- {
			if err := l.fs.Remove(l.segs[i].path); err != nil {
				return l.fail(err)
			}
		}
		if err := l.fs.SyncDir(l.dir); err != nil {
			return l.fail(err)
		}
		l.segs = l.segs[:k+1]
		f, err := l.fs.Open(l.segs[k].path)
		if err != nil {
			return l.fail(err)
		}
		l.active = f
	}

	seg := l.segs[k]
	cut := int64(-1)
	err := func() error {
		_, err := scanSegment(l.active, seg.path, seg.firstSeq, l.opts.MaxRecordSize, false,
			func(s uint64, off int64, _ []byte) error {
				if s == target {
					cut = off
					return errStop
				}
				return nil
			})
		if errors.Is(err, errStop) {
			return nil
		}
		return err
	}()
	if err != nil {
		return l.fail(err)
	}
	if cut < 0 {
		return l.fail(corrupt(seg.path, 0, "record %d not found while truncating", target))
	}
	if err := l.active.Truncate(cut); err != nil {
		return l.fail(err)
	}
	if err := l.active.Sync(); err != nil {
		return l.fail(err)
	}
	if _, err := l.active.Seek(cut, io.SeekStart); err != nil {
		return l.fail(err)
	}
	seg.lastSeq = seq
	seg.size = cut
	l.nextSeq = seq + 1
	l.dirty = false
	return nil
}

// DiscardBefore deletes segments whose records all precede seq. Compaction is by
// whole segment, so records below seq may remain (FirstSeq reports where the
// log really starts), and the active segment is never deleted. Oldest segments
// are removed first, so a crash leaves a contiguous suffix.
func (l *Log) DiscardBefore(seq uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.usable(); err != nil {
		return err
	}
	n := 0
	for n < len(l.segs)-1 && l.segs[n+1].firstSeq <= seq {
		n++
	}
	if n == 0 {
		return nil
	}
	for i := 0; i < n; i++ {
		if err := l.fs.Remove(l.segs[i].path); err != nil {
			return l.fail(err)
		}
	}
	if err := l.fs.SyncDir(l.dir); err != nil {
		return l.fail(err)
	}
	l.segs = l.segs[n:]
	return nil
}

// Reset discards the entire log and restarts it at nextSeq (e.g. after
// installing a snapshot that replaces the log). It is NOT atomic: a crash
// leaves either a contiguous suffix of the old log or an empty log. A caller
// that needs Reset's effect must persist its own intent first (such as the
// snapshot) and re-issue Reset on recovery if LastSeq is not what it expects;
// pass the same value as Options.FirstSeq so an emptied log restarts correctly.
func (l *Log) Reset(nextSeq uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.usable(); err != nil {
		return err
	}
	if nextSeq == 0 {
		return fmt.Errorf("%w: sequence numbers start at 1", ErrSeqOutOfRange)
	}
	if err := l.active.Close(); err != nil {
		return l.fail(err)
	}
	l.active = nil
	l.buf = l.buf[:0]
	for _, s := range l.segs {
		if err := l.fs.Remove(s.path); err != nil {
			return l.fail(err)
		}
	}
	if err := l.fs.SyncDir(l.dir); err != nil {
		return l.fail(err)
	}
	l.segs = nil
	if err := l.startSegment(nextSeq); err != nil {
		return l.fail(err)
	}
	l.nextSeq = nextSeq
	l.dirty = false
	return nil
}

// -- reading ----------------------------------------------------------------

// Read calls fn for every record with sequence number >= from, in order,
// including records not yet synced. from must be in [FirstSeq(), LastSeq()+1].
// payload is valid only during the call. fn runs under the log's lock and must
// not call back into the Log. A non-nil error from fn stops the read and is
// returned unchanged.
func (l *Log) Read(from uint64, fn func(seq uint64, payload []byte) error) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.usable(); err != nil {
		return err
	}
	if from < l.segs[0].firstSeq || from > l.nextSeq {
		return fmt.Errorf("%w: Read(%d) on log [%d, %d]", ErrSeqOutOfRange, from, l.segs[0].firstSeq, l.nextSeq-1)
	}
	if from == l.nextSeq {
		return nil
	}
	if err := l.flush(); err != nil {
		return l.fail(err)
	}
	for _, seg := range l.segs {
		if seg.empty() || seg.lastSeq < from {
			continue
		}
		// A separate handle, so the active file's write position is untouched.
		f, err := l.fs.Open(seg.path)
		if err != nil {
			return l.fail(err)
		}
		_, err = scanSegment(f, seg.path, seg.firstSeq, l.opts.MaxRecordSize, false,
			func(s uint64, _ int64, p []byte) error {
				if s < from {
					return nil
				}
				return fn(s, p)
			})
		f.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// Close syncs buffered records and releases the files. After a failure it only
// releases them.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	var err error
	if l.failed == nil && l.active != nil {
		if err = l.flush(); err == nil && l.dirty {
			err = l.active.Sync()
		}
	}
	if l.active != nil {
		if cerr := l.active.Close(); err == nil {
			err = cerr
		}
	}
	return err
}
