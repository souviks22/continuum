package vfs

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/rand/v2"
	"path"
	"sort"
	"sync"
)

// Op names a filesystem operation, for fault injection.
type Op string

const (
	OpCreate   Op = "create"
	OpOpen     Op = "open"
	OpWrite    Op = "write"
	OpSync     Op = "sync"
	OpTruncate Op = "truncate"
	OpRemove   Op = "remove"
	OpRename   Op = "rename"
	OpSyncDir  Op = "syncdir"
)

var (
	// ErrNoSpace is returned by writes that exceed the configured capacity.
	ErrNoSpace = errors.New("vfs: no space left on device")
	// ErrCrashed is returned by file handles that predate a Crash.
	ErrCrashed = errors.New("vfs: file handle invalidated by crash")
)

// CrashOptions controls what survives a simulated power cut.
type CrashOptions struct {
	Seed uint64
	// Torn lets part of each file's unsynced appended tail survive, the way a
	// real crash can: the file may be extended to some length, with only a
	// prefix of the new bytes actually persisted and the rest zero. When false,
	// every file reverts exactly to its last synced contents.
	Torn bool
}

type inode struct {
	data    []byte // what readers see (the page cache)
	durable []byte // what is guaranteed to survive a crash (last Sync)
}

// Mem is an in-memory FS that models durability. It keeps two views of the
// world: the live one, and the one that would survive a power cut.
//
//   - File contents become durable only on File.Sync.
//   - Names (create, rename, remove) become durable only on SyncDir of the
//     parent directory. A file whose directory entry was never synced
//     vanishes in a crash, even if its contents were synced.
//
// Crash applies those rules and invalidates all open handles. A fault hook and
// a capacity limit allow injecting I/O errors and ENOSPC. Safe for concurrent
// use.
type Mem struct {
	mu           sync.Mutex
	names        map[string]*inode
	durableNames map[string]*inode
	hook         func(op Op, name string) error
	capacity     int64
	epoch        int
}

// NewMem returns an empty simulated filesystem.
func NewMem() *Mem {
	return &Mem{
		names:        make(map[string]*inode),
		durableNames: make(map[string]*inode),
	}
}

// SetHook installs a function consulted before every operation; returning an
// error fails the operation with no effect. The hook runs with Mem's lock held
// and must not call back into Mem.
func (m *Mem) SetHook(h func(op Op, name string) error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hook = h
}

// SetCapacity limits total live file bytes; writes beyond it are partial and
// return ErrNoSpace. Zero or negative means unlimited.
func (m *Mem) SetCapacity(n int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.capacity = n
}

// Crash simulates a power cut and restart: names revert to their last
// SyncDir'd state, file contents revert per CrashOptions, and every open
// handle becomes invalid.
func (m *Mem) Crash(o CrashOptions) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.epoch++
	rng := rand.New(rand.NewPCG(o.Seed, 0xC0FFEE))

	keys := make([]string, 0, len(m.durableNames))
	for k := range m.durableNames {
		keys = append(keys, k)
	}
	sort.Strings(keys) // deterministic RNG consumption
	seen := make(map[*inode]bool)
	for _, k := range keys {
		ino := m.durableNames[k]
		if seen[ino] {
			continue
		}
		seen[ino] = true
		ino.data = survivingContents(ino, rng, o.Torn)
		ino.durable = bytes.Clone(ino.data)
	}
	m.names = make(map[string]*inode, len(m.durableNames))
	for k, v := range m.durableNames {
		m.names[k] = v
	}
}

func survivingContents(ino *inode, rng *rand.Rand, torn bool) []byte {
	if torn && len(ino.data) > len(ino.durable) && bytes.HasPrefix(ino.data, ino.durable) {
		extra := len(ino.data) - len(ino.durable)
		extended := rng.IntN(extra + 1) // how far the file got extended
		real := rng.IntN(extended + 1)  // how much of it was actually written
		out := bytes.Clone(ino.durable)
		out = append(out, ino.data[len(ino.durable):len(ino.durable)+real]...)
		return append(out, make([]byte, extended-real)...)
	}
	return bytes.Clone(ino.durable)
}

// ReadFile returns the live contents of name (test helper).
func (m *Mem) ReadFile(name string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ino, ok := m.names[path.Clean(name)]
	if !ok {
		return nil, pathErr("read", name, fs.ErrNotExist)
	}
	return bytes.Clone(ino.data), nil
}

// PutFile creates or replaces name with data, durably (test helper for
// crafting on-disk states).
func (m *Mem) PutFile(name string, data []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	name = path.Clean(name)
	ino := &inode{data: bytes.Clone(data), durable: bytes.Clone(data)}
	m.names[name] = ino
	m.durableNames[name] = ino
}

func pathErr(op, name string, err error) error {
	return &fs.PathError{Op: op, Path: name, Err: err}
}

func (m *Mem) fault(op Op, name string) error {
	if m.hook == nil {
		return nil
	}
	if err := m.hook(op, name); err != nil {
		return pathErr(string(op), name, err)
	}
	return nil
}

func (m *Mem) used() int64 {
	var n int64
	for _, ino := range m.names {
		n += int64(len(ino.data))
	}
	return n
}

// MkdirAll implements FS. Directories are implicit in Mem.
func (m *Mem) MkdirAll(string) error { return nil }

// Create implements FS.
func (m *Mem) Create(name string) (File, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	name = path.Clean(name)
	if err := m.fault(OpCreate, name); err != nil {
		return nil, err
	}
	if _, ok := m.names[name]; ok {
		return nil, pathErr("create", name, fs.ErrExist)
	}
	ino := &inode{}
	m.names[name] = ino
	return &memFile{m: m, ino: ino, name: name, epoch: m.epoch}, nil
}

// Open implements FS.
func (m *Mem) Open(name string) (File, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	name = path.Clean(name)
	if err := m.fault(OpOpen, name); err != nil {
		return nil, err
	}
	ino, ok := m.names[name]
	if !ok {
		return nil, pathErr("open", name, fs.ErrNotExist)
	}
	return &memFile{m: m, ino: ino, name: name, epoch: m.epoch}, nil
}

// Remove implements FS.
func (m *Mem) Remove(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	name = path.Clean(name)
	if err := m.fault(OpRemove, name); err != nil {
		return err
	}
	if _, ok := m.names[name]; !ok {
		return pathErr("remove", name, fs.ErrNotExist)
	}
	delete(m.names, name)
	return nil
}

// Rename implements FS.
func (m *Mem) Rename(oldname, newname string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	oldname, newname = path.Clean(oldname), path.Clean(newname)
	if err := m.fault(OpRename, oldname); err != nil {
		return err
	}
	ino, ok := m.names[oldname]
	if !ok {
		return pathErr("rename", oldname, fs.ErrNotExist)
	}
	m.names[newname] = ino
	delete(m.names, oldname)
	return nil
}

// List implements FS.
func (m *Mem) List(dir string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	dir = path.Clean(dir)
	var out []string
	for name := range m.names {
		if path.Dir(name) == dir {
			out = append(out, path.Base(name))
		}
	}
	sort.Strings(out)
	return out, nil
}

// SyncDir implements FS: the directory's live entries become its durable ones.
func (m *Mem) SyncDir(dir string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	dir = path.Clean(dir)
	if err := m.fault(OpSyncDir, dir); err != nil {
		return err
	}
	for name := range m.durableNames {
		if path.Dir(name) == dir {
			delete(m.durableNames, name)
		}
	}
	for name, ino := range m.names {
		if path.Dir(name) == dir {
			m.durableNames[name] = ino
		}
	}
	return nil
}

type memFile struct {
	m      *Mem
	ino    *inode
	name   string
	off    int64
	epoch  int
	closed bool
}

func (f *memFile) check() error {
	if f.closed {
		return fs.ErrClosed
	}
	if f.epoch != f.m.epoch {
		return ErrCrashed
	}
	return nil
}

func (f *memFile) Read(p []byte) (int, error) {
	f.m.mu.Lock()
	defer f.m.mu.Unlock()
	if err := f.check(); err != nil {
		return 0, err
	}
	if f.off >= int64(len(f.ino.data)) {
		return 0, io.EOF
	}
	n := copy(p, f.ino.data[f.off:])
	f.off += int64(n)
	return n, nil
}

func (f *memFile) Write(p []byte) (int, error) {
	f.m.mu.Lock()
	defer f.m.mu.Unlock()
	if err := f.check(); err != nil {
		return 0, err
	}
	if err := f.m.fault(OpWrite, f.name); err != nil {
		return 0, err
	}
	n := len(p)
	var werr error
	if c := f.m.capacity; c > 0 {
		growth := f.off + int64(n) - int64(len(f.ino.data))
		if growth < 0 {
			growth = 0
		}
		if free := c - f.m.used(); growth > free {
			if free < 0 {
				free = 0
			}
			n -= int(growth - free) // write only what fits
			werr = pathErr("write", f.name, ErrNoSpace)
		}
	}
	if end := f.off + int64(n); end > int64(len(f.ino.data)) {
		f.ino.data = append(f.ino.data, make([]byte, end-int64(len(f.ino.data)))...)
	}
	copy(f.ino.data[f.off:], p[:n])
	f.off += int64(n)
	return n, werr
}

func (f *memFile) Seek(offset int64, whence int) (int64, error) {
	f.m.mu.Lock()
	defer f.m.mu.Unlock()
	if err := f.check(); err != nil {
		return 0, err
	}
	var base int64
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		base = f.off
	case io.SeekEnd:
		base = int64(len(f.ino.data))
	default:
		return 0, fmt.Errorf("vfs: bad whence %d", whence)
	}
	if base+offset < 0 {
		return 0, fmt.Errorf("vfs: negative seek position")
	}
	f.off = base + offset
	return f.off, nil
}

func (f *memFile) Truncate(size int64) error {
	f.m.mu.Lock()
	defer f.m.mu.Unlock()
	if err := f.check(); err != nil {
		return err
	}
	if err := f.m.fault(OpTruncate, f.name); err != nil {
		return err
	}
	if size < 0 {
		return fmt.Errorf("vfs: negative truncate size")
	}
	if size <= int64(len(f.ino.data)) {
		f.ino.data = f.ino.data[:size]
	} else {
		f.ino.data = append(f.ino.data, make([]byte, size-int64(len(f.ino.data)))...)
	}
	return nil
}

func (f *memFile) Sync() error {
	f.m.mu.Lock()
	defer f.m.mu.Unlock()
	if err := f.check(); err != nil {
		return err
	}
	if err := f.m.fault(OpSync, f.name); err != nil {
		return err
	}
	f.ino.durable = bytes.Clone(f.ino.data)
	return nil
}

func (f *memFile) Size() (int64, error) {
	f.m.mu.Lock()
	defer f.m.mu.Unlock()
	if err := f.check(); err != nil {
		return 0, err
	}
	return int64(len(f.ino.data)), nil
}

func (f *memFile) Close() error {
	f.m.mu.Lock()
	defer f.m.mu.Unlock()
	f.closed = true
	return nil
}
