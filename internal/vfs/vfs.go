// Package vfs is the filesystem seam. Durable components (WAL, snapshots) talk
// to an FS, never to package os, so their crash behavior can be tested against
// Mem, a simulated filesystem that models what a power cut actually does.
//
// Paths use forward slashes. Durability rules the interfaces make explicit,
// because getting them wrong is how data is lost:
//
//   - File.Sync makes the file's contents durable, but not its directory entry.
//   - A created, renamed or removed name is durable only after SyncDir on its
//     parent directory.
package vfs

import "io/fs"

// ErrNotExist and ErrExist are the errors FS implementations wrap, so callers
// can use errors.Is without caring which implementation they have.
var (
	ErrNotExist = fs.ErrNotExist
	ErrExist    = fs.ErrExist
)

// File is a read-write file with a single cursor shared by Read, Write and Seek.
type File interface {
	Read(p []byte) (int, error)
	Write(p []byte) (int, error)
	Seek(offset int64, whence int) (int64, error)
	Truncate(size int64) error
	Sync() error
	Size() (int64, error)
	Close() error
}

// FS is the set of filesystem operations durable components need.
type FS interface {
	// MkdirAll creates dir and any missing parents.
	MkdirAll(dir string) error
	// Create makes a new read-write file. It fails with ErrExist if name exists.
	Create(name string) (File, error)
	// Open opens an existing file read-write. It fails with ErrNotExist if
	// name is missing.
	Open(name string) (File, error)
	// Remove deletes name.
	Remove(name string) error
	// Rename atomically moves oldname to newname, replacing newname if present.
	Rename(oldname, newname string) error
	// List returns the base names in dir, sorted.
	List(dir string) ([]string, error)
	// SyncDir makes dir's entries (creates, renames, removes) durable.
	SyncDir(dir string) error
}
