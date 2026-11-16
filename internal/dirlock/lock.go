//go:build linux || darwin

// Package dirlock provides an advisory process lock for a Continuum data
// directory. The lock file is persistent; ownership is represented by the
// kernel lock on its open file description, not by the file's existence.
package dirlock

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

const lockFileName = ".continuum.lock"

// ErrLocked means another process already owns the data directory.
var ErrLocked = errors.New("data directory is already open")

// Lock holds an exclusive advisory lock until Close.
type Lock struct {
	file *os.File
}

// Acquire creates dir if needed and acquires its non-blocking exclusive lock.
func Acquire(dir string) (*Lock, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}
	f, err := os.OpenFile(filepath.Join(dir, lockFileName), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open data directory lock: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, fmt.Errorf("%w: %s", ErrLocked, dir)
		}
		return nil, fmt.Errorf("lock data directory: %w", err)
	}
	return &Lock{file: f}, nil
}

// Close releases the kernel lock and closes its descriptor.
func (l *Lock) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	f := l.file
	l.file = nil
	unlockErr := syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	closeErr := f.Close()
	return errors.Join(unlockErr, closeErr)
}
