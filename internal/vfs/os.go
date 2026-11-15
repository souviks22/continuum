package vfs

import (
	"os"
)

type osFS struct{}

// OS returns an FS backed by the real operating system.
func OS() FS { return osFS{} }

type osFile struct{ *os.File }

func (f osFile) Size() (int64, error) {
	st, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return st.Size(), nil
}

func (osFS) MkdirAll(dir string) error { return os.MkdirAll(dir, 0o755) }

func (osFS) Create(name string) (File, error) {
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, err
	}
	return osFile{f}, nil
}

func (osFS) Open(name string) (File, error) {
	f, err := os.OpenFile(name, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	return osFile{f}, nil
}

func (osFS) Remove(name string) error             { return os.Remove(name) }
func (osFS) Rename(oldname, newname string) error { return os.Rename(oldname, newname) }

func (osFS) List(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir) // sorted by filename
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names, nil
}

func (osFS) SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		d.Close()
		return err
	}
	return d.Close()
}
