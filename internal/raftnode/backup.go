package raftnode

import (
	"errors"
	"fmt"
	"path"
	"time"

	"github.com/souviks22/continuum-go/internal/atomicfile"
	"github.com/souviks22/continuum-go/internal/snapshot"
	"github.com/souviks22/continuum-go/internal/vfs"
)

var ErrRestoreTargetNotEmpty = errors.New("raftnode: backup restore target is not empty")

// Backup returns a checksummed portable snapshot after a ReadIndex barrier.
// The callback runs on this node's serial context; callers should move the
// returned bytes to external storage outside that context. The archive includes
// the applied state and Raft membership at the same applied index.
func (n *Node) Backup(timeout time.Duration, cb func([]byte, error)) error {
	if n.stopped {
		return ErrStopped
	}
	if timeout <= 0 {
		return errors.New("raftnode: backup timeout must be positive")
	}
	if cb == nil {
		return errors.New("raftnode: nil backup callback")
	}
	return n.LinearizableRead(timeout, func(err error) {
		if err != nil {
			cb(nil, err)
			return
		}
		data, err := n.sm.Snapshot()
		if err != nil {
			cb(nil, fmt.Errorf("raftnode: snapshot state for backup: %w", err))
			return
		}
		term, err := n.ms.Term(n.applied)
		if err != nil {
			cb(nil, err)
			return
		}
		extra, err := encodeExtra(snapKindLocal, n.confState)
		if err != nil {
			cb(nil, err)
			return
		}
		archive, err := snapshot.Marshal(snapshot.Meta{Index: n.applied, Term: term, Extra: extra}, data)
		n.observe(EventBackupCreated, n.applied, 0, err)
		cb(archive, err)
	})
}

// RestoreBackup installs a verified backup into an empty node directory. The
// resulting snapshot is marked installed, so recovery cannot replay unrelated
// log history below it. Open the node with Bootstrap disabled afterward, and
// configure transport identities for every member in the archived ConfState.
func RestoreBackup(fsys vfs.FS, dir string, archive []byte) error {
	if fsys == nil || dir == "" {
		return errors.New("raftnode: RestoreBackup requires filesystem and directory")
	}
	backup, err := snapshot.Unmarshal(archive)
	if err != nil {
		return err
	}
	kind, cs, err := decodeExtra(backup.Meta.Extra)
	if err != nil {
		return fmt.Errorf("raftnode: invalid backup membership metadata: %w", err)
	}
	if kind != snapKindLocal && kind != snapKindInstalled {
		return errors.New("raftnode: unsupported backup snapshot kind")
	}
	if err := fsys.MkdirAll(dir); err != nil {
		return err
	}
	for _, subdir := range []string{"", snapDirName, walDirName} {
		entries, listErr := fsys.List(path.Join(dir, subdir))
		if errors.Is(listErr, vfs.ErrNotExist) {
			continue
		}
		if listErr != nil {
			return listErr
		}
		if len(entries) != 0 {
			return ErrRestoreTargetNotEmpty
		}
	}
	if _, err := atomicfile.Read(fsys, path.Join(dir, hardStateFile)); err == nil {
		return ErrRestoreTargetNotEmpty
	} else if !errors.Is(err, vfs.ErrNotExist) {
		return err
	}
	extra, err := encodeExtra(snapKindInstalled, cs)
	if err != nil {
		return err
	}
	store, err := snapshot.Open(fsys, path.Join(dir, snapDirName))
	if err != nil {
		return err
	}
	w, err := store.Create(snapshot.Meta{Index: backup.Meta.Index, Term: backup.Meta.Term, Extra: extra})
	if err != nil {
		return err
	}
	if _, err := w.Write(backup.Data); err != nil {
		w.Abort()
		return err
	}
	if err := w.Commit(); err != nil {
		return err
	}
	return nil
}
