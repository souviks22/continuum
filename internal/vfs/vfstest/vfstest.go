// Package vfstest holds crash-testing helpers shared by every package that
// writes durable state.
package vfstest

import (
	"errors"
	"testing"

	"github.com/souviks22/continuum-go/internal/vfs"
)

// ErrInjected is the error the harness makes filesystem calls fail with.
var ErrInjected = errors.New("injected crash")

func mutating(op vfs.Op) bool {
	switch op {
	case vfs.OpCreate, vfs.OpWrite, vfs.OpTruncate, vfs.OpRemove, vfs.OpRename, vfs.OpSync, vfs.OpSyncDir:
		return true
	}
	return false
}

// CrashAtEveryOp runs op once per mutating filesystem call it makes. In run k,
// call k and every call after it fails, as if the process died there; then the
// machine loses power (both a clean and a torn variant) and check inspects what
// survived. completed is true only for the final run, where op was never
// interrupted. setup builds the starting state on a fresh Mem each time; its
// effects are durable because setup is expected to sync what it writes.
//
// The harness itself verifies the op reports failure whenever a call failed:
// code that swallows an I/O error is a bug this catches.
func CrashAtEveryOp(t *testing.T,
	setup func(t *testing.T, m *vfs.Mem),
	op func(m *vfs.Mem) error,
	check func(t *testing.T, m *vfs.Mem, completed bool)) {
	t.Helper()
	for k := 0; ; k++ {
		for _, torn := range []bool{false, true} {
			m := vfs.NewMem()
			setup(t, m)
			count, fired := 0, false
			m.SetHook(func(o vfs.Op, _ string) error {
				if !mutating(o) {
					return nil
				}
				if count >= k {
					fired = true
					return ErrInjected
				}
				count++
				return nil
			})
			err := op(m)
			m.SetHook(nil)
			if !fired && err != nil {
				t.Fatalf("k=%d: op failed with no injected fault: %v", k, err)
			}
			if fired && err == nil {
				t.Fatalf("k=%d: op reported success although a filesystem call failed", k)
			}
			m.Crash(vfs.CrashOptions{Seed: uint64(k), Torn: torn})
			check(t, m, !fired)
			if t.Failed() {
				t.Fatalf("invariant violated: crash after %d mutating calls, torn=%v", count, torn)
			}
			if !fired {
				t.Logf("crash points exercised: the operation makes %d mutating filesystem calls, each crashed at (clean and torn)", count)
				return
			}
		}
	}
}
