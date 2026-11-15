package wal_test

import (
	"errors"
	"testing"

	"github.com/souviks22/continuum-go/internal/vfs"
	"github.com/souviks22/continuum-go/internal/wal"
)

var errInjected = errors.New("injected crash")

func mutating(op vfs.Op) bool {
	switch op {
	case vfs.OpCreate, vfs.OpWrite, vfs.OpTruncate, vfs.OpRemove, vfs.OpRename, vfs.OpSync, vfs.OpSyncDir:
		return true
	}
	return false
}

// crashAtEveryOp runs op once per mutating filesystem call it makes. In run k,
// call k and everything after it fails (the process "dies" there); then the
// machine loses power, drops unsynced state (torn and clean variants), and the
// log is reopened. check must hold for every k, and completed is true for the
// final k where op ran to the end without being interrupted.
func crashAtEveryOp(t *testing.T, opts wal.Options,
	setup func(*testing.T, *vfs.Mem) *wal.Log,
	op func(*wal.Log) error,
	check func(t *testing.T, l *wal.Log, completed bool)) {
	t.Helper()
	for k := 0; ; k++ {
		for _, torn := range []bool{false, true} {
			m := vfs.NewMem()
			l := setup(t, m)
			count, fired := 0, false
			m.SetHook(func(o vfs.Op, _ string) error {
				if !mutating(o) {
					return nil
				}
				if count >= k {
					fired = true
					return errInjected
				}
				count++
				return nil
			})
			err := op(l)
			m.SetHook(nil)
			if !fired && err != nil {
				t.Fatalf("k=%d: op failed without an injected fault: %v", k, err)
			}
			if fired && err == nil {
				t.Fatalf("k=%d: op reported success although a filesystem call failed", k)
			}
			m.Crash(vfs.CrashOptions{Seed: uint64(k), Torn: torn})
			l2, oerr := wal.Open(m, dir, opts)
			if oerr != nil {
				t.Fatalf("k=%d torn=%v: log unrecoverable after crash: %v", k, torn, oerr)
			}
			check(t, l2, !fired)
			if t.Failed() {
				t.Fatalf("invariant violated at k=%d torn=%v (crash after %d mutating calls)", k, torn, count)
			}
			l2.Close()
			if !fired {
				return
			}
		}
	}
}

func tenRecords(t *testing.T, m *vfs.Mem) *wal.Log {
	t.Helper()
	l := openLog(t, m, small) // segments start at 1, 4, 7, 10
	appendN(t, l, 10, 30)
	must(t, l.Sync())
	return l
}

func prefixOK(t *testing.T, l *wal.Log, first, minLast, maxLast uint64) {
	t.Helper()
	if l.FirstSeq() != first || l.LastSeq() < minLast || l.LastSeq() > maxLast {
		t.Errorf("log is [%d,%d], want first=%d and last in [%d,%d]", l.FirstSeq(), l.LastSeq(), first, minLast, maxLast)
		return
	}
	expect(t, l, first, l.LastSeq(), 30) // contiguous, correct payloads
}

func TestCrashAtEveryStepOfTruncateTo(t *testing.T) {
	for _, to := range []uint64{5, 6, 0} {
		t.Run("", func(t *testing.T) {
			crashAtEveryOp(t, small, tenRecords,
				func(l *wal.Log) error { return l.TruncateTo(to) },
				func(t *testing.T, l *wal.Log, completed bool) {
					// Never loses records <= to, never leaves a gap; a crash
					// mid-truncate leaves a prefix of the original log.
					prefixOK(t, l, 1, to, 10)
					if completed && l.LastSeq() != to {
						t.Errorf("TruncateTo(%d) returned nil but LastSeq=%d after crash", to, l.LastSeq())
					}
				})
		})
	}
}

func TestCrashAtEveryStepOfDiscardBefore(t *testing.T) {
	crashAtEveryOp(t, small, tenRecords,
		func(l *wal.Log) error { return l.DiscardBefore(8) },
		func(t *testing.T, l *wal.Log, completed bool) {
			// Oldest segments go first, so what remains is a contiguous suffix.
			if f := l.FirstSeq(); f != 1 && f != 4 && f != 7 {
				t.Errorf("FirstSeq=%d is not a segment boundary", f)
			}
			if l.LastSeq() != 10 {
				t.Errorf("LastSeq=%d, discard must never touch the newest records", l.LastSeq())
			}
			expect(t, l, l.FirstSeq(), 10, 30)
			if completed && l.FirstSeq() != 7 {
				t.Errorf("completed discard left FirstSeq=%d, want 7", l.FirstSeq())
			}
		})
}

func TestCrashAtEveryStepOfReset(t *testing.T) {
	opts := wal.Options{SegmentSize: 200, FirstSeq: 50} // what a caller would pass on recovery
	crashAtEveryOp(t, opts, tenRecords,
		func(l *wal.Log) error { return l.Reset(50) },
		func(t *testing.T, l *wal.Log, completed bool) {
			if l.LastSeq() == 49 { // reset finished (or emptied the log)
				if l.FirstSeq() != 50 || len(readAll(t, l)) != 0 {
					t.Errorf("empty log is [%d,%d]", l.FirstSeq(), l.LastSeq())
				}
				return
			}
			// Otherwise a contiguous suffix of the old log, as documented.
			if completed {
				t.Errorf("completed Reset left the old log (last=%d)", l.LastSeq())
			}
			if l.LastSeq() != 10 {
				t.Errorf("LastSeq=%d: neither empty nor an old-log suffix", l.LastSeq())
				return
			}
			expect(t, l, l.FirstSeq(), 10, 30)
		})
}

func TestCrashAtEveryStepOfAppendThatRotates(t *testing.T) {
	setup := func(t *testing.T, m *vfs.Mem) *wal.Log {
		l := openLog(t, m, small)
		appendN(t, l, 3, 30) // the first segment is now full
		must(t, l.Sync())
		return l
	}
	crashAtEveryOp(t, small, setup,
		func(l *wal.Log) error {
			if _, err := l.Append(payloadOf(4, 30)); err != nil {
				return err
			}
			return l.Sync()
		},
		func(t *testing.T, l *wal.Log, completed bool) {
			prefixOK(t, l, 1, 3, 4) // earlier synced records always survive
			if completed && l.LastSeq() != 4 {
				t.Errorf("synced record 4 lost: LastSeq=%d", l.LastSeq())
			}
		})
}

func TestCrashAtEveryStepOfRecoveryItself(t *testing.T) {
	// Recovery repairs a torn tail by truncating; a crash during that repair
	// must not make the next recovery worse.
	base, r6 := fiveRecords(t)
	for k := 0; ; k++ {
		m := vfs.NewMem()
		m.PutFile(segName(1), append(append([]byte(nil), base...), r6[:25]...))
		count, fired := 0, false
		m.SetHook(func(o vfs.Op, _ string) error {
			if mutating(o) {
				if count >= k {
					fired = true
					return errInjected
				}
				count++
			}
			return nil
		})
		_, err := wal.Open(m, dir, wal.Options{})
		m.SetHook(nil)
		m.Crash(vfs.CrashOptions{Seed: uint64(k), Torn: true})
		l := openLog(t, m, wal.Options{})
		expect(t, l, 1, 5, 10)
		l.Close()
		if !fired {
			if err != nil {
				t.Fatalf("open failed: %v", err)
			}
			return
		}
	}
}
