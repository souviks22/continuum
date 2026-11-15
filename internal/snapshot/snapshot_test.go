package snapshot_test

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/souviks22/continuum/internal/snapshot"
	"github.com/souviks22/continuum/internal/vfs"
	"github.com/souviks22/continuum/internal/vfs/vfstest"
)

const dir = "snaps"

func TestPortableArchiveRoundTripAndIntegrity(t *testing.T) {
	in := snapshot.Snapshot{Meta: snapshot.Meta{Index: 17, Term: 4, Extra: []byte("conf-state")}, Data: data(3, 4096)}
	wire, err := snapshot.Marshal(in.Meta, in.Data)
	if err != nil {
		t.Fatal(err)
	}
	out, err := snapshot.Unmarshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	if out.Meta.Index != in.Meta.Index || out.Meta.Term != in.Meta.Term || !bytes.Equal(out.Meta.Extra, in.Meta.Extra) || !bytes.Equal(out.Data, in.Data) {
		t.Fatalf("round trip mismatch: %+v", out.Meta)
	}
	for _, mutate := range []func([]byte) []byte{
		func(b []byte) []byte { b[4] ^= 1; return b },
		func(b []byte) []byte { b[40] ^= 1; return b },
		func(b []byte) []byte { b[len(b)-17] ^= 1; return b },
		func(b []byte) []byte { return b[:len(b)-1] },
		func(b []byte) []byte { return append(b, 0) },
	} {
		bad := mutate(bytes.Clone(wire))
		if _, err := snapshot.Unmarshal(bad); !errors.Is(err, snapshot.ErrInvalidArchive) {
			t.Fatalf("corrupt archive error=%v", err)
		}
	}
}

func data(seed byte, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = seed + byte(i%251)
	}
	return b
}

func write(t *testing.T, s *snapshot.Store, meta snapshot.Meta, d []byte) {
	t.Helper()
	w, err := s.Create(meta)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(d); err != nil {
		t.Fatal(err)
	}
	if err := w.Commit(); err != nil {
		t.Fatal(err)
	}
}

func open(t *testing.T, fsys vfs.FS) *snapshot.Store {
	t.Helper()
	s, err := snapshot.Open(fsys, dir)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func fileName(idx uint64) string {
	return dir + "/snap-" + string(append(bytes.Repeat([]byte("0"), 20-len(itoa(idx))), itoa(idx)...)) + ".snap"
}

func itoa(n uint64) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for ; n > 0; n /= 10 {
		b = append([]byte{byte('0' + n%10)}, b...)
	}
	return string(b)
}

func TestWriteLoadRoundTrip(t *testing.T) {
	for _, fsys := range []vfs.FS{vfs.NewMem(), vfs.OS()} {
		d := dir
		if _, ok := fsys.(*vfs.Mem); !ok {
			d = t.TempDir()
		}
		s, err := snapshot.Open(fsys, d)
		if err != nil {
			t.Fatal(err)
		}
		meta := snapshot.Meta{Index: 42, Term: 7, Extra: []byte("conf:1,2,3")}
		payload := data(1, 3<<20) // several buffer flushes
		write(t, s, meta, payload)

		got, err := s.Load(42)
		if err != nil {
			t.Fatal(err)
		}
		if got.Meta.Index != 42 || got.Meta.Term != 7 || string(got.Meta.Extra) != "conf:1,2,3" || !bytes.Equal(got.Data, payload) {
			t.Fatalf("round trip mismatch: %+v (%d bytes)", got.Meta, len(got.Data))
		}
	}
}

func TestEmptyDataAndEmptyExtra(t *testing.T) {
	s := open(t, vfs.NewMem())
	write(t, s, snapshot.Meta{Index: 1, Term: 1}, nil)
	got, err := s.Load(1)
	if err != nil || len(got.Data) != 0 || len(got.Meta.Extra) != 0 {
		t.Fatalf("got %+v err=%v", got, err)
	}
}

func TestStreamingReaderDeliversDataInSmallChunksAndVerifiesAtEOF(t *testing.T) {
	s := open(t, vfs.NewMem())
	payload := data(3, 10000)
	write(t, s, snapshot.Meta{Index: 5, Term: 2, Extra: []byte("x")}, payload)
	r, err := s.Open(5)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var got []byte
	buf := make([]byte, 7)
	for {
		n, err := r.Read(buf)
		got = append(got, buf[:n]...)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("streamed data differs")
	}
	if n, err := r.Read(buf); n != 0 || err != io.EOF {
		t.Fatalf("read after EOF: %d %v", n, err)
	}
}

func TestEveryByteFlipAndEveryTruncationIsDetected(t *testing.T) {
	m := vfs.NewMem()
	s := open(t, m)
	write(t, s, snapshot.Meta{Index: 9, Term: 3, Extra: []byte("abc")}, data(5, 200))
	good, err := m.ReadFile(fileName(9))
	if err != nil {
		t.Fatal(err)
	}
	mustFail := func(label string, damaged []byte) {
		t.Helper()
		m.PutFile(fileName(9), damaged)
		if _, err := s.Load(9); !errors.Is(err, snapshot.ErrCorrupt) {
			t.Fatalf("%s: Load err=%v, want ErrCorrupt (data must never be served)", label, err)
		}
		entries, _ := s.Scan()
		if len(entries) != 1 || !errors.Is(entries[0].Err, snapshot.ErrCorrupt) {
			t.Fatalf("%s: Scan did not flag the file: %+v", label, entries)
		}
	}
	for i := range good {
		f := bytes.Clone(good)
		f[i] ^= 0x01
		mustFail("flip", f)
	}
	for n := 0; n < len(good); n++ {
		mustFail("truncate", good[:n])
	}
	mustFail("extra trailing byte", append(bytes.Clone(good), 0))
}

func TestOpenRemovesStaleTempFilesAndIgnoresStrangers(t *testing.T) {
	m := vfs.NewMem()
	m.PutFile(dir+"/snap-00000000000000000003.snap.tmp", []byte("partial"))
	m.PutFile(dir+"/notes.txt", []byte("hi"))
	s := open(t, m)
	names, _ := m.List(dir)
	if len(names) != 1 || names[0] != "notes.txt" {
		t.Fatalf("names=%v", names)
	}
	if e, _ := s.Scan(); len(e) != 0 {
		t.Fatalf("scan saw %v", e)
	}
}

func TestAbortLeavesNothing(t *testing.T) {
	m := vfs.NewMem()
	s := open(t, m)
	w, _ := s.Create(snapshot.Meta{Index: 4, Term: 1})
	w.Write(data(1, 100))
	w.Abort()
	if names, _ := m.List(dir); len(names) != 0 {
		t.Fatalf("leftovers: %v", names)
	}
	if err := w.Commit(); err == nil {
		t.Fatal("commit after abort accepted")
	}
}

func TestWriteFailureLeavesNoSnapshotAndStoreStillWorks(t *testing.T) {
	m := vfs.NewMem()
	s := open(t, m)
	write(t, s, snapshot.Meta{Index: 1, Term: 1}, data(1, 50))

	m.SetCapacity(300)
	w, err := s.Create(snapshot.Meta{Index: 2, Term: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(data(2, 1<<20)); !errors.Is(err, vfs.ErrNoSpace) { // overflows the disk
		t.Fatalf("Write err=%v, want ErrNoSpace", err)
	}
	if err := w.Commit(); err == nil {
		t.Fatal("Commit succeeded after a failed write")
	}
	m.SetCapacity(0)
	entries, _ := s.Scan()
	if len(entries) != 1 || entries[0].Index != 1 || entries[0].Err != nil {
		t.Fatalf("entries=%+v: failed write must leave only the old snapshot", entries)
	}
	if names, _ := m.List(dir); len(names) != 1 {
		t.Fatalf("temp file leaked: %v", names)
	}
	write(t, s, snapshot.Meta{Index: 2, Term: 1}, data(2, 50)) // retry works
}

func TestReplacingASnapshotAtTheSameIndex(t *testing.T) {
	s := open(t, vfs.NewMem())
	write(t, s, snapshot.Meta{Index: 3, Term: 1}, []byte("first"))
	write(t, s, snapshot.Meta{Index: 3, Term: 2}, []byte("second"))
	got, err := s.Load(3)
	if err != nil || string(got.Data) != "second" || got.Meta.Term != 2 {
		t.Fatalf("%+v %v", got, err)
	}
}

func TestScanOrdersByIndexAndPruneKeepsNewestValid(t *testing.T) {
	m := vfs.NewMem()
	s := open(t, m)
	for _, i := range []uint64{30, 10, 20, 40, 50} {
		write(t, s, snapshot.Meta{Index: i, Term: 1}, data(byte(i), 40))
	}
	// 20 is corrupt (older than what we keep); 50 is corrupt too (newer).
	b, _ := m.ReadFile(fileName(20))
	b[40] ^= 0xFF
	m.PutFile(fileName(20), b)
	b, _ = m.ReadFile(fileName(50))
	b[40] ^= 0xFF
	m.PutFile(fileName(50), b)

	entries, err := s.Scan()
	if err != nil {
		t.Fatal(err)
	}
	var idx []uint64
	for _, e := range entries {
		idx = append(idx, e.Index)
	}
	if len(idx) != 5 || idx[0] != 10 || idx[4] != 50 {
		t.Fatalf("scan order %v", idx)
	}

	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(s.Prune(2)) // newest two VALID are 40 and 30; delete < 30 (10, 20-corrupt); keep 50 (corrupt but newer)
	entries, _ = s.Scan()
	idx = idx[:0]
	for _, e := range entries {
		idx = append(idx, e.Index)
	}
	if len(idx) != 3 || idx[0] != 30 || idx[1] != 40 || idx[2] != 50 {
		t.Fatalf("after prune: %v", idx)
	}
	if err := s.Prune(0); err == nil {
		t.Fatal("Prune(0) must be refused")
	}
	must(s.Prune(10)) // asked to keep more valid snapshots than exist: delete nothing
	if e, _ := s.Scan(); len(e) != 3 {
		t.Fatalf("Prune(10) deleted files: %d left", len(e))
	}
	m.Crash(vfs.CrashOptions{})
	if e, _ := open(t, m).Scan(); len(e) != 3 {
		t.Fatalf("prune not durable: %d left", len(e))
	}
}

// The core property, checked at every crash point: after power loss a snapshot
// is either absent or complete and verified. Never present-but-damaged. And
// once Commit returned nil it is there.
func TestCrashDuringWriteLeavesNoPartialSnapshot(t *testing.T) {
	payload := data(9, 600<<10) // bigger than the write buffer
	extra := []byte("conf")
	vfstest.CrashAtEveryOp(t,
		func(t *testing.T, m *vfs.Mem) {
			s := open(t, m)
			write(t, s, snapshot.Meta{Index: 10, Term: 1, Extra: extra}, data(1, 100))
		},
		func(m *vfs.Mem) error {
			s, err := snapshot.Open(m, dir)
			if err != nil {
				return err
			}
			w, err := s.Create(snapshot.Meta{Index: 20, Term: 2, Extra: extra})
			if err != nil {
				return err
			}
			if _, err := w.Write(payload); err != nil {
				return err
			}
			return w.Commit()
		},
		func(t *testing.T, m *vfs.Mem, completed bool) {
			s, err := snapshot.Open(m, dir) // recovery: also sweeps stale temp files
			if err != nil {
				t.Errorf("reopen: %v", err)
				return
			}
			entries, err := s.Scan()
			if err != nil {
				t.Errorf("scan: %v", err)
				return
			}
			have20 := false
			for _, e := range entries {
				if e.Err != nil {
					t.Errorf("snapshot %d visible but damaged: %v", e.Index, e.Err)
				}
				if e.Index == 20 {
					have20 = true
					got, err := s.Load(20)
					if err != nil || !bytes.Equal(got.Data, payload) || got.Meta.Term != 2 {
						t.Errorf("snapshot 20 is present but wrong: %v", err)
					}
				}
			}
			if len(entries) == 0 || entries[0].Index != 10 {
				t.Errorf("the previous snapshot disappeared: %+v", entries)
			}
			if completed && !have20 {
				t.Error("Commit returned nil but the snapshot vanished after a crash")
			}
			if names, _ := m.List(dir); len(names) != len(entries) {
				t.Errorf("stray files after recovery: %v", names)
			}
		})
}

func TestSnapshotFileUnderTheWrongNameIsRejected(t *testing.T) {
	// e.g. an operator copies snap-5 to snap-6, or a restore mislabels files.
	// Serving it would make the node believe it has state through index 6.
	m := vfs.NewMem()
	s := open(t, m)
	write(t, s, snapshot.Meta{Index: 5, Term: 1}, data(1, 50))
	b, _ := m.ReadFile(fileName(5))
	m.PutFile(fileName(6), b)

	if _, err := s.Load(6); !errors.Is(err, snapshot.ErrCorrupt) {
		t.Fatalf("Load(6): %v, want ErrCorrupt", err)
	}
	entries, _ := s.Scan()
	if len(entries) != 2 || entries[0].Err != nil || !errors.Is(entries[1].Err, snapshot.ErrCorrupt) {
		t.Fatalf("scan: %+v", entries)
	}
}
