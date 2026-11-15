package vfs_test

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/souviks22/continuum-go/internal/vfs"
)

// conformance runs against both implementations: Mem must behave like a real
// filesystem for everything except crash semantics.
func conformance(t *testing.T, fsys vfs.FS, dir string) {
	t.Helper()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(fsys.MkdirAll(dir))

	if _, err := fsys.Open(dir + "/missing"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("open missing: %v, want ErrNotExist", err)
	}
	f, err := fsys.Create(dir + "/a")
	must(err)
	if _, err := fsys.Create(dir + "/a"); !errors.Is(err, vfs.ErrExist) {
		t.Fatalf("create existing: %v, want ErrExist", err)
	}
	_, err = f.Write([]byte("hello world"))
	must(err)
	if n, _ := f.Size(); n != 11 {
		t.Fatalf("size=%d", n)
	}
	_, err = f.Seek(6, io.SeekStart)
	must(err)
	buf := make([]byte, 5)
	if _, err := io.ReadFull(f, buf); err != nil || string(buf) != "world" {
		t.Fatalf("read %q err=%v", buf, err)
	}
	if n, err := f.Read(buf); n != 0 || err != io.EOF {
		t.Fatalf("read at EOF: %d %v", n, err)
	}
	must(f.Truncate(5))
	_, err = f.Seek(0, io.SeekEnd)
	must(err)
	_, err = f.Write([]byte("!"))
	must(err)
	must(f.Sync())
	must(f.Close())

	g, err := fsys.Open(dir + "/a")
	must(err)
	all, err := io.ReadAll(g)
	must(err)
	if string(all) != "hello!" {
		t.Fatalf("contents %q", all)
	}
	must(g.Close())

	h, err := fsys.Create(dir + "/b")
	must(err)
	_, err = h.Write([]byte("B"))
	must(err)
	must(h.Close())
	must(fsys.Rename(dir+"/b", dir+"/a")) // replaces
	names, err := fsys.List(dir)
	must(err)
	if !reflect.DeepEqual(names, []string{"a"}) {
		t.Fatalf("list=%v", names)
	}
	r, err := fsys.Open(dir + "/a")
	must(err)
	all, _ = io.ReadAll(r)
	if string(all) != "B" {
		t.Fatalf("after rename: %q", all)
	}
	r.Close()

	must(fsys.SyncDir(dir))
	must(fsys.Remove(dir + "/a"))
	if err := fsys.Remove(dir + "/a"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("remove missing: %v", err)
	}
	if names, _ := fsys.List(dir); len(names) != 0 {
		t.Fatalf("list after remove: %v", names)
	}
}

func TestConformance_Mem(t *testing.T) { conformance(t, vfs.NewMem(), "d") }
func TestConformance_OS(t *testing.T)  { conformance(t, vfs.OS(), t.TempDir()) }

// -- crash model --------------------------------------------------------------

func write(t *testing.T, f vfs.File, s string) {
	t.Helper()
	if _, err := f.Write([]byte(s)); err != nil {
		t.Fatal(err)
	}
}

func create(t *testing.T, m *vfs.Mem, name string) vfs.File {
	t.Helper()
	f, err := m.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func contents(t *testing.T, m *vfs.Mem, name string) (string, bool) {
	t.Helper()
	b, err := m.ReadFile(name)
	if errors.Is(err, vfs.ErrNotExist) {
		return "", false
	}
	if err != nil {
		t.Fatal(err)
	}
	return string(b), true
}

func TestCrash_UnsyncedContentsAreLost_SyncedSurvive(t *testing.T) {
	m := vfs.NewMem()
	f := create(t, m, "d/f")
	write(t, f, "synced")
	f.Sync()
	write(t, f, "-unsynced")
	m.SyncDir("d")

	m.Crash(vfs.CrashOptions{})
	if got, _ := contents(t, m, "d/f"); got != "synced" {
		t.Fatalf("got %q, want only the synced prefix", got)
	}
}

func TestCrash_FileWhoseDirectoryEntryWasNeverSyncedVanishes(t *testing.T) {
	m := vfs.NewMem()
	f := create(t, m, "d/f")
	write(t, f, "data")
	f.Sync() // contents durable, but the name is not

	m.Crash(vfs.CrashOptions{})
	if _, ok := contents(t, m, "d/f"); ok {
		t.Fatal("file survived a crash without SyncDir; the model is too generous")
	}

	f = create(t, m, "d/g")
	write(t, f, "data")
	f.Sync()
	m.SyncDir("d")
	m.Crash(vfs.CrashOptions{})
	if got, ok := contents(t, m, "d/g"); !ok || got != "data" {
		t.Fatalf("got %q ok=%v", got, ok)
	}
}

func TestCrash_RenameIsDurableOnlyAfterSyncDir(t *testing.T) {
	m := vfs.NewMem()
	m.PutFile("d/f", []byte("old"))
	tmp := create(t, m, "d/f.tmp")
	write(t, tmp, "new")
	tmp.Sync()
	if err := m.Rename("d/f.tmp", "d/f"); err != nil {
		t.Fatal(err)
	}

	m.Crash(vfs.CrashOptions{})
	if got, _ := contents(t, m, "d/f"); got != "old" {
		t.Fatalf("got %q: an unsynced rename must not survive", got)
	}

	tmp = create(t, m, "d/f.tmp")
	write(t, tmp, "new")
	tmp.Sync()
	m.Rename("d/f.tmp", "d/f")
	m.SyncDir("d")
	m.Crash(vfs.CrashOptions{})
	if got, _ := contents(t, m, "d/f"); got != "new" {
		t.Fatalf("got %q after a synced rename", got)
	}
}

func TestCrash_RemoveIsDurableOnlyAfterSyncDir(t *testing.T) {
	m := vfs.NewMem()
	m.PutFile("d/f", []byte("x"))
	m.Remove("d/f")
	m.Crash(vfs.CrashOptions{})
	if _, ok := contents(t, m, "d/f"); !ok {
		t.Fatal("an unsynced remove must not survive")
	}
	m.Remove("d/f")
	m.SyncDir("d")
	m.Crash(vfs.CrashOptions{})
	if _, ok := contents(t, m, "d/f"); ok {
		t.Fatal("a synced remove must survive")
	}
}

func TestCrash_TornTailKeepsSyncedPrefixAndNeverInventsBytes(t *testing.T) {
	sawPartial, sawZeros := false, false
	for seed := uint64(0); seed < 200; seed++ {
		m := vfs.NewMem()
		f := create(t, m, "d/f")
		write(t, f, "AAAA")
		f.Sync()
		m.SyncDir("d")
		write(t, f, "BBBBBBBB")

		m.Crash(vfs.CrashOptions{Seed: seed, Torn: true})
		got, _ := contents(t, m, "d/f")
		if len(got) < 4 || got[:4] != "AAAA" {
			t.Fatalf("seed %d: synced prefix damaged: %q", seed, got)
		}
		tail := []byte(got[4:])
		realLen := 0
		for realLen < len(tail) && tail[realLen] == 'B' {
			realLen++
		}
		if !bytes.Equal(tail[realLen:], make([]byte, len(tail)-realLen)) {
			t.Fatalf("seed %d: tail %q is neither real bytes nor zeros", seed, tail)
		}
		if realLen > 0 && realLen < 8 {
			sawPartial = true
		}
		if len(tail) > realLen {
			sawZeros = true
		}
	}
	if !sawPartial || !sawZeros {
		t.Fatalf("200 seeds never produced a partial tail (%v) or a zero-filled tail (%v)", sawPartial, sawZeros)
	}
}

func TestCrash_InvalidatesOpenHandles(t *testing.T) {
	m := vfs.NewMem()
	f := create(t, m, "d/f")
	m.Crash(vfs.CrashOptions{})
	if _, err := f.Write([]byte("x")); !errors.Is(err, vfs.ErrCrashed) {
		t.Fatalf("err=%v, want ErrCrashed", err)
	}
}

func TestFaultHookFailsOperationWithNoEffect(t *testing.T) {
	m := vfs.NewMem()
	f := create(t, m, "d/f")
	boom := errors.New("boom")
	m.SetHook(func(op vfs.Op, _ string) error {
		if op == vfs.OpWrite {
			return boom
		}
		return nil
	})
	if _, err := f.Write([]byte("x")); !errors.Is(err, boom) {
		t.Fatalf("err=%v", err)
	}
	m.SetHook(nil)
	if got, _ := contents(t, m, "d/f"); got != "" {
		t.Fatalf("failed write had an effect: %q", got)
	}
}

func TestCapacityProducesPartialWriteThenENOSPC(t *testing.T) {
	m := vfs.NewMem()
	m.SetCapacity(10)
	f := create(t, m, "d/f")
	n, err := f.Write([]byte("0123456789ABCDEF"))
	if n != 10 || !errors.Is(err, vfs.ErrNoSpace) {
		t.Fatalf("n=%d err=%v, want 10 bytes then ErrNoSpace", n, err)
	}
	if got, _ := contents(t, m, "d/f"); got != "0123456789" {
		t.Fatalf("got %q", got)
	}
	if n, err := f.Write([]byte("x")); n != 0 || !errors.Is(err, vfs.ErrNoSpace) {
		t.Fatalf("full disk: n=%d err=%v", n, err)
	}
}
