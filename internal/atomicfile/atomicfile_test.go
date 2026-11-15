package atomicfile_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"testing"

	"github.com/souviks22/continuum/internal/atomicfile"
	"github.com/souviks22/continuum/internal/vfs"
	"github.com/souviks22/continuum/internal/vfs/vfstest"
)

const name = "d/state"

func roundTrip(t *testing.T, fsys vfs.FS, file string) {
	t.Helper()
	if _, err := atomicfile.Read(fsys, file); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("read missing: %v, want ErrNotExist", err)
	}
	for _, p := range [][]byte{[]byte("hello"), nil, bytes.Repeat([]byte{7}, 1<<20), []byte("replaced")} {
		if err := atomicfile.Write(fsys, file, p); err != nil {
			t.Fatal(err)
		}
		got, err := atomicfile.Read(fsys, file)
		if err != nil || !bytes.Equal(got, p) {
			t.Fatalf("got %d bytes err=%v, want %d bytes", len(got), err, len(p))
		}
	}
}

func TestRoundTrip_Mem(t *testing.T) { roundTrip(t, vfs.NewMem(), name) }
func TestRoundTrip_OS(t *testing.T)  { roundTrip(t, vfs.OS(), t.TempDir()+"/state") }

func TestAnyDamageIsDetectedNeverServed(t *testing.T) {
	m := vfs.NewMem()
	want := []byte("term=7 vote=2")
	if err := atomicfile.Write(m, name, want); err != nil {
		t.Fatal(err)
	}
	good, _ := m.ReadFile(name)

	check := func(label string, damaged []byte) {
		t.Helper()
		m.PutFile(name, damaged)
		got, err := atomicfile.Read(m, name)
		if !errors.Is(err, atomicfile.ErrCorrupt) {
			t.Fatalf("%s: err=%v got=%q, want ErrCorrupt", label, err, got)
		}
	}
	for i := range good {
		flipped := bytes.Clone(good)
		flipped[i] ^= 0x01
		check("bit flip", flipped)
	}
	for n := 0; n < len(good); n++ {
		check("truncation", good[:n])
	}
	check("trailing byte", append(bytes.Clone(good), 0))
}

func TestStaleTempFileIsIgnoredAndReplaced(t *testing.T) {
	m := vfs.NewMem()
	if err := atomicfile.Write(m, name, []byte("v1")); err != nil {
		t.Fatal(err)
	}
	m.PutFile(name+".tmp", []byte("half-written garbage"))
	if got, err := atomicfile.Read(m, name); err != nil || string(got) != "v1" {
		t.Fatalf("read with stale tmp: %q %v", got, err)
	}
	if err := atomicfile.Write(m, name, []byte("v2")); err != nil {
		t.Fatalf("write with stale tmp: %v", err)
	}
	if got, _ := atomicfile.Read(m, name); string(got) != "v2" {
		t.Fatalf("got %q", got)
	}
	if names, _ := m.List("d"); len(names) != 1 {
		t.Fatalf("leftover files: %v", names)
	}
}

func TestFailedWriteLeavesOldValueAndNoTempFile(t *testing.T) {
	for _, failing := range []vfs.Op{vfs.OpCreate, vfs.OpWrite, vfs.OpSync, vfs.OpRename} {
		m := vfs.NewMem()
		if err := atomicfile.Write(m, name, []byte("old")); err != nil {
			t.Fatal(err)
		}
		boom := errors.New("boom")
		m.SetHook(func(op vfs.Op, _ string) error {
			if op == failing {
				return boom
			}
			return nil
		})
		if err := atomicfile.Write(m, name, []byte("new")); !errors.Is(err, boom) {
			t.Fatalf("%s: err=%v, want the injected error", failing, err)
		}
		m.SetHook(nil)
		if got, err := atomicfile.Read(m, name); err != nil || string(got) != "old" {
			t.Fatalf("%s: old value damaged: %q %v", failing, got, err)
		}
		if names, _ := m.List("d"); len(names) != 1 {
			t.Fatalf("%s: leftover files %v", failing, names)
		}
	}
}

func TestDiskFullLeavesOldValue(t *testing.T) {
	m := vfs.NewMem()
	if err := atomicfile.Write(m, name, []byte("old")); err != nil {
		t.Fatal(err)
	}
	m.SetCapacity(40)
	if err := atomicfile.Write(m, name, bytes.Repeat([]byte("x"), 200)); !errors.Is(err, vfs.ErrNoSpace) {
		t.Fatalf("err=%v, want ErrNoSpace", err)
	}
	m.SetCapacity(0)
	if got, err := atomicfile.Read(m, name); err != nil || string(got) != "old" {
		t.Fatalf("old value damaged: %q %v", got, err)
	}
}

// The core property: whenever power is lost during a replace, the file reads as
// exactly the old value or exactly the new one, and a Write that returned nil
// is the new one.
func TestCrashDuringReplaceYieldsOldOrNewNeverTornOrMissing(t *testing.T) {
	old, nu := bytes.Repeat([]byte("O"), 3000), bytes.Repeat([]byte("N"), 5000)
	vfstest.CrashAtEveryOp(t,
		func(t *testing.T, m *vfs.Mem) {
			if err := atomicfile.Write(m, name, old); err != nil {
				t.Fatal(err)
			}
		},
		func(m *vfs.Mem) error { return atomicfile.Write(m, name, nu) },
		func(t *testing.T, m *vfs.Mem, completed bool) {
			got, err := atomicfile.Read(m, name)
			if err != nil {
				t.Errorf("file unreadable after crash: %v", err)
				return
			}
			isOld, isNew := bytes.Equal(got, old), bytes.Equal(got, nu)
			if !isOld && !isNew {
				t.Errorf("file is neither the old nor the new value (%d bytes)", len(got))
			}
			if completed && !isNew {
				t.Errorf("Write returned nil but the old value came back after a crash")
			}
		})
}

func TestCrashDuringFirstWriteYieldsMissingOrNew(t *testing.T) {
	nu := []byte("first value")
	vfstest.CrashAtEveryOp(t,
		func(t *testing.T, m *vfs.Mem) {},
		func(m *vfs.Mem) error { return atomicfile.Write(m, name, nu) },
		func(t *testing.T, m *vfs.Mem, completed bool) {
			got, err := atomicfile.Read(m, name)
			switch {
			case errors.Is(err, vfs.ErrNotExist):
				if completed {
					t.Error("Write returned nil but the file is missing after a crash")
				}
			case err != nil:
				t.Errorf("unreadable after crash: %v", err)
			case !bytes.Equal(got, nu):
				t.Errorf("wrong contents %q", got)
			}
		})
}

func TestWrongLengthFieldIsRejectedEvenWithAValidChecksum(t *testing.T) {
	// A file written by buggy code (or restored badly) can carry a consistent
	// checksum over inconsistent contents; the length field must still be
	// validated against the file itself.
	m := vfs.NewMem()
	payload := []byte("0123456789")
	craft := func(declared uint64) []byte {
		b := []byte("CABF\x01\x00\x00\x00")
		b = binary.LittleEndian.AppendUint64(b, declared)
		b = append(b, payload...)
		return binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, crc32.MakeTable(crc32.Castagnoli)))
	}
	m.PutFile(name, craft(uint64(len(payload))))
	if got, err := atomicfile.Read(m, name); err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("control file should be valid: %q %v", got, err)
	}
	for _, declared := range []uint64{0, 9, 11, 1 << 40} {
		m.PutFile(name, craft(declared))
		if _, err := atomicfile.Read(m, name); !errors.Is(err, atomicfile.ErrCorrupt) {
			t.Fatalf("declared length %d: err=%v, want ErrCorrupt", declared, err)
		}
	}
}
