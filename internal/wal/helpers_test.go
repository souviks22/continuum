package wal_test

import (
	"fmt"
	"testing"

	"github.com/souviks22/continuum-go/internal/vfs"
	"github.com/souviks22/continuum-go/internal/wal"
)

const dir = "wal"

// payloadOf is deterministic and never contains a zero byte, so a zero-filled
// torn tail can never be mistaken for real data.
func payloadOf(seq uint64, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(1 + (int(seq)*31+i*7)%250)
	}
	return b
}

type rec struct {
	seq uint64
	p   string
}

func segName(first uint64) string { return fmt.Sprintf("%s/%020d.wal", dir, first) }

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func openLog(t *testing.T, fsys vfs.FS, o wal.Options) *wal.Log {
	t.Helper()
	l, err := wal.Open(fsys, dir, o)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return l
}

func readAll(t *testing.T, l *wal.Log) []rec {
	t.Helper()
	var out []rec
	err := l.Read(l.FirstSeq(), func(seq uint64, p []byte) error {
		out = append(out, rec{seq, string(p)})
		return nil
	})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return out
}

// appendN appends n records of the given payload size, checking each seq.
func appendN(t *testing.T, l *wal.Log, n, size int) {
	t.Helper()
	for i := 0; i < n; i++ {
		want := l.LastSeq() + 1
		seq, err := l.Append(payloadOf(want, size))
		if err != nil {
			t.Fatalf("append: %v", err)
		}
		if seq != want {
			t.Fatalf("append returned seq %d, want %d", seq, want)
		}
	}
}

// expect checks the log holds exactly seqs [from, to] with payloadOf(seq, size).
func expect(t *testing.T, l *wal.Log, from, to uint64, size int) {
	t.Helper()
	got := readAll(t, l)
	if uint64(len(got)) != to-from+1 && !(to < from && len(got) == 0) {
		t.Fatalf("got %d records, want seqs [%d,%d]", len(got), from, to)
	}
	for i, r := range got {
		want := from + uint64(i)
		if r.seq != want || r.p != string(payloadOf(want, size)) {
			t.Fatalf("record %d: seq=%d payload ok=%v, want seq %d", i, r.seq, r.p == string(payloadOf(want, size)), want)
		}
	}
	if l.FirstSeq() != from || l.LastSeq() != to {
		t.Fatalf("FirstSeq/LastSeq = %d/%d, want %d/%d", l.FirstSeq(), l.LastSeq(), from, to)
	}
}

func reopen(t *testing.T, m *vfs.Mem, l *wal.Log, o wal.Options) *wal.Log {
	t.Helper()
	must(t, l.Close())
	return openLog(t, m, o)
}
