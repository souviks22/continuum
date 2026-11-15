package wal_test

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/souviks22/continuum-go/internal/vfs"
	"github.com/souviks22/continuum-go/internal/wal"
)

// The same code must work on the real filesystem, not just in the simulator.
func TestOSFilesystemRoundTripAndTornTail(t *testing.T) {
	d := t.TempDir()
	l, err := wal.Open(vfs.OS(), d, small)
	must(t, err)
	for i := 1; i <= 8; i++ {
		_, err := l.Append(payloadOf(uint64(i), 30))
		must(t, err)
	}
	must(t, l.Sync())
	must(t, l.Close())

	// Leave a torn record on the real last segment.
	segs, err := filepath.Glob(filepath.Join(d, "*.wal"))
	must(t, err)
	if len(segs) != 3 {
		t.Fatalf("segments on disk: %v", segs)
	}
	f, err := os.OpenFile(segs[len(segs)-1], os.O_APPEND|os.O_WRONLY, 0)
	must(t, err)
	_, err = f.Write([]byte{1, 2, 3, 4, 5, 6, 7})
	must(t, err)
	must(t, f.Close())

	l, err = wal.Open(vfs.OS(), d, small)
	must(t, err)
	defer l.Close()
	if l.TruncatedOnOpen() != 7 {
		t.Fatalf("TruncatedOnOpen=%d", l.TruncatedOnOpen())
	}
	expect(t, l, 1, 8, 30)
	must(t, l.TruncateTo(4))
	must(t, l.DiscardBefore(4))
	expect(t, l, 4, 4, 30)
}

func TestConcurrentAppendersAndSyncers(t *testing.T) {
	d := t.TempDir()
	opts := wal.Options{SegmentSize: 4096}
	l, err := wal.Open(vfs.OS(), d, opts)
	must(t, err)

	const workers, per = 6, 150
	var wg sync.WaitGroup
	var mu sync.Mutex
	seen := map[uint64]string{}
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				p := fmt.Sprintf("w%d-%d", w, i)
				seq, err := l.Append([]byte(p))
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				seen[seq] = p
				mu.Unlock()
				if i%10 == 0 {
					if err := l.Sync(); err != nil {
						t.Error(err)
						return
					}
				}
			}
		}(w)
	}
	wg.Wait()
	must(t, l.Close())

	l, err = wal.Open(vfs.OS(), d, opts)
	must(t, err)
	defer l.Close()
	got := readAll(t, l)
	if len(got) != workers*per || len(seen) != workers*per {
		t.Fatalf("got %d records, tracked %d, want %d", len(got), len(seen), workers*per)
	}
	for i, r := range got {
		if r.seq != uint64(i+1) || r.p != seen[r.seq] {
			t.Fatalf("record %d: seq=%d payload mismatch", i, r.seq)
		}
	}
}
