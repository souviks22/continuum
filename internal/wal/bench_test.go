package wal_test

import (
	"fmt"
	"testing"

	"github.com/souviks22/continuum-go/internal/vfs"
	"github.com/souviks22/continuum-go/internal/wal"
)

// Indicative only: these run against whatever disk the machine has. They exist
// so the durability cost model (one fsync per Sync, amortized by batching) is
// measurable on real hardware, not to validate the targets in REQUIREMENTS.md.
func BenchmarkAppendSyncEvery(b *testing.B) {
	for _, batch := range []int{1, 16, 128} {
		b.Run(fmt.Sprintf("batch=%d", batch), func(b *testing.B) {
			l, err := wal.Open(vfs.OS(), b.TempDir(), wal.Options{})
			if err != nil {
				b.Fatal(err)
			}
			defer l.Close()
			payload := payloadOf(1, 128)
			b.SetBytes(int64(len(payload)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := l.Append(payload); err != nil {
					b.Fatal(err)
				}
				if (i+1)%batch == 0 {
					if err := l.Sync(); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

func BenchmarkRecovery(b *testing.B) {
	dir := b.TempDir()
	l, err := wal.Open(vfs.OS(), dir, wal.Options{})
	if err != nil {
		b.Fatal(err)
	}
	payload := payloadOf(1, 128)
	const n = 200000
	for i := 0; i < n; i++ {
		if _, err := l.Append(payload); err != nil {
			b.Fatal(err)
		}
	}
	if err := l.Close(); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l, err := wal.Open(vfs.OS(), dir, wal.Options{})
		if err != nil {
			b.Fatal(err)
		}
		l.Close()
	}
	b.ReportMetric(float64(n), "records/op")
}
