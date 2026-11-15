package wal_test

import (
	"errors"
	"math/rand/v2"
	"reflect"
	"testing"

	"github.com/souviks22/continuum/internal/vfs"
	"github.com/souviks22/continuum/internal/wal"
)

// small segments: header 20 + 3 records of (20+30) = 170; the 4th rotates.
var small = wal.Options{SegmentSize: 200}

func TestEmptyLog(t *testing.T) {
	l := openLog(t, vfs.NewMem(), wal.Options{})
	if l.FirstSeq() != 1 || l.LastSeq() != 0 || l.SegmentCount() != 1 || len(readAll(t, l)) != 0 {
		t.Fatalf("first=%d last=%d segs=%d", l.FirstSeq(), l.LastSeq(), l.SegmentCount())
	}
}

func TestAppendSyncReopenReplaysEverything(t *testing.T) {
	m := vfs.NewMem()
	l := openLog(t, m, wal.Options{})
	for i := 1; i <= 10; i++ {
		_, err := l.Append(payloadOf(uint64(i), i*3))
		must(t, err)
	}
	must(t, l.Sync())
	l = reopen(t, m, l, wal.Options{})

	got := readAll(t, l)
	if len(got) != 10 {
		t.Fatalf("got %d records", len(got))
	}
	for i, r := range got {
		if r.seq != uint64(i+1) || r.p != string(payloadOf(r.seq, (i+1)*3)) {
			t.Fatalf("record %d wrong: %+v", i, r.seq)
		}
	}
	seq, err := l.Append([]byte("next"))
	must(t, err)
	if seq != 11 {
		t.Fatalf("next seq %d, want 11", seq)
	}
}

func TestFirstSeqOptionAppliesOnlyToAnEmptyLog(t *testing.T) {
	m := vfs.NewMem()
	l := openLog(t, m, wal.Options{FirstSeq: 100})
	appendN(t, l, 3, 5)
	if l.FirstSeq() != 100 || l.LastSeq() != 102 {
		t.Fatalf("first=%d last=%d", l.FirstSeq(), l.LastSeq())
	}
	l = reopen(t, m, l, wal.Options{FirstSeq: 7}) // existing log wins
	expect(t, l, 100, 102, 5)
}

func TestEmptyPayloadAndSizeLimit(t *testing.T) {
	l := openLog(t, vfs.NewMem(), wal.Options{MaxRecordSize: 16})
	if _, err := l.Append(nil); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Append(make([]byte, 17)); !errors.Is(err, wal.ErrRecordTooLarge) {
		t.Fatalf("err=%v, want ErrRecordTooLarge", err)
	}
	if _, err := l.Append([]byte("still fine")); err != nil { // not poisoned by a bad argument
		t.Fatal(err)
	}
	if got := readAll(t, l); len(got) != 2 || got[0].p != "" {
		t.Fatalf("got %+v", got)
	}
}

func TestRotationAcrossSegmentsSurvivesReopen(t *testing.T) {
	m := vfs.NewMem()
	l := openLog(t, m, small)
	appendN(t, l, 10, 30)
	if l.SegmentCount() != 4 {
		t.Fatalf("segments=%d, want 4 (3 records each)", l.SegmentCount())
	}
	expect(t, l, 1, 10, 30)
	must(t, l.Sync())
	l = reopen(t, m, l, small)
	expect(t, l, 1, 10, 30)
	if l.SegmentCount() != 4 {
		t.Fatalf("segments after reopen=%d", l.SegmentCount())
	}
}

func TestOversizedRecordStillLandsInOneSegment(t *testing.T) {
	m := vfs.NewMem()
	l := openLog(t, m, small)
	appendN(t, l, 1, 500) // bigger than SegmentSize
	appendN(t, l, 1, 10)
	must(t, l.Sync())
	l = reopen(t, m, l, small)
	got := readAll(t, l)
	if len(got) != 2 || len(got[0].p) != 500 {
		t.Fatalf("got %d records", len(got))
	}
}

func TestReadFromSeqAndRangeChecks(t *testing.T) {
	l := openLog(t, vfs.NewMem(), small)
	appendN(t, l, 10, 30) // unsynced on purpose: Read must still see them
	var seqs []uint64
	must(t, l.Read(6, func(s uint64, _ []byte) error { seqs = append(seqs, s); return nil }))
	if !reflect.DeepEqual(seqs, []uint64{6, 7, 8, 9, 10}) {
		t.Fatalf("seqs=%v", seqs)
	}
	must(t, l.Read(11, func(uint64, []byte) error { t.Fatal("no records expected"); return nil }))
	if err := l.Read(12, nil); !errors.Is(err, wal.ErrSeqOutOfRange) {
		t.Fatalf("err=%v", err)
	}
	if err := l.Read(0, nil); !errors.Is(err, wal.ErrSeqOutOfRange) {
		t.Fatalf("err=%v", err)
	}
	stop := errors.New("stop")
	if err := l.Read(1, func(uint64, []byte) error { return stop }); err != stop {
		t.Fatalf("callback error not returned unchanged: %v", err)
	}
	appendN(t, l, 1, 30) // the log keeps working after reads
	expect(t, l, 1, 11, 30)
}

// -- crash behavior ----------------------------------------------------------

func TestCrashDropsUnsyncedRecordsButNeverSyncedOnes(t *testing.T) {
	m := vfs.NewMem()
	l := openLog(t, m, wal.Options{})
	appendN(t, l, 3, 20)
	must(t, l.Sync())
	appendN(t, l, 2, 20) // never synced
	m.Crash(vfs.CrashOptions{})
	l = openLog(t, m, wal.Options{})
	expect(t, l, 1, 3, 20)
}

func TestSyncedRecordsSurviveRandomCrashes_Property(t *testing.T) {
	for seed := uint64(0); seed < 300; seed++ {
		rng := rand.New(rand.NewPCG(seed, 1))
		m := vfs.NewMem()
		opts := wal.Options{SegmentSize: 150 + int64(rng.IntN(300)), FlushThreshold: 1 + rng.IntN(200)}
		l := openLog(t, m, opts)

		var sizes []int // sizes[i] is the payload size of seq i+1
		synced := 0
		for ops := rng.IntN(80); ops > 0; ops-- {
			if rng.IntN(10) < 7 {
				n := rng.IntN(60)
				seq, err := l.Append(payloadOf(uint64(len(sizes)+1), n))
				if err != nil || seq != uint64(len(sizes)+1) {
					t.Fatalf("seed %d: append seq=%d err=%v", seed, seq, err)
				}
				sizes = append(sizes, n)
			} else {
				must(t, l.Sync())
				synced = len(sizes)
			}
		}

		m.Crash(vfs.CrashOptions{Seed: seed, Torn: seed%2 == 0})
		l2, err := wal.Open(m, dir, opts)
		if err != nil {
			t.Fatalf("seed %d: recovery failed: %v", seed, err)
		}
		got := readAll(t, l2)
		if len(got) < synced || len(got) > len(sizes) {
			t.Fatalf("seed %d: recovered %d records; synced=%d appended=%d", seed, len(got), synced, len(sizes))
		}
		for i, r := range got {
			if r.seq != uint64(i+1) || r.p != string(payloadOf(r.seq, sizes[i])) {
				t.Fatalf("seed %d: record %d damaged or out of order", seed, i)
			}
		}
		seq, err := l2.Append([]byte("after"))
		if err != nil || seq != uint64(len(got)+1) {
			t.Fatalf("seed %d: append after recovery: seq=%d err=%v", seed, seq, err)
		}
		must(t, l2.Sync())
	}
}

// fiveRecords builds a one-segment log of 5 records of 10 bytes (30 bytes each)
// plus the bytes of a sixth record, for crafting torn tails.
func fiveRecords(t *testing.T) (base, sixth []byte) {
	t.Helper()
	build := func(n int) []byte {
		m := vfs.NewMem()
		l := openLog(t, m, wal.Options{})
		appendN(t, l, n, 10)
		must(t, l.Close())
		b, err := m.ReadFile(segName(1))
		must(t, err)
		return b
	}
	b5, b6 := build(5), build(6)
	return b5, b6[len(b5):]
}

func TestTornTailVariantsAreTruncatedReportedAndRepairable(t *testing.T) {
	base, r6 := fiveRecords(t)
	flipped := append([]byte(nil), r6...)
	flipped[len(flipped)-1] ^= 0xFF

	cases := map[string][]byte{
		"partial header":                r6[:7],
		"header only":                   r6[:20],
		"partial payload":               r6[:25],
		"zero-filled tail":              make([]byte, 100),
		"partial header then zeros":     append(append([]byte(nil), r6[:5]...), make([]byte, 25)...),
		"final record bad payload crc":  flipped,
		"zero-filled payload after hdr": append(append([]byte(nil), r6[:20]...), make([]byte, 10)...),
		// Regression, found by the property test: payload half real and half
		// zero, with the zero-fill continuing past the record's end.
		"half-real payload then zero fill": append(append(append([]byte(nil), r6[:25]...), make([]byte, 5)...), make([]byte, 50)...),
	}
	for name, tail := range cases {
		t.Run(name, func(t *testing.T) {
			m := vfs.NewMem()
			m.PutFile(segName(1), append(append([]byte(nil), base...), tail...))
			l := openLog(t, m, wal.Options{})
			expect(t, l, 1, 5, 10)
			if l.TruncatedOnOpen() != int64(len(tail)) {
				t.Fatalf("TruncatedOnOpen=%d, want %d", l.TruncatedOnOpen(), len(tail))
			}
			appendN(t, l, 1, 10) // the repaired log accepts new writes cleanly
			must(t, l.Sync())
			l = reopen(t, m, l, wal.Options{})
			expect(t, l, 1, 6, 10)
			if l.TruncatedOnOpen() != 0 {
				t.Fatalf("second open still truncating %d bytes", l.TruncatedOnOpen())
			}
		})
	}
}

func openErr(t *testing.T, m *vfs.Mem, o wal.Options) error {
	t.Helper()
	l, err := wal.Open(m, dir, o)
	if err == nil {
		l.Close()
	}
	return err
}

func TestMidFileCorruptionFailsLoudly(t *testing.T) {
	base, _ := fiveRecords(t)
	// record n starts at 20 + 30*(n-1); its header is 20 bytes, payload 10.
	flipAt := map[string]int{
		"payload of record 2":      20 + 30 + 20 + 3,
		"length field of record 2": 20 + 30,
		"seq field of record 2":    20 + 30 + 5,
		"header crc of record 3":   20 + 60 + 13,
		"segment header magic":     1,
	}
	for name, off := range flipAt {
		t.Run(name, func(t *testing.T) {
			b := append([]byte(nil), base...)
			b[off] ^= 0x40
			m := vfs.NewMem()
			m.PutFile(segName(1), b)
			err := openErr(t, m, wal.Options{})
			if !errors.Is(err, wal.ErrCorrupt) {
				t.Fatalf("err=%v, want ErrCorrupt", err)
			}
			var ce *wal.CorruptError
			if !errors.As(err, &ce) || ce.Path != segName(1) {
				t.Fatalf("no usable CorruptError: %v", err)
			}
		})
	}
}

func TestDamagedFinalRecordIsTreatedAsTornNotCorrupt(t *testing.T) {
	// Documented limitation: media corruption of the very last record cannot be
	// told apart from a torn write. It is truncated and reported, not hidden.
	base, _ := fiveRecords(t)
	b := append([]byte(nil), base...)
	b[len(b)-3] ^= 0x01
	m := vfs.NewMem()
	m.PutFile(segName(1), b)
	l := openLog(t, m, wal.Options{})
	expect(t, l, 1, 4, 10)
	if l.TruncatedOnOpen() != 30 {
		t.Fatalf("TruncatedOnOpen=%d, want 30", l.TruncatedOnOpen())
	}
}

func syncedLog(t *testing.T, m *vfs.Mem, n, size int, o wal.Options) {
	t.Helper()
	l := openLog(t, m, o)
	appendN(t, l, n, size)
	must(t, l.Close())
}

func TestDamageInANonFinalSegmentIsAlwaysCorrupt(t *testing.T) {
	for name, mutate := range map[string]func([]byte) []byte{
		"cut short":           func(b []byte) []byte { return b[:len(b)-4] },
		"flip in last record": func(b []byte) []byte { b[len(b)-2] ^= 1; return b },
		"zero-filled tail":    func(b []byte) []byte { return append(b, make([]byte, 40)...) },
	} {
		t.Run(name, func(t *testing.T) {
			m := vfs.NewMem()
			syncedLog(t, m, 10, 30, small)
			b, err := m.ReadFile(segName(1))
			must(t, err)
			m.PutFile(segName(1), mutate(b))
			if err := openErr(t, m, small); !errors.Is(err, wal.ErrCorrupt) {
				t.Fatalf("err=%v, want ErrCorrupt: a non-final segment was fsynced before rotation", err)
			}
		})
	}
}

func TestMissingMiddleSegmentIsDetected(t *testing.T) {
	m := vfs.NewMem()
	syncedLog(t, m, 10, 30, small)
	must(t, m.Remove(segName(4)))
	if err := openErr(t, m, small); !errors.Is(err, wal.ErrCorrupt) {
		t.Fatalf("err=%v, want ErrCorrupt (gap in sequence)", err)
	}
}

func TestSegmentHeaderDisagreeingWithItsFileNameIsCorrupt(t *testing.T) {
	m := vfs.NewMem()
	syncedLog(t, m, 2, 30, wal.Options{})
	b, err := m.ReadFile(segName(1))
	must(t, err)
	must(t, m.Remove(segName(1)))
	m.PutFile(segName(999), b)
	if err := openErr(t, m, wal.Options{}); !errors.Is(err, wal.ErrCorrupt) {
		t.Fatalf("err=%v, want ErrCorrupt", err)
	}
}

func TestUnsupportedSegmentVersionIsRefused(t *testing.T) {
	m := vfs.NewMem()
	syncedLog(t, m, 2, 30, wal.Options{})
	b, _ := m.ReadFile(segName(1))
	b[4] = 2 // version byte; the header CRC now fails first, which is also a refusal
	m.PutFile(segName(1), b)
	if err := openErr(t, m, wal.Options{}); !errors.Is(err, wal.ErrCorrupt) {
		t.Fatalf("err=%v", err)
	}
}

func TestTornSegmentHeaderOnTheLastSegmentIsRebuilt(t *testing.T) {
	m := vfs.NewMem()
	syncedLog(t, m, 10, 30, small)
	m.PutFile(segName(10), []byte("CWAL\x01")) // crash right after creating segment 10
	l := openLog(t, m, small)
	expect(t, l, 1, 9, 30)
	if l.SegmentCount() != 4 {
		t.Fatalf("segments=%d", l.SegmentCount())
	}
	appendN(t, l, 1, 30)
	must(t, l.Sync())
	l = reopen(t, m, l, small)
	expect(t, l, 1, 10, 30)
}

// -- truncate / discard / reset ----------------------------------------------

func TestTruncateTo(t *testing.T) {
	cases := []struct {
		name     string
		to       uint64
		segments int
	}{
		{"inside a middle segment", 5, 2},
		{"exactly at a segment boundary", 6, 3},
		{"within the active segment", 9, 4},
		{"to empty", 0, 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m := vfs.NewMem()
			l := openLog(t, m, small)
			appendN(t, l, 10, 30)
			must(t, l.Sync())

			must(t, l.TruncateTo(c.to))
			expect(t, l, 1, c.to, 30)
			if l.SegmentCount() != c.segments {
				t.Fatalf("segments=%d, want %d", l.SegmentCount(), c.segments)
			}
			appendN(t, l, 4, 30) // continues from the truncation point
			expect(t, l, 1, c.to+4, 30)
			must(t, l.Sync())
			l = reopen(t, m, l, small)
			expect(t, l, 1, c.to+4, 30)
		})
	}
}

func TestTruncateToIsDurableWithoutAnotherSync(t *testing.T) {
	m := vfs.NewMem()
	l := openLog(t, m, small)
	appendN(t, l, 10, 30)
	must(t, l.Sync())
	must(t, l.TruncateTo(4))
	m.Crash(vfs.CrashOptions{})
	l = openLog(t, m, small)
	expect(t, l, 1, 4, 30)
}

func TestTruncateToRangeChecks(t *testing.T) {
	l := openLog(t, vfs.NewMem(), wal.Options{FirstSeq: 10})
	appendN(t, l, 3, 5) // 10..12
	for _, bad := range []uint64{13, 100, 8, 0} {
		if err := l.TruncateTo(bad); !errors.Is(err, wal.ErrSeqOutOfRange) {
			t.Fatalf("TruncateTo(%d): %v, want ErrSeqOutOfRange", bad, err)
		}
	}
	must(t, l.TruncateTo(12)) // no-op
	must(t, l.TruncateTo(9))  // FirstSeq-1: empties the log
	if l.FirstSeq() != 10 || l.LastSeq() != 9 {
		t.Fatalf("first=%d last=%d", l.FirstSeq(), l.LastSeq())
	}
	if _, err := l.Append([]byte("x")); err != nil { // out-of-range calls must not poison the log
		t.Fatal(err)
	}
}

func TestDiscardBefore(t *testing.T) {
	m := vfs.NewMem()
	l := openLog(t, m, small)
	appendN(t, l, 10, 30) // segments start at 1, 4, 7, 10
	must(t, l.Sync())

	must(t, l.DiscardBefore(0))
	must(t, l.DiscardBefore(3))
	if l.FirstSeq() != 1 {
		t.Fatalf("nothing should be deletable yet, FirstSeq=%d", l.FirstSeq())
	}
	must(t, l.DiscardBefore(5)) // segment [1..3] is entirely below 5; [4..6] is not
	if l.FirstSeq() != 4 || l.SegmentCount() != 3 {
		t.Fatalf("first=%d segs=%d, want 4 and 3: compaction is by whole segment", l.FirstSeq(), l.SegmentCount())
	}
	expect(t, l, 4, 10, 30)
	must(t, l.DiscardBefore(1000)) // the active segment is never deleted
	if l.FirstSeq() != 10 || l.SegmentCount() != 1 {
		t.Fatalf("first=%d segs=%d", l.FirstSeq(), l.SegmentCount())
	}
	m.Crash(vfs.CrashOptions{})
	l = openLog(t, m, small)
	expect(t, l, 10, 10, 30)
}

func TestReset(t *testing.T) {
	m := vfs.NewMem()
	l := openLog(t, m, small)
	appendN(t, l, 10, 30)
	must(t, l.Sync())

	if err := l.Reset(0); !errors.Is(err, wal.ErrSeqOutOfRange) {
		t.Fatalf("Reset(0): %v", err)
	}
	must(t, l.Reset(50))
	if l.FirstSeq() != 50 || l.LastSeq() != 49 || l.SegmentCount() != 1 {
		t.Fatalf("first=%d last=%d segs=%d", l.FirstSeq(), l.LastSeq(), l.SegmentCount())
	}
	appendN(t, l, 2, 30)
	must(t, l.Sync())
	m.Crash(vfs.CrashOptions{})
	l = openLog(t, m, small)
	expect(t, l, 50, 51, 30)
}

// -- failure handling --------------------------------------------------------

func TestFsyncFailurePoisonsTheLog(t *testing.T) {
	m := vfs.NewMem()
	l := openLog(t, m, wal.Options{})
	appendN(t, l, 2, 20)
	must(t, l.Sync())
	appendN(t, l, 2, 20)

	boom := errors.New("EIO")
	m.SetHook(func(op vfs.Op, _ string) error {
		if op == vfs.OpSync {
			return boom
		}
		return nil
	})
	if err := l.Sync(); !errors.Is(err, boom) {
		t.Fatalf("err=%v, want the fsync error", err)
	}
	m.SetHook(nil) // the disk "recovers"; the log must not trust that
	if err := l.Sync(); !errors.Is(err, wal.ErrFailed) {
		t.Fatalf("Sync after failed fsync: %v, want ErrFailed (a retry could ack lost data)", err)
	}
	if _, err := l.Append([]byte("x")); !errors.Is(err, wal.ErrFailed) {
		t.Fatalf("Append after failure: %v, want ErrFailed", err)
	}
	if err := l.TruncateTo(1); !errors.Is(err, wal.ErrFailed) {
		t.Fatalf("TruncateTo after failure: %v", err)
	}
	must(t, l.Close())

	m.Crash(vfs.CrashOptions{Torn: true, Seed: 3})
	l = openLog(t, m, wal.Options{})
	if len(readAll(t, l)) < 2 {
		t.Fatal("records synced before the failure were lost")
	}
}

func TestDiskFullPoisonsTheLogAndRecoversOnReopen(t *testing.T) {
	m := vfs.NewMem()
	l := openLog(t, m, wal.Options{})
	appendN(t, l, 3, 40)
	must(t, l.Sync())

	m.SetCapacity(300) // segment header + 3 records already use 200 bytes
	appendN(t, l, 5, 40)
	if err := l.Sync(); !errors.Is(err, vfs.ErrNoSpace) {
		t.Fatalf("err=%v, want ErrNoSpace", err)
	}
	if _, err := l.Append([]byte("x")); !errors.Is(err, wal.ErrFailed) {
		t.Fatalf("append after ENOSPC: %v, want ErrFailed", err)
	}
	must(t, l.Close())

	m.SetCapacity(0)
	m.Crash(vfs.CrashOptions{Torn: true, Seed: 1})
	l = openLog(t, m, wal.Options{})
	got := readAll(t, l)
	if len(got) < 3 {
		t.Fatalf("synced records lost: %d", len(got))
	}
	for i, r := range got {
		if r.p != string(payloadOf(uint64(i+1), 40)) {
			t.Fatalf("record %d damaged after partial write", i)
		}
	}
	appendN(t, l, 1, 40)
	must(t, l.Sync())
}

func TestClosedLogRejectsUseAndCloseSyncs(t *testing.T) {
	m := vfs.NewMem()
	l := openLog(t, m, wal.Options{})
	appendN(t, l, 3, 10) // never explicitly synced
	must(t, l.Close())
	must(t, l.Close()) // idempotent
	if _, err := l.Append([]byte("x")); !errors.Is(err, wal.ErrClosed) {
		t.Fatalf("err=%v", err)
	}
	m.Crash(vfs.CrashOptions{})
	expect(t, openLog(t, m, wal.Options{}), 1, 3, 10)
}

func TestSpliceOrDuplicateWithValidChecksumsIsStillCorrupt(t *testing.T) {
	// Every checksum is valid, so only the sequence-continuity check can tell
	// these logs are wrong (e.g. a software bug or a bad restore that dropped
	// or repeated a record).
	b5, _ := fiveRecords(t)
	rec := func(n int) []byte { return b5[20+30*(n-1) : 20+30*n] } // record n of 5
	cat := func(parts ...[]byte) []byte {
		var out []byte
		for _, p := range parts {
			out = append(out, p...)
		}
		return out
	}
	hdr := b5[:20]
	cases := map[string][]byte{
		"skipped record 3":  cat(hdr, rec(1), rec(2), rec(4), rec(5)),
		"duplicated record": cat(hdr, rec(1), rec(2), rec(2), rec(3)),
		"records reordered": cat(hdr, rec(1), rec(3), rec(2)),
	}
	for name, file := range cases {
		t.Run(name, func(t *testing.T) {
			m := vfs.NewMem()
			m.PutFile(segName(1), file)
			if err := openErr(t, m, wal.Options{}); !errors.Is(err, wal.ErrCorrupt) {
				t.Fatalf("err=%v, want ErrCorrupt", err)
			}
		})
	}
}
