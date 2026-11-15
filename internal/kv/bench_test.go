package kv_test

import (
	"fmt"
	mrand "math/rand/v2"
	"testing"

	"github.com/souviks22/continuum-go/internal/kv"
)

// Indicative only; they exist to put numbers on the sorted-slice key index,
// whose inserts are O(n), so the size at which it stops being adequate is known.

func loaded(n int) *kv.StateMachine {
	sm := kv.NewStateMachine()
	// Ascending keys, so loading is append-only and does not itself pay the insert cost.
	for i := 0; i < n; i++ {
		cmd, _ := kv.Put([]byte(fmt.Sprintf("key/%09d", i*2)), []byte("value-0123456789")).Encode(kv.Limits{})
		sm.Apply(uint64(i+10), cmd)
	}
	return sm
}

func benchPut(b *testing.B, n int, newKeys bool) {
	sm := loaded(n)
	rng := mrand.New(mrand.NewPCG(1, 2))
	cmds := make([][]byte, 1024)
	for i := range cmds {
		k := rng.IntN(n) * 2
		if newKeys {
			k++ // odd keys do not exist yet: a real insert in the middle
		}
		cmds[i], _ = kv.Put([]byte(fmt.Sprintf("key/%09d", k)), []byte("value-0123456789")).Encode(kv.Limits{})
	}
	idx := uint64(n + 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx++
		sm.Apply(idx, cmds[i%len(cmds)])
	}
}

func BenchmarkPut(b *testing.B) {
	for _, n := range []int{1_000, 100_000, 500_000} {
		b.Run(fmt.Sprintf("existing-key/keys=%d", n), func(b *testing.B) { benchPut(b, n, false) })
		b.Run(fmt.Sprintf("new-key-mid-insert/keys=%d", n), func(b *testing.B) { benchPut(b, n, true) })
	}
}

func BenchmarkRange100Keys(b *testing.B) {
	sm := loaded(100_000)
	st := sm.Store()
	start, end := []byte("key/000050000"), []byte("key/000050200")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		st.Range(start, end, kv.RangeOptions{})
	}
}

func BenchmarkSnapshotAndRestore100kKeys(b *testing.B) {
	sm := loaded(100_000)
	snap, _ := sm.Snapshot()
	b.Run("snapshot", func(b *testing.B) {
		b.SetBytes(int64(len(snap)))
		for i := 0; i < b.N; i++ {
			sm.Snapshot()
		}
	})
	b.Run("restore", func(b *testing.B) {
		b.SetBytes(int64(len(snap)))
		for i := 0; i < b.N; i++ {
			kv.NewStateMachine().Restore(snap)
		}
	})
}

// Cost of the session layer on the apply path, against a plain Txn.
func benchApply(b *testing.B, session bool) {
	m := kv.NewStateMachine()
	sid, _ := kv.DecodeSession(m.Apply(1, kv.EncodeRegister()))
	idx := uint64(1)
	for i := 0; i < b.N; i++ {
		t := kv.Put([]byte("key"), []byte("value"))
		var cmd []byte
		if session {
			cmd, _ = t.EncodeSession(sid, uint64(i+1), kv.Limits{})
		} else {
			cmd, _ = t.Encode(kv.Limits{})
		}
		idx++
		m.Apply(idx, cmd)
	}
}

func BenchmarkApplyPlainTxn(b *testing.B)   { benchApply(b, false) }
func BenchmarkApplySessionTxn(b *testing.B) { benchApply(b, true) }

// Registering at the table bound scans for the least recently used session.
func BenchmarkRegisterAtCapacity(b *testing.B) {
	m := kv.NewStateMachine()
	idx := uint64(0)
	for i := 0; i < kv.MaxSessions; i++ {
		idx++
		m.Apply(idx, kv.EncodeRegister())
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx++
		m.Apply(idx, kv.EncodeRegister())
	}
}
