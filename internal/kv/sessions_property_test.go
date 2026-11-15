package kv_test

import (
	"bytes"
	"fmt"
	mrand "math/rand/v2"
	"testing"

	"github.com/souviks22/continuum/internal/kv"
)

// randWrite builds a random write transaction over a tiny key space, so that
// requests constantly interact (compares, overwrites, deletes, PrevKV).
func randWrite(rng *mrand.Rand) kv.Txn {
	key := func() []byte { return []byte(fmt.Sprintf("k%d", rng.IntN(4))) }
	val := func() []byte { return []byte(fmt.Sprintf("v%d", rng.IntN(1000))) }
	switch rng.IntN(4) {
	case 0:
		return kv.Txn{Success: []kv.Op{{Type: kv.OpPut, Key: key(), Value: val(), PrevKV: true}}}
	case 1:
		return kv.Txn{Success: []kv.Op{{Type: kv.OpDeleteRange, Key: key(), PrevKV: true}}}
	case 2: // create if absent, else read it
		k := key()
		return kv.Txn{
			Compares: []kv.Compare{{Target: kv.CompareMod, Result: kv.Equal, Key: k, Int: 0}},
			Success:  []kv.Op{{Type: kv.OpPut, Key: k, Value: val()}},
			Failure:  []kv.Op{{Type: kv.OpRange, Key: k}},
		}
	default: // increment-like: write a value derived from the version
		k := key()
		return kv.Txn{
			Compares: []kv.Compare{{Target: kv.CompareVersion, Result: kv.Greater, Key: k, Int: 0}},
			Success:  []kv.Op{{Type: kv.OpRange, Key: k}, {Type: kv.OpPut, Key: k, Value: val()}},
			Failure:  []kv.Op{{Type: kv.OpPut, Key: k, Value: val()}},
		}
	}
}

// The property that makes sessions worth having: delivering a request any number
// of times, at any later log positions, is equivalent to delivering it once.
//
// Replica A receives requests with random duplicates, stale resends and
// snapshot/restore cycles. Reference B receives each distinct request exactly
// once, its log positions padded so revisions line up. They must end in the same
// store, every first attempt must return identical bytes on both, and every
// duplicate must return what the first attempt returned.
func TestRetriesAreEquivalentToApplyingOnce(t *testing.T) {
	for seed := uint64(1); seed <= 200; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := mrand.New(mrand.NewPCG(seed, seed*7919))
			a, ref := newEnv(t), newEnv(t)

			const clients = 4
			var sid [clients]uint64
			var seq [clients]uint64
			var history [clients][][]byte // every request each client has sent
			var results [clients][]byte   // the first attempt's result for the latest request
			for c := range sid {
				sid[c] = a.register()
				if rid := ref.register(); rid != sid[c] {
					t.Fatalf("session IDs diverged: %d vs %d", sid[c], rid)
				}
			}

			for step := 0; step < 150; step++ {
				c := rng.IntN(clients)
				switch r := rng.IntN(10); {
				case r < 5 || seq[c] == 0: // a new request
					seq[c]++
					cmd := sessCmd(t, randWrite(rng), sid[c], seq[c])
					history[c] = append(history[c], cmd)
					got, want := a.raw(cmd), ref.raw(cmd)
					if !bytes.Equal(got, want) {
						t.Fatalf("step %d: first attempt differs from the reference", step)
					}
					results[c] = got
				case r < 8: // retry the latest request
					got := a.raw(history[c][len(history[c])-1])
					ref.skip(1)
					if !bytes.Equal(got, results[c]) {
						t.Fatalf("step %d: retry did not return the first attempt's result", step)
					}
				case r < 9 && len(history[c]) > 1: // resend an older one
					old := history[c][rng.IntN(len(history[c])-1)]
					_, err := kv.DecodeResult(a.raw(old))
					ref.skip(1)
					wantErr(t, err, kv.ErrStaleRequest)
				default: // a node restart: the state machine comes back from a snapshot
					snap, err := a.sm.Snapshot()
					if err != nil {
						t.Fatal(err)
					}
					fresh := kv.NewStateMachine()
					if err := fresh.Restore(snap); err != nil {
						t.Fatalf("restore: %v", err)
					}
					if fresh.Hash() != a.sm.Hash() {
						t.Fatalf("step %d: restored state differs", step)
					}
					a.sm = fresh
					ref.skip(0)
				}
				if a.sm.Store().Hash() != ref.sm.Store().Hash() {
					t.Fatalf("step %d: store diverged from the apply-once reference", step)
				}
				if a.idx != ref.idx {
					t.Fatalf("test bug: log positions out of step (%d vs %d)", a.idx, ref.idx)
				}
			}
		})
	}
}
