package raftnode_test

import (
	"bytes"
	"fmt"
	mrand "math/rand/v2"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/souviks22/continuum-go/internal/kv"
	"github.com/souviks22/continuum-go/internal/raftnode"
)

// auditLog records every (index, command, result) that any replica, in any
// incarnation, ever applies, and flags the ways replicated state machines go wrong.
type auditLog struct {
	byIndex   map[uint64]auditEntry
	byCmd     map[string]uint64
	violation string
}

type auditEntry struct{ cmd, res []byte }

func newAuditLog() *auditLog {
	return &auditLog{byIndex: map[uint64]auditEntry{}, byCmd: map[string]uint64{}}
}

func (l *auditLog) record(index uint64, cmd, res []byte) {
	if e, ok := l.byIndex[index]; ok {
		if (!bytes.Equal(e.cmd, cmd) || !bytes.Equal(e.res, res)) && l.violation == "" {
			l.violation = fmt.Sprintf("log index %d was applied with a different command or result on different replicas or incarnations", index)
		}
		return
	}
	l.byIndex[index] = auditEntry{bytes.Clone(cmd), bytes.Clone(res)}
	if len(cmd) > 1 && cmd[1] == 1 { // a transaction (compactions legitimately repeat); txns are made unique by the workload
		if prev, dup := l.byCmd[string(cmd)]; dup && prev != index && l.violation == "" {
			l.violation = fmt.Sprintf("one proposed command was applied at both log index %d and %d", prev, index)
		}
		l.byCmd[string(cmd)] = index
	}
}

// auditSM wraps the real KV state machine and reports what it applies.
type auditSM struct {
	inner *kv.StateMachine
	log   *auditLog
}

func (a *auditSM) Apply(index uint64, cmd []byte) []byte {
	res := a.inner.Apply(index, cmd)
	a.log.record(index, cmd, res)
	return res
}
func (a *auditSM) Snapshot() ([]byte, error) { return a.inner.Snapshot() }
func (a *auditSM) Restore(d []byte) error    { return a.inner.Restore(d) }

var kvKeys = []string{"k0", "k1", "k2", "k3", "p/a", "p/b", "p/c"}

// kvCommand builds a random command. Each transaction carries a compare on a key
// that never exists, which is always true and changes nothing, but makes the
// command's bytes unique so the audit can tell "applied twice" from "proposed twice".
func kvCommand(rng *mrand.Rand, n int, st *kv.Store) []byte {
	pick := func() []byte { return []byte(kvKeys[rng.IntN(len(kvKeys))]) }
	if rng.IntN(100) < 8 {
		lo, hi := st.CompactRevision(), st.Revision()
		return kv.EncodeCompact(lo + uint64(rng.IntN(int(hi-lo)+2))) // mostly valid, sometimes not
	}
	t := kv.Txn{Compares: []kv.Compare{{Target: kv.CompareMod, Result: kv.Equal, Key: []byte(fmt.Sprintf("uniq/%d", n)), Int: 0}}}
	val := func() []byte { return []byte(fmt.Sprintf("v%d", rng.IntN(1000))) }
	switch r := rng.IntN(100); {
	case r < 35:
		for i := 0; i <= rng.IntN(3); i++ {
			t.Success = append(t.Success, kv.Op{Type: kv.OpPut, Key: pick(), Value: val(), PrevKV: rng.IntN(2) == 0})
		}
	case r < 50:
		op := kv.Op{Type: kv.OpDeleteRange, Key: pick(), PrevKV: rng.IntN(2) == 0}
		if rng.IntN(3) == 0 {
			op.Key, op.RangeEnd = []byte("p/"), kv.PrefixEnd([]byte("p/"))
		}
		t.Success = []kv.Op{op}
	case r < 80: // compare-and-set against what this leader currently sees
		key := pick()
		cur := uint64(0)
		if res, err := st.Range(key, nil, kv.RangeOptions{}); err == nil && len(res.KVs) > 0 {
			cur = res.KVs[0].ModRevision
		}
		t.Compares = append(t.Compares, kv.Compare{Target: kv.CompareMod, Result: kv.Equal, Key: key, Int: cur})
		t.Success = []kv.Op{{Type: kv.OpPut, Key: key, Value: val(), PrevKV: true}}
		t.Failure = []kv.Op{{Type: kv.OpRange, Key: key}}
	default:
		t.Success = []kv.Op{
			{Type: kv.OpRange, Key: []byte("p/"), RangeEnd: kv.PrefixEnd([]byte("p/")), Limit: uint64(rng.IntN(3))},
			{Type: kv.OpPut, Key: pick(), Value: val()},
			{Type: kv.OpRange, Key: []byte("\x00"), RangeEnd: []byte{0}, Revision: uint64(rng.IntN(int(st.Revision()) + 2))},
		}
	}
	b, err := t.Encode(kv.Limits{})
	if err != nil {
		panic(err)
	}
	return b
}

// The KV store inside the real cluster, under the same crashes, partitions, loss,
// duplication and mid-handler power cuts as the Raft chaos test. Beyond "replicas
// agree", it checks that the replicated state is a pure function of the log:
//
//   - no log index is ever applied differently by two replicas or incarnations;
//   - no command is applied at two indexes;
//   - every result a client was given equals the committed result;
//   - replaying the whole audited log on a fresh store reproduces exactly the
//     state every replica ended in.
func TestChaos_KVStateIsAPureFunctionOfTheLog(t *testing.T) {
	seeds := uint64(60)
	if testing.Short() {
		seeds = 10
	}
	if v := os.Getenv("CHAOS_SEEDS"); v != "" {
		if n, err := strconv.ParseUint(v, 10, 64); err == nil {
			seeds = n
		}
	}
	for seed := uint64(1); seed <= seeds; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) { kvChaos(t, seed) })
	}
}

type ackedCmd struct{ cmd, res []byte }

func kvChaos(t *testing.T, seed uint64) {
	rng := mrand.New(mrand.NewPCG(seed, 4242))
	log := newAuditLog()
	c := newCluster(t, seed, 3, opts{snapEvery: 8, trailing: 2, loss: 0.03, dup: 0.03, walSegment: 400, fsFault: 0.004,
		newSM: func() raftnode.StateMachine { return &auditSM{inner: kv.NewStateMachine(), log: log} }})
	storeOf := func(p *proc) *kv.Store { return p.alt.(*auditSM).inner.Store() }

	var acked []ackedCmd
	propose := func(p *proc, cmd []byte, timeout time.Duration) {
		p.node.Propose(cmd, timeout, func(res []byte, err error) {
			if err == nil {
				acked = append(acked, ackedCmd{cmd, res})
			}
		})
	}
	live := func() (out []*proc) {
		for _, id := range c.ids {
			if c.procs[id].live() {
				out = append(out, c.procs[id])
			}
		}
		return
	}
	dead := func() (out []*proc) {
		for _, id := range c.ids {
			if !c.procs[id].live() {
				out = append(out, c.procs[id])
			}
		}
		return
	}

	n, crashes := 0, 0
	for step := 0; step < 150; step++ {
		switch a := rng.IntN(100); {
		case a < 50:
			target := c.leader()
			if l := live(); target == nil || rng.IntN(10) < 3 {
				target = nil
				if len(l) > 0 {
					target = l[rng.IntN(len(l))]
				}
			}
			if target != nil {
				propose(target, kvCommand(rng, n, storeOf(target)), 4*time.Second)
				n++
			}
		case a < 58:
			if l := live(); len(l) > 0 {
				c.crash(l[rng.IntN(len(l))].id, rng.IntN(2) == 0, rng.Uint64())
				crashes++
			}
		case a < 72:
			if d := dead(); len(d) > 0 {
				c.restart(d[rng.IntN(len(d))].id)
			}
		case a < 80:
			c.isolate(c.ids[rng.IntN(len(c.ids))])
		case a < 90:
			c.healAll()
		}
		c.run(time.Duration(50+rng.IntN(350)) * ms)
		if log.violation != "" {
			t.Fatal(log.violation)
		}
	}

	// Heal and settle, then force one more committed write so every node has
	// applied everything that was ever committed.
	c.fsFault = 0
	c.healAll()
	for _, p := range dead() {
		c.restart(p.id)
	}
	c.waitLeader(120 * time.Second)
	done := false
	for attempt := 0; attempt < 6 && !done; attempt++ {
		l := c.waitLeader(120 * time.Second)
		cmd := kvCommand(rng, 1_000_000+attempt, storeOf(l))
		r := &result{}
		if err := l.node.Propose(cmd, 20*time.Second, func(res []byte, err error) {
			r.done, r.err = true, err
			if err == nil {
				acked = append(acked, ackedCmd{cmd, res})
			}
		}); err != nil {
			c.run(500 * ms)
			continue
		}
		c.waitUntil("the final command to resolve", 60*time.Second, func() bool { return r.done })
		done = r.err == nil
	}
	if !done {
		t.Fatalf("the healed cluster could not commit anything\n%s", c.describe())
	}
	c.waitUntil("every node to hold the same applied state", 120*time.Second, func() bool {
		var ref [32]byte
		var refApplied uint64
		for i, id := range c.ids {
			p := c.procs[id]
			h, ap := storeOf(p).Hash(), p.node.Applied()
			if i == 0 {
				ref, refApplied = h, ap
			} else if h != ref || ap != refApplied {
				return false
			}
		}
		return true
	})
	c.run(2 * time.Second)
	if log.violation != "" {
		t.Fatal(log.violation)
	}

	// Replay the entire audited log on a fresh store.
	var maxIdx uint64
	for idx := range log.byIndex {
		maxIdx = max(maxIdx, idx)
	}
	fresh := kv.NewStateMachine()
	replayed := 0
	for idx := uint64(1); idx <= maxIdx; idx++ {
		e, ok := log.byIndex[idx]
		if !ok {
			continue // an index that carried no state machine command
		}
		if got := fresh.Apply(idx, e.cmd); !bytes.Equal(got, e.res) {
			t.Fatalf("replaying index %d gives a different result than the replicas produced: the state machine is not deterministic", idx)
		}
		replayed++
	}
	want := fresh.Store().Hash()
	for _, id := range c.ids {
		if got := storeOf(c.procs[id]).Hash(); got != want {
			t.Fatalf("node %d's state differs from a fresh replay of the committed log (%d commands)", id, replayed)
		}
	}

	// Every acknowledged result is the committed one.
	for _, a := range acked {
		if len(a.cmd) > 1 && a.cmd[1] == 1 {
			idx, ok := log.byCmd[string(a.cmd)]
			if !ok {
				t.Fatalf("an acknowledged transaction was never applied anywhere")
			}
			if !bytes.Equal(log.byIndex[idx].res, a.res) {
				t.Fatalf("the result given to the client for index %d differs from the committed result", idx)
			}
		}
	}
	t.Logf("KVSTAT proposed=%d acked=%d applied=%d crashes=%d fscrashes=%d finalRevision=%d", n, len(acked), replayed, crashes, c.crashes, fresh.Store().Revision())
}
