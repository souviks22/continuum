package raftnode_test

import (
	"fmt"
	mrand "math/rand/v2"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The invariants Raft exists to provide, checked under a randomized schedule of
// proposals, power cuts (clean and torn), restarts, partitions, and lossy,
// duplicating, reordering links:
//
//   - at most one leader per term, at every instant;
//   - every node applies commands in strictly increasing index order;
//   - once the cluster heals, every node applies the identical sequence;
//   - every acknowledged command is in that sequence (nothing acked is lost);
//   - no command is applied twice;
//   - no node ever fails on a storage or consistency error.
//
// A failing seed replays exactly (see seedRaft).
func TestChaos_SafetyHoldsUnderCrashesPartitionsAndLossyLinks(t *testing.T) {
	seeds := uint64(60)
	if testing.Short() {
		seeds = 10
	}
	if v := os.Getenv("CHAOS_SEEDS"); v != "" { // e.g. CHAOS_SEEDS=1000 for a long soak
		if n, err := strconv.ParseUint(v, 10, 64); err == nil {
			seeds = n
		}
	}
	for seed := uint64(1); seed <= seeds; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) { chaos(t, seed) })
	}
}

func chaos(t *testing.T, seed uint64) {
	rng := mrand.New(mrand.NewPCG(seed, 77))
	c := newCluster(t, seed, 3, opts{snapEvery: 8, trailing: 2, loss: 0.03, dup: 0.03, walSegment: 300, fsFault: 0.004})

	livePicks := func() []*proc {
		var out []*proc
		for _, id := range c.ids {
			if c.procs[id].live() {
				out = append(out, c.procs[id])
			}
		}
		return out
	}
	deadPicks := func() []*proc {
		var out []*proc
		for _, id := range c.ids {
			if !c.procs[id].live() {
				out = append(out, c.procs[id])
			}
		}
		return out
	}

	counter, crashes, torn := 0, 0, 0
	for step := 0; step < 150; step++ {
		switch a := rng.IntN(100); {
		case a < 45:
			// Mostly the leader; sometimes anyone, which exercises rejections by
			// followers and proposals on a leader that has been cut off.
			target := c.leader()
			if live := livePicks(); target == nil || rng.IntN(10) < 3 {
				target = nil
				if len(live) > 0 {
					target = live[rng.IntN(len(live))]
				}
			}
			if target != nil {
				cmd := fmt.Sprintf("c%03d", counter)
				counter++
				c.proposeAsync(target, cmd, 4*time.Second) // a rejection is fine; only acks are promises
			}
		case a < 53:
			if live := livePicks(); len(live) > 0 {
				isTorn := rng.IntN(2) == 0
				c.crash(live[rng.IntN(len(live))].id, isTorn, rng.Uint64())
				crashes++
				if isTorn {
					torn++
				}
			}
		case a < 68:
			if dead := deadPicks(); len(dead) > 0 {
				c.restart(dead[rng.IntN(len(dead))].id)
			}
		case a < 78:
			c.isolate(c.ids[rng.IntN(len(c.ids))])
		case a < 88:
			c.healAll()
		}
		c.run(time.Duration(50+rng.IntN(350)) * ms)
	}

	// Heal everything and let the cluster settle.
	c.fsFault = 0
	c.healAll()
	for _, p := range deadPicks() {
		c.restart(p.id)
	}
	c.waitLeader(120 * time.Second)
	final := ""
	for attempt := 0; attempt < 6 && final == ""; attempt++ {
		l := c.waitLeader(120 * time.Second)
		cmd := fmt.Sprintf("final-%d", attempt)
		r, err := c.proposeAsync(l, cmd, 20*time.Second)
		if err != nil {
			c.run(500 * ms)
			continue
		}
		c.waitUntil("final proposal to resolve", 60*time.Second, func() bool { return r.done })
		if r.err == nil {
			final = cmd
		}
	}
	if final == "" {
		t.Fatalf("the healed cluster could not commit anything\n%s", c.describe())
	}
	c.waitUntil("every node to apply the final command", 120*time.Second, func() bool {
		for _, id := range c.ids {
			ok := false
			for _, cmd := range c.procs[id].sm.cmds() {
				if cmd == final {
					ok = true
				}
			}
			if !ok {
				return false
			}
		}
		return true
	})
	c.run(2 * time.Second)

	maxTerm, installs := uint64(0), 0
	for _, id := range c.ids {
		p := c.procs[id]
		maxTerm = max(maxTerm, p.node.Term())
		installs += p.sm.restores - 1 // minus the restore every boot performs
	}
	t.Logf("CHAOSSTAT acked=%d proposed=%d crashes=%d torn=%d fscrashes=%d finalTerm=%d snapshotRestoresAfterBoot=%d", len(c.acked), counter, crashes, torn, c.crashes, maxTerm, max(installs, 0))

	seqs := c.sequences()
	ref := strings.Join(seqs[c.ids[0]], ",")
	for _, id := range sortedKeys(seqs) {
		if got := strings.Join(seqs[id], ","); got != ref {
			t.Fatalf("nodes disagree after healing:\n  node %d: %s\n  node %d: %s\n%s", c.ids[0], ref, id, got, c.describe())
		}
	}
	count := map[string]int{}
	for _, cmd := range seqs[c.ids[0]] {
		count[cmd]++
	}
	for _, cmd := range c.acked {
		if count[cmd] == 0 {
			t.Fatalf("acknowledged command %s is missing from the final state (acked=%d, applied=%d)\nfinal: %s", cmd, len(c.acked), len(seqs[c.ids[0]]), ref)
		}
	}
	for cmd, n := range count {
		if n > 1 {
			t.Fatalf("command %s applied %d times", cmd, n)
		}
	}
}
