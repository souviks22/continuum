package raftnode_test

import (
	"errors"
	"fmt"
	mrand "math/rand/v2"
	"testing"
	"time"

	"github.com/souviks22/continuum-go/internal/raftnode"
)

// readResult records one LinearizableRead. When the read succeeded, stale names
// the first write that had been acknowledged before the read began but is
// missing from the state machine the callback saw: it must always be empty.
type readResult struct {
	calls int
	err   error
	stale string
}

func (r *readResult) done() bool { return r.calls > 0 }

// readAsync starts a linearizable read on p. The staleness check runs inside the
// callback, which is where a real caller would read the state machine.
func (c *cluster) readAsync(p *proc, timeout time.Duration) (*readResult, error) {
	r := &readResult{}
	sm := p.sm // this incarnation's state machine
	acked := append([]string(nil), c.acked...)
	err := p.node.LinearizableRead(timeout, func(err error) {
		r.calls++
		r.err = err
		if err != nil {
			return
		}
		have := map[string]bool{}
		for _, cmd := range sm.cmds() {
			have[cmd] = true
		}
		for _, cmd := range acked {
			if !have[cmd] {
				r.stale = cmd
				return
			}
		}
	})
	if err != nil {
		return nil, err
	}
	return r, nil
}

// readOK retries a read on p until one succeeds, failing the test if none does.
func (c *cluster) readOK(p *proc, limit time.Duration) *readResult {
	c.t.Helper()
	end := c.sched.Now() + limit
	for c.sched.Now() < end {
		r, err := c.readAsync(p, 3*time.Second)
		if err != nil {
			c.run(50 * ms) // not the leader / not ready yet: retry shortly
			continue
		}
		c.waitUntil("read to resolve", 10*time.Second, r.done)
		if r.err == nil {
			return r
		}
	}
	c.t.Fatalf("no read on node %d succeeded within %v\n%s", p.id, limit, c.describe())
	return nil
}

func TestRead_SeesEveryCompletedWriteOnEveryNode(t *testing.T) {
	c := newCluster(t, 1, 3, opts{})
	l := c.waitLeader(10 * time.Second)
	for _, cmd := range []string{"a", "b", "c"} {
		if r := c.propose(l, cmd); r.err != nil {
			t.Fatal(r.err)
		}
	}
	// The write is acknowledged, but followers may not have applied it yet; a
	// read on them must wait rather than answer from a stale state.
	for _, id := range c.ids {
		r := c.readOK(c.procs[id], 20*time.Second)
		if r.stale != "" {
			t.Fatalf("node %d served a read missing acknowledged write %q", id, r.stale)
		}
	}
}

func TestRead_WaitsForALaggingFollowerToApply(t *testing.T) {
	c := newCluster(t, 2, 3, opts{})
	l := c.waitLeader(10 * time.Second)
	var f *proc
	for _, id := range c.ids {
		if id != l.id {
			f = c.procs[id]
			break
		}
	}
	if r := c.propose(l, "before"); r.err != nil {
		t.Fatal(r.err)
	}
	c.waitConverged(10*time.Second, 1)
	c.isolate(f.id)
	if r := c.propose(l, "while-cut-off"); r.err != nil { // the other two form a quorum
		t.Fatal(r.err)
	}
	c.healAll()
	r := c.readOK(f, 30*time.Second) // f is behind at the moment of the call
	if r.stale != "" {
		t.Fatalf("lagging follower answered without the write %q", r.stale)
	}
}

func TestRead_RefusedWhenNoLeaderIsKnown(t *testing.T) {
	c := newCluster(t, 3, 3, opts{})
	_, err := c.readAsync(c.procs[1], time.Second) // nothing has run: no election yet
	if !errors.Is(err, raftnode.ErrNotLeader) {
		t.Fatalf("got %v, want ErrNotLeader", err)
	}
}

func TestRead_RejectsNonPositiveTimeout(t *testing.T) {
	c := newCluster(t, 4, 3, opts{})
	l := c.waitLeader(10 * time.Second)
	if _, err := c.readAsync(l, 0); err == nil {
		t.Fatal("zero timeout accepted")
	}
}

func TestRead_LeaderWithoutQuorumNeverAnswers(t *testing.T) {
	c := newCluster(t, 5, 3, opts{})
	l := c.waitLeader(10 * time.Second)
	if r := c.propose(l, "x"); r.err != nil {
		t.Fatal(r.err)
	}
	c.waitConverged(10*time.Second, 1)
	c.isolate(l.id)
	// Shorter than the election timeout, so the leader still believes it leads.
	r, err := c.readAsync(l, 300*ms)
	if err != nil {
		t.Fatalf("a leader should accept the read: %v", err)
	}
	c.run(2 * time.Second)
	if !errors.Is(r.err, raftnode.ErrReadTimeout) || r.calls != 1 {
		t.Fatalf("calls=%d err=%v, want exactly one ErrReadTimeout", r.calls, r.err)
	}
	c.run(5 * time.Second)
	if r.calls != 1 {
		t.Fatalf("callback ran %d times", r.calls)
	}
}

// The scenario linearizability exists for: the old leader is cut off, the rest
// elect a new one and commit a write, and a client still talking to the old
// leader must not read the pre-write state as if it were current.
func TestRead_DeposedLeaderNeverServesStaleData(t *testing.T) {
	c := newCluster(t, 6, 3, opts{})
	old := c.waitLeader(10 * time.Second)
	if r := c.propose(old, "v1"); r.err != nil {
		t.Fatal(r.err)
	}
	c.waitConverged(10*time.Second, 1)
	c.isolate(old.id)
	c.waitUntil("the others to elect a new leader", 30*time.Second, func() bool {
		l := c.leader()
		return l != nil && l.id != old.id
	})
	fresh := c.leader()
	if r := c.propose(fresh, "v2"); r.err != nil {
		t.Fatal(r.err)
	}
	// "v2" is acknowledged. A read through the cut-off node must now fail.
	if r, err := c.readAsync(old, 2*time.Second); err == nil {
		c.run(5 * time.Second)
		if r.err == nil {
			t.Fatalf("the cut-off node answered a read (stale=%q)", r.stale)
		}
	}
	c.healAll()
	c.waitUntil("the old leader to follow the new one", 30*time.Second, func() bool { return old.node.Leader() == fresh.id })
	if r := c.readOK(old, 30*time.Second); r.stale != "" {
		t.Fatalf("after healing, the old leader's read is missing %q", r.stale)
	}
}

func TestRead_PendingReadsFailWhenTheNodeStops(t *testing.T) {
	c := newCluster(t, 7, 3, opts{})
	l := c.waitLeader(10 * time.Second)
	if r := c.propose(l, "x"); r.err != nil {
		t.Fatal(r.err)
	}
	c.waitConverged(10*time.Second, 1)
	r, err := c.readAsync(l, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.node.Stop(); err != nil {
		t.Fatal(err)
	}
	if r.calls != 1 || !errors.Is(r.err, raftnode.ErrStopped) {
		t.Fatalf("calls=%d err=%v, want one ErrStopped", r.calls, r.err)
	}
	if err := l.node.LinearizableRead(time.Second, func(error) { t.Error("callback ran on a stopped node") }); !errors.Is(err, raftnode.ErrStopped) {
		t.Fatalf("read on a stopped node: %v", err)
	}
	c.run(15 * time.Second)
	if r.calls != 1 {
		t.Fatalf("callback ran %d times", r.calls)
	}
}

// Right after an election the leader has not committed in its own term and must
// say so instead of leaving the caller to time out. Both outcomes are legal here
// (the no-op may already have committed); what must hold is that the refusal is
// the specific error, and that a retry then succeeds.
func TestRead_NewLeaderRefusesUntilItCommitsInItsTerm(t *testing.T) {
	c := newCluster(t, 8, 3, opts{})
	var l *proc
	for l == nil {
		c.sched.RunUntil(c.sched.Now() + 1*ms)
		c.sample()
		l = c.leader()
		if c.sched.Now() > 30*time.Second {
			t.Fatal("no leader")
		}
	}
	if _, err := c.readAsync(l, 2*time.Second); err != nil && !errors.Is(err, raftnode.ErrLeaderNotReady) {
		t.Fatalf("unexpected refusal: %v", err)
	}
	if r := c.readOK(l, 20*time.Second); r.stale != "" {
		t.Fatalf("stale read %q", r.stale)
	}
}

// Linearizability under the full fault mix. Writers and readers share a cluster
// suffering power cuts, partitions, loss, duplication and reordering. The only
// real-time rule that matters for these reads: a read that begins after a write
// was acknowledged and succeeds must see that write. Reads may fail or time out
// freely; they may never succeed with stale data.
func TestChaos_ReadsAreLinearizable(t *testing.T) {
	seeds := uint64(40)
	if testing.Short() {
		seeds = 8
	}
	var ok, failed int
	for seed := uint64(1); seed <= seeds; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			o, f := chaosReads(t, seed)
			ok += o
			failed += f
		})
	}
	t.Logf("READSTAT succeeded=%d failed_or_timed_out=%d", ok, failed)
	if ok == 0 {
		t.Fatal("no read ever succeeded: the test checked nothing")
	}
}

func chaosReads(t *testing.T, seed uint64) (succeeded, failed int) {
	rng := mrand.New(mrand.NewPCG(seed, 991))
	c := newCluster(t, seed, 3, opts{snapEvery: 8, trailing: 2, loss: 0.03, dup: 0.03, walSegment: 300, fsFault: 0.004})
	var reads []*readResult
	live := func() []*proc {
		var out []*proc
		for _, id := range c.ids {
			if c.procs[id].live() {
				out = append(out, c.procs[id])
			}
		}
		return out
	}
	counter := 0
	for step := 0; step < 150; step++ {
		switch a := rng.IntN(100); {
		case a < 30:
			if target := c.leader(); target != nil {
				c.proposeAsync(target, fmt.Sprintf("w%03d", counter), 4*time.Second)
				counter++
			}
		case a < 65: // reads, mostly on arbitrary nodes (followers forward to the leader)
			if ps := live(); len(ps) > 0 {
				p := ps[rng.IntN(len(ps))]
				if r, err := c.readAsync(p, 2*time.Second); err == nil {
					reads = append(reads, r)
				} else {
					failed++
				}
			}
		case a < 71:
			if ps := live(); len(ps) > 0 {
				c.crash(ps[rng.IntN(len(ps))].id, rng.IntN(2) == 0, rng.Uint64())
			}
		case a < 81:
			for _, id := range c.ids {
				if !c.procs[id].live() && rng.IntN(2) == 0 {
					c.restart(id)
				}
			}
		case a < 89:
			c.isolate(c.ids[rng.IntN(len(c.ids))])
		case a < 96:
			c.healAll()
		}
		c.run(time.Duration(30+rng.IntN(250)) * ms)
		for _, r := range reads {
			if r.done() && r.err == nil && r.stale != "" {
				t.Fatalf("a successful read missed write %q that was acknowledged before it began\n%s", r.stale, c.describe())
			}
		}
	}
	c.fsFault = 0
	c.healAll()
	c.run(10 * time.Second)
	for _, r := range reads {
		switch {
		case !r.done():
			// Its node died; the process took the callback with it.
		case r.calls != 1:
			t.Fatalf("a read callback ran %d times", r.calls)
		case r.err == nil:
			succeeded++
			if r.stale != "" {
				t.Fatalf("a successful read missed acknowledged write %q", r.stale)
			}
		default:
			failed++
		}
	}
	return succeeded, failed
}
