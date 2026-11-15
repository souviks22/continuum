package raftnode_test

import (
	crand "crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	mrand "math/rand/v2"
	"sort"
	"testing"
	"time"

	"github.com/souviks22/continuum-go/internal/raftnode"
	"github.com/souviks22/continuum-go/internal/rpc"
	"github.com/souviks22/continuum-go/internal/sim"
	"github.com/souviks22/continuum-go/internal/vfs"
	"github.com/souviks22/continuum-go/internal/vfs/vfstest"
)

const ms = time.Millisecond

// seededReader makes etcd-io/raft's election-timeout randomization (which reads
// crypto/rand.Reader at call time and has no seeding hook) reproducible, so a
// failing simulation seed can be replayed exactly. Test-only, and global, so
// tests using it must not run in parallel.
type seededReader struct{ r *mrand.Rand }

func (s seededReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = byte(s.r.Uint32())
	}
	return len(p), nil
}

func seedRaft(t *testing.T, seed uint64) {
	t.Helper()
	old := crand.Reader
	crand.Reader = seededReader{mrand.New(mrand.NewPCG(seed, 0x5eed))}
	t.Cleanup(func() { crand.Reader = old })
}

type rec struct {
	Index uint64
	Cmd   string
}

// recordSM is a state machine that remembers what it applied. It lives in
// process memory, so after a crash it is rebuilt from snapshot plus log, as a
// real one would be.
type recordSM struct {
	recs        []rec
	restores    int
	badOrder    string
	lastApplied uint64
}

func (s *recordSM) Apply(index uint64, cmd []byte) []byte {
	if index <= s.lastApplied && s.badOrder == "" {
		s.badOrder = fmt.Sprintf("apply index %d after %d", index, s.lastApplied)
	}
	s.lastApplied = index
	s.recs = append(s.recs, rec{index, string(cmd)})
	return []byte("ok:" + string(cmd))
}

func (s *recordSM) Snapshot() ([]byte, error) { return json.Marshal(s.recs) }

func (s *recordSM) Restore(data []byte) error {
	s.restores++
	s.recs = nil
	s.lastApplied = 0
	if len(data) > 0 {
		if err := json.Unmarshal(data, &s.recs); err != nil {
			return err
		}
		if n := len(s.recs); n > 0 {
			s.lastApplied = s.recs[n-1].Index
		}
	}
	return nil
}

func (s *recordSM) cmds() []string {
	out := make([]string, len(s.recs))
	for i, r := range s.recs {
		out[i] = r.Cmd
	}
	return out
}

type proc struct {
	id              uint64
	name            string
	mem             *vfs.Mem
	tr              *sim.Transport
	ep              *rpc.Endpoint
	clk             *sim.ProcessClock
	node            *raftnode.Node
	sm              *recordSM             // set unless the cluster runs a custom state machine
	alt             raftnode.StateMachine // the custom state machine, when opts.newSM is set
	incarnation     uint64
	fatal           error
	injected        bool // the process is dying from an injected filesystem fault
	joining         bool // starts empty and waits for a replicated membership addition
	bootstrapVoters []uint64
}

func (p *proc) live() bool { return p.node != nil }

type opts struct {
	snapEvery, trailing uint64
	loss, dup           float64
	walSegment          int64
	// fsFault is the chance that any single mutating filesystem call is the
	// instant power is lost. Unlike a crash between events, this lands in the
	// middle of a handler: after some messages were sent, before the rest of
	// the state was written.
	fsFault float64
	// newSM, if set, replaces the default recording state machine.
	newSM    func() raftnode.StateMachine
	observer raftnode.Observer
}

type cluster struct {
	t       *testing.T
	sched   *sim.Scheduler
	net     *sim.Network
	procs   map[uint64]*proc
	ids     []uint64
	peers   map[uint64]string
	opts    opts
	fsFault float64
	fsRng   *mrand.Rand
	crashes int               // power cuts injected at filesystem calls
	leaders map[uint64]uint64 // term -> the one leader seen in it
	acked   []string
	pending int
}

func newCluster(t *testing.T, seed uint64, n int, o opts) *cluster {
	t.Helper()
	seedRaft(t, seed)
	s := sim.NewScheduler()
	net := sim.NewNetwork(s, seed)
	if err := net.SetDefaultLink(sim.LinkConfig{MinDelay: 1 * ms, MaxDelay: 5 * ms, LossRate: o.loss, DuplicateRate: o.dup}); err != nil {
		t.Fatal(err)
	}
	c := &cluster{t: t, sched: s, net: net, procs: map[uint64]*proc{}, peers: map[uint64]string{}, opts: o,
		fsRng: mrand.New(mrand.NewPCG(seed, 0xfa17)), leaders: map[uint64]uint64{}}
	for id := uint64(1); id <= uint64(n); id++ {
		c.ids = append(c.ids, id)
		c.peers[id] = fmt.Sprintf("n%d", id)
	}
	for _, id := range c.ids {
		tr, err := sim.NewTransport(c.peers[id], net)
		if err != nil {
			t.Fatal(err)
		}
		p := &proc{id: id, name: c.peers[id], mem: vfs.NewMem(), tr: tr}
		c.procs[id] = p
		c.installFaults(p)
		c.boot(p)
	}
	c.fsFault = o.fsFault // faults begin once the cluster exists
	return c
}

func mutatingOp(op vfs.Op) bool {
	switch op {
	case vfs.OpCreate, vfs.OpWrite, vfs.OpTruncate, vfs.OpRemove, vfs.OpRename, vfs.OpSync, vfs.OpSyncDir:
		return true
	}
	return false
}

// installFaults lets a node's disk lose power at a random mutating call. From
// that call on every operation fails, as it would for a process that is gone.
func (c *cluster) installFaults(p *proc) {
	p.mem.SetHook(func(op vfs.Op, _ string) error {
		if !mutatingOp(op) {
			return nil
		}
		if p.injected {
			return vfstest.ErrInjected
		}
		if c.fsFault > 0 && c.fsRng.Float64() < c.fsFault {
			p.injected = true
			return vfstest.ErrInjected
		}
		return nil
	})
}

// powerLoss kills p's process now and applies power-loss semantics to its disk.
func (c *cluster) powerLoss(p *proc, torn bool, seed uint64) {
	c.net.CrashNow(p.name)
	if p.clk != nil {
		p.clk.Kill()
	}
	p.node, p.fatal, p.injected = nil, nil, false
	p.mem.Crash(vfs.CrashOptions{Seed: seed, Torn: torn})
}

// boot starts a fresh process incarnation of p from whatever is on its disk.
func (c *cluster) boot(p *proc) {
	c.t.Helper()
	p.incarnation++
	p.clk = c.sched.NewProcessClock()
	var sm raftnode.StateMachine
	if c.opts.newSM != nil {
		p.sm, p.alt = nil, c.opts.newSM()
		sm = p.alt
	} else {
		p.sm, p.alt = &recordSM{}, nil
		sm = p.sm
	}
	ep := rpc.NewEndpoint(p.tr, p.clk, rpc.Options{})
	p.ep = ep
	bootstrapVoters := p.bootstrapVoters
	if !p.joining {
		bootstrapVoters = nil
	}
	node, err := raftnode.Open(raftnode.Config{
		ID: p.id, Peers: c.peers, Bootstrap: !p.joining,
		BootstrapLearner: p.joining, InitialVoters: bootstrapVoters,
		Endpoint: ep, Clock: p.clk, FS: p.mem, Dir: "node", StateMachine: sm,
		SnapshotEvery: c.opts.snapEvery, SnapshotTrailing: c.opts.trailing, WALSegmentSize: c.opts.walSegment,
		RequestIDBase: p.incarnation << 32,
		Observer:      c.opts.observer,
		OnFatal:       func(err error) { p.fatal = err },
	})
	if err != nil {
		if errors.Is(err, vfstest.ErrInjected) { // power was lost again during recovery
			c.crashes++
			c.powerLoss(p, c.fsRng.IntN(2) == 0, c.fsRng.Uint64())
			return
		}
		c.t.Fatalf("node %d: open failed: %v", p.id, err)
	}
	p.node = node
}

// crash is a power cut: the process dies without a chance to flush, and the disk
// keeps only what was durable (plus, if torn, part of what was in flight).
func (c *cluster) crash(id uint64, torn bool, seed uint64) {
	c.t.Helper()
	p := c.procs[id]
	if !p.live() {
		return
	}
	c.powerLoss(p, torn, seed)
}

func (c *cluster) restart(id uint64) {
	c.t.Helper()
	p := c.procs[id]
	if p.live() {
		return
	}
	if err := c.net.RestartNow(p.name, func() { c.boot(p) }); err != nil {
		c.t.Fatal(err)
	}
	if !p.live() { // the boot itself lost power
		c.net.CrashNow(p.name)
	}
}

func (c *cluster) isolate(id uint64) {
	var others []string
	for _, o := range c.ids {
		if o != id {
			others = append(others, c.peers[o])
		}
	}
	c.net.Partition([]string{c.peers[id]}, others)
}

func (c *cluster) healAll() {
	for _, a := range c.ids {
		for _, b := range c.ids {
			if a != b {
				c.net.Unblock(c.peers[a], c.peers[b])
			}
		}
	}
}

// sample checks the invariants that must hold at every instant.
func (c *cluster) sample() {
	c.t.Helper()
	for _, id := range c.ids {
		p := c.procs[id]
		if p.fatal != nil {
			if errors.Is(p.fatal, vfstest.ErrInjected) { // the injected power cut took effect mid-handler
				c.crashes++
				c.powerLoss(p, c.fsRng.IntN(2) == 0, c.fsRng.Uint64())
				continue
			}
			c.t.Fatalf("node %d failed: %v", id, p.fatal)
		}
		if !p.live() {
			continue
		}
		if p.sm != nil && p.sm.badOrder != "" {
			c.t.Fatalf("node %d: %s", id, p.sm.badOrder)
		}
		if p.node.IsLeader() {
			term := p.node.Term()
			if prev, ok := c.leaders[term]; ok && prev != id {
				c.t.Fatalf("election safety violated: nodes %d and %d both leaders in term %d", prev, id, term)
			}
			c.leaders[term] = id
		}
	}
}

func (c *cluster) run(d time.Duration) {
	c.t.Helper()
	end := c.sched.Now() + d
	for c.sched.Now() < end {
		c.sched.RunUntil(min(c.sched.Now()+10*ms, end))
		c.sample()
	}
}

func (c *cluster) waitUntil(what string, limit time.Duration, cond func() bool) {
	c.t.Helper()
	end := c.sched.Now() + limit
	for !cond() {
		if c.sched.Now() >= end {
			c.t.Fatalf("timed out after %v waiting for: %s\n%s", limit, what, c.describe())
		}
		c.run(10 * ms)
	}
}

func (c *cluster) describe() string {
	out := ""
	for _, id := range c.ids {
		p := c.procs[id]
		if !p.live() {
			out += fmt.Sprintf("  node %d: DOWN\n", id)
			continue
		}
		cmds := -1
		if p.sm != nil {
			cmds = len(p.sm.recs)
		}
		out += fmt.Sprintf("  node %d: term=%d leader=%d isLeader=%v applied=%d cmds=%d\n",
			id, p.node.Term(), p.node.Leader(), p.node.IsLeader(), p.node.Applied(), cmds)
	}
	return out
}

// leader returns the live leader with the highest term, or nil.
func (c *cluster) leader() *proc {
	var best *proc
	for _, id := range c.ids {
		p := c.procs[id]
		if p.live() && p.node.IsLeader() && (best == nil || p.node.Term() > best.node.Term()) {
			best = p
		}
	}
	return best
}

func (c *cluster) waitLeader(limit time.Duration) *proc {
	c.t.Helper()
	c.waitUntil("a leader", limit, func() bool { return c.leader() != nil })
	return c.leader()
}

type result struct {
	done    bool
	payload string
	err     error
}

// proposeAsync submits cmd on p. Successful results are recorded in c.acked.
func (c *cluster) proposeAsync(p *proc, cmd string, timeout time.Duration) (*result, error) {
	r := &result{}
	c.pending++
	err := p.node.Propose([]byte(cmd), timeout, func(res []byte, err error) {
		c.pending--
		r.done, r.payload, r.err = true, string(res), err
		if err == nil {
			c.acked = append(c.acked, cmd)
		}
	})
	if err != nil {
		c.pending--
		return nil, err
	}
	return r, nil
}

func (c *cluster) propose(p *proc, cmd string) result {
	c.t.Helper()
	r, err := c.proposeAsync(p, cmd, 10*time.Second)
	if err != nil {
		c.t.Fatalf("propose %q on node %d rejected: %v", cmd, p.id, err)
	}
	c.waitUntil("proposal "+cmd, 15*time.Second, func() bool { return r.done })
	return *r
}

func (c *cluster) waitConverged(limit time.Duration, want int) {
	c.t.Helper()
	c.waitUntil(fmt.Sprintf("every live node to apply %d commands identically", want), limit, func() bool {
		var ref []string
		for _, id := range c.ids {
			p := c.procs[id]
			if !p.live() {
				continue
			}
			got := p.sm.cmds()
			if len(got) != want {
				return false
			}
			if ref == nil {
				ref = got
			} else if fmt.Sprint(ref) != fmt.Sprint(got) {
				return false
			}
		}
		return true
	})
}

func (c *cluster) sequences() map[uint64][]string {
	out := map[uint64][]string{}
	for _, id := range c.ids {
		if p := c.procs[id]; p.live() {
			out[id] = p.sm.cmds()
		}
	}
	return out
}

func sortedKeys(m map[uint64][]string) []uint64 {
	var ks []uint64
	for k := range m {
		ks = append(ks, k)
	}
	sort.Slice(ks, func(i, j int) bool { return ks[i] < ks[j] })
	return ks
}
