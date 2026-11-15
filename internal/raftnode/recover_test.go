package raftnode

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/souviks22/continuum-go/internal/atomicfile"
	"github.com/souviks22/continuum-go/internal/rpc"
	"github.com/souviks22/continuum-go/internal/sim"
	"github.com/souviks22/continuum-go/internal/snapshot"
	"github.com/souviks22/continuum-go/internal/vfs"
	"github.com/souviks22/continuum-go/internal/vfs/vfstest"
)

type isSM struct{ cmds []string }

func (s *isSM) Apply(_ uint64, cmd []byte) []byte { s.cmds = append(s.cmds, string(cmd)); return nil }
func (s *isSM) Snapshot() ([]byte, error)         { return json.Marshal(s.cmds) }
func (s *isSM) Restore(d []byte) error {
	s.cmds = nil
	if len(d) > 0 {
		return json.Unmarshal(d, &s.cmds)
	}
	return nil
}

type soloOpts struct {
	snapEvery, trailing uint64
	walSegment          int64
}

// solo is a one-member cluster on its own disk: the simplest way to build and
// reopen a known on-disk state.
type solo struct {
	t     *testing.T
	sched *sim.Scheduler
	node  *Node
	sm    *isSM
	acked []string
	fatal error
}

func startSolo(t *testing.T, mem *vfs.Mem, o soloOpts) (*solo, error) {
	t.Helper()
	s := &solo{t: t, sched: sim.NewScheduler(), sm: &isSM{}}
	net := sim.NewNetwork(s.sched, 1)
	tr, err := sim.NewTransport("n1", net)
	if err != nil {
		t.Fatal(err)
	}
	clk := s.sched.NewProcessClock()
	node, err := Open(Config{
		ID: 1, Peers: map[uint64]string{1: "n1"}, Bootstrap: true,
		Endpoint: rpc.NewEndpoint(tr, clk, rpc.Options{}), Clock: clk, FS: mem, Dir: "node", StateMachine: s.sm,
		SnapshotEvery: o.snapEvery, SnapshotTrailing: o.trailing, WALSegmentSize: o.walSegment,
		RequestIDBase: 1 << 32, OnFatal: func(err error) { s.fatal = err },
	})
	if err != nil {
		return nil, err
	}
	s.node = node
	return s, nil
}

func (s *solo) run(d time.Duration) { s.sched.RunUntil(s.sched.Now() + d) }

func (s *solo) waitLeader() {
	s.t.Helper()
	for i := 0; !s.node.IsLeader(); i++ {
		if i > 3000 || s.fatal != nil {
			s.t.Fatalf("no leader (fatal=%v)", s.fatal)
		}
		s.run(10 * time.Millisecond)
	}
}

// propose waits until cmd is applied. It returns the proposal's error.
func (s *solo) propose(cmd string) error {
	s.t.Helper()
	done, var_err := false, error(nil)
	if err := s.node.Propose([]byte(cmd), 10*time.Second, func(_ []byte, err error) {
		done, var_err = true, err
		if err == nil {
			s.acked = append(s.acked, cmd)
		}
	}); err != nil {
		return err
	}
	for i := 0; !done; i++ {
		if i > 3000 || s.fatal != nil {
			return fmt.Errorf("proposal %s did not resolve (fatal=%v)", cmd, s.fatal)
		}
		s.run(10 * time.Millisecond)
	}
	return var_err
}

func cmdNames(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("k%02d", i)
	}
	return out
}

// buildDisk bootstraps a solo node, applies 20 commands (log indexes 3..22:
// index 1 is the bootstrap snapshot, index 2 the leader's no-op) and stops cleanly.
func buildDisk(t *testing.T, o soloOpts) *vfs.Mem {
	t.Helper()
	mem := vfs.NewMem()
	s, err := startSolo(t, mem, o)
	if err != nil {
		t.Fatal(err)
	}
	s.waitLeader()
	for _, c := range cmdNames(20) {
		if err := s.propose(c); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.node.Stop(); err != nil {
		t.Fatal(err)
	}
	return mem
}

func putSnapshot(t *testing.T, mem *vfs.Mem, index, term uint64, kind byte, cmds []string) {
	t.Helper()
	st, err := snapshot.Open(mem, "node/snap")
	if err != nil {
		t.Fatal(err)
	}
	extra, err := encodeExtra(kind, &pb.ConfState{Voters: []uint64{1}})
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(cmds)
	w, err := st.Create(snapshot.Meta{Index: index, Term: term, Extra: extra})
	if err != nil {
		t.Fatal(err)
	}
	w.Write(data)
	if err := w.Commit(); err != nil {
		t.Fatal(err)
	}
}

func reopenSolo(t *testing.T, mem *vfs.Mem) *solo {
	t.Helper()
	s, err := startSolo(t, mem, soloOpts{})
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	s.waitLeader()
	s.run(2 * time.Second)
	return s
}

func TestRecoverPlainRestartReplaysTheWholeLog(t *testing.T) {
	mem := buildDisk(t, soloOpts{})
	s := reopenSolo(t, mem)
	if !slices.Equal(s.sm.cmds, cmdNames(20)) {
		t.Fatalf("got %v", s.sm.cmds)
	}
}

func TestRecoverInstalledSnapshotSupersedesAStaleLog(t *testing.T) {
	// A crash between committing a snapshot received from a leader and resetting
	// the log leaves old entries above the snapshot. They belong to a different
	// history and must not be resurrected.
	mem := buildDisk(t, soloOpts{})
	putSnapshot(t, mem, 10, 2, snapKindInstalled, []string{"from-leader-1", "from-leader-2"})
	s := reopenSolo(t, mem)
	if want := []string{"from-leader-1", "from-leader-2"}; !slices.Equal(s.sm.cmds, want) {
		t.Fatalf("stale log entries were replayed on top of an installed snapshot: %v", s.sm.cmds)
	}
	if s.node.log.FirstSeq() != 11 {
		t.Fatalf("log should have been reset to start at 11, starts at %d", s.node.log.FirstSeq())
	}
}

func TestRecoverLocalSnapshotKeepsTheLogAboveIt(t *testing.T) {
	// The contrast: a snapshot this node took itself is a prefix of its own log,
	// so the entries above it are legitimate and must be replayed.
	mem := buildDisk(t, soloOpts{})
	putSnapshot(t, mem, 10, 2, snapKindLocal, cmdNames(8)) // k00..k07 are log indexes 3..10
	s := reopenSolo(t, mem)
	if !slices.Equal(s.sm.cmds, cmdNames(20)) {
		t.Fatalf("got %v", s.sm.cmds)
	}
}

func TestRecoverLogShorterThanTheSnapshotIsSuperseded(t *testing.T) {
	mem := buildDisk(t, soloOpts{})
	putSnapshot(t, mem, 50, 2, snapKindLocal, []string{"far-ahead"})
	s := reopenSolo(t, mem)
	if !slices.Equal(s.sm.cmds, []string{"far-ahead"}) {
		t.Fatalf("got %v", s.sm.cmds)
	}
}

func corruptNewestSnapshots(t *testing.T, mem *vfs.Mem, howMany int) {
	t.Helper()
	st, err := snapshot.Open(mem, "node/snap")
	if err != nil {
		t.Fatal(err)
	}
	entries, _ := st.Scan()
	for i := 0; i < howMany && i < len(entries); i++ {
		name := fmt.Sprintf("node/snap/snap-%020d.snap", entries[len(entries)-1-i].Index)
		b, err := mem.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		b[len(b)/2] ^= 0xFF
		mem.PutFile(name, b)
	}
}

func TestRecoverReportsALogGapInsteadOfInventingState(t *testing.T) {
	// Tiny segments and frequent snapshots make the log start far above the
	// older snapshot, so falling back to it would silently skip entries.
	o := soloOpts{snapEvery: 5, trailing: 1, walSegment: 40}
	for name, howMany := range map[string]int{"newest snapshot corrupt": 1, "every snapshot corrupt": 10} {
		t.Run(name, func(t *testing.T) {
			mem := buildDisk(t, o)
			if _, err := startSolo(t, mem, o); err != nil { // control: intact disk recovers
				t.Fatalf("control: %v", err)
			}
			corruptNewestSnapshots(t, mem, howMany)
			_, err := startSolo(t, mem, o)
			if !errors.Is(err, ErrLogGap) {
				t.Fatalf("err=%v, want ErrLogGap", err)
			}
		})
	}
}

func TestRecoverRefusesAnUnreadableHardStateInsteadOfGuessingTermAndVote(t *testing.T) {
	mem := buildDisk(t, soloOpts{})
	b, err := mem.ReadFile("node/hardstate")
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)-1] ^= 0xFF
	mem.PutFile("node/hardstate", b)
	if _, err := startSolo(t, mem, soloOpts{}); !errors.Is(err, atomicfile.ErrCorrupt) {
		t.Fatalf("err=%v, want it to wrap atomicfile.ErrCorrupt", err)
	}
}

func TestRecoverWithNoHardStateFileDerivesTermFromTheSnapshot(t *testing.T) {
	mem := buildDisk(t, soloOpts{})
	if err := mem.Remove("node/hardstate"); err != nil {
		t.Fatal(err)
	}
	mem.SyncDir("node")
	s := reopenSolo(t, mem)
	if !slices.Equal(s.sm.cmds, cmdNames(20)) {
		t.Fatalf("got %v", s.sm.cmds)
	}
	if s.node.Term() < 2 {
		t.Fatalf("term=%d", s.node.Term())
	}
}

// Bootstrap must be atomic from a crash's point of view: whatever instant power
// is lost during the very first start, the next start works.
func TestCrashAtEveryStepOfBootstrapStillStarts(t *testing.T) {
	vfstest.CrashAtEveryOp(t,
		func(t *testing.T, m *vfs.Mem) {},
		func(m *vfs.Mem) error {
			s, err := startSolo(t, m, soloOpts{})
			if err != nil {
				return err
			}
			s.run(3 * time.Second) // let the first Ready persist
			return s.node.Err()
		},
		func(t *testing.T, m *vfs.Mem, completed bool) {
			s, err := startSolo(t, m, soloOpts{})
			if err != nil {
				t.Errorf("restart after a crash during bootstrap failed: %v", err)
				return
			}
			s.waitLeader()
			if err := s.propose("works"); err != nil {
				t.Errorf("cluster unusable after crash during bootstrap: %v", err)
			}
			if !slices.Contains(s.sm.cmds, "works") {
				t.Errorf("cmds=%v", s.sm.cmds)
			}
		})
}

// Snapshots, log compaction, hard-state writes and restarts, crashed at every
// mutating filesystem call: every acknowledged command survives, in order,
// exactly once, and the node keeps working.
func TestCrashAtEveryStepWhileSnapshottingAndCompactingKeepsAcknowledgedCommands(t *testing.T) {
	o := soloOpts{snapEvery: 4, trailing: 1, walSegment: 60}
	var acked []string
	vfstest.CrashAtEveryOp(t,
		func(t *testing.T, m *vfs.Mem) {
			acked = nil
			s, err := startSolo(t, m, o)
			if err != nil {
				t.Fatal(err)
			}
			s.waitLeader()
			for _, c := range []string{"s0", "s1"} {
				if err := s.propose(c); err != nil {
					t.Fatal(err)
				}
			}
			acked = append(acked, s.acked...)
			s.node.Stop()
		},
		func(m *vfs.Mem) error {
			s, err := startSolo(t, m, o)
			if err != nil {
				return err
			}
			for i := 0; i < 3000 && !s.node.IsLeader() && s.node.Err() == nil; i++ {
				s.run(10 * time.Millisecond)
			}
			for _, c := range []string{"t0", "t1", "t2", "t3", "t4", "t5"} {
				if s.node.Err() != nil {
					break
				}
				if err := s.propose(c); err == nil {
					acked = append(acked, c)
				}
			}
			return s.node.Err()
		},
		func(t *testing.T, m *vfs.Mem, completed bool) {
			s, err := startSolo(t, m, o)
			if err != nil {
				t.Errorf("recovery failed: %v", err)
				return
			}
			s.waitLeader()
			s.run(2 * time.Second)
			seen := map[string]int{}
			for _, c := range s.sm.cmds {
				seen[c]++
				if seen[c] > 1 {
					t.Errorf("%s applied twice: %v", c, s.sm.cmds)
				}
			}
			last := -1
			for _, c := range acked {
				if seen[c] != 1 {
					t.Errorf("acknowledged command %s lost: %v (acked %v)", c, s.sm.cmds, acked)
				}
				at := slices.Index(s.sm.cmds, c)
				if at < last {
					t.Errorf("acknowledged commands out of order: %v", s.sm.cmds)
				}
				last = at
			}
			if err := s.propose("after"); err != nil {
				t.Errorf("node unusable after recovery: %v", err)
			}
		})
}
