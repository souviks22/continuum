package raftnode_test

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/souviks22/continuum/internal/kv"
	"github.com/souviks22/continuum/internal/raftnode"
	"github.com/souviks22/continuum/internal/sim"
	"github.com/souviks22/continuum/internal/vfs"
)

type eventRecorder struct{ events []raftnode.Event }

func (r *eventRecorder) Observe(e raftnode.Event) { r.events = append(r.events, e) }

func TestObserverEventsAndHealthReadiness(t *testing.T) {
	recorder := &eventRecorder{}
	c := newCluster(t, 0x6f6273, 3, opts{snapEvery: 1, observer: recorder})
	for _, id := range c.ids {
		if health := c.procs[id].node.Health(); !health.Healthy || health.Ready {
			t.Fatalf("pre-election node %d health: %+v", id, health)
		}
	}
	leader := c.waitLeader(30 * time.Second)
	c.propose(leader, "observed-write")
	c.waitConverged(5*time.Second, 1)
	follower := c.procs[1]
	if follower.id == leader.id {
		follower = c.procs[2]
	}
	var readDone bool
	if err := follower.node.LinearizableRead(time.Second, func(err error) {
		if err != nil {
			t.Errorf("read: %v", err)
		}
		readDone = true
	}); err != nil {
		t.Fatal(err)
	}
	c.waitUntil("observed read", 3*time.Second, func() bool { return readDone })
	seen := map[string]bool{}
	proposalIDs := map[string]uint64{}
	for _, event := range recorder.events {
		seen[event.Name] = true
		if event.NodeID == 0 {
			t.Fatal("observer event omitted node ID")
		}
		if event.Name == raftnode.EventProposalAccepted || event.Name == raftnode.EventProposalApplied {
			proposalIDs[event.Name] = event.RequestID
		}
	}
	for _, name := range []string{raftnode.EventProposalAccepted, raftnode.EventProposalApplied, raftnode.EventReadCompleted, raftnode.EventSnapshotCreated} {
		if !seen[name] {
			t.Errorf("observer did not receive %q", name)
		}
	}
	if proposalIDs[raftnode.EventProposalAccepted] == 0 || proposalIDs[raftnode.EventProposalAccepted] != proposalIDs[raftnode.EventProposalApplied] {
		t.Fatalf("proposal event IDs do not correlate: %v", proposalIDs)
	}
	for _, id := range c.ids {
		health := c.procs[id].node.Health()
		if !health.Healthy || !health.Ready || health.Applied < health.Commit {
			t.Errorf("node %d health: %+v", id, health)
		}
	}
}

func TestSameSeedReproducesTheElectionExactlyAndSeedsDiffer(t *testing.T) {
	type trace struct {
		leader uint64
		term   uint64
		at     time.Duration
	}
	run := func(seed uint64) trace {
		c := newCluster(t, seed, 3, opts{})
		l := c.waitLeader(30 * time.Second)
		return trace{l.id, l.node.Term(), c.sched.Now()}
	}
	if a, b := run(11), run(11); a != b {
		t.Fatalf("same seed, different elections: %+v vs %+v", a, b)
	}
	distinct := map[trace]bool{}
	for seed := uint64(1); seed <= 8; seed++ {
		distinct[run(seed)] = true
	}
	if len(distinct) < 2 {
		t.Fatalf("8 different seeds produced one identical election: the seed is being ignored")
	}
}

func TestReplicationAppliesTheSameSequenceOnEveryNode(t *testing.T) {
	c := newCluster(t, 1, 3, opts{})
	l := c.waitLeader(30 * time.Second)
	var want []string
	for i := 0; i < 30; i++ {
		cmd := fmt.Sprintf("cmd-%02d", i)
		r := c.propose(l, cmd)
		if r.err != nil || r.payload != "ok:"+cmd {
			t.Fatalf("propose %s: %+v", cmd, r)
		}
		want = append(want, cmd)
	}
	c.waitConverged(10*time.Second, 30)
	for id, got := range c.sequences() {
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("node %d applied %v", id, got)
		}
	}
}

func TestMembershipRemovesVoterAndReplicatesConfState(t *testing.T) {
	c := newCluster(t, 0x6d656d, 3, opts{})
	leader := c.waitLeader(30 * time.Second)
	var done bool
	var changeErr error
	if err := leader.node.RemoveMember(3, time.Second, func(err error) { done, changeErr = true, err }); err != nil {
		t.Fatal(err)
	}
	if err := leader.node.RemoveMember(2, time.Second, func(error) {}); !errors.Is(err, raftnode.ErrConfChangePending) {
		t.Fatalf("second change returned %v, want ErrConfChangePending", err)
	}
	c.waitUntil("configuration removal to apply", 10*time.Second, func() bool { return done })
	if changeErr != nil {
		t.Fatalf("remove callback: %v", changeErr)
	}
	c.waitUntil("all replicas to learn the new voter set", 10*time.Second, func() bool {
		for _, id := range c.ids {
			got := c.procs[id].node.Membership()
			if !slices.Equal(got.GetVoters(), []uint64{1, 2}) {
				return false
			}
		}
		return true
	})
	var secondDone bool
	if err := leader.node.RemoveMember(2, time.Second, func(err error) { secondDone = err == nil }); err != nil {
		t.Fatal(err)
	}
	c.waitUntil("second membership change to apply", 10*time.Second, func() bool { return secondDone })
	c.waitUntil("all replicas to learn the single-voter set", 10*time.Second, func() bool {
		for _, id := range []uint64{1, 2} {
			if !slices.Equal(c.procs[id].node.Membership().GetVoters(), []uint64{1}) {
				return false
			}
		}
		return true
	})
	if err := c.procs[1].node.RemoveMember(1, time.Second, func(error) {}); !errors.Is(err, raftnode.ErrLastVoter) {
		t.Fatalf("last voter removal returned %v", err)
	}
	result := c.propose(c.leader(), "after-membership-change")
	if result.err != nil || result.payload != "ok:after-membership-change" {
		t.Fatalf("proposal after membership change: %+v", result)
	}
}

func TestMembershipAwareStateMachineReceivesAppliedConfiguration(t *testing.T) {
	c := newCluster(t, 0x6d656d6170706c79, 3, opts{newSM: func() raftnode.StateMachine { return &membershipRecordSM{} }})
	leader := c.waitLeader(30 * time.Second)
	first := c.propose(leader, "before-change")
	if first.err != nil {
		t.Fatalf("proposal before membership change: %v", first.err)
	}
	c.waitUntil("all replicas to apply the first membership-aware command", 5*time.Second, func() bool {
		for _, id := range c.ids {
			sm := c.procs[id].alt.(*membershipRecordSM)
			if len(sm.recs) != 1 {
				return false
			}
		}
		return true
	})
	for _, id := range c.ids {
		sm := c.procs[id].alt.(*membershipRecordSM)
		if !slices.Equal(sm.lastMembership.voters, []uint64{1, 2, 3}) || len(sm.lastMembership.learners) != 0 {
			t.Fatalf("node %d saw membership %+v", id, sm.lastMembership)
		}
	}
	var removed bool
	removeID := uint64(3)
	if leader.id == removeID {
		removeID = 2
	}
	if err := leader.node.RemoveMember(removeID, time.Second, func(err error) { removed = err == nil }); err != nil {
		t.Fatal(err)
	}
	c.waitUntil("membership removal", 10*time.Second, func() bool { return removed })
	wantVoters := []uint64{1, 2, 3}
	wantVoters = slices.Delete(wantVoters, int(removeID-1), int(removeID))
	c.waitUntil("all replicas to apply the updated configuration", 5*time.Second, func() bool {
		for _, id := range c.ids {
			if !slices.Equal(c.procs[id].node.Membership().GetVoters(), wantVoters) {
				return false
			}
		}
		return true
	})
	second := c.propose(c.leader(), "after-change")
	if second.err != nil {
		t.Fatalf("proposal after membership change: %v", second.err)
	}
	for _, id := range c.ids {
		if id == removeID {
			continue
		}
		sm := c.procs[id].alt.(*membershipRecordSM)
		if !slices.Equal(sm.lastMembership.voters, wantVoters) {
			t.Fatalf("node %d saw stale membership %+v", id, sm.lastMembership)
		}
	}
}

func TestCompatibilityActivationIsReplicatedAndBoundToCurrentMembership(t *testing.T) {
	newLegacyMachine := func() raftnode.StateMachine {
		sm := kv.NewStateMachine()
		if err := sm.Restore(kv.NewStore().Snapshot()); err != nil {
			t.Fatalf("restore legacy snapshot: %v", err)
		}
		return sm
	}
	c := newCluster(t, 0x636f6d706174, 3, opts{newSM: newLegacyMachine})
	leader := c.waitLeader(30 * time.Second)
	oldConfig := leader.node.Membership()
	oldDigest := kv.MembershipDigest(oldConfig.GetVoters(), oldConfig.GetLearners(), oldConfig.GetVotersOutgoing(), oldConfig.GetLearnersNext(), oldConfig.GetAutoLeave())
	removeID := uint64(3)
	if leader.id == removeID {
		removeID = 2
	}
	var removed bool
	if err := leader.node.RemoveMember(removeID, time.Second, func(err error) { removed = err == nil }); err != nil {
		t.Fatal(err)
	}
	c.waitUntil("membership change before compatibility activation", 10*time.Second, func() bool { return removed })
	wantVoters := []uint64{1, 2, 3}
	wantVoters = slices.Delete(wantVoters, int(removeID-1), int(removeID))
	c.waitUntil("configuration to reach every replica", 5*time.Second, func() bool {
		for _, id := range c.ids {
			if !slices.Equal(c.procs[id].node.Membership().GetVoters(), wantVoters) {
				return false
			}
		}
		return true
	})
	staleCommand, err := kv.EncodeActivateCompatibility(kv.CompatibilityLevelSessions, oldDigest)
	if err != nil {
		t.Fatal(err)
	}
	stale := c.propose(c.leader(), string(staleCommand))
	if stale.err != nil {
		t.Fatalf("stale activation proposal: %v", stale.err)
	}
	if _, err := kv.DecodeResult([]byte(stale.payload)); !errors.Is(err, kv.ErrMembershipChanged) {
		t.Fatalf("stale activation result error=%v", err)
	}
	for _, id := range wantVoters {
		if got := c.procs[id].alt.(*kv.StateMachine).CompatibilityLevel(); got != kv.CompatibilityLevelBase {
			t.Fatalf("node %d activated despite changed membership: %d", id, got)
		}
	}
	currentLeader := c.leader()
	current := currentLeader.node.Membership()
	currentDigest := kv.MembershipDigest(current.GetVoters(), current.GetLearners(), current.GetVotersOutgoing(), current.GetLearnersNext(), current.GetAutoLeave())
	command, err := kv.EncodeActivateCompatibility(kv.CompatibilityLevelSessions, currentDigest)
	if err != nil {
		t.Fatal(err)
	}
	active := c.propose(currentLeader, string(command))
	if active.err != nil {
		t.Fatalf("valid activation proposal: %v", active.err)
	}
	if _, err := kv.DecodeResult([]byte(active.payload)); err != nil {
		t.Fatalf("valid activation result: %v", err)
	}
	c.waitUntil("compatibility activation to replicate", 5*time.Second, func() bool {
		for _, id := range wantVoters {
			if c.procs[id].alt.(*kv.StateMachine).CompatibilityLevel() != kv.CompatibilityLevelSessions {
				return false
			}
		}
		return true
	})
	register := c.propose(c.leader(), string(kv.EncodeRegister()))
	if register.err != nil {
		t.Fatalf("session command after activation: %v", register.err)
	}
	if _, err := kv.DecodeSession([]byte(register.payload)); err != nil {
		t.Fatalf("session command result: %v", err)
	}
}

// legacyKVMachine models the old binary's state-machine interface: it can read
// and write the base format but does not expose the compatibility activation
// or command-validation hooks added by the new binary.
type legacyKVMachine struct{ inner *kv.StateMachine }

func newBaseKVStateMachine(t *testing.T) *kv.StateMachine {
	t.Helper()
	sm := kv.NewStateMachine()
	if err := sm.Restore(kv.NewStore().Snapshot()); err != nil {
		t.Fatalf("restore base snapshot: %v", err)
	}
	return sm
}

func (m *legacyKVMachine) Apply(index uint64, command []byte) []byte {
	return m.inner.Apply(index, command)
}
func (m *legacyKVMachine) Snapshot() ([]byte, error) { return m.inner.Snapshot() }
func (m *legacyKVMachine) Restore(data []byte) error { return m.inner.Restore(data) }

func TestMixedCompatibilityRolloutAcrossRestartAndMembershipChange(t *testing.T) {
	upgraded := map[uint64]bool{3: true}
	newMachine := func(id uint64) raftnode.StateMachine {
		inner := newBaseKVStateMachine(t)
		if upgraded[id] {
			return inner
		}
		return &legacyKVMachine{inner: inner}
	}
	c := newCluster(t, 0x726f6c6c6f7574, 3, opts{
		snapEvery: 1, trailing: 1,
		newSMByID: newMachine,
	})
	leader := c.waitLeader(30 * time.Second)
	encodePut := func(key, value string) string {
		cmd, err := kv.Put([]byte(key), []byte(value)).Encode(kv.Limits{})
		if err != nil {
			t.Fatalf("encode put: %v", err)
		}
		return string(cmd)
	}
	write := func(key, value string) {
		t.Helper()
		result := c.propose(c.leader(), encodePut(key, value))
		if result.err != nil {
			t.Fatalf("write %q: %v", key, result.err)
		}
		if _, err := kv.DecodeResult([]byte(result.payload)); err != nil {
			t.Fatalf("write %q result: %v", key, err)
		}
	}
	machine := func(id uint64) *kv.StateMachine {
		switch sm := c.procs[id].alt.(type) {
		case *legacyKVMachine:
			return sm.inner
		case *kv.StateMachine:
			return sm
		default:
			t.Fatalf("node %d has unexpected state machine %T", id, c.procs[id].alt)
			return nil
		}
	}
	checkReplicas := func() {
		t.Helper()
		c.waitUntil("mixed-version replicas to apply the same state", 10*time.Second, func() bool {
			var want [32]byte
			first := true
			for _, id := range c.ids {
				p := c.procs[id]
				if !p.live() {
					return false
				}
				got := machine(id).Hash()
				if first {
					want, first = got, false
				} else if got != want {
					return false
				}
			}
			return true
		})
	}

	// N and N+1 run together at the old base feature level.
	write("rollout/one", "old-format")
	checkReplicas()
	for _, id := range c.ids {
		got, err := machine(id).Store().Range([]byte("rollout/one"), nil, kv.RangeOptions{})
		if err != nil || len(got.KVs) != 1 || string(got.KVs[0].Value) != "old-format" {
			t.Fatalf("node %d mixed-version read=%+v err=%v", id, got, err)
		}
	}
	if leader.node.LastSnapshotIndex() == 0 {
		t.Fatal("mixed-version phase did not create a snapshot")
	}
	if got := machine(1).CompatibilityLevel(); got != kv.CompatibilityLevelBase {
		t.Fatalf("legacy compatibility level=%d, want base", got)
	}

	// A session command remains inactive while any legacy binary is present.
	blocked := c.propose(c.leader(), string(kv.EncodeRegister()))
	if _, err := kv.DecodeSession([]byte(blocked.payload)); !errors.Is(err, kv.ErrFeatureInactive) {
		t.Fatalf("pre-activation session result error=%v", err)
	}
	checkReplicas()

	// Move leadership while the cluster is mixed and confirm writes still
	// replicate through the old snapshot format.
	oldLeader := c.leader().id
	c.isolate(oldLeader)
	withoutQuorum, err := c.proposeAsync(c.procs[oldLeader], encodePut("rollout/no-quorum", "must-not-commit"), 250*time.Millisecond)
	if err != nil {
		t.Fatalf("start proposal without quorum: %v", err)
	}
	c.run(time.Second)
	if !withoutQuorum.done || withoutQuorum.err == nil {
		t.Fatalf("proposal without quorum result=%+v", withoutQuorum)
	}
	c.waitUntil("leader change during mixed-version phase", 30*time.Second, func() bool {
		return c.leader() != nil && c.leader().id != oldLeader
	})
	write("rollout/two", "leader-changed")
	c.healAll()
	checkReplicas()

	// Upgrade old binaries one at a time by restarting them with the new
	// implementation. Each process restores the existing on-disk snapshot.
	for _, id := range []uint64{1, 2} {
		c.crash(id, false, uint64(0x55504752+id))
		upgraded[id] = true
		c.restart(id)
		c.waitUntil(fmt.Sprintf("upgraded node %d to recover", id), 10*time.Second, func() bool {
			p := c.procs[id]
			return p.live() && p.node.Health().Ready
		})
		checkReplicas()
	}

	// All members now run the new implementation. Bind activation to the
	// committed configuration, then enable sessions and verify snapshot v2.
	leader = c.leader()
	conf := leader.node.Membership()
	digest := kv.MembershipDigest(conf.GetVoters(), conf.GetLearners(), conf.GetVotersOutgoing(), conf.GetLearnersNext(), conf.GetAutoLeave())
	activation, err := kv.EncodeActivateCompatibility(kv.CompatibilityLevelSessions, digest)
	if err != nil {
		t.Fatal(err)
	}
	activated := c.propose(leader, string(activation))
	if _, err := kv.DecodeResult([]byte(activated.payload)); err != nil {
		t.Fatalf("activate after rolling binary upgrade: %v", err)
	}
	checkReplicas()
	register := c.propose(c.leader(), string(kv.EncodeRegister()))
	if _, err := kv.DecodeSession([]byte(register.payload)); err != nil {
		t.Fatalf("session after activation: %v", err)
	}
	checkReplicas()
	for _, id := range c.ids {
		snapshot, err := machine(id).Snapshot()
		if err != nil || len(snapshot) == 0 || snapshot[0] != 2 {
			t.Fatalf("node %d activated snapshot format=%v err=%v", id, snapshot, err)
		}
	}
	activeLeader := c.leader().id
	c.isolate(activeLeader)
	withoutQuorum, err = c.proposeAsync(c.procs[activeLeader], encodePut("rollout/active-no-quorum", "must-not-commit"), 250*time.Millisecond)
	if err == nil {
		c.run(time.Second)
		if !withoutQuorum.done || withoutQuorum.err == nil {
			t.Fatalf("post-activation proposal without quorum result=%+v", withoutQuorum)
		}
	}
	c.healAll()
	c.waitUntil("leader change after post-activation quorum loss", 30*time.Second, func() bool {
		return c.leader() != nil && c.leader().id != activeLeader
	})
	checkReplicas()

	// A restarted node restores the active level, and the cluster can still
	// change membership after activation.
	c.crash(1, false, 0x41435449)
	c.restart(1)
	c.waitUntil("post-activation restart to recover", 10*time.Second, func() bool {
		return c.procs[1].live() && c.procs[1].node.Health().Ready
	})
	if got := machine(1).CompatibilityLevel(); got != kv.CompatibilityLevelSessions {
		t.Fatalf("restored active compatibility level=%d", got)
	}
	checkReplicas()

	leader = c.leader()
	removeID := uint64(3)
	if leader.id == removeID {
		removeID = 2
	}
	var removed bool
	if err := leader.node.RemoveMember(removeID, time.Second, func(err error) { removed = err == nil }); err != nil {
		t.Fatal(err)
	}
	c.waitUntil("post-upgrade membership change", 10*time.Second, func() bool { return removed })
	c.waitUntil("membership change to apply on every replica", 5*time.Second, func() bool {
		for _, id := range []uint64{1, 2, 3} {
			if !c.procs[id].live() || slices.Contains(c.procs[id].node.Membership().GetVoters(), removeID) {
				return false
			}
		}
		return true
	})
	write("rollout/after-membership", "active")
	c.waitUntil("remaining voters to apply post-membership write", 10*time.Second, func() bool {
		var want [32]byte
		first := true
		for _, id := range []uint64{1, 2, 3} {
			if id == removeID {
				continue
			}
			got := machine(id).Hash()
			if first {
				want, first = got, false
			} else if got != want {
				return false
			}
		}
		return true
	})
}

func TestMembershipReplacesVoterThroughLearnerCatchup(t *testing.T) {
	c := newCluster(t, 0x7265706c, 3, opts{snapEvery: 1, trailing: 1})
	leader := c.waitLeader(30 * time.Second)
	c.peers[4] = "n4"
	tr, err := sim.NewTransport("n4", c.net)
	if err != nil {
		t.Fatal(err)
	}
	joining := &proc{id: 4, name: "n4", mem: vfs.NewMem(), tr: tr, joining: true, bootstrapVoters: []uint64{1, 2, 3}}
	c.procs[4] = joining
	c.ids = append(c.ids, 4)
	c.installFaults(joining)
	c.boot(joining)

	var addDone bool
	if err := leader.node.AddLearner(4, time.Second, func(err error) { addDone = err == nil }); err != nil {
		t.Fatal(err)
	}
	c.waitUntil("learner addition to commit", 10*time.Second, func() bool { return addDone })
	c.waitUntil("new learner to catch up", 10*time.Second, func() bool { return joining.node.Applied() >= leader.node.Applied() })
	var promoteDone bool
	if err := leader.node.PromoteLearner(4, time.Second, func(err error) { promoteDone = err == nil }); err != nil {
		t.Fatal(err)
	}
	c.waitUntil("learner promotion to commit", 10*time.Second, func() bool { return promoteDone })
	if !slices.Equal(leader.node.Membership().GetVoters(), []uint64{1, 2, 3, 4}) {
		t.Fatalf("promoted voter set: %v", leader.node.Membership().GetVoters())
	}
	var removeDone bool
	if err := leader.node.RemoveMember(3, time.Second, func(err error) { removeDone = err == nil }); err != nil {
		t.Fatal(err)
	}
	c.waitUntil("old voter removal to commit", 10*time.Second, func() bool { return removeDone })
	c.waitUntil("remaining replicas to converge on replacement set", 10*time.Second, func() bool {
		for _, id := range []uint64{1, 2, 4} {
			if !slices.Equal(c.procs[id].node.Membership().GetVoters(), []uint64{1, 2, 4}) {
				return false
			}
		}
		return true
	})
	c.crash(1, false, 0x91)
	c.restart(1)
	c.waitUntil("restarted replica to recover replacement ConfState", 10*time.Second, func() bool {
		return slices.Equal(c.procs[1].node.Membership().GetVoters(), []uint64{1, 2, 4})
	})
}

func TestLearnerPromotionWithoutQuorumRemainsUncommitted(t *testing.T) {
	c := newCluster(t, 0x70726f6d6f7465, 3, opts{})
	leader := c.waitLeader(30 * time.Second)
	c.peers[4] = "n4"
	tr, err := sim.NewTransport("n4", c.net)
	if err != nil {
		t.Fatal(err)
	}
	joining := &proc{id: 4, name: "n4", mem: vfs.NewMem(), tr: tr, joining: true, bootstrapVoters: []uint64{1, 2, 3}}
	c.procs[4] = joining
	c.ids = append(c.ids, 4)
	c.installFaults(joining)
	c.boot(joining)

	var addDone bool
	if err := leader.node.AddLearner(4, time.Second, func(err error) { addDone = err == nil }); err != nil {
		t.Fatal(err)
	}
	c.waitUntil("learner addition to commit", 10*time.Second, func() bool { return addDone })
	c.waitUntil("new learner to catch up", 10*time.Second, func() bool { return joining.node.Applied() >= leader.node.Applied() })

	c.isolate(leader.id)
	var promoteDone bool
	var promoteErr error
	if err := leader.node.PromoteLearner(4, 250*time.Millisecond, func(err error) {
		promoteDone, promoteErr = true, err
	}); err != nil {
		t.Fatal(err)
	}
	c.run(time.Second)
	if !promoteDone || !errors.Is(promoteErr, raftnode.ErrResultUnknown) {
		t.Fatalf("promotion without quorum completed=%v err=%v, want ambiguous result", promoteDone, promoteErr)
	}
	if slices.Contains(leader.node.Membership().GetVoters(), 4) {
		t.Fatalf("isolated leader reported uncommitted learner as voter: %v", leader.node.Membership().GetVoters())
	}
	c.healAll()
	c.run(3 * time.Second)
}

func TestMembershipTimeoutIsAmbiguousAndLateResolutionDoesNotRepeat(t *testing.T) {
	c := newCluster(t, 0x74696d656f7574, 3, opts{})
	leader := c.waitLeader(30 * time.Second)
	var calls int
	var resultErr error
	c.isolate(leader.id)
	if err := leader.node.RemoveMember(3, time.Second, func(err error) { calls++; resultErr = err }); err != nil {
		t.Fatal(err)
	}
	if err := leader.node.RemoveMember(2, time.Second, func(error) {}); !errors.Is(err, raftnode.ErrConfChangePending) {
		t.Fatalf("unresolved change allowed overlap: %v", err)
	}
	c.run(1500 * time.Millisecond)
	if calls != 1 || !errors.Is(resultErr, raftnode.ErrResultUnknown) {
		t.Fatalf("calls=%d result=%v, want one ambiguous completion", calls, resultErr)
	}
	c.healAll()
	c.run(3 * time.Second)
	if calls != 1 {
		t.Fatalf("late configuration resolution invoked callback again: %d", calls)
	}
}

func TestConsistentBackupRestoresAndReplicaRejoins(t *testing.T) {
	c := newCluster(t, 0x6261636b, 3, opts{snapEvery: 100})
	leader := c.waitLeader(30 * time.Second)
	for _, command := range []string{"before-a", "before-b", "before-c"} {
		c.propose(leader, command)
	}
	var archive []byte
	var done bool
	var backupErr error
	if err := leader.node.Backup(time.Second, func(b []byte, err error) { archive, done, backupErr = b, true, err }); err != nil {
		t.Fatal(err)
	}
	c.waitUntil("consistent backup ReadIndex", 5*time.Second, func() bool { return done })
	if backupErr != nil || len(archive) == 0 {
		t.Fatalf("backup len=%d err=%v", len(archive), backupErr)
	}
	followerID := uint64(1)
	if followerID == leader.id {
		followerID = 2
	}
	if followerID == leader.id {
		followerID = 3
	}
	c.crash(followerID, false, 0)
	mem := vfs.NewMem()
	corrupt := append([]byte(nil), archive...)
	corrupt[len(corrupt)/2] ^= 1
	if err := raftnode.RestoreBackup(mem, "node", corrupt); err == nil {
		t.Fatal("corrupt backup was accepted")
	}
	if err := raftnode.RestoreBackup(mem, "node", archive); err != nil {
		t.Fatal(err)
	}
	if err := raftnode.RestoreBackup(mem, "node", archive); !errors.Is(err, raftnode.ErrRestoreTargetNotEmpty) {
		t.Fatalf("restore into non-empty target returned %v", err)
	}
	c.procs[followerID].mem = mem
	c.restart(followerID)
	c.waitConverged(10*time.Second, 3)
}

func TestFollowerRejectsProposalsWithALeaderHintAndTheHintedNodeAccepts(t *testing.T) {
	c := newCluster(t, 2, 3, opts{})
	l := c.waitLeader(30 * time.Second)
	var follower *proc
	for _, id := range c.ids {
		if id != l.id {
			follower = c.procs[id]
			break
		}
	}
	c.waitUntil("follower to learn the leader", 10*time.Second, func() bool { return follower.node.Leader() == l.id })

	_, err := c.proposeAsync(follower, "to-follower", time.Second)
	var nle *raftnode.NotLeaderError
	if !errors.Is(err, raftnode.ErrNotLeader) || !errors.As(err, &nle) || nle.Leader != l.id {
		t.Fatalf("got %v, want NotLeaderError pointing at node %d", err, l.id)
	}
	c.run(2 * time.Second)
	if len(follower.sm.cmds()) != 0 {
		t.Fatal("a rejected proposal was applied")
	}
	r := c.propose(c.procs[nle.Leader], "redirected")
	if r.err != nil || r.payload != "ok:redirected" {
		t.Fatalf("got %+v", r)
	}
	c.waitConverged(10*time.Second, 1)
}

func TestProposalWithNoKnownLeaderSaysSo(t *testing.T) {
	c := newCluster(t, 9, 3, opts{})
	p := c.procs[1]
	_, err := c.proposeAsync(p, "x", time.Second) // before any election
	var nle *raftnode.NotLeaderError
	if !errors.As(err, &nle) || nle.Leader != 0 {
		t.Fatalf("got %v, want NotLeaderError with no hint", err)
	}
}

func TestLeaderPowerLossFailsOverAndTheRestartedNodeCatchesUp(t *testing.T) {
	c := newCluster(t, 3, 3, opts{})
	old := c.waitLeader(30 * time.Second)
	for i := 0; i < 10; i++ {
		c.propose(old, fmt.Sprintf("a%d", i))
	}
	c.crash(old.id, true, 99)

	c.waitUntil("a new leader among the survivors", 30*time.Second, func() bool {
		l := c.leader()
		return l != nil && l.id != old.id
	})
	nl := c.leader()
	for i := 0; i < 10; i++ {
		if r := c.propose(nl, fmt.Sprintf("b%d", i)); r.err != nil {
			t.Fatalf("b%d: %v", i, r.err)
		}
	}
	c.restart(old.id)
	c.waitConverged(30*time.Second, 20)
	if got := c.procs[old.id].sm.cmds(); len(got) != 20 || got[0] != "a0" || got[19] != "b9" {
		t.Fatalf("restarted node has %v", got)
	}
}

func TestEveryNodePowerLossKeepsEveryAcknowledgedCommand(t *testing.T) {
	for _, torn := range []bool{false, true} {
		t.Run(fmt.Sprintf("torn=%v", torn), func(t *testing.T) {
			c := newCluster(t, 4, 3, opts{})
			l := c.waitLeader(30 * time.Second)
			var want []string
			for i := 0; i < 15; i++ {
				cmd := fmt.Sprintf("x%d", i)
				if r := c.propose(l, cmd); r.err != nil {
					t.Fatal(r.err)
				}
				want = append(want, cmd)
			}
			for _, id := range c.ids {
				c.crash(id, torn, uint64(id))
			}
			for _, id := range c.ids {
				c.restart(id)
			}
			nl := c.waitLeader(60 * time.Second)
			for i := 0; i < 5; i++ {
				cmd := fmt.Sprintf("y%d", i)
				if r := c.propose(nl, cmd); r.err != nil {
					t.Fatal(r.err)
				}
				want = append(want, cmd)
			}
			c.waitConverged(30*time.Second, 20)
			for id, got := range c.sequences() {
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("node %d has %v, want %v", id, got, want)
				}
			}
		})
	}
}

func TestIsolatedLeaderCannotCommitAndItsStaleEntriesAreOverwrittenAfterHealing(t *testing.T) {
	c := newCluster(t, 5, 3, opts{})
	old := c.waitLeader(30 * time.Second)
	c.propose(old, "before")
	c.waitConverged(10*time.Second, 1)

	c.isolate(old.id)
	// Proposed immediately, while the old leader still believes it leads: it
	// lands in its log but can never reach a quorum.
	stale, err := c.proposeAsync(old, "stale-write", 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	c.waitUntil("majority to elect a new leader", 30*time.Second, func() bool {
		l := c.leader()
		return l != nil && l.id != old.id
	})
	nl := c.leader()
	for i := 0; i < 5; i++ {
		if r := c.propose(nl, fmt.Sprintf("after%d", i)); r.err != nil {
			t.Fatal(r.err)
		}
	}
	c.waitUntil("the isolated leader's proposal to time out", 10*time.Second, func() bool { return stale.done })
	if !errors.Is(stale.err, raftnode.ErrResultUnknown) {
		t.Fatalf("a proposal that could not commit must report unknown, got %v", stale.err)
	}

	c.healAll()
	c.waitConverged(30*time.Second, 6)
	for id, got := range c.sequences() {
		if slices.Contains(got, "stale-write") {
			t.Fatalf("node %d applied an entry that was never committed: %v", id, got)
		}
		if got[0] != "before" {
			t.Fatalf("node %d: %v", id, got)
		}
	}
}

func TestLaggingFollowerIsBroughtUpToDateByASnapshot(t *testing.T) {
	c := newCluster(t, 6, 3, opts{snapEvery: 10, trailing: 3})
	l := c.waitLeader(30 * time.Second)
	var lagger *proc
	for _, id := range c.ids {
		if id != l.id {
			lagger = c.procs[id]
			break
		}
	}
	c.isolate(lagger.id)
	for i := 0; i < 40; i++ {
		if r := c.propose(l, fmt.Sprintf("s%02d", i)); r.err != nil {
			t.Fatal(r.err)
		}
	}
	if l.node.LastSnapshotIndex() < 10 {
		t.Fatalf("leader never snapshotted (last=%d)", l.node.LastSnapshotIndex())
	}
	restoresBefore := lagger.sm.restores

	c.healAll()
	c.waitConverged(30*time.Second, 40)
	if lagger.sm.restores <= restoresBefore {
		t.Fatalf("the follower caught up without a snapshot restore; the log should have been compacted past it")
	}
	// And it survives its own power loss from the snapshot it was sent.
	c.crash(lagger.id, true, 7)
	c.restart(lagger.id)
	c.waitConverged(30*time.Second, 40)
}

func TestProposalsAreRejectedOrReportedHonestlyWhenThereIsNoQuorum(t *testing.T) {
	c := newCluster(t, 7, 3, opts{})
	l := c.waitLeader(30 * time.Second)
	for _, id := range c.ids {
		if id != l.id {
			c.crash(id, false, 1)
		}
	}
	r, err := c.proposeAsync(l, "no-quorum", 2*time.Second)
	if err != nil {
		return // already stepped down: rejected outright, also honest
	}
	c.waitUntil("the proposal to resolve", 10*time.Second, func() bool { return r.done })
	if !errors.Is(r.err, raftnode.ErrResultUnknown) {
		t.Fatalf("got %v, want ErrResultUnknown", r.err)
	}
	if slices.Contains(l.sm.cmds(), "no-quorum") {
		t.Fatal("applied a command without a quorum")
	}
}

func TestStopCompletesPendingProposalsAndRejectsNewOnes(t *testing.T) {
	c := newCluster(t, 8, 3, opts{})
	l := c.waitLeader(30 * time.Second)
	c.isolate(l.id)
	r, err := c.proposeAsync(l, "pending", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.node.Stop(); err != nil {
		t.Fatal(err)
	}
	if !r.done || !errors.Is(r.err, raftnode.ErrResultUnknown) {
		t.Fatalf("pending proposal not completed with ErrResultUnknown: %+v", r)
	}
	if _, err := c.proposeAsync(l, "late", time.Second); !errors.Is(err, raftnode.ErrStopped) {
		t.Fatalf("err=%v", err)
	}
}

// forge sends a raft message from `via`'s transport carrying whatever From and
// To the test wants, as a buggy or hostile peer could.
func (c *cluster) forge(via, to uint64, m *pb.Message) {
	c.t.Helper()
	b, err := proto.Marshal(m)
	if err != nil {
		c.t.Fatal(err)
	}
	if err := c.procs[via].ep.Notify(c.peers[to], "raft", b); err != nil {
		c.t.Fatal(err)
	}
}

func TestMessagesFromTheWrongSenderOrForTheWrongNodeAreIgnored(t *testing.T) {
	c := newCluster(t, 12, 3, opts{})
	l := c.waitLeader(30 * time.Second)
	c.run(time.Second)
	target := c.procs[1]
	if l.id == 1 {
		target = c.procs[2]
	}
	before := target.node.Term()

	// Node 3's transport claims to be node 2 and announces a far-future term. If
	// believed, the target would adopt term 999 and the cluster would be disrupted.
	liar := uint64(3)
	claimed := uint64(2)
	if target.id == 2 {
		claimed = 1
	}
	if target.id == 3 {
		liar = 1
	}
	c.forge(liar, target.id, &pb.Message{Type: pb.MsgHeartbeat.Enum(), From: new(claimed), To: new(target.id), Term: new(uint64(999))})
	// A message addressed to some other member, delivered here by mistake.
	other := uint64(1)
	if target.id == 1 {
		other = 2
	}
	c.forge(liar, target.id, &pb.Message{Type: pb.MsgHeartbeat.Enum(), From: new(liar), To: new(other), Term: new(uint64(999))})
	c.run(500 * ms)

	if got := target.node.Term(); got != before {
		t.Fatalf("term moved from %d to %d: a forged or misdirected message was acted on", before, got)
	}
}
