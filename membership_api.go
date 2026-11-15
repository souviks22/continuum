package continuum

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	internalClient "github.com/souviks22/continuum/internal/client"
	"github.com/souviks22/continuum/internal/kv"
	"github.com/souviks22/continuum/internal/raftnode"
	"github.com/souviks22/continuum/internal/rpc"
)

// MembershipState is the node's locally applied Raft membership. Slices are
// independent copies and may be retained or modified by the caller.
type MembershipState struct {
	Voters         []uint64
	Learners       []uint64
	VotersOutgoing []uint64
	LearnersNext   []uint64
	AutoLeave      bool
}

var (
	ErrMembershipChangePending = raftnode.ErrConfChangePending
	ErrMemberExists            = raftnode.ErrMemberExists
	ErrMemberNotFound          = raftnode.ErrMemberNotFound
	ErrLastVoter               = raftnode.ErrLastVoter
	// ErrIncompatibleMember means a candidate's support cannot be verified for
	// the cluster's active command and snapshot formats.
	ErrIncompatibleMember = errors.New("continuum: member does not support the active compatibility level")
)

type compatibilityMembershipNode struct {
	raw      *raftnode.Node
	endpoint *rpc.Endpoint
	peers    map[uint64]string
	machine  *kv.StateMachine
	timeout  time.Duration
}

func newCompatibilityMembershipNode(raw *raftnode.Node, endpoint *rpc.Endpoint, peers map[uint64]Peer, machine *kv.StateMachine, timeout time.Duration) *compatibilityMembershipNode {
	names := make(map[uint64]string, len(peers))
	for id, peer := range peers {
		names[id] = peer.Name
	}
	return &compatibilityMembershipNode{raw: raw, endpoint: endpoint, peers: names, machine: machine, timeout: timeout}
}

func (n *compatibilityMembershipNode) AddLearner(id uint64, timeout time.Duration, cb func(error)) error {
	return n.checkThenChange(id, timeout, cb, func(done func(error)) error { return n.raw.AddLearner(id, timeout, done) })
}

func (n *compatibilityMembershipNode) PromoteLearner(id uint64, timeout time.Duration, cb func(error)) error {
	return n.checkThenChange(id, timeout, cb, func(done func(error)) error { return n.raw.PromoteLearner(id, timeout, done) })
}

func (n *compatibilityMembershipNode) RemoveMember(id uint64, timeout time.Duration, cb func(error)) error {
	return n.raw.RemoveMember(id, timeout, cb)
}

func (n *compatibilityMembershipNode) checkThenChange(id uint64, timeout time.Duration, cb func(error), change func(func(error)) error) error {
	name := n.peers[id]
	if name == "" {
		return fmt.Errorf("%w: member %d has no configured compatibility identity", ErrIncompatibleMember, id)
	}
	if timeout <= 0 {
		timeout = n.timeout
	}
	return internalClient.QueryCompatibility(n.endpoint, name, timeout, func(report internalClient.Compatibility, err error) {
		if err != nil {
			cb(fmt.Errorf("%w: cannot verify member %d: %v", ErrIncompatibleMember, id, err))
			return
		}
		if err := n.checkReport(report); err != nil {
			cb(fmt.Errorf("%w: member %d: %v", ErrIncompatibleMember, id, err))
			return
		}
		if err := change(cb); err != nil {
			cb(err)
		}
	})
}

func (n *compatibilityMembershipNode) checkReport(report internalClient.Compatibility) error {
	command, snapshotWrite, maxKind, _, active, snapshotRead := n.machine.CompatibilityFormats()
	if report.CompatibilityReportVersion < 3 {
		return errors.New("compatibility report is missing or too old")
	}
	if report.RPCWireVersion != rpc.WireVersion || report.KVCommandVersion != command {
		return errors.New("RPC or KV command format differs")
	}
	if report.SupportedCompatibilityLevel < active {
		return fmt.Errorf("supports level %d; active cluster requires %d", report.SupportedCompatibilityLevel, active)
	}
	if report.MaxKVCommandKind < maxKind {
		return fmt.Errorf("supports command kind %d; cluster requires %d", report.MaxKVCommandKind, maxKind)
	}
	if !containsVersion(report.KVSnapshotReadVersions, snapshotWrite) {
		return fmt.Errorf("cannot read active snapshot format %d", snapshotWrite)
	}
	if !containsVersion(snapshotRead, report.KVSnapshotWriteVersion) {
		return fmt.Errorf("cluster cannot read member snapshot format %d", report.KVSnapshotWriteVersion)
	}
	return nil
}

func containsVersion(versions []uint8, want uint8) bool {
	for _, version := range versions {
		if version == want {
			return true
		}
	}
	return false
}

func (n *Node) compatibilityReport(ctx context.Context, id uint64) (internalClient.Compatibility, error) {
	if ctx == nil {
		return internalClient.Compatibility{}, errors.New("continuum: nil context")
	}
	name := n.peerNames[id]
	if name == "" || n.endpoint == nil || n.kvMachine == nil {
		return internalClient.Compatibility{}, fmt.Errorf("%w: member %d has no configured compatibility identity", ErrIncompatibleMember, id)
	}
	type outcome struct {
		report internalClient.Compatibility
		err    error
	}
	done := make(chan outcome, 1)
	if err := internalClient.QueryCompatibility(n.endpoint, name, n.timeout, func(report internalClient.Compatibility, err error) {
		done <- outcome{report: report, err: err}
	}); err != nil {
		return internalClient.Compatibility{}, err
	}
	select {
	case result := <-done:
		if result.err != nil {
			return internalClient.Compatibility{}, fmt.Errorf("%w: cannot verify member %d: %v", ErrIncompatibleMember, id, result.err)
		}
		return result.report, nil
	case <-ctx.Done():
		return internalClient.Compatibility{}, ctx.Err()
	}
}

// Membership returns this node's locally applied membership. A follower can
// briefly lag a committed change; callers needing current cluster membership
// should read from a ready leader or retry after observing the change.
func (n *Node) Membership(ctx context.Context) (MembershipState, error) {
	if ctx == nil {
		return MembershipState{}, errors.New("continuum: nil context")
	}
	if n.closing.Load() {
		return MembershipState{}, ErrStopped
	}
	type outcome struct {
		state MembershipState
		err   error
	}
	done := make(chan outcome, 1)
	if err := n.loop.Post(ctx, func() {
		if err := ctx.Err(); err != nil {
			done <- outcome{err: err}
			return
		}
		if n.raw == nil {
			done <- outcome{err: ErrStopped}
			return
		}
		cs := n.raw.Membership()
		done <- outcome{state: MembershipState{
			Voters:         append([]uint64(nil), cs.GetVoters()...),
			Learners:       append([]uint64(nil), cs.GetLearners()...),
			VotersOutgoing: append([]uint64(nil), cs.GetVotersOutgoing()...),
			LearnersNext:   append([]uint64(nil), cs.GetLearnersNext()...),
			AutoLeave:      cs.GetAutoLeave(),
		}}
	}); err != nil {
		return MembershipState{}, err
	}
	select {
	case out := <-done:
		return out.state, out.err
	case <-ctx.Done():
		return MembershipState{}, ctx.Err()
	}
}

// AddLearner adds a non-voting member. Its transport identity must already be
// present in Config.Peers on every current member. Bootstrap the new process
// with BootstrapLearner and the current InitialVoters before calling this.
// Promote it only after Membership shows it has caught up. This call must be
// made on the leader; a follower returns *NotLeaderError with its leader hint.
func (n *Node) AddLearner(ctx context.Context, id uint64) error {
	var report internalClient.Compatibility
	var err error
	if n.kvMachine != nil {
		report, err = n.compatibilityReport(ctx, id)
		if err != nil {
			return err
		}
	}
	return n.runMembershipChange(ctx, func(cb func(error)) error {
		if n.kvMachine != nil {
			guard := compatibilityMembershipNode{machine: n.kvMachine.machine}
			if err := guard.checkReport(report); err != nil {
				return fmt.Errorf("%w: member %d: %v", ErrIncompatibleMember, id, err)
			}
		}
		return n.raw.AddLearner(id, n.timeout, cb)
	})
}

// PromoteLearner makes a caught-up learner a voter. Raft serializes membership
// changes; a second unresolved change returns ErrMembershipChangePending. This
// call must be made on the leader.
func (n *Node) PromoteLearner(ctx context.Context, id uint64) error {
	var report internalClient.Compatibility
	var err error
	if n.kvMachine != nil {
		report, err = n.compatibilityReport(ctx, id)
		if err != nil {
			return err
		}
	}
	return n.runMembershipChange(ctx, func(cb func(error)) error {
		if n.kvMachine != nil {
			guard := compatibilityMembershipNode{machine: n.kvMachine.machine}
			if err := guard.checkReport(report); err != nil {
				return fmt.Errorf("%w: member %d: %v", ErrIncompatibleMember, id, err)
			}
		}
		return n.raw.PromoteLearner(id, n.timeout, cb)
	})
}

// RemoveMember removes a learner or voter. To replace a voter, add and catch up
// a learner, promote it, then remove the old voter. Removing the last voter is
// rejected. If the context expires after proposal acceptance, ErrResultUnknown
// means the change may still commit. This call must be made on the leader.
func (n *Node) RemoveMember(ctx context.Context, id uint64) error {
	return n.runMembershipChange(ctx, func(cb func(error)) error {
		return n.raw.RemoveMember(id, n.timeout, cb)
	})
}

type membershipChangeState struct {
	sync.Mutex
	cancelled bool
	accepted  bool
}

func (n *Node) runMembershipChange(ctx context.Context, start func(func(error)) error) error {
	if ctx == nil {
		return errors.New("continuum: nil context")
	}
	if n.closing.Load() {
		return ErrStopped
	}
	if n.raw == nil {
		return ErrStopped
	}
	state := &membershipChangeState{}
	started := make(chan error, 1)
	result := make(chan error, 1)
	if err := n.loop.Post(ctx, func() {
		state.Lock()
		if state.cancelled || ctx.Err() != nil || n.closing.Load() {
			state.Unlock()
			err := ctx.Err()
			if err == nil {
				err = ErrStopped
			}
			started <- err
			return
		}
		err := start(func(err error) { result <- err })
		state.accepted = err == nil
		state.Unlock()
		started <- err
	}); err != nil {
		return err
	}
	select {
	case err := <-started:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		state.Lock()
		if state.accepted {
			state.Unlock()
			select {
			case err := <-result:
				return err
			default:
				return unknownContext(ctx.Err())
			}
		}
		state.cancelled = true
		state.Unlock()
		select {
		case err := <-started:
			if err == nil {
				return unknownContext(ctx.Err())
			}
			return err
		default:
			return ctx.Err()
		}
	}
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		select {
		case err := <-result:
			return err
		default:
			return unknownContext(ctx.Err())
		}
	}
}
