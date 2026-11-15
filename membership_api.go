package continuum

import (
	"context"
	"errors"
	"sync"

	"github.com/souviks22/continuum-go/internal/raftnode"
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
)

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
	return n.runMembershipChange(ctx, func(cb func(error)) error {
		return n.raw.AddLearner(id, n.timeout, cb)
	})
}

// PromoteLearner makes a caught-up learner a voter. Raft serializes membership
// changes; a second unresolved change returns ErrMembershipChangePending. This
// call must be made on the leader.
func (n *Node) PromoteLearner(ctx context.Context, id uint64) error {
	return n.runMembershipChange(ctx, func(cb func(error)) error {
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
