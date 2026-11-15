package raftnode

import (
	"encoding/binary"
	"errors"
	"time"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

var (
	ErrConfChangePending = errors.New("raftnode: a membership change is unresolved")
	ErrMemberExists      = errors.New("raftnode: member already exists")
	ErrMemberNotFound    = errors.New("raftnode: member not found")
	ErrLastVoter         = errors.New("raftnode: cannot remove the last voter")
)

// AddLearner adds a non-voting replica. Configure its transport identity in
// Peers on every existing node and bootstrap the new node with the current
// voter IDs in InitialVoters before calling this method. Promote it only after
// it has caught up.
func (n *Node) AddLearner(id uint64, timeout time.Duration, cb func(error)) error {
	if id == 0 || n.cfg.Peers[id] == "" {
		return errors.New("raftnode: learner must have a configured transport identity")
	}
	if n.memberExists(id) {
		return ErrMemberExists
	}
	return n.proposeConfChange(raftpb.ConfChangeAddLearnerNode, id, timeout, cb)
}

// PromoteLearner changes a caught-up learner into a voter using Raft's
// one-change-at-a-time membership protocol.
func (n *Node) PromoteLearner(id uint64, timeout time.Duration, cb func(error)) error {
	if !containsID(n.confState.GetLearners(), id) {
		return ErrMemberNotFound
	}
	return n.proposeConfChange(raftpb.ConfChangeAddNode, id, timeout, cb)
}

// RemoveMember removes one learner or voter. Replace a voter by adding and
// catching up a learner, promoting it, then removing the old voter.
func (n *Node) RemoveMember(id uint64, timeout time.Duration, cb func(error)) error {
	if !n.memberExists(id) {
		return ErrMemberNotFound
	}
	if containsID(n.confState.GetVoters(), id) && len(n.confState.GetVoters()) == 1 {
		return ErrLastVoter
	}
	return n.proposeConfChange(raftpb.ConfChangeRemoveNode, id, timeout, cb)
}

func (n *Node) proposeConfChange(typ raftpb.ConfChangeType, id uint64, timeout time.Duration, cb func(error)) error {
	if n.stopped {
		return ErrStopped
	}
	if !n.IsLeader() {
		return &NotLeaderError{Leader: n.Leader()}
	}
	if timeout <= 0 {
		return errors.New("raftnode: membership timeout must be positive")
	}
	if cb == nil {
		return errors.New("raftnode: nil membership callback")
	}
	if n.confPending {
		return ErrConfChangePending
	}
	req := n.nextReq
	n.nextReq++
	context := encodeConfContext(n.cfg.ID, req)
	w := &waiter{cb: func(_ []byte, err error) { cb(err) }, started: n.cfg.Clock.Now()}
	w.timer = n.cfg.Clock.AfterFunc(timeout, func() {
		if got := n.confWaiters[req]; got == w {
			delete(n.confWaiters, req)
			n.observeRequest(EventProposalUnknown, req, n.confIndex, n.cfg.Clock.Now()-w.started, ErrResultUnknown)
			cb(ErrResultUnknown)
		}
	})
	n.confWaiters[req] = w
	n.confPending = true
	n.confIndex, _ = n.ms.LastIndex()
	n.confIndex++
	cc := &raftpb.ConfChangeV2{Changes: []*raftpb.ConfChangeSingle{{Type: &typ, NodeId: &id}}, Context: context}
	if err := n.rn.ProposeConfChange(cc); err != nil {
		w.timer.Stop()
		delete(n.confWaiters, req)
		n.confPending = false
		n.confIndex = 0
		return err
	}
	n.process()
	return nil
}

func (n *Node) resolveConfChange(context []byte, index uint64) {
	req, from, ok := decodeConfContext(context)
	if n.confPending && index < n.confIndex {
		return
	}
	n.confPending, n.confIndex = false, 0
	requestID := uint64(0)
	if ok {
		requestID = req
	}
	n.observeRequest(EventMembershipApplied, requestID, index, 0, nil)
	for id, w := range n.confWaiters {
		delete(n.confWaiters, id)
		w.timer.Stop()
		if ok && from == n.cfg.ID && req == id {
			w.cb(nil, nil)
		} else {
			w.cb(nil, ErrResultUnknown)
		}
	}
}

func (n *Node) invalidateConfChange() {
	if !n.confPending {
		return
	}
	for id, w := range n.confWaiters {
		delete(n.confWaiters, id)
		w.timer.Stop()
		w.cb(nil, ErrResultUnknown)
	}
	n.confPending, n.confIndex = false, 0
}

func encodeConfContext(from, req uint64) []byte {
	b := []byte{1}
	b = binary.AppendUvarint(b, from)
	return binary.AppendUvarint(b, req)
}
func decodeConfContext(b []byte) (uint64, uint64, bool) {
	if len(b) < 3 || b[0] != 1 {
		return 0, 0, false
	}
	from, n := binary.Uvarint(b[1:])
	if n <= 0 {
		return 0, 0, false
	}
	b = b[1+n:]
	req, n := binary.Uvarint(b)
	if n <= 0 || n != len(b) {
		return 0, 0, false
	}
	return req, from, true
}

func (n *Node) memberExists(id uint64) bool {
	return containsID(n.confState.GetVoters(), id) || containsID(n.confState.GetLearners(), id) || containsID(n.confState.GetVotersOutgoing(), id) || containsID(n.confState.GetLearnersNext(), id)
}
func containsID(ids []uint64, id uint64) bool {
	for _, got := range ids {
		if got == id {
			return true
		}
	}
	return false
}

func (n *Node) Membership() *raftpb.ConfState { return proto.Clone(n.confState).(*raftpb.ConfState) }
