package client

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/souviks22/continuum/internal/raftnode"
	"github.com/souviks22/continuum/internal/rpc"
)

const membershipMethod = "continuum.membership.v1"
const membershipStateMethod = "continuum.membership.state.v1"
const membershipStateRetry byte = 4

// MembershipSnapshot is a copied, applied Raft configuration view.
type MembershipSnapshot struct {
	Voters, Learners, VotersOutgoing, LearnersNext []uint64
	AutoLeave                                      bool
}

const (
	membershipAddLearner byte = iota + 1
	membershipPromoteLearner
	membershipRemove
)

type membershipNode interface {
	AddLearner(uint64, time.Duration, func(error)) error
	PromoteLearner(uint64, time.Duration, func(error)) error
	RemoveMember(uint64, time.Duration, func(error)) error
}

type membershipReadNode interface {
	LinearizableRead(time.Duration, func(error)) error
}

// RegisterMembershipStateRPC serves the leader's applied configuration only
// after a ReadIndex barrier confirms the state is current.
func RegisterMembershipStateRPC(endpoint *rpc.Endpoint, node membershipReadNode, state func() MembershipSnapshot, timeout time.Duration) error {
	if endpoint == nil || node == nil || state == nil || timeout <= 0 {
		return errors.New("client: RegisterMembershipStateRPC requires endpoint, node, state and positive timeout")
	}
	return endpoint.RegisterAsync(membershipStateMethod, func(_ string, request []byte, reply func([]byte, error)) {
		if len(request) != 1 || request[0] != 1 {
			reply(encodeMembershipResponse(proposalRejected, 0, "client: malformed membership state request"), nil)
			return
		}
		if err := node.LinearizableRead(timeout, func(err error) {
			if err != nil {
				var redirect *raftnode.NotLeaderError
				if errors.As(err, &redirect) {
					reply(encodeMembershipResponse(proposalRedirect, redirect.Leader, ""), nil)
					return
				}
				if membershipReadRetryable(err) {
					reply(encodeMembershipStateRetry(err), nil)
					return
				}
				reply(encodeMembershipResponse(proposalRejected, 0, err.Error()), nil)
				return
			}
			reply(encodeMembershipStateResponse(state()), nil)
		}); err != nil {
			var redirect *raftnode.NotLeaderError
			if errors.As(err, &redirect) {
				reply(encodeMembershipResponse(proposalRedirect, redirect.Leader, ""), nil)
				return
			}
			if membershipReadRetryable(err) {
				reply(encodeMembershipStateRetry(err), nil)
				return
			}
			reply(encodeMembershipResponse(proposalRejected, 0, err.Error()), nil)
		}
	})
}

// RegisterMembershipRPC exposes serialized membership changes over an RPC
// endpoint. The node's configuration must already contain transport identities
// for every member that may be added.
func RegisterMembershipRPC(endpoint *rpc.Endpoint, node membershipNode, timeout time.Duration) error {
	if endpoint == nil || node == nil || timeout <= 0 {
		return errors.New("client: RegisterMembershipRPC requires endpoint, node and positive timeout")
	}
	return endpoint.RegisterAsync(membershipMethod, func(_ string, request []byte, reply func([]byte, error)) {
		action, id, err := decodeMembershipRequest(request)
		if err != nil {
			reply(encodeMembershipResponse(proposalRejected, 0, err.Error()), nil)
			return
		}
		cb := func(err error) {
			if errors.Is(err, raftnode.ErrResultUnknown) {
				reply(encodeMembershipResponse(proposalUnknown, 0, ""), nil)
				return
			}
			if err != nil {
				reply(encodeMembershipResponse(proposalRejected, 0, err.Error()), nil)
				return
			}
			reply(encodeMembershipResponse(proposalOK, 0, ""), nil)
		}
		var callErr error
		switch action {
		case membershipAddLearner:
			callErr = node.AddLearner(id, timeout, cb)
		case membershipPromoteLearner:
			callErr = node.PromoteLearner(id, timeout, cb)
		case membershipRemove:
			callErr = node.RemoveMember(id, timeout, cb)
		default:
			callErr = errors.New("client: unsupported membership action")
		}
		if callErr == nil {
			return
		}
		var redirect *raftnode.NotLeaderError
		if errors.As(callErr, &redirect) {
			reply(encodeMembershipResponse(proposalRedirect, redirect.Leader, ""), nil)
			return
		}
		reply(encodeMembershipResponse(proposalRejected, 0, callErr.Error()), nil)
	})
}

// MembershipClient routes membership administration to the current leader.
// Operations are serialized per client because the Raft membership protocol
// allows only one unresolved configuration change at a time.
type MembershipClient struct {
	peers   []*RemotePeer
	timeout time.Duration
	busy    bool
}

func NewMembershipClient(peers []*RemotePeer, timeout time.Duration) (*MembershipClient, error) {
	if len(peers) == 0 {
		return nil, ErrNoPeers
	}
	if timeout <= 0 {
		return nil, errors.New("client: membership timeout must be positive")
	}
	ordered := append([]*RemotePeer(nil), peers...)
	for _, p := range ordered {
		if p == nil {
			return nil, errors.New("client: nil membership peer")
		}
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].ID() < ordered[j].ID() })
	for i := 1; i < len(ordered); i++ {
		if ordered[i-1].ID() == ordered[i].ID() {
			return nil, errors.New("client: duplicate membership peer ID")
		}
	}
	return &MembershipClient{peers: ordered, timeout: timeout}, nil
}

func (m *MembershipClient) AddLearner(id uint64, cb func(error)) error {
	return m.change(membershipAddLearner, id, cb)
}
func (m *MembershipClient) PromoteLearner(id uint64, cb func(error)) error {
	return m.change(membershipPromoteLearner, id, cb)
}
func (m *MembershipClient) RemoveMember(id uint64, cb func(error)) error {
	return m.change(membershipRemove, id, cb)
}

// State reads the applied membership from a peer after its ReadIndex barrier.
// Leader redirects update the peer hints and are followed within this client.
func (m *MembershipClient) State(cb func(MembershipSnapshot, error)) error {
	if cb == nil {
		return errors.New("client: nil membership callback")
	}
	attempted := make(map[uint64]bool, len(m.peers))
	var route func(uint64) error
	var attempt func(*RemotePeer) error
	finish := func(state MembershipSnapshot, err error) { cb(state, err) }
	attempt = func(p *RemotePeer) error {
		attempted[p.ID()] = true
		return p.membershipState(m.timeout, func(leader uint64, state MembershipSnapshot, err error) {
			var redirect *raftnode.NotLeaderError
			if errors.As(err, &redirect) {
				if redirect.Leader != 0 {
					p.leader = redirect.Leader
				}
				if nextErr := route(redirect.Leader); nextErr != nil {
					finish(MembershipSnapshot{}, nextErr)
				}
				return
			}
			if err == nil {
				p.leader = p.ID()
				finish(state, nil)
				return
			}
			if errors.Is(err, raftnode.ErrReadTimeout) || errors.Is(err, raftnode.ErrStopped) || errors.Is(err, raftnode.ErrLeaderNotReady) {
				if nextErr := route(leader); nextErr != nil {
					if errors.Is(nextErr, raftnode.ErrNotLeader) {
						finish(MembershipSnapshot{}, err)
						return
					}
					finish(MembershipSnapshot{}, nextErr)
				}
				return
			}
			finish(MembershipSnapshot{}, err)
		})
	}
	route = func(hint uint64) error {
		var lastErr error
		if hint != 0 && !attempted[hint] {
			for _, p := range m.peers {
				if p.ID() == hint {
					if err := attempt(p); err == nil {
						return nil
					} else {
						lastErr = err
					}
					break
				}
			}
		}
		for _, p := range m.peers {
			if !attempted[p.ID()] {
				if err := attempt(p); err == nil {
					return nil
				} else {
					lastErr = err
				}
			}
		}
		if lastErr != nil {
			return lastErr
		}
		return &raftnode.NotLeaderError{}
	}
	var hint uint64
	for _, p := range m.peers {
		if p.IsLeader() {
			hint = p.ID()
			break
		}
		if hint == 0 && p.Leader() != 0 {
			hint = p.Leader()
		}
	}
	return route(hint)
}

func (m *MembershipClient) change(action byte, id uint64, cb func(error)) error {
	if m.busy {
		return ErrBusy
	}
	if id == 0 {
		return errors.New("client: membership ID must be non-zero")
	}
	if cb == nil {
		return errors.New("client: nil membership callback")
	}
	m.busy = true
	attempted := make(map[uint64]bool, len(m.peers))
	done := false
	finish := func(err error) {
		if done {
			return
		}
		done = true
		m.busy = false
		cb(err)
	}
	var route func(uint64) error
	var attempt func(*RemotePeer) error
	attempt = func(p *RemotePeer) error {
		attempted[p.ID()] = true
		return p.changeMembership(action, id, m.timeout, func(_ uint64, err error) {
			if done {
				return
			}
			var redirect *raftnode.NotLeaderError
			if errors.As(err, &redirect) {
				if redirect.Leader != 0 {
					p.leader = redirect.Leader
				}
				if e := route(redirect.Leader); e != nil {
					finish(e)
				}
				return
			}
			if err == nil {
				p.leader = p.ID()
			}
			finish(err)
		})
	}
	route = func(hint uint64) error {
		var lastErr error
		if hint != 0 && !attempted[hint] {
			for _, p := range m.peers {
				if p.ID() == hint {
					err := attempt(p)
					if err == nil {
						return nil
					}
					lastErr = err
					break
				}
			}
		}
		for _, p := range m.peers {
			if attempted[p.ID()] {
				continue
			}
			err := attempt(p)
			if err == nil {
				return nil
			}
			lastErr = err
		}
		if lastErr != nil {
			return lastErr
		}
		return &raftnode.NotLeaderError{Leader: hint}
	}
	var hint uint64
	for _, p := range m.peers {
		if p.IsLeader() {
			hint = p.ID()
			if err := attempt(p); err == nil {
				return nil
			}
			// A synchronous send failure means this peer did not accept the RPC;
			// let the normal candidate route try the other configured peers.
			attempted[p.ID()] = false
			break
		}
	}
	for _, p := range m.peers {
		if p.Leader() != 0 {
			hint = p.Leader()
			break
		}
	}
	if err := route(hint); err != nil {
		m.busy = false
		return err
	}
	return nil
}

func (p *RemotePeer) changeMembership(action byte, id uint64, timeout time.Duration, cb func(uint64, error)) error {
	if timeout <= 0 {
		timeout = p.timeout
	}
	return p.endpoint.Call(p.name, membershipMethod, encodeMembershipRequest(action, id), timeout, func(payload []byte, err error) {
		if err != nil {
			if errors.Is(err, rpc.ErrTimeout) {
				err = raftnode.ErrResultUnknown
			}
			cb(0, err)
			return
		}
		status, leader, message, err := decodeMembershipResponse(payload)
		if err != nil {
			cb(0, err)
			return
		}
		switch status {
		case proposalOK:
			cb(0, nil)
		case proposalRedirect:
			cb(leader, &raftnode.NotLeaderError{Leader: leader})
		case proposalUnknown:
			cb(0, raftnode.ErrResultUnknown)
		case proposalRejected:
			cb(0, fmt.Errorf("%w: %s", ErrProposalRejected, message))
		default:
			cb(0, errors.New("client: unsupported membership response"))
		}
	})
}

func (p *RemotePeer) membershipState(timeout time.Duration, cb func(uint64, MembershipSnapshot, error)) error {
	if timeout <= 0 {
		timeout = p.timeout
	}
	return p.endpoint.Call(p.name, membershipStateMethod, []byte{1}, timeout, func(payload []byte, err error) {
		if err != nil {
			if errors.Is(err, rpc.ErrTimeout) {
				err = raftnode.ErrReadTimeout
			}
			cb(0, MembershipSnapshot{}, err)
			return
		}
		state, leader, message, err := decodeMembershipStateResponse(payload)
		if err != nil {
			var redirect *raftnode.NotLeaderError
			if errors.As(err, &redirect) {
				cb(leader, MembershipSnapshot{}, err)
				return
			}
			cb(0, MembershipSnapshot{}, err)
			return
		}
		if message != "" {
			cb(0, MembershipSnapshot{}, fmt.Errorf("%w: %s", ErrProposalRejected, message))
			return
		}
		cb(leader, state, nil)
	})
}

func membershipReadRetryable(err error) bool {
	return errors.Is(err, raftnode.ErrReadTimeout) || errors.Is(err, raftnode.ErrStopped) || errors.Is(err, raftnode.ErrLeaderNotReady)
}

func encodeMembershipStateRetry(err error) []byte {
	b := []byte{1, membershipStateRetry}
	return appendBytes(b, []byte(err.Error()))
}

func encodeMembershipRequest(action byte, id uint64) []byte {
	b := []byte{1, action}
	return binary.AppendUvarint(b, id)
}
func decodeMembershipRequest(b []byte) (byte, uint64, error) {
	if len(b) < 3 || b[0] != 1 {
		return 0, 0, errors.New("client: malformed membership request")
	}
	id, n := binary.Uvarint(b[2:])
	if n <= 0 || n != len(b)-2 || id == 0 {
		return 0, 0, errors.New("client: malformed membership request")
	}
	return b[1], id, nil
}
func encodeMembershipResponse(status byte, leader uint64, message string) []byte {
	b := []byte{1, status}
	switch status {
	case proposalRedirect:
		b = binary.AppendUvarint(b, leader)
	case proposalRejected:
		b = appendBytes(b, []byte(message))
	}
	return b
}
func decodeMembershipResponse(b []byte) (byte, uint64, string, error) {
	bad := func() (byte, uint64, string, error) {
		return 0, 0, "", errors.New("client: malformed membership response")
	}
	if len(b) < 2 || b[0] != 1 {
		return bad()
	}
	status := b[1]
	b = b[2:]
	switch status {
	case proposalOK, proposalUnknown:
		if len(b) != 0 {
			return bad()
		}
		return status, 0, "", nil
	case proposalRedirect:
		leader, rest, e := takeVarint(b)
		if e != nil || len(rest) != 0 {
			return bad()
		}
		return status, leader, "", nil
	case proposalRejected:
		message, rest, e := takeBytes(b)
		if e != nil || len(rest) != 0 {
			return bad()
		}
		return status, 0, string(message), nil
	default:
		return bad()
	}
}

const maxMembershipEntries = 1 << 16

func encodeMembershipStateResponse(s MembershipSnapshot) []byte {
	b := []byte{1, proposalOK}
	for _, members := range [][]uint64{s.Voters, s.Learners, s.VotersOutgoing, s.LearnersNext} {
		b = binary.AppendUvarint(b, uint64(len(members)))
		for _, id := range members {
			b = binary.AppendUvarint(b, id)
		}
	}
	if s.AutoLeave {
		return append(b, 1)
	}
	return append(b, 0)
}

func decodeMembershipStateResponse(b []byte) (MembershipSnapshot, uint64, string, error) {
	bad := func() (MembershipSnapshot, uint64, string, error) {
		return MembershipSnapshot{}, 0, "", errors.New("client: malformed membership state response")
	}
	if len(b) < 2 || b[0] != 1 {
		return bad()
	}
	status := b[1]
	b = b[2:]
	switch status {
	case membershipStateRetry:
		message, rest, err := takeBytes(b)
		if err != nil || len(rest) != 0 {
			return bad()
		}
		switch string(message) {
		case raftnode.ErrReadTimeout.Error():
			return MembershipSnapshot{}, 0, "", raftnode.ErrReadTimeout
		case raftnode.ErrStopped.Error():
			return MembershipSnapshot{}, 0, "", raftnode.ErrStopped
		case raftnode.ErrLeaderNotReady.Error():
			return MembershipSnapshot{}, 0, "", raftnode.ErrLeaderNotReady
		default:
			return bad()
		}
	case proposalRedirect:
		leader, rest, err := takeVarint(b)
		if err != nil || len(rest) != 0 {
			return bad()
		}
		return MembershipSnapshot{}, leader, "", &raftnode.NotLeaderError{Leader: leader}
	case proposalRejected:
		message, rest, err := takeBytes(b)
		if err != nil || len(rest) != 0 {
			return bad()
		}
		return MembershipSnapshot{}, 0, string(message), nil
	case proposalOK:
	default:
		return bad()
	}
	var state MembershipSnapshot
	sets := []*[]uint64{&state.Voters, &state.Learners, &state.VotersOutgoing, &state.LearnersNext}
	for _, set := range sets {
		count, rest, err := takeVarint(b)
		if err != nil || count > maxMembershipEntries {
			return bad()
		}
		b = rest
		members := make([]uint64, 0, int(count))
		for i := uint64(0); i < count; i++ {
			id, rest, err := takeVarint(b)
			if err != nil || id == 0 {
				return bad()
			}
			members = append(members, id)
			b = rest
		}
		*set = members
	}
	if len(b) != 1 || b[0] > 1 {
		return bad()
	}
	state.AutoLeave = b[0] == 1
	return state, 0, "", nil
}
