package client

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/souviks22/continuum/internal/kv"
	"github.com/souviks22/continuum/internal/raftnode"
	"github.com/souviks22/continuum/internal/rpc"
)

const proposalMethod = "continuum.propose.v1"
const rangeMethod = "continuum.range.v1"

const (
	proposalOK byte = iota
	proposalRedirect
	proposalUnknown
	proposalRejected
)

type proposalNode interface {
	Propose([]byte, time.Duration, func([]byte, error)) error
}

type readNode interface {
	LinearizableRead(time.Duration, func(error)) error
}

// RegisterRangeRPC exposes a linearizable range read on this node. The store
// must be the state machine advanced by node on the same serial context.
func RegisterRangeRPC(endpoint *rpc.Endpoint, node readNode, store *kv.Store, timeout time.Duration) error {
	if endpoint == nil || node == nil || store == nil || timeout <= 0 {
		return errors.New("client: RegisterRangeRPC requires endpoint, node, store and positive timeout")
	}
	return endpoint.RegisterAsync(rangeMethod, func(_ string, request []byte, reply func([]byte, error)) {
		key, end, opts, err := decodeRangeRequest(request)
		if err != nil {
			reply(encodeRangeResponse(kv.RangeResult{}, err), nil)
			return
		}
		if err := node.LinearizableRead(timeout, func(err error) {
			if err == nil {
				var result kv.RangeResult
				result, err = store.Range(key, end, opts)
				reply(encodeRangeResponse(result, err), nil)
				return
			}
			reply(encodeRangeResponse(kv.RangeResult{}, err), nil)
		}); err != nil {
			reply(encodeRangeResponse(kv.RangeResult{}, err), nil)
		}
	})
}

// RegisterNodeRPC exposes committed proposals over an RPC endpoint. The node
// can be a local raftnode.Node; its callbacks must run on the endpoint's serial
// context. timeout bounds how long the node waits for a proposal result.
func RegisterNodeRPC(endpoint *rpc.Endpoint, node proposalNode, timeout time.Duration) error {
	if endpoint == nil || node == nil || timeout <= 0 {
		return errors.New("client: RegisterNodeRPC requires endpoint, node and positive timeout")
	}
	return endpoint.RegisterAsync(proposalMethod, func(_ string, command []byte, reply func([]byte, error)) {
		if err := node.Propose(command, timeout, func(result []byte, err error) {
			if errors.Is(err, raftnode.ErrResultUnknown) {
				reply(encodeProposalResponse(proposalUnknown, 0, nil, ""), nil)
				return
			}
			if err != nil {
				reply(encodeProposalResponse(proposalRejected, 0, nil, err.Error()), nil)
				return
			}
			reply(encodeProposalResponse(proposalOK, 0, result, ""), nil)
		}); err != nil {
			var notLeader *raftnode.NotLeaderError
			if errors.As(err, &notLeader) {
				reply(encodeProposalResponse(proposalRedirect, notLeader.Leader, nil, ""), nil)
				return
			}
			reply(encodeProposalResponse(proposalRejected, 0, nil, err.Error()), nil)
		}
	})
}

// RemotePeer adapts one network node to Client's proposal interface. Clients
// should construct one RemotePeer for each reachable Raft node and use a
// shared rpc.Endpoint. Leader is learned from redirection or accepted writes.
type RemotePeer struct {
	id       uint64
	name     string
	endpoint *rpc.Endpoint
	timeout  time.Duration
	leader   uint64
}

func NewRemotePeer(id uint64, name string, endpoint *rpc.Endpoint, timeout time.Duration) (*RemotePeer, error) {
	if id == 0 || name == "" || endpoint == nil || timeout <= 0 {
		return nil, errors.New("client: remote peer requires non-zero ID, name, endpoint and positive timeout")
	}
	return &RemotePeer{id: id, name: name, endpoint: endpoint, timeout: timeout}, nil
}

func (p *RemotePeer) ID() uint64     { return p.id }
func (p *RemotePeer) Leader() uint64 { return p.leader }
func (p *RemotePeer) IsLeader() bool { return p.leader == p.id }

func (p *RemotePeer) Propose(command []byte, timeout time.Duration, cb func([]byte, error)) error {
	if cb == nil {
		return errors.New("client: remote proposal callback is nil")
	}
	if timeout <= 0 {
		timeout = p.timeout
	}
	return p.endpoint.Call(p.name, proposalMethod, command, timeout, func(payload []byte, err error) {
		if err != nil {
			if errors.Is(err, rpc.ErrTimeout) {
				err = raftnode.ErrResultUnknown
			}
			cb(nil, err)
			return
		}
		status, leader, result, message, err := decodeProposalResponse(payload)
		if err != nil {
			cb(nil, err)
			return
		}
		switch status {
		case proposalOK:
			p.leader = p.id
			cb(result, nil)
		case proposalRedirect:
			p.leader = leader
			cb(nil, &raftnode.NotLeaderError{Leader: leader})
		case proposalUnknown:
			cb(nil, raftnode.ErrResultUnknown)
		case proposalRejected:
			cb(nil, fmt.Errorf("%w: %s", ErrProposalRejected, message))
		default:
			cb(nil, errors.New("client: unsupported remote proposal response"))
		}
	})
}

func (p *RemotePeer) remoteRange(key, end []byte, opts kv.RangeOptions, timeout time.Duration, cb func(kv.RangeResult, error)) error {
	if cb == nil {
		return errors.New("client: remote range callback is nil")
	}
	if timeout <= 0 {
		timeout = p.timeout
	}
	return p.endpoint.Call(p.name, rangeMethod, encodeRangeRequest(key, end, opts), timeout, func(payload []byte, err error) {
		if err != nil {
			if errors.Is(err, rpc.ErrTimeout) {
				err = raftnode.ErrReadTimeout
			}
			cb(kv.RangeResult{}, err)
			return
		}
		result, err := decodeRangeResponse(payload)
		cb(result, err)
	})
}

func (p *RemotePeer) RangeAsync(key, end []byte, opts kv.RangeOptions, timeout time.Duration, cb func(kv.RangeResult, error)) error {
	return p.remoteRange(key, end, opts, timeout, cb)
}

func encodeRangeRequest(key, end []byte, o kv.RangeOptions) []byte {
	b := []byte{1, boolByte(o.KeysOnly), boolByte(o.CountOnly)}
	b = binary.AppendUvarint(b, o.Limit)
	b = binary.AppendUvarint(b, o.Revision)
	b = appendBytes(b, key)
	return appendBytes(b, end)
}

func decodeRangeRequest(b []byte) ([]byte, []byte, kv.RangeOptions, error) {
	bad := func() ([]byte, []byte, kv.RangeOptions, error) {
		return nil, nil, kv.RangeOptions{}, errors.New("client: malformed range request")
	}
	if len(b) < 3 || b[0] != 1 || b[1] > 1 || b[2] > 1 {
		return bad()
	}
	o := kv.RangeOptions{KeysOnly: b[1] == 1, CountOnly: b[2] == 1}
	b = b[3:]
	var err error
	if o.Limit, b, err = takeVarint(b); err != nil {
		return bad()
	}
	if o.Revision, b, err = takeVarint(b); err != nil {
		return bad()
	}
	key, b, err := takeBytes(b)
	if err != nil {
		return bad()
	}
	end, b, err := takeBytes(b)
	if err != nil || len(b) != 0 || len(key) > kv.MaxKeySize || len(end) > kv.MaxKeySize {
		return bad()
	}
	return key, end, o, nil
}

func encodeRangeResponse(result kv.RangeResult, err error) []byte {
	b := []byte{1}
	if err != nil {
		code := byte(0)
		switch {
		case errors.Is(err, raftnode.ErrReadTimeout):
			code = 1
		case errors.Is(err, raftnode.ErrStopped):
			code = 2
		case errors.Is(err, kv.ErrCompacted):
			code = 3
		case errors.Is(err, kv.ErrFutureRevision):
			code = 4
		}
		b = append(b, 1, code)
		return appendBytes(b, []byte(err.Error()))
	}
	b = append(b, 0, boolByte(result.More))
	b = binary.AppendUvarint(b, result.Count)
	b = binary.AppendUvarint(b, result.Revision)
	b = binary.AppendUvarint(b, uint64(len(result.KVs)))
	for _, item := range result.KVs {
		b = appendBytes(b, item.Key)
		b = appendBytes(b, item.Value)
		b = binary.AppendUvarint(b, item.CreateRevision)
		b = binary.AppendUvarint(b, item.ModRevision)
		b = binary.AppendUvarint(b, item.Version)
	}
	return b
}

func decodeRangeResponse(b []byte) (kv.RangeResult, error) {
	bad := func() (kv.RangeResult, error) {
		return kv.RangeResult{}, errors.New("client: malformed remote range response")
	}
	if len(b) < 2 || b[0] != 1 {
		return bad()
	}
	status := b[1]
	b = b[2:]
	if status == 1 {
		if len(b) < 1 {
			return bad()
		}
		code := b[0]
		msg, rest, err := takeBytes(b[1:])
		if err != nil || len(rest) != 0 {
			return bad()
		}
		switch code {
		case 1:
			return kv.RangeResult{}, raftnode.ErrReadTimeout
		case 2:
			return kv.RangeResult{}, raftnode.ErrStopped
		case 3:
			return kv.RangeResult{}, kv.ErrCompacted
		case 4:
			return kv.RangeResult{}, kv.ErrFutureRevision
		case 0:
			return kv.RangeResult{}, errors.New(string(msg))
		default:
			return bad()
		}
	}
	if status != 0 || len(b) < 1 || b[0] > 1 {
		return bad()
	}
	r := kv.RangeResult{More: b[0] == 1}
	b = b[1:]
	var err error
	if r.Count, b, err = takeVarint(b); err != nil {
		return bad()
	}
	if r.Revision, b, err = takeVarint(b); err != nil {
		return bad()
	}
	n, rest, err := takeVarint(b)
	if err != nil || n > uint64(len(rest)) || n > kv.MaxRequestSize/2 {
		return bad()
	}
	b = rest
	r.KVs = make([]kv.KeyValue, 0, int(n))
	for i := uint64(0); i < n; i++ {
		key, rem, e := takeBytes(b)
		if e != nil || len(key) > kv.MaxKeySize {
			return bad()
		}
		b = rem
		value, rem, e := takeBytes(b)
		if e != nil || len(value) > kv.MaxValueSize {
			return bad()
		}
		b = rem
		item := kv.KeyValue{Key: key, Value: value}
		if item.CreateRevision, b, e = takeVarint(b); e != nil {
			return bad()
		}
		if item.ModRevision, b, e = takeVarint(b); e != nil {
			return bad()
		}
		if item.Version, b, e = takeVarint(b); e != nil {
			return bad()
		}
		r.KVs = append(r.KVs, item)
	}
	if len(b) != 0 {
		return bad()
	}
	return r, nil
}

func boolByte(v bool) byte {
	if v {
		return 1
	}
	return 0
}
func appendBytes(b, v []byte) []byte {
	b = binary.AppendUvarint(b, uint64(len(v)))
	return append(b, v...)
}
func takeVarint(b []byte) (uint64, []byte, error) {
	v, n := binary.Uvarint(b)
	if n <= 0 {
		return 0, nil, errors.New("bad varint")
	}
	return v, b[n:], nil
}
func takeBytes(b []byte) ([]byte, []byte, error) {
	n, rest, err := takeVarint(b)
	if err != nil || n > uint64(len(rest)) || n > kv.MaxRequestSize {
		return nil, nil, errors.New("bad byte string")
	}
	return append([]byte(nil), rest[:int(n)]...), rest[int(n):], nil
}

func encodeProposalResponse(status byte, leader uint64, result []byte, message string) []byte {
	b := []byte{1, status}
	switch status {
	case proposalOK:
		b = binary.AppendUvarint(b, uint64(len(result)))
		b = append(b, result...)
	case proposalRedirect:
		b = binary.AppendUvarint(b, leader)
	case proposalRejected:
		b = binary.AppendUvarint(b, uint64(len(message)))
		b = append(b, message...)
	}
	return b
}

func decodeProposalResponse(b []byte) (byte, uint64, []byte, string, error) {
	bad := func(why string) (byte, uint64, []byte, string, error) {
		return 0, 0, nil, "", fmt.Errorf("client: malformed remote proposal response: %s", why)
	}
	if len(b) < 2 || b[0] != 1 {
		return bad("unsupported version or short header")
	}
	status := b[1]
	b = b[2:]
	readVarint := func() (uint64, error) {
		v, n := binary.Uvarint(b)
		if n <= 0 {
			return 0, errors.New("bad varint")
		}
		b = b[n:]
		return v, nil
	}
	switch status {
	case proposalOK, proposalRejected:
		n, err := readVarint()
		if err != nil || n > uint64(len(b)) || n > kv.MaxRequestSize {
			return bad("invalid payload size")
		}
		body := append([]byte(nil), b[:int(n)]...)
		b = b[int(n):]
		if len(b) != 0 {
			return bad("trailing bytes")
		}
		if status == proposalOK {
			return status, 0, body, "", nil
		}
		return status, 0, nil, string(body), nil
	case proposalRedirect:
		leader, err := readVarint()
		if err != nil || len(b) != 0 {
			return bad("invalid leader redirect")
		}
		return status, leader, nil, "", nil
	case proposalUnknown:
		if len(b) != 0 {
			return bad("trailing bytes")
		}
		return status, 0, nil, "", nil
	default:
		return bad("unknown status")
	}
}
