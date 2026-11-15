package continuum

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"net"
	"time"

	internalClient "github.com/souviks22/continuum-go/internal/client"
	"github.com/souviks22/continuum-go/internal/clock"
	"github.com/souviks22/continuum-go/internal/rpc"
	"github.com/souviks22/continuum-go/internal/runtime"
	"github.com/souviks22/continuum-go/internal/transport"
)

// RemoteClientConfig describes one externally reachable TCP client identity.
// Every server must include this ID, name and ListenAddr in its Config.Peers;
// reserve an ID that is not used by a Raft member. ListenAddr must be reachable
// by the servers because request replies use the same bounded peer transport.
type RemoteClientConfig struct {
	ID         uint64
	Name       string
	ListenAddr string
	Peers      map[uint64]Peer // Raft server IDs, names and listen addresses

	MaxFrameBytes     int
	QueueSize         int
	MaxConnections    int
	MaxInboundPending int
	DialTimeout       time.Duration
	WriteTimeout      time.Duration
	TLSClient         func(peer string) *tls.Config
	TLSServer         *tls.Config
	PeerName          func(tls.ConnectionState) (string, error)
	InsecureLocal     bool
	OnTransportError  func(error)
}

// RemoteClient is a context-aware Go client that talks to KV-enabled Node
// instances over TCP RPC. Its callbacks run on an owned serial loop. As with
// Client, Watch callbacks must return quickly and must not call blocking client
// methods or Close.
type RemoteClient struct {
	client     *Client
	membership *internalClient.MembershipClient
	endpoint   *rpc.Endpoint
	peers      []remoteCompatibilityPeer
	addr       net.Addr
}

type remoteCompatibilityPeer struct {
	id   uint64
	peer Peer
}

// PeerCompatibility describes one configured server's reported format support.
// Known is false when that server predates the compatibility RPC; unknown
// support must never be treated as compatible.
type PeerCompatibility struct {
	ID                uint64
	Name              string
	Known             bool
	RPCWireVersion    uint8
	KVCommandVersion  uint8
	KVSnapshotVersion uint8
	MaxKVCommandKind  uint8
}

// NewRemoteClient opens a bounded TCP client transport, then builds the same
// retry-safe session client used by embedded callers. For mTLS, all three TLS
// fields must be supplied; servers must trust this client's certificate and
// map it to cfg.Name. Plaintext requires InsecureLocal explicitly.
func NewRemoteClient(cfg RemoteClientConfig, opts ClientOptions) (*RemoteClient, error) {
	if cfg.ID == 0 || cfg.Name == "" || cfg.ListenAddr == "" || len(cfg.Peers) == 0 {
		return nil, errors.New("continuum: remote client requires a unique non-zero identity, listener and server peers")
	}
	if opts.Timeout == 0 {
		opts.Timeout = 5 * time.Second
	}
	if opts.QueueSize == 0 {
		opts.QueueSize = 1024
	}
	if opts.MaxAttempts == 0 {
		opts.MaxAttempts = 3
	}
	if opts.RetryBackoff == 0 {
		opts.RetryBackoff = 25 * time.Millisecond
	}
	if opts.ReadMaxAttempts == 0 {
		opts.ReadMaxAttempts = 3
	}
	if opts.ReadRetryBackoff == 0 {
		opts.ReadRetryBackoff = 25 * time.Millisecond
	}
	if opts.Timeout <= 0 || opts.QueueSize < 1 || opts.MaxAttempts < 1 || opts.ReadMaxAttempts < 1 || opts.RetryBackoff < 0 || opts.ReadRetryBackoff < 0 {
		return nil, errors.New("continuum: invalid remote client timeout, queue or retry options")
	}
	names := map[string]bool{cfg.Name: true}
	transportPeers := make(map[string]string, len(cfg.Peers)+1)
	transportPeers[cfg.Name] = cfg.ListenAddr
	remotePeers := make([]internalClient.Peer, 0, len(cfg.Peers))
	membershipPeers := make([]*internalClient.RemotePeer, 0, len(cfg.Peers))
	compatibilityPeers := make([]remoteCompatibilityPeer, 0, len(cfg.Peers))
	for id, peer := range cfg.Peers {
		if id == 0 || id == cfg.ID || peer.Name == "" || peer.Address == "" || names[peer.Name] {
			return nil, errors.New("continuum: remote server IDs, names and addresses must be unique and must not include the client ID")
		}
		names[peer.Name] = true
		transportPeers[peer.Name] = peer.Address
		compatibilityPeers = append(compatibilityPeers, remoteCompatibilityPeer{id: id, peer: peer})
	}
	loop, err := runtime.NewLoop(opts.QueueSize)
	if err != nil {
		return nil, err
	}
	go loop.Run()
	serialClock, err := clock.NewSerial(loop.Dispatch)
	if err != nil {
		loop.Close()
		return nil, err
	}
	tcp, err := transport.NewTCPTransport(transport.TCPOptions{
		Name: cfg.Name, ListenAddr: cfg.ListenAddr, Peers: transportPeers,
		Dispatch: func(f func()) {
			if err := loop.Dispatch(f); err != nil && cfg.OnTransportError != nil {
				cfg.OnTransportError(err)
			}
		},
		MaxFrameBytes: cfg.MaxFrameBytes, QueueSize: cfg.QueueSize,
		MaxConnections: cfg.MaxConnections, MaxInboundPending: cfg.MaxInboundPending,
		DialTimeout: cfg.DialTimeout, WriteTimeout: cfg.WriteTimeout,
		TLSClient: cfg.TLSClient, TLSServer: cfg.TLSServer, PeerName: cfg.PeerName,
		InsecureLocal: cfg.InsecureLocal, OnError: cfg.OnTransportError,
	})
	if err != nil {
		loop.Close()
		return nil, err
	}
	var randomID [8]byte
	if _, err := rand.Read(randomID[:]); err != nil {
		_ = tcp.Close()
		loop.Close()
		return nil, err
	}
	idBase := binary.LittleEndian.Uint64(randomID[:])
	if idBase == 0 {
		idBase = 1
	}
	endpoint := rpc.NewEndpoint(tcp, serialClock, rpc.Options{IDBase: idBase})
	for id, peer := range cfg.Peers {
		remote, err := internalClient.NewRemotePeer(id, peer.Name, endpoint, opts.Timeout)
		if err != nil {
			_ = tcp.Close()
			loop.Close()
			return nil, err
		}
		remotePeers = append(remotePeers, remote)
		membershipPeers = append(membershipPeers, remote)
	}
	inner, err := internalClient.NewWithOptions(remotePeers, internalClient.Options{
		Timeout: opts.Timeout, Clock: serialClock,
		MaxAttempts: opts.MaxAttempts, RetryBackoff: opts.RetryBackoff,
		ReadMaxAttempts: opts.ReadMaxAttempts, ReadRetryBackoff: opts.ReadRetryBackoff,
	})
	if err != nil {
		_ = tcp.Close()
		loop.Close()
		return nil, err
	}
	membership, err := internalClient.NewMembershipClient(membershipPeers, opts.Timeout)
	if err != nil {
		inner.Close()
		_ = tcp.Close()
		loop.Close()
		return nil, err
	}
	runtimeNode := &Node{kvClient: inner, loop: loop, timeout: opts.Timeout, clock: serialClock, closeDone: make(chan struct{})}
	wrapped := &Client{runtime: runtimeNode, loop: loop, inner: inner, closer: tcp, endpointClose: endpoint.Close, done: make(chan struct{})}
	return &RemoteClient{client: wrapped, membership: membership, endpoint: endpoint, peers: compatibilityPeers, addr: tcp.Addr()}, nil
}

func (c *RemoteClient) Addr() net.Addr { return c.addr }

// Compatibility queries each configured server for its supported RPC and KV
// formats. Unknown-method responses identify older binaries; other query errors
// are returned because a partial report cannot establish a cluster barrier.
// This is an inventory API only and does not enable newer commands.
func (c *RemoteClient) Compatibility(ctx context.Context) ([]PeerCompatibility, error) {
	if ctx == nil {
		return nil, errors.New("continuum: nil context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result := make(chan struct {
		reports []PeerCompatibility
		err     error
	}, 1)
	if err := c.client.loop.Post(ctx, func() {
		reports := make([]PeerCompatibility, len(c.peers))
		remaining := len(c.peers)
		var queryErr error
		finished := false
		finish := func() {
			if remaining == 0 && !finished {
				finished = true
				result <- struct {
					reports []PeerCompatibility
					err     error
				}{reports, queryErr}
			}
		}
		for i, target := range c.peers {
			reports[i] = PeerCompatibility{ID: target.id, Name: target.peer.Name}
			index := i
			err := internalClient.QueryCompatibility(c.endpoint, target.peer.Name, c.client.runtime.timeout, func(supported internalClient.Compatibility, err error) {
				if errors.Is(err, rpc.ErrUnknownMethod) {
					reports[index].Known = false
				} else if err != nil {
					if queryErr == nil {
						queryErr = err
					}
				} else {
					reports[index].Known = true
					reports[index].RPCWireVersion = supported.RPCWireVersion
					reports[index].KVCommandVersion = supported.KVCommandVersion
					reports[index].KVSnapshotVersion = supported.KVSnapshotVersion
					reports[index].MaxKVCommandKind = supported.MaxKVCommandKind
				}
				remaining--
				finish()
			})
			if err != nil {
				if queryErr == nil {
					queryErr = err
				}
				remaining--
				finish()
			}
		}
	}); err != nil {
		return nil, err
	}
	select {
	case out := <-result:
		return out.reports, out.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func (c *RemoteClient) Txn(ctx context.Context, txn Txn) (TxnResult, error) {
	return c.client.Txn(ctx, txn)
}
func (c *RemoteClient) RetryTxn(ctx context.Context) (TxnResult, error) {
	return c.client.RetryTxn(ctx)
}
func (c *RemoteClient) Range(ctx context.Context, key, end []byte, opts RangeOptions) (RangeResult, error) {
	return c.client.Range(ctx, key, end, opts)
}
func (c *RemoteClient) Watch(ctx context.Context, key, end []byte, opts WatchOptions, cb WatchCallback) (*WatchHandle, error) {
	return c.client.Watch(ctx, key, end, opts, cb)
}
func (c *RemoteClient) GrantLease(ctx context.Context, ttlTicks uint64) (Lease, error) {
	return c.client.GrantLease(ctx, ttlTicks)
}
func (c *RemoteClient) KeepAlive(ctx context.Context, leaseID uint64) (Lease, error) {
	return c.client.KeepAlive(ctx, leaseID)
}
func (c *RemoteClient) RevokeLease(ctx context.Context, leaseID uint64) error {
	return c.client.RevokeLease(ctx, leaseID)
}
func (c *RemoteClient) Compact(ctx context.Context, revision uint64) error {
	return c.client.Compact(ctx, revision)
}
func (c *RemoteClient) ExportSessionState(ctx context.Context) ([]byte, error) {
	return c.client.ExportSessionState(ctx)
}
func (c *RemoteClient) RestoreSessionState(ctx context.Context, state []byte) error {
	return c.client.RestoreSessionState(ctx, state)
}

// Membership returns the leader's applied membership after a ReadIndex barrier.
func (c *RemoteClient) Membership(ctx context.Context) (MembershipState, error) {
	var snapshot internalClient.MembershipSnapshot
	err := c.runMembership(ctx, false, func(cb func(error)) error {
		return c.membership.State(func(state internalClient.MembershipSnapshot, err error) {
			snapshot = state
			cb(err)
		})
	})
	if err != nil {
		return MembershipState{}, err
	}
	return MembershipState{
		Voters: snapshot.Voters, Learners: snapshot.Learners,
		VotersOutgoing: snapshot.VotersOutgoing, LearnersNext: snapshot.LearnersNext,
		AutoLeave: snapshot.AutoLeave,
	}, nil
}

// AddLearner adds a preconfigured transport identity as a non-voting member.
func (c *RemoteClient) AddLearner(ctx context.Context, id uint64) error {
	return c.runMembership(ctx, true, func(cb func(error)) error { return c.membership.AddLearner(id, cb) })
}

// PromoteLearner adds a caught-up learner to the voting configuration.
func (c *RemoteClient) PromoteLearner(ctx context.Context, id uint64) error {
	return c.runMembership(ctx, true, func(cb func(error)) error { return c.membership.PromoteLearner(id, cb) })
}

// RemoveMember removes a learner or voter. Replace voters only after another
// learner has caught up and been promoted.
func (c *RemoteClient) RemoveMember(ctx context.Context, id uint64) error {
	return c.runMembership(ctx, true, func(cb func(error)) error { return c.membership.RemoveMember(id, cb) })
}

func (c *RemoteClient) runMembership(ctx context.Context, unknownOnCancel bool, start func(func(error)) error) error {
	if ctx == nil {
		return errors.New("continuum: nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	started := make(chan error, 1)
	result := make(chan error, 1)
	if err := c.client.loop.Post(ctx, func() {
		err := start(func(err error) { result <- err })
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
		select {
		case err := <-started:
			if err != nil {
				return err
			}
		default:
			if unknownOnCancel {
				return unknownContext(ctx.Err())
			}
			return ctx.Err()
		}
		if unknownOnCancel {
			return unknownContext(ctx.Err())
		}
		return ctx.Err()
	}
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		select {
		case err := <-result:
			return err
		default:
			if unknownOnCancel {
				return unknownContext(ctx.Err())
			}
			return ctx.Err()
		}
	}
}

func (c *RemoteClient) Close() error { return c.client.Close() }
