// Package continuum exposes an embeddable Raft-backed state-machine node.
// Open owns a serialized event loop and real clock, with bounded TCP transport
// for cross-process clusters or shared in-process transport for local clusters.
// Callers do not access the single-threaded raftnode directly.
package continuum

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/souviks22/continuum-go/internal/client"
	"github.com/souviks22/continuum-go/internal/clock"
	"github.com/souviks22/continuum-go/internal/raftnode"
	"github.com/souviks22/continuum-go/internal/rpc"
	runtimeLoop "github.com/souviks22/continuum-go/internal/runtime"
	"github.com/souviks22/continuum-go/internal/transport"
	"github.com/souviks22/continuum-go/internal/vfs"
)

// Public error identities returned by Node methods. These aliases let callers
// use errors.Is/errors.As without importing Continuum's internal packages.
var (
	ErrStopped          = raftnode.ErrStopped
	ErrNotLeader        = raftnode.ErrNotLeader
	ErrResultUnknown    = raftnode.ErrResultUnknown
	ErrReadTimeout      = raftnode.ErrReadTimeout
	ErrLeaderNotReady   = raftnode.ErrLeaderNotReady
	ErrProposalRejected = client.ErrProposalRejected
)

// NotLeaderError reports the known leader when a proposal reaches a follower.
type NotLeaderError = raftnode.NotLeaderError

// StateMachine must be deterministic across Raft replicas.
type StateMachine interface {
	Apply(index uint64, command []byte) []byte
	Snapshot() ([]byte, error)
	Restore(data []byte) error
}

type Peer struct {
	Name    string
	Address string
}

// InProcessNetwork routes messages between nodes hosted in the same Go
// process. Share one instance across every Open call in that local cluster.
type InProcessNetwork struct{ network *transport.LocalNetwork }

func NewInProcessNetwork() *InProcessNetwork {
	return &InProcessNetwork{network: transport.NewLocalNetwork()}
}

type Event struct {
	Name                           string
	NodeID, RequestID, Term, Index uint64
	Duration                       time.Duration
	Err                            error
}
type EventObserver func(Event)
type Health struct {
	ID, Term, Leader, Commit, Applied, LastIndex, LastSnapshotIndex uint64
	IsLeader, Healthy, Ready                                        bool
	Err                                                             error
}

// Config describes one embedded process. Peers must include this node and all
// configured transport identities, including voting/learner IDs, remote client
// identities and future members. Set InitialVoters explicitly when bootstrapping
// if Peers contains non-voting identities. For BootstrapLearner, InitialVoters
// must contain the current voter IDs. TCP plaintext requires InsecureLocal. Set
// InProcess to use a shared same-process transport instead of TCP.
type Config struct {
	ID               uint64
	Peers            map[uint64]Peer // transport identities; voter membership is InitialVoters/ConfState
	ListenAddr       string
	InProcess        *InProcessNetwork
	DataDir          string
	StateMachine     StateMachine
	Bootstrap        bool
	BootstrapLearner bool
	InitialVoters    []uint64

	TickInterval      time.Duration
	LeaseTickInterval time.Duration
	// KVCompactionInterval optionally enables leader-driven replicated MVCC
	// compaction. KVCompactionRetain is the number of recent data revisions
	// retained; older revisions become unavailable to historical Range and
	// Watch callers. Both settings must be configured together and identically
	// on every member; zero interval and zero retain disable the policy.
	KVCompactionInterval time.Duration
	KVCompactionRetain   uint64
	ElectionTick         int
	HeartbeatTick        int
	SnapshotEvery        uint64
	SnapshotTrailing     uint64
	ProposalTimeout      time.Duration

	QueueSize         int
	MaxFrameBytes     int
	MaxConnections    int
	MaxInboundPending int
	DialTimeout       time.Duration
	WriteTimeout      time.Duration
	TLSClient         func(peer string) *tls.Config
	TLSServer         *tls.Config
	PeerName          func(tls.ConnectionState) (string, error)
	InsecureLocal     bool

	Observer         EventObserver
	OnFatal          func(error)
	OnTransportError func(error) // may run concurrently; must be quick and thread-safe
}

type result struct {
	data []byte
	err  error
}

// Node is the public, thread-safe wrapper around Continuum's single-threaded
// Raft node. Its callbacks and state-machine methods execute on one owned loop.
type Node struct {
	raw           *raftnode.Node
	id            uint64
	kvMachine     *KVStateMachine
	kvClient      *client.Client
	loop          *runtimeLoop.Loop
	clock         clock.Clock
	closer        interface{ Close() error }
	endpointClose func()
	addr          net.Addr
	timeout       time.Duration
	closing       atomic.Bool
	closeOnce     sync.Once
	closeDone     chan struct{}
	closeErr      error
	clusterMu     sync.Mutex
	clusterPeers  map[*embeddedPeer]struct{}
}

func Open(cfg Config) (*Node, error) {
	if cfg.ID == 0 || cfg.DataDir == "" || cfg.StateMachine == nil || (cfg.InProcess == nil && cfg.ListenAddr == "") {
		return nil, errors.New("continuum: ID, DataDir, StateMachine and either ListenAddr or InProcess are required")
	}
	if cfg.InProcess != nil && cfg.InProcess.network == nil {
		return nil, errors.New("continuum: InProcess must be created with NewInProcessNetwork")
	}
	if cfg.InProcess != nil && (cfg.ListenAddr != "" || cfg.TLSClient != nil || cfg.TLSServer != nil || cfg.PeerName != nil || cfg.InsecureLocal) {
		return nil, errors.New("continuum: InProcess cannot be combined with ListenAddr or TCP security options")
	}
	if kvSM, ok := cfg.StateMachine.(*KVStateMachine); ok {
		if err := kvSM.validate(); err != nil {
			return nil, err
		}
	}
	if _, ok := cfg.Peers[cfg.ID]; !ok {
		return nil, errors.New("continuum: Peers must include this node")
	}
	if cfg.QueueSize == 0 {
		cfg.QueueSize = 1024
	}
	if cfg.ProposalTimeout == 0 {
		cfg.ProposalTimeout = 5 * time.Second
	}
	if cfg.QueueSize < 1 || cfg.ProposalTimeout <= 0 {
		return nil, errors.New("continuum: queue size and ProposalTimeout must be positive")
	}
	names := make(map[string]bool, len(cfg.Peers))
	transportPeers := make(map[string]string, len(cfg.Peers))
	raftPeers := make(map[uint64]string, len(cfg.Peers))
	for id, p := range cfg.Peers {
		if id == 0 || p.Name == "" || (cfg.InProcess == nil && id != cfg.ID && p.Address == "") || names[p.Name] {
			return nil, errors.New("continuum: peer IDs, names and addresses must be unique and non-empty")
		}
		names[p.Name] = true
		if cfg.InProcess == nil {
			addr := p.Address
			if id == cfg.ID {
				addr = cfg.ListenAddr
			}
			transportPeers[p.Name] = addr
		}
		raftPeers[id] = p.Name
	}
	local := cfg.Peers[cfg.ID]
	loop, err := runtimeLoop.NewLoop(cfg.QueueSize)
	if err != nil {
		return nil, err
	}
	go loop.Run()
	serialClock, err := clock.NewSerial(loop.Dispatch)
	if err != nil {
		loop.Close()
		return nil, err
	}
	dispatch := func(f func()) error { return loop.Dispatch(f) }
	var nodeTransport transport.Transport
	var transportCloser interface{ Close() error }
	var nodeAddr net.Addr
	if cfg.InProcess != nil {
		localPeers := make(map[string]struct{}, len(raftPeers))
		for _, name := range raftPeers {
			localPeers[name] = struct{}{}
		}
		local, err := transport.NewLocalTransport(transport.LocalOptions{Name: local.Name, Peers: localPeers, Network: cfg.InProcess.network, Dispatch: dispatch, QueueSize: cfg.QueueSize, MaxFrameBytes: cfg.MaxFrameBytes, OnError: cfg.OnTransportError})
		if err != nil {
			loop.Close()
			return nil, err
		}
		nodeTransport, transportCloser, nodeAddr = local, local, local.Addr()
	} else {
		tcp, err := transport.NewTCPTransport(transport.TCPOptions{Name: local.Name, ListenAddr: cfg.ListenAddr, Peers: transportPeers, Dispatch: func(f func()) {
			if err := dispatch(f); err != nil && cfg.OnTransportError != nil {
				cfg.OnTransportError(err)
			}
		}, MaxFrameBytes: cfg.MaxFrameBytes, QueueSize: cfg.QueueSize, MaxConnections: cfg.MaxConnections, MaxInboundPending: cfg.MaxInboundPending, DialTimeout: cfg.DialTimeout, WriteTimeout: cfg.WriteTimeout, TLSClient: cfg.TLSClient, TLSServer: cfg.TLSServer, PeerName: cfg.PeerName, InsecureLocal: cfg.InsecureLocal, OnError: cfg.OnTransportError})
		if err != nil {
			loop.Close()
			return nil, err
		}
		nodeTransport, transportCloser, nodeAddr = tcp, tcp, tcp.Addr()
	}
	var randomID [8]byte
	if _, err := rand.Read(randomID[:]); err != nil {
		_ = transportCloser.Close()
		loop.Close()
		return nil, err
	}
	idBase := binary.LittleEndian.Uint64(randomID[:])
	endpoint := rpc.NewEndpoint(nodeTransport, serialClock, rpc.Options{IDBase: idBase})
	var raw *raftnode.Node
	var kvClient *client.Client
	var kvMachine *KVStateMachine
	openResult := make(chan error, 1)
	postErr := loop.Post(context.Background(), func() {
		var openErr error
		raw, openErr = raftnode.Open(raftnode.Config{ID: cfg.ID, Peers: raftPeers, InitialVoters: append([]uint64(nil), cfg.InitialVoters...), Bootstrap: cfg.Bootstrap, BootstrapLearner: cfg.BootstrapLearner, Endpoint: endpoint, Clock: serialClock, FS: vfs.OS(), Dir: cfg.DataDir, StateMachine: cfg.StateMachine, TickInterval: cfg.TickInterval, LeaseTickInterval: cfg.LeaseTickInterval, KVCompactionInterval: cfg.KVCompactionInterval, KVCompactionRetain: cfg.KVCompactionRetain, ElectionTick: cfg.ElectionTick, HeartbeatTick: cfg.HeartbeatTick, SnapshotEvery: cfg.SnapshotEvery, SnapshotTrailing: cfg.SnapshotTrailing, OnFatal: cfg.OnFatal, Observer: observerFunc(cfg.Observer)})
		if openErr == nil {
			if kvSM, ok := cfg.StateMachine.(*KVStateMachine); ok {
				kvMachine = kvSM
				if peer, err := client.WithStore(raw, kvSM.machine.Store()); err != nil {
					openErr = err
					_ = raw.Stop()
				} else {
					kvClient, openErr = client.NewWithOptions([]client.Peer{peer}, client.Options{Timeout: cfg.ProposalTimeout, Clock: serialClock})
					if openErr != nil {
						_ = raw.Stop()
					}
				}
				if openErr == nil {
					store := kvSM.machine.Store()
					for _, register := range []func() error{
						func() error { return client.RegisterNodeRPC(endpoint, raw, cfg.ProposalTimeout) },
						func() error { return client.RegisterRangeRPC(endpoint, raw, store, cfg.ProposalTimeout) },
						func() error { return client.RegisterWatchRPC(endpoint, raw, store, serialClock, cfg.ProposalTimeout) },
						func() error { return client.RegisterMembershipRPC(endpoint, raw, cfg.ProposalTimeout) },
						func() error {
							return client.RegisterMembershipStateRPC(endpoint, raw, func() client.MembershipSnapshot {
								state := raw.Membership()
								return client.MembershipSnapshot{
									Voters:         append([]uint64(nil), state.GetVoters()...),
									Learners:       append([]uint64(nil), state.GetLearners()...),
									VotersOutgoing: append([]uint64(nil), state.GetVotersOutgoing()...),
									LearnersNext:   append([]uint64(nil), state.GetLearnersNext()...),
									AutoLeave:      state.GetAutoLeave(),
								}
							}, cfg.ProposalTimeout)
						},
						func() error { return client.RegisterCompatibilityRPC(endpoint) },
					} {
						if openErr = register(); openErr != nil {
							_ = raw.Stop()
							break
						}
					}
				}
			}
		}
		openResult <- openErr
	})
	if postErr != nil {
		_ = transportCloser.Close()
		loop.Close()
		return nil, postErr
	}
	if err := <-openResult; err != nil {
		_ = transportCloser.Close()
		loop.Close()
		return nil, err
	}
	return &Node{raw: raw, id: cfg.ID, kvMachine: kvMachine, kvClient: kvClient, loop: loop, clock: serialClock, closer: transportCloser, endpointClose: endpoint.Close, addr: nodeAddr, timeout: cfg.ProposalTimeout, closeDone: make(chan struct{})}, nil
}

// Propose submits one command. If the context expires after Raft accepted it,
// Propose returns ErrResultUnknown because the command may still commit.
func (n *Node) Propose(ctx context.Context, command []byte) ([]byte, error) {
	if ctx == nil {
		return nil, errors.New("continuum: nil context")
	}
	if n.closing.Load() {
		return nil, raftnode.ErrStopped
	}
	timeout := n.timeout
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, ctx.Err()
		}
		if remaining < timeout {
			timeout = remaining
		}
	}
	started := make(chan error, 1)
	resultCh := make(chan result, 1)
	cmd := append([]byte(nil), command...)
	err := n.loop.Post(ctx, func() {
		if n.closing.Load() {
			started <- raftnode.ErrStopped
			return
		}
		if err := ctx.Err(); err != nil {
			started <- err
			return
		}
		err := n.raw.Propose(cmd, timeout, func(data []byte, err error) { resultCh <- result{data: append([]byte(nil), data...), err: err} })
		started <- err
	})
	if err != nil {
		return nil, err
	}
	select {
	case err := <-started:
		if err != nil {
			return nil, err
		}
	case <-ctx.Done():
		err := <-started
		if err != nil {
			return nil, err
		}
		return nil, unknownContext(ctx.Err())
	}
	select {
	case r := <-resultCh:
		return r.data, r.err
	case <-ctx.Done():
		select {
		case r := <-resultCh:
			return r.data, r.err
		default:
			return nil, unknownContext(ctx.Err())
		}
	}
}

// ReadIndex waits for a quorum-confirmed ReadIndex and applied state.
func (n *Node) ReadIndex(ctx context.Context) error {
	if ctx == nil {
		return errors.New("continuum: nil context")
	}
	if n.closing.Load() {
		return raftnode.ErrStopped
	}
	timeout := n.timeout
	if deadline, ok := ctx.Deadline(); ok {
		remain := time.Until(deadline)
		if remain <= 0 {
			return ctx.Err()
		}
		if remain < timeout {
			timeout = remain
		}
	}
	started := make(chan error, 1)
	resultCh := make(chan error, 1)
	err := n.loop.Post(ctx, func() {
		if n.closing.Load() {
			started <- raftnode.ErrStopped
			return
		}
		if err := ctx.Err(); err != nil {
			started <- err
			return
		}
		err := n.raw.LinearizableRead(timeout, func(err error) { resultCh <- err })
		started <- err
	})
	if err != nil {
		return err
	}
	select {
	case err := <-started:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-resultCh:
		return err
	case <-ctx.Done():
		select {
		case err := <-resultCh:
			return err
		default:
			return ctx.Err()
		}
	}
}

// Health returns a serialized health/readiness snapshot.
func (n *Node) Health(ctx context.Context) (Health, error) {
	if ctx == nil {
		return Health{}, errors.New("continuum: nil context")
	}
	if n.closing.Load() {
		return Health{}, raftnode.ErrStopped
	}
	out := make(chan Health, 1)
	if err := n.loop.Post(ctx, func() {
		h := n.raw.Health()
		out <- Health{ID: h.ID, Term: h.Term, Leader: h.Leader, Commit: h.Commit, Applied: h.Applied, LastIndex: h.LastIndex, LastSnapshotIndex: h.LastSnapshotIndex, IsLeader: h.IsLeader, Healthy: h.Healthy, Ready: h.Ready, Err: h.Err}
	}); err != nil {
		return Health{}, err
	}
	select {
	case h := <-out:
		return h, nil
	case <-ctx.Done():
		return Health{}, ctx.Err()
	}
}

func (n *Node) Addr() net.Addr { return n.addr }

// Close stops Raft, closes TCP workers and drains the serialized loop. It is
// safe for concurrent callers and must not be invoked from an Observer callback.
func (n *Node) Close() error {
	n.closeOnce.Do(func() {
		n.closing.Store(true)
		stopped := make(chan struct{})
		if err := n.loop.Post(context.Background(), func() {
			n.clusterMu.Lock()
			clusterPeers := make([]*embeddedPeer, 0, len(n.clusterPeers))
			for peer := range n.clusterPeers {
				clusterPeers = append(clusterPeers, peer)
			}
			n.clusterMu.Unlock()
			for _, peer := range clusterPeers {
				peer.stop()
			}
			if n.kvClient != nil {
				n.kvClient.Close()
			}
			_ = n.raw.Stop()
			if n.endpointClose != nil {
				n.endpointClose()
			}
			close(stopped)
		}); err == nil {
			<-stopped
		}
		transportErr := n.closer.Close()
		n.loop.Close()
		n.closeErr = transportErr
		close(n.closeDone)
	})
	<-n.closeDone
	return n.closeErr
}

func unknownContext(err error) error { return fmt.Errorf("%w: %v", raftnode.ErrResultUnknown, err) }
func observerFunc(fn EventObserver) raftnode.Observer {
	if fn == nil {
		return nil
	}
	return observerAdapter{fn}
}

type observerAdapter struct{ fn EventObserver }

func (a observerAdapter) Observe(event raftnode.Event) {
	a.fn(Event{Name: event.Name, NodeID: event.NodeID, RequestID: event.RequestID, Term: event.Term, Index: event.Index, Duration: event.Duration, Err: event.Err})
}
