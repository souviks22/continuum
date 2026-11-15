package continuum

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	internalClient "github.com/souviks22/continuum-go/internal/client"
	"github.com/souviks22/continuum-go/internal/clock"
	"github.com/souviks22/continuum-go/internal/kv"
	"github.com/souviks22/continuum-go/internal/raftnode"
	runtimeLoop "github.com/souviks22/continuum-go/internal/runtime"
)

// ClientOptions controls a cluster-wide KV client. Zero values select bounded
// defaults: a 5 second request timeout, three attempts, and 25 millisecond
// retry backoffs.
type ClientOptions struct {
	Timeout          time.Duration
	QueueSize        int
	MaxAttempts      int
	RetryBackoff     time.Duration
	ReadMaxAttempts  int
	ReadRetryBackoff time.Duration
}

// Client routes operations across embedded nodes. Its internal session and
// callbacks are serialized on an owned loop; methods are safe for concurrent
// callers. Watch callbacks run on that loop and must return quickly. Do not
// call blocking Client methods or Close from a Watch callback.
type Client struct {
	runtime        *Node
	loop           *runtimeLoop.Loop
	inner          *internalClient.Client
	peers          []*embeddedPeer
	closer         interface{ Close() error }
	endpointClose  func()
	membershipGate chan struct{}
	once           sync.Once
	done           chan struct{}
	err            error
}

// NewClient creates a routing client over KV-enabled nodes. A node can be
// added only once and must remain open while it is part of this client.
func NewClient(nodes []*Node, opts ClientOptions) (*Client, error) {
	if len(nodes) == 0 {
		return nil, errors.New("continuum: cluster client requires at least one node")
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
		return nil, errors.New("continuum: invalid cluster client timeout, queue or retry options")
	}

	loop, err := runtimeLoop.NewLoop(opts.QueueSize)
	if err != nil {
		return nil, err
	}
	go loop.Run()
	serialClock, err := clock.NewSerial(loop.Dispatch)
	if err != nil {
		loop.Close()
		return nil, err
	}
	leader := &atomic.Uint64{}
	peers := make([]internalClient.Peer, 0, len(nodes))
	adapters := make([]*embeddedPeer, 0, len(nodes))
	seen := make(map[uint64]bool, len(nodes))
	for _, node := range nodes {
		if node == nil || node.id == 0 || node.kvMachine == nil || node.closing.Load() {
			unregisterEmbeddedPeers(adapters)
			loop.Close()
			return nil, errors.New("continuum: cluster client nodes must be open and KV-enabled")
		}
		if seen[node.id] {
			unregisterEmbeddedPeers(adapters)
			loop.Close()
			return nil, fmt.Errorf("continuum: duplicate cluster client node ID %d", node.id)
		}
		seen[node.id] = true
		adapter := &embeddedPeer{node: node, clientLoop: loop, leader: leader, watches: make(map[uint64]*embeddedWatch)}
		if !node.registerClusterPeer(adapter) {
			unregisterEmbeddedPeers(adapters)
			loop.Close()
			return nil, fmt.Errorf("continuum: node %d is closing", node.id)
		}
		adapters = append(adapters, adapter)
		peers = append(peers, adapter)
	}
	inner, err := internalClient.NewWithOptions(peers, internalClient.Options{
		Timeout: opts.Timeout, Clock: serialClock,
		MaxAttempts: opts.MaxAttempts, RetryBackoff: opts.RetryBackoff,
		ReadMaxAttempts: opts.ReadMaxAttempts, ReadRetryBackoff: opts.ReadRetryBackoff,
	})
	if err != nil {
		unregisterEmbeddedPeers(adapters)
		loop.Close()
		return nil, err
	}
	runtime := &Node{kvClient: inner, loop: loop, timeout: opts.Timeout, clock: serialClock, closeDone: make(chan struct{})}
	return &Client{runtime: runtime, loop: loop, inner: inner, peers: adapters, membershipGate: make(chan struct{}, 1), done: make(chan struct{})}, nil
}

func (c *Client) Txn(ctx context.Context, txn Txn) (TxnResult, error) {
	return c.runtime.Txn(ctx, txn)
}
func (c *Client) RetryTxn(ctx context.Context) (TxnResult, error) {
	return c.runtime.RetryTxn(ctx)
}
func (c *Client) ExportSessionState(ctx context.Context) ([]byte, error) {
	return c.runtime.ExportSessionState(ctx)
}
func (c *Client) RestoreSessionState(ctx context.Context, state []byte) error {
	return c.runtime.RestoreSessionState(ctx, state)
}
func (c *Client) Range(ctx context.Context, key, end []byte, opts RangeOptions) (RangeResult, error) {
	return c.runtime.Range(ctx, key, end, opts)
}
func (c *Client) Watch(ctx context.Context, key, end []byte, opts WatchOptions, cb WatchCallback) (*WatchHandle, error) {
	return c.runtime.Watch(ctx, key, end, opts, cb)
}
func (c *Client) GrantLease(ctx context.Context, ttlTicks uint64) (Lease, error) {
	return c.runtime.GrantLease(ctx, ttlTicks)
}
func (c *Client) KeepAlive(ctx context.Context, leaseID uint64) (Lease, error) {
	return c.runtime.KeepAlive(ctx, leaseID)
}
func (c *Client) RevokeLease(ctx context.Context, leaseID uint64) error {
	return c.runtime.RevokeLease(ctx, leaseID)
}
func (c *Client) Compact(ctx context.Context, revision uint64) error {
	return c.runtime.Compact(ctx, revision)
}

// Membership performs a ReadIndex barrier on the known ready leader, then
// returns its applied membership state.
func (c *Client) Membership(ctx context.Context) (MembershipState, error) {
	if ctx == nil {
		return MembershipState{}, errors.New("continuum: nil context")
	}
	nodes, err := c.membershipNodes(ctx)
	if err != nil {
		return MembershipState{}, err
	}
	for _, node := range nodes {
		health, healthErr := node.Health(ctx)
		if healthErr != nil || !health.Ready || !health.IsLeader {
			if ctx.Err() != nil {
				return MembershipState{}, ctx.Err()
			}
			continue
		}
		if err := node.ReadIndex(ctx); err != nil {
			if ctx.Err() != nil {
				return MembershipState{}, ctx.Err()
			}
			continue
		}
		return node.Membership(ctx)
	}
	return MembershipState{}, ErrLeaderNotReady
}

// AddLearner routes a membership change to the current leader. The learner's
// transport identity must already be configured on every existing member.
func (c *Client) AddLearner(ctx context.Context, id uint64) error {
	return c.changeMembership(ctx, func(n *Node) error { return n.AddLearner(ctx, id) })
}

// PromoteLearner routes promotion to the current leader. Catch the learner up
// before calling this method.
func (c *Client) PromoteLearner(ctx context.Context, id uint64) error {
	return c.changeMembership(ctx, func(n *Node) error { return n.PromoteLearner(ctx, id) })
}

// RemoveMember routes removal to the current leader. Replace voters by adding
// and promoting a caught-up learner before removing the old voter.
func (c *Client) RemoveMember(ctx context.Context, id uint64) error {
	return c.changeMembership(ctx, func(n *Node) error { return n.RemoveMember(ctx, id) })
}

func (c *Client) changeMembership(ctx context.Context, change func(*Node) error) error {
	if ctx == nil {
		return errors.New("continuum: nil context")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return ErrStopped
	case c.membershipGate <- struct{}{}:
	}
	defer func() { <-c.membershipGate }()
	nodes, err := c.membershipNodes(ctx)
	if err != nil {
		return err
	}
	attempted := make(map[uint64]bool, len(nodes))
	var hint uint64
	var lastRedirect error
	for len(attempted) < len(nodes) {
		var candidate *Node
		if hint != 0 && !attempted[hint] {
			for _, node := range nodes {
				if node.id == hint {
					candidate = node
					break
				}
			}
		}
		if candidate == nil {
			for _, node := range nodes {
				if !attempted[node.id] {
					candidate = node
					break
				}
			}
		}
		if candidate == nil {
			break
		}
		attempted[candidate.id] = true
		err := change(candidate)
		if err == nil {
			return nil
		}
		var notLeader *NotLeaderError
		if errors.As(err, &notLeader) {
			lastRedirect = err
			hint = notLeader.Leader
			continue
		}
		if errors.Is(err, ErrStopped) {
			continue
		}
		return err
	}
	if lastRedirect != nil {
		return lastRedirect
	}
	return ErrLeaderNotReady
}

func (c *Client) membershipNodes(ctx context.Context) ([]*Node, error) {
	if c.runtime.closing.Load() {
		return nil, ErrStopped
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.done:
		return nil, ErrStopped
	default:
	}
	nodes := make([]*Node, 0, len(c.peers))
	for _, peer := range c.peers {
		nodes = append(nodes, peer.node)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].id < nodes[j].id })
	for i, node := range nodes {
		health, err := node.Health(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			continue
		}
		if health.Ready && health.IsLeader {
			copy(nodes[1:i+1], nodes[0:i])
			nodes[0] = node
			break
		}
	}
	return nodes, nil
}

// Close resolves pending writes as unknown, closes watches and stops the
// client's event loop. It is safe for concurrent callers.
func (c *Client) Close() error {
	c.once.Do(func() {
		c.runtime.closing.Store(true)
		closed := make(chan struct{})
		if err := c.loop.Post(context.Background(), func() {
			c.inner.Close()
			if c.endpointClose != nil {
				c.endpointClose()
			}
			close(closed)
		}); err == nil {
			<-closed
		}
		if c.closer != nil {
			c.err = c.closer.Close()
		}
		c.loop.Close()
		unregisterEmbeddedPeers(c.peers)
		close(c.done)
	})
	<-c.done
	return c.err
}

type embeddedPeer struct {
	node       *Node
	clientLoop *runtimeLoop.Loop
	leader     *atomic.Uint64
	watches    map[uint64]*embeddedWatch // owned by node.loop
}

func (p *embeddedPeer) ID() uint64     { return p.node.id }
func (p *embeddedPeer) Leader() uint64 { return p.leader.Load() }
func (p *embeddedPeer) IsLeader() bool { return p.Leader() == p.ID() }

func (p *embeddedPeer) Propose(command []byte, timeout time.Duration, cb func([]byte, error)) error {
	command = cloneBytes(command)
	return p.enqueue(func() {
		if p.node.closing.Load() {
			p.deliverProposal(cb, nil, raftnode.ErrStopped)
			return
		}
		err := p.node.raw.Propose(command, timeout, func(result []byte, err error) {
			if err == nil {
				p.leader.Store(p.node.id)
			} else {
				p.learnLeader(err)
			}
			p.deliverProposal(cb, result, err)
		})
		if err != nil {
			p.learnLeader(err)
			p.deliverProposal(cb, nil, err)
		}
	})
}

func (p *embeddedPeer) RangeAsync(key, end []byte, opts kv.RangeOptions, timeout time.Duration, cb func(kv.RangeResult, error)) error {
	key, end = cloneBytes(key), cloneBytes(end)
	return p.enqueue(func() {
		if p.node.closing.Load() {
			p.deliverRange(cb, kv.RangeResult{}, raftnode.ErrStopped)
			return
		}
		err := p.node.raw.LinearizableRead(timeout, func(err error) {
			var result kv.RangeResult
			if err == nil {
				result, err = p.node.kvMachine.machine.Store().Range(key, end, opts)
			}
			p.deliverRange(cb, result, err)
		})
		if err != nil {
			p.deliverRange(cb, kv.RangeResult{}, err)
		}
	})
}

func (p *embeddedPeer) WatchAsync(id uint64, key, end []byte, opts kv.WatchOptions, timeout time.Duration, cb func(kv.WatchResult, error)) error {
	key, end = cloneBytes(key), cloneBytes(end)
	return p.enqueue(func() {
		if p.node.closing.Load() {
			p.deliverWatch(cb, kv.WatchResult{}, raftnode.ErrStopped)
			return
		}
		if previous := p.watches[id]; previous != nil {
			previous.cancel()
		}
		call := &embeddedWatch{peer: p, id: id, cb: cb}
		p.watches[id] = call
		heartbeat := timeout / 2
		if heartbeat <= 0 {
			heartbeat = timeout
		}
		err := p.node.raw.LinearizableRead(timeout, func(err error) {
			if call.done {
				return
			}
			if err != nil {
				call.finish(kv.WatchResult{}, err)
				return
			}
			sub, err := p.node.kvMachine.machine.Store().Subscribe(key, end, opts, func(page kv.WatchResult, watchErr error) {
				if watchErr != nil || len(page.Events) > 0 {
					call.finish(page, watchErr)
				}
			})
			if err != nil {
				call.finish(kv.WatchResult{}, err)
				return
			}
			if call.done {
				sub.Cancel()
				return
			}
			call.sub = sub
			// Do not report a heartbeat until the read barrier is confirmed and
			// the subscription is active on this replica.
			call.timer = p.node.clock.AfterFunc(heartbeat, func() {
				revision := p.node.kvMachine.machine.Store().Revision()
				cursor := opts.Cursor
				if cursor.Revision == 0 && revision < ^uint64(0) {
					cursor.Revision = revision + 1
				}
				call.finish(kv.WatchResult{Revision: revision, Next: cursor}, nil)
			})
		})
		if err != nil {
			call.finish(kv.WatchResult{}, err)
		}
	})
}

func (p *embeddedPeer) CancelWatch(id uint64) {
	task := func() {
		if call := p.watches[id]; call != nil {
			call.cancel()
		}
	}
	if err := p.node.loop.TryPost(task); errors.Is(err, runtimeLoop.ErrBackpressure) {
		go func() { _ = p.node.loop.Post(context.Background(), task) }()
	}
}

func (p *embeddedPeer) enqueue(task func()) error {
	if p.node.closing.Load() {
		return raftnode.ErrStopped
	}
	err := p.node.loop.TryPost(task)
	if err != nil {
		return raftnode.ErrStopped
	}
	return nil
}

func (p *embeddedPeer) deliverProposal(cb func([]byte, error), result []byte, err error) {
	result = cloneBytes(result)
	_ = p.clientLoop.Dispatch(func() { p.learnLeader(err); cb(result, err) })
}

func (p *embeddedPeer) deliverRange(cb func(kv.RangeResult, error), result kv.RangeResult, err error) {
	_ = p.clientLoop.Dispatch(func() { cb(result, err) })
}

func (p *embeddedPeer) deliverWatch(cb func(kv.WatchResult, error), result kv.WatchResult, err error) {
	_ = p.clientLoop.Dispatch(func() { cb(result, err) })
}

func (p *embeddedPeer) learnLeader(err error) {
	var notLeader *raftnode.NotLeaderError
	if errors.As(err, &notLeader) && notLeader.Leader != 0 {
		p.leader.Store(notLeader.Leader)
	}
}

func (p *embeddedPeer) stop() {
	for _, call := range p.watches {
		call.finish(kv.WatchResult{}, raftnode.ErrStopped)
	}
}

type embeddedWatch struct {
	peer  *embeddedPeer
	id    uint64
	cb    func(kv.WatchResult, error)
	timer clock.Timer
	sub   *kv.WatchSubscription
	done  bool
}

func (w *embeddedWatch) finish(page kv.WatchResult, err error) {
	if w.done {
		return
	}
	w.done = true
	if w.timer != nil {
		w.timer.Stop()
		w.timer = nil
	}
	if w.sub != nil {
		w.sub.Cancel()
		w.sub = nil
	}
	if w.peer.watches[w.id] == w {
		delete(w.peer.watches, w.id)
	}
	w.peer.deliverWatch(w.cb, page, err)
}

func (w *embeddedWatch) cancel() {
	if w.done {
		return
	}
	w.done = true
	if w.timer != nil {
		w.timer.Stop()
		w.timer = nil
	}
	if w.sub != nil {
		w.sub.Cancel()
		w.sub = nil
	}
	if w.peer.watches[w.id] == w {
		delete(w.peer.watches, w.id)
	}
}

func (n *Node) registerClusterPeer(peer *embeddedPeer) bool {
	n.clusterMu.Lock()
	defer n.clusterMu.Unlock()
	if n.closing.Load() {
		return false
	}
	if n.clusterPeers == nil {
		n.clusterPeers = make(map[*embeddedPeer]struct{})
	}
	n.clusterPeers[peer] = struct{}{}
	return true
}

func (n *Node) unregisterClusterPeer(peer *embeddedPeer) {
	n.clusterMu.Lock()
	delete(n.clusterPeers, peer)
	n.clusterMu.Unlock()
}

func unregisterEmbeddedPeers(peers []*embeddedPeer) {
	for _, peer := range peers {
		peer.node.unregisterClusterPeer(peer)
	}
}

var (
	_ internalClient.Peer           = (*embeddedPeer)(nil)
	_ internalClient.AsyncRangePeer = (*embeddedPeer)(nil)
	_ internalClient.AsyncWatchPeer = (*embeddedPeer)(nil)
)
