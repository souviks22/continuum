// Package client coordinates retry-safe transactions over a set of Raft nodes.
//
// Calls and callbacks run on the nodes' shared serial context. The client is
// deliberately single-threaded, like raftnode.Node; a runtime that exposes it
// to concurrent callers must serialize those calls first.
package client

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/souviks22/continuum-go/internal/clock"
	"github.com/souviks22/continuum-go/internal/kv"
	"github.com/souviks22/continuum-go/internal/raftnode"
	"github.com/souviks22/continuum-go/internal/rpc"
)

// Peer is the subset of a Raft node used by Client. raftnode.Node implements it.
type Peer interface {
	ID() uint64
	Leader() uint64
	IsLeader() bool
	Propose([]byte, time.Duration, func([]byte, error)) error
}

var (
	ErrNoPeers          = errors.New("client: no peers")
	ErrBusy             = errors.New("client: a transaction is already unresolved")
	ErrNoSession        = errors.New("client: session has not been opened")
	ErrUnknownResult    = errors.New("client: prior transaction outcome is unknown")
	ErrRetryNotReady    = errors.New("client: retry is available only after an unknown result")
	ErrClosed           = errors.New("client: closed")
	ErrProposalRejected = errors.New("client: proposal rejected before acceptance")
)

const maxWriteAttempts = 16

// Options configures a Client. MaxAttempts counts the first proposal; zero
// selects one attempt. More than one attempt requires Clock and RetryBackoff.
type Options struct {
	Timeout          time.Duration
	Clock            clock.Clock
	MaxAttempts      int // transaction attempts; zero means one
	RetryBackoff     time.Duration
	ReadMaxAttempts  int // ReadIndex attempts per Range; zero means one
	ReadRetryBackoff time.Duration
}

// Client maintains one deduplication session. It permits one unresolved
// transaction at a time, which makes its sequence number unambiguous.
type Client struct {
	peers            []Peer
	timeout          time.Duration
	clock            clock.Clock
	maxAttempts      int
	retryBackoff     time.Duration
	readMaxAttempts  int
	readRetryBackoff time.Duration
	session          uint64
	seq              uint64
	pending          []byte
	txnCallback      func(kv.TxnResult, error)
	openCallback     func(error)
	busy             bool
	unknown          bool // pending request's last callback reported an ambiguous outcome
	retryScheduled   bool
	retryTimer       clock.Timer
	attempts         int
	attemptID        uint64
	nextReadID       uint64
	reads            map[uint64]*readRequest
	nextWatchID      uint64
	watches          map[uint64]*WatchHandle
	nextLeaseOpID    uint64
	leaseOps         map[uint64]*leaseRequest
	closed           bool
}

// New creates a single-threaded client. peers must be the nodes the caller can
// reach; they are ordered by ID so leader discovery is deterministic.
func New(peers []Peer, timeout time.Duration) (*Client, error) {
	return NewWithOptions(peers, Options{Timeout: timeout})
}

// NewWithOptions creates a client with bounded retries for ambiguous writes
// and failed linearizable reads. Callers must serialize all methods and
// callbacks on the supplied Clock's serial context. Setting MaxAttempts above
// one retries the exact session command after RetryBackoff, so every attempt is
// deduplicated by the state machine. If attempts are exhausted the callback
// receives ErrUnknownResult and the request remains available to Retry.
func NewWithOptions(peers []Peer, opts Options) (*Client, error) {
	if len(peers) == 0 {
		return nil, ErrNoPeers
	}
	if opts.Timeout <= 0 {
		return nil, fmt.Errorf("client: timeout must be positive, got %v", opts.Timeout)
	}
	if opts.MaxAttempts == 0 {
		opts.MaxAttempts = 1
	}
	if opts.MaxAttempts < 1 || opts.MaxAttempts > maxWriteAttempts {
		return nil, fmt.Errorf("client: MaxAttempts must be in [1,%d]", maxWriteAttempts)
	}
	if opts.RetryBackoff < 0 || opts.ReadRetryBackoff < 0 {
		return nil, errors.New("client: retry backoffs cannot be negative")
	}
	if opts.MaxAttempts > 1 && (opts.Clock == nil || opts.RetryBackoff <= 0) {
		return nil, errors.New("client: retries require a Clock and positive RetryBackoff")
	}
	if opts.ReadMaxAttempts == 0 {
		opts.ReadMaxAttempts = 1
	}
	if opts.ReadMaxAttempts < 1 || opts.ReadMaxAttempts > maxWriteAttempts {
		return nil, fmt.Errorf("client: ReadMaxAttempts must be in [1,%d]", maxWriteAttempts)
	}
	if opts.ReadMaxAttempts > 1 && (opts.Clock == nil || opts.ReadRetryBackoff <= 0) {
		return nil, errors.New("client: read retries require a Clock and positive ReadRetryBackoff")
	}
	ordered := append([]Peer(nil), peers...)
	for _, p := range ordered {
		if p == nil || p.ID() == 0 {
			return nil, errors.New("client: peers must have unique, non-zero IDs")
		}
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].ID() < ordered[j].ID() })
	for i, p := range ordered {
		if i > 0 && ordered[i-1].ID() == p.ID() {
			return nil, errors.New("client: peers must have unique, non-zero IDs")
		}
	}
	return &Client{
		peers: ordered, timeout: opts.Timeout, clock: opts.Clock,
		maxAttempts: opts.MaxAttempts, retryBackoff: opts.RetryBackoff,
		readMaxAttempts: opts.ReadMaxAttempts, readRetryBackoff: opts.ReadRetryBackoff,
		reads: make(map[uint64]*readRequest), watches: make(map[uint64]*WatchHandle),
		leaseOps: make(map[uint64]*leaseRequest),
	}, nil
}

// OpenSession registers a deduplication session. If the callback receives
// raftnode.ErrResultUnknown, registration may still commit; opening another
// session is safe for writes but can leave the old session to normal bounded
// eviction. The callback runs once if this method returns nil.
func (c *Client) OpenSession(cb func(error)) error {
	if c.closed {
		return ErrClosed
	}
	if c.busy {
		return ErrBusy
	}
	if c.session != 0 {
		return errors.New("client: session already open")
	}
	if cb == nil {
		return errors.New("client: nil callback")
	}
	c.busy = true
	c.openCallback = cb
	c.attemptID++
	id := c.attemptID
	err := c.propose(kv.EncodeRegister(), func(b []byte, err error) {
		if c.closed || c.attemptID != id {
			return
		}
		if err == nil {
			c.session, err = kv.DecodeSession(b)
		}
		c.busy = false
		c.openCallback = nil
		c.attemptID++
		cb(err)
	})
	if err != nil {
		c.busy = false
		c.openCallback = nil
		c.attemptID++
	}
	return err
}

// Txn proposes a transaction using the session's next sequence number. On an
// ambiguous timeout, Retry resends exactly the same bytes and sequence, so a
// committed first attempt is returned from the state machine's dedup cache.
// The callback runs once if this method returns nil.
func (c *Client) Txn(tx kv.Txn, cb func(kv.TxnResult, error)) error {
	if c.closed {
		return ErrClosed
	}
	if c.session == 0 {
		return ErrNoSession
	}
	if c.busy {
		return ErrBusy
	}
	if cb == nil {
		return errors.New("client: nil callback")
	}
	cmd, err := tx.EncodeSession(c.session, c.seq+1, kv.Limits{})
	if err != nil {
		return err
	}
	c.pending = cmd
	c.txnCallback = cb
	c.busy = true
	c.unknown = false
	c.attempts = 0
	if err := c.startTxnAttempt(); err != nil {
		// A synchronous error means Propose did not accept this first attempt.
		c.clearPending()
		return err
	}
	return nil
}

// Retry retries the unresolved transaction after an ErrResultUnknown result.
// It preserves the original command and sequence number. A successful result
// or a definitive state-machine result clears the pending request.
func (c *Client) Retry(cb func(kv.TxnResult, error)) error {
	if c.closed {
		return ErrClosed
	}
	if !c.busy || len(c.pending) == 0 {
		return errors.New("client: no unresolved transaction to retry")
	}
	if !c.unknown {
		return ErrRetryNotReady
	}
	if cb == nil {
		return errors.New("client: nil callback")
	}
	c.stopRetryTimer()
	c.txnCallback = cb
	c.attempts = 0
	c.unknown = false
	if err := c.startTxnAttempt(); err != nil {
		c.unknown = true
		c.txnCallback = nil // Retry returned synchronously; its callback must not run
		return err
	}
	return nil
}

func (c *Client) startTxnAttempt() error {
	c.attempts++
	c.attemptID++
	id := c.attemptID
	return c.propose(c.pending, func(b []byte, err error) {
		if !c.busy || c.attemptID != id {
			return // stale or duplicate completion
		}
		cb := c.txnCallback
		var result kv.TxnResult
		if err != nil {
			if errors.Is(err, raftnode.ErrNotLeader) || errors.Is(err, rpc.ErrUnknownMethod) || errors.Is(err, ErrProposalRejected) {
				c.clearPending()
				cb(kv.TxnResult{}, err)
				return
			}
			c.retryOrReportUnknown()
			return
		}
		result, err = kv.DecodeResult(b)
		if err == nil {
			c.seq++
			c.clearPending()
		} else if errors.Is(err, kv.ErrSessionExpired) || errors.Is(err, kv.ErrResultNotCached) {
			err = fmt.Errorf("%w: %v", ErrUnknownResult, err)
			c.clearPending()
			c.session, c.seq = 0, 0
			cb(kv.TxnResult{}, err)
			return
		} else if errors.Is(err, kv.ErrInvalidCommand) || errors.Is(err, kv.ErrCompacted) || errors.Is(err, kv.ErrFutureRevision) || errors.Is(err, kv.ErrStaleRequest) {
			// A valid state-machine error is a definitive result. The sequence
			// was consumed, so the next request can safely advance.
			c.seq++
			c.clearPending()
		} else {
			// An undecodable reply cannot establish what the state machine did.
			c.retryOrReportUnknown()
			return
		}
		cb(result, err)
	})
}

func (c *Client) retryOrReportUnknown() {
	if c.attempts < c.maxAttempts && c.clock != nil {
		c.unknown = false
		c.retryScheduled = true
		c.retryTimer = c.clock.AfterFunc(c.backoff(c.attempts), func() {
			if !c.busy || !c.retryScheduled || c.closed {
				return
			}
			c.retryScheduled = false
			c.retryTimer = nil
			if err := c.startTxnAttempt(); err != nil {
				c.retryOrReportUnknown()
			}
		})
		return
	}
	c.unknown = true
	cb := c.txnCallback
	c.txnCallback = nil // the caller has been told; a later Retry supplies a new callback
	cb(kv.TxnResult{}, ErrUnknownResult)
}

func (c *Client) backoff(attempt int) time.Duration {
	return exponentialBackoff(c.retryBackoff, attempt)
}

func exponentialBackoff(base time.Duration, attempt int) time.Duration {
	d := base
	for i := 1; i < attempt && d < time.Second; i++ {
		if d > time.Second/2 {
			return time.Second
		}
		d *= 2
	}
	if d > time.Second {
		return time.Second
	}
	return d
}

func (c *Client) stopRetryTimer() {
	if c.retryTimer != nil {
		c.retryTimer.Stop()
		c.retryTimer = nil
	}
	c.retryScheduled = false
}

func (c *Client) clearPending() {
	c.stopRetryTimer()
	c.attemptID++ // invalidate any late completion from a finished request
	c.pending, c.txnCallback = nil, nil
	c.busy, c.unknown = false, false
	c.attempts = 0
}

// propose tries reachable peers in stable ID order. Propose rejects followers
// synchronously, so only the leader starts a request. Node callbacks deliver
// committed results or ambiguous timeouts; the client applies its retry policy
// to those outcomes.
func (c *Client) propose(cmd []byte, cb func([]byte, error)) error {
	attempted := make(map[uint64]bool, len(c.peers))
	var leaderHint uint64
	var lastNotLeader error
	var lastStopped error
	done := false
	var tryCandidates func(uint64) error
	var attempt func(Peer) error
	attempt = func(p Peer) error {
		attempted[p.ID()] = true
		if err := p.Propose(cmd, c.timeout, func(b []byte, err error) {
			if done {
				return
			}
			var nle *raftnode.NotLeaderError
			if errors.As(err, &nle) {
				if nle.Leader != 0 {
					leaderHint = nle.Leader
				}
				if nextErr := tryCandidates(leaderHint); nextErr != nil {
					done = true
					cb(nil, nextErr)
				}
				return
			}
			done = true
			cb(b, err)
		}); err != nil {
			var nle *raftnode.NotLeaderError
			if errors.As(err, &nle) {
				if nle.Leader != 0 {
					leaderHint = nle.Leader
				}
				lastNotLeader = err
			} else if errors.Is(err, raftnode.ErrStopped) {
				lastStopped = err
			}
			return err
		}
		return nil
	}
	tryCandidates = func(hint uint64) error {
		for {
			if hint != 0 && !attempted[hint] {
				found := false
				for _, p := range c.peers {
					if p.ID() != hint {
						continue
					}
					found = true
					err := attempt(p)
					if err == nil {
						return nil // proposal accepted or its async response is pending
					}
					var nle *raftnode.NotLeaderError
					if !errors.As(err, &nle) && !errors.Is(err, raftnode.ErrStopped) {
						return err
					}
					if errors.Is(err, raftnode.ErrStopped) {
						hint = 0
						break
					}
					hint = leaderHint
					break
				}
				if !found {
					hint = 0
				}
				if hint != 0 && !attempted[hint] {
					continue
				}
			}
			advanced := false
			for _, p := range c.peers {
				if attempted[p.ID()] {
					continue
				}
				err := attempt(p)
				if err == nil {
					return nil
				}
				var nle *raftnode.NotLeaderError
				if errors.Is(err, raftnode.ErrStopped) {
					advanced = true
					continue
				}
				if !errors.As(err, &nle) {
					return err
				}
				advanced = true
				if leaderHint != 0 && !attempted[leaderHint] {
					hint = leaderHint
					break
				}
			}
			if hint != 0 && !attempted[hint] {
				continue
			}
			if !advanced {
				break
			}
		}
		if lastNotLeader != nil {
			return lastNotLeader
		}
		if lastStopped != nil {
			return lastStopped
		}
		return &raftnode.NotLeaderError{Leader: leaderHint}
	}

	for _, p := range c.peers {
		if p.IsLeader() {
			if err := attempt(p); err != nil {
				var nle *raftnode.NotLeaderError
				if !errors.As(err, &nle) && !errors.Is(err, raftnode.ErrStopped) {
					return err
				}
			} else {
				return nil
			}
		}
	}
	for _, p := range c.peers {
		if leader := p.Leader(); leader != 0 {
			leaderHint = leader
			if !attempted[leader] {
				return tryCandidates(leader)
			}
		}
	}
	return tryCandidates(leaderHint)
}

// Close cancels a scheduled retry. An unresolved transaction callback receives
// ErrUnknownResult because it may already have committed; an unresolved session
// registration callback receives raftnode.ErrResultUnknown. Late node
// completions are ignored.
func (c *Client) Close() {
	if c.closed {
		return
	}
	c.closed = true
	var txnCB func(kv.TxnResult, error)
	var openCB func(error)
	if c.busy && c.txnCallback != nil {
		txnCB = c.txnCallback
		c.clearPending()
	} else if c.busy && c.openCallback != nil {
		openCB = c.openCallback
		c.openCallback = nil
		c.busy = false
		c.attemptID++
	} else {
		c.clearPending()
	}
	ids := make([]uint64, 0, len(c.reads))
	for id := range c.reads {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		if req := c.reads[id]; req != nil {
			c.finishRead(req, kv.RangeResult{}, ErrClosed)
		}
	}
	watchIDs := make([]uint64, 0, len(c.watches))
	for id := range c.watches {
		watchIDs = append(watchIDs, id)
	}
	sort.Slice(watchIDs, func(i, j int) bool { return watchIDs[i] < watchIDs[j] })
	for _, id := range watchIDs {
		if w := c.watches[id]; w != nil {
			c.finishWatch(w, kv.WatchResult{}, ErrClosed)
		}
	}
	leaseIDs := make([]uint64, 0, len(c.leaseOps))
	for id := range c.leaseOps {
		leaseIDs = append(leaseIDs, id)
	}
	sort.Slice(leaseIDs, func(i, j int) bool { return leaseIDs[i] < leaseIDs[j] })
	for _, id := range leaseIDs {
		if req := c.leaseOps[id]; req != nil {
			c.finishLeaseOp(req, kv.Lease{}, ErrUnknownResult)
		}
	}
	if txnCB != nil {
		txnCB(kv.TxnResult{}, ErrUnknownResult)
	}
	if openCB != nil {
		openCB(raftnode.ErrResultUnknown)
	}
}

// Session returns the registered session ID, or zero before OpenSession
// completes successfully.
func (c *Client) Session() uint64 { return c.session }

// NextSequence is the next sequence number that Txn will use.
func (c *Client) NextSequence() uint64 { return c.seq + 1 }
