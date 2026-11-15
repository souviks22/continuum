package client

import (
	"errors"
	"fmt"
	"time"

	"github.com/souviks22/continuum/internal/clock"
	"github.com/souviks22/continuum/internal/kv"
	"github.com/souviks22/continuum/internal/raftnode"
)

var ErrNoReadPeers = errors.New("client: no peer has a local store and linearizable reads")

type linearReader interface {
	LinearizableRead(time.Duration, func(error)) error
}

type localRangeReader interface {
	linearReader
	Range([]byte, []byte, kv.RangeOptions) (kv.RangeResult, error)
}

type localWatchReader interface {
	linearReader
	Subscribe([]byte, []byte, kv.WatchOptions, kv.WatchCallback) (*kv.WatchSubscription, error)
}

type remoteRangeReader interface {
	remoteRange([]byte, []byte, kv.RangeOptions, time.Duration, func(kv.RangeResult, error)) error
}

// AsyncRangePeer performs a ReadIndex and local store read on its own serial
// context, then delivers the result on the Client's serial context.
type AsyncRangePeer interface {
	RangeAsync([]byte, []byte, kv.RangeOptions, time.Duration, func(kv.RangeResult, error)) error
}

type storePeer struct {
	Peer
	linearReader
	store *kv.Store
}

type readRequest struct {
	id             uint64
	key, end       []byte
	opts           kv.RangeOptions
	cb             func(kv.RangeResult, error)
	attempts       int
	generation     uint64
	retryScheduled bool
	retryTimer     clock.Timer
	skipped        map[uint64]bool
	lastErr        error
	done           bool
}

// WithStore binds a Raft peer to its local state machine store so Client.Range
// can read that replica after its ReadIndex barrier completes. Store must be
// the store advanced by this peer's raftnode.StateMachine on the same serial
// context.
func WithStore(peer Peer, store *kv.Store) (Peer, error) {
	reader, ok := peer.(linearReader)
	if peer == nil || !ok || store == nil {
		return nil, errors.New("client: WithStore requires a readable peer and non-nil store")
	}
	return &storePeer{Peer: peer, linearReader: reader, store: store}, nil
}

func (p *storePeer) Range(key, end []byte, opts kv.RangeOptions) (kv.RangeResult, error) {
	return p.store.Range(key, end, opts)
}

func (p *storePeer) Subscribe(key, end []byte, opts kv.WatchOptions, cb kv.WatchCallback) (*kv.WatchSubscription, error) {
	return p.store.Subscribe(key, end, opts, cb)
}

// Range executes a linearizable read on an available replica. The local store
// is read only after Raft confirms ReadIndex and the node has applied through
// that index. With Revision zero, the result reflects at least every write
// completed before Range began. A non-zero Revision selects that historical
// view, subject to the store's compaction rules.
//
// A nil return means cb will run once. A synchronous error means no read was
// started. Read failures are safe to retry; this method tries another peer for
// synchronous routing/readiness errors, then applies the configured bounded
// ReadIndex retry policy to asynchronous barrier failures. Store errors after a
// successful barrier are definitive and are returned directly to cb.
func (c *Client) Range(key, end []byte, opts kv.RangeOptions, cb func(kv.RangeResult, error)) error {
	if c.closed {
		return ErrClosed
	}
	if cb == nil {
		return errors.New("client: nil callback")
	}
	c.nextReadID++
	req := &readRequest{id: c.nextReadID, key: append([]byte(nil), key...), end: append([]byte(nil), end...), opts: opts, cb: cb}
	c.reads[req.id] = req
	if err := c.startRead(req, true); err != nil {
		req.done = true
		delete(c.reads, req.id)
		return err
	}
	return nil
}

func (c *Client) startRead(req *readRequest, initial bool) error {
	if req.done || c.closed {
		return ErrClosed
	}
	if req.skipped == nil {
		req.attempts++
		req.lastErr = nil
	}
	req.retryScheduled = false
	req.generation++
	gen := req.generation
	readers := 0
	var lastErr error
	for _, peer := range c.peers {
		if req.skipped != nil && req.skipped[peer.ID()] {
			continue
		}
		if rp, ok := peer.(AsyncRangePeer); ok {
			readers++
			err := rp.RangeAsync(req.key, req.end, req.opts, c.timeout, func(result kv.RangeResult, err error) {
				if req.done || req.generation != gen {
					return
				}
				if err != nil {
					if retryableReadStart(err) || errors.Is(err, raftnode.ErrReadTimeout) {
						req.lastErr = err
						if req.skipped == nil {
							req.skipped = make(map[uint64]bool)
						}
						req.skipped[peer.ID()] = true
						if startErr := c.startRead(req, false); startErr != nil {
							c.finishRead(req, kv.RangeResult{}, startErr)
						}
					} else {
						c.finishRead(req, kv.RangeResult{}, err)
					}
					return
				}
				c.finishRead(req, result, nil)
			})
			if err == nil {
				return nil
			}
			lastErr = err
			if !retryableReadStart(err) {
				return err
			}
			continue
		}
		if rp, ok := peer.(remoteRangeReader); ok {
			readers++
			err := rp.remoteRange(req.key, req.end, req.opts, c.timeout, func(result kv.RangeResult, err error) {
				if req.done || req.generation != gen {
					return
				}
				if err != nil {
					if errors.Is(err, raftnode.ErrReadTimeout) || errors.Is(err, raftnode.ErrStopped) {
						c.retryRead(req, err)
					} else {
						c.finishRead(req, kv.RangeResult{}, err)
					}
					return
				}
				c.finishRead(req, result, nil)
			})
			if err == nil {
				return nil
			}
			lastErr = err
			if !retryableReadStart(err) {
				return err
			}
			continue
		}
		rp, ok := peer.(localRangeReader)
		if !ok {
			continue
		}
		readers++
		err := rp.LinearizableRead(c.timeout, func(err error) {
			if req.done || req.generation != gen {
				return
			}
			if err != nil {
				c.retryRead(req, err)
				return
			}
			result, err := rp.Range(req.key, req.end, req.opts)
			c.finishRead(req, result, err)
		})
		if err == nil {
			return nil
		}
		lastErr = err
		// These errors mean no read was started and another replica may be
		// ready. Invalid calls (notably a bad timeout) are common to every peer.
		if !retryableReadStart(err) {
			return err
		}
	}
	if readers == 0 {
		if req.lastErr != nil {
			return req.lastErr
		}
		return ErrNoReadPeers
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("client: could not start linearizable range")
	}
	if initial {
		return lastErr
	}
	c.retryRead(req, lastErr)
	return nil
}

func retryableReadStart(err error) bool {
	return errors.Is(err, raftnode.ErrNotLeader) || errors.Is(err, raftnode.ErrLeaderNotReady) || errors.Is(err, raftnode.ErrStopped)
}

func (c *Client) retryRead(req *readRequest, err error) {
	if req.done {
		return
	}
	if req.attempts < c.readMaxAttempts && c.clock != nil {
		req.retryScheduled = true
		req.generation++ // invalidate duplicate completion from the old read round
		req.retryTimer = c.clock.AfterFunc(exponentialBackoff(c.readRetryBackoff, req.attempts), func() {
			if req.done || !req.retryScheduled || c.closed {
				return
			}
			req.retryTimer = nil
			req.skipped = nil
			if err := c.startRead(req, false); err != nil {
				c.finishRead(req, kv.RangeResult{}, err)
			}
		})
		return
	}
	c.finishRead(req, kv.RangeResult{}, err)
}

func (c *Client) finishRead(req *readRequest, result kv.RangeResult, err error) {
	if req.done {
		return
	}
	req.done = true
	req.generation++
	req.retryScheduled = false
	if req.retryTimer != nil {
		req.retryTimer.Stop()
		req.retryTimer = nil
	}
	delete(c.reads, req.id)
	cb := req.cb
	req.cb = nil
	cb(result, err)
}
