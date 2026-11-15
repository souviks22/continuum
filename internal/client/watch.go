package client

import (
	"errors"

	"github.com/souviks22/continuum-go/internal/clock"
	"github.com/souviks22/continuum-go/internal/kv"
	"github.com/souviks22/continuum-go/internal/raftnode"
)

// WatchHandle is a cancellable client watch. Ready means the linearizable
// barrier completed and the local store subscription is installed.
type WatchHandle struct {
	client         *Client
	id             uint64
	key, end       []byte
	opts           kv.WatchOptions
	cb             kv.WatchCallback
	sub            *kv.WatchSubscription
	remote         any
	ready          bool
	attempts       int
	generation     uint64
	retryScheduled bool
	retryTimer     clock.Timer
	active         bool
	skipped        map[uint64]bool
	lastErr        error
}

// Watch starts a resumable watch on an available replica. It first establishes
// a ReadIndex barrier, then subscribes on that same node's serial context, so
// no committed change can fall between the consistency barrier and registration.
// The callback receives retained pages and subsequent changes. A zero cursor
// starts after the store revision observed when the subscription is installed.
func (c *Client) Watch(key, end []byte, opts kv.WatchOptions, cb kv.WatchCallback) (*WatchHandle, error) {
	if c.closed {
		return nil, ErrClosed
	}
	if cb == nil {
		return nil, errors.New("client: nil watch callback")
	}
	if len(c.watches) >= kv.MaxWatchers {
		return nil, kv.ErrTooManyWatchers
	}
	c.nextWatchID++
	w := &WatchHandle{
		client: c, id: c.nextWatchID,
		key: append([]byte(nil), key...), end: append([]byte(nil), end...),
		opts: opts, cb: cb, active: true,
	}
	c.watches[w.id] = w
	if err := c.startWatch(w, true); err != nil {
		w.cancel(false)
		return nil, err
	}
	return w, nil
}

// Cancel stops the watch without invoking its callback again.
func (w *WatchHandle) Cancel() bool { return w.cancel(false) }

// Active reports whether the watch is pending setup or receiving events.
func (w *WatchHandle) Active() bool { return w != nil && w.active }

// Ready reports whether the linearizable barrier completed and a watch is
// installed (locally or remotely).
func (w *WatchHandle) Ready() bool {
	return w != nil && w.active && w.ready
}

func (w *WatchHandle) cancel(notify bool) bool {
	if w == nil || !w.active {
		return false
	}
	w.active = false
	w.generation++
	if w.retryTimer != nil {
		w.retryTimer.Stop()
		w.retryTimer = nil
	}
	w.retryScheduled = false
	if w.sub != nil {
		w.sub.Cancel()
		w.sub = nil
	}
	if w.remote != nil {
		switch remote := w.remote.(type) {
		case AsyncWatchPeer:
			remote.CancelWatch(w.id)
		case remoteWatchReader:
			remote.cancelRemoteWatch(w.id)
		}
		w.remote = nil
	}
	if w.client != nil {
		delete(w.client.watches, w.id)
	}
	cb := w.cb
	w.cb = nil
	w.key, w.end, w.client = nil, nil, nil
	if notify && cb != nil {
		cb(kv.WatchResult{}, ErrClosed)
	}
	return true
}

func (c *Client) startWatch(w *WatchHandle, initial bool) error {
	if !w.active || c.closed {
		return ErrClosed
	}
	if w.skipped == nil {
		w.attempts++
		w.lastErr = nil
	}
	w.retryScheduled = false
	w.generation++
	gen := w.generation
	readers := 0
	var lastErr error
	for _, peer := range c.peers {
		if w.skipped != nil && w.skipped[peer.ID()] {
			continue
		}
		if rp, ok := peer.(AsyncWatchPeer); ok {
			readers++
			w.remote = rp
			err := rp.WatchAsync(w.id, w.key, w.end, w.opts, c.timeout, func(page kv.WatchResult, err error) {
				if !w.active || w.generation != gen || c.closed {
					return
				}
				if err != nil {
					if retryableReadStart(err) || errors.Is(err, raftnode.ErrReadTimeout) {
						w.lastErr = err
						if w.skipped == nil {
							w.skipped = make(map[uint64]bool)
						}
						w.skipped[peer.ID()] = true
						if startErr := c.startWatch(w, false); startErr != nil {
							c.finishWatch(w, kv.WatchResult{}, startErr)
						}
					} else {
						c.finishWatch(w, page, err)
					}
					return
				}
				w.ready = true // a successful remote page follows its ReadIndex+subscribe barrier
				w.opts.Cursor = page.Next
				c.deliverWatch(w, page, nil)
				if w.active {
					w.attempts = 0
					w.skipped = nil
					w.lastErr = nil
					if startErr := c.startWatch(w, false); startErr != nil {
						c.finishWatch(w, kv.WatchResult{}, startErr)
					}
				}
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
		if rp, ok := peer.(remoteWatchReader); ok {
			readers++
			w.remote = rp
			err := rp.remoteWatch(w.id, w.key, w.end, w.opts, c.timeout, func(page kv.WatchResult, err error) {
				if !w.active || w.generation != gen || c.closed {
					return
				}
				if err != nil {
					if errors.Is(err, raftnode.ErrReadTimeout) || errors.Is(err, raftnode.ErrStopped) {
						c.retryWatch(w, err)
					} else {
						c.finishWatch(w, page, err)
					}
					return
				}
				w.ready = true // the remote page follows its ReadIndex+subscribe barrier
				w.opts.Cursor = page.Next
				c.deliverWatch(w, page, nil)
				if w.active {
					w.attempts = 0 // successful pages start a fresh bounded retry window
					if startErr := c.startWatch(w, false); startErr != nil {
						c.finishWatch(w, kv.WatchResult{}, startErr)
					}
				}
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
		wp, ok := peer.(localWatchReader)
		if !ok {
			continue
		}
		readers++
		err := wp.LinearizableRead(c.timeout, func(err error) {
			if !w.active || w.generation != gen || c.closed {
				return
			}
			if err != nil {
				c.retryWatch(w, err)
				return
			}
			w.ready = true // callbacks remain serialized until Subscribe has installed the watcher
			sub, err := wp.Subscribe(w.key, w.end, w.opts, func(page kv.WatchResult, err error) {
				c.deliverWatch(w, page, err)
			})
			if err != nil {
				c.finishWatch(w, kv.WatchResult{}, err)
				return
			}
			if !w.active {
				sub.Cancel() // a retained-page callback may cancel during Subscribe
				return
			}
			w.sub = sub
		})
		if err == nil {
			return nil
		}
		lastErr = err
		if !retryableReadStart(err) {
			return err
		}
	}
	if readers == 0 {
		if w.lastErr != nil {
			return w.lastErr
		}
		return ErrNoReadPeers
	}
	if lastErr == nil {
		return errors.New("client: could not start watch ReadIndex")
	}
	if initial {
		return lastErr
	}
	c.retryWatch(w, lastErr)
	return nil
}

func (c *Client) retryWatch(w *WatchHandle, err error) {
	if !w.active {
		return
	}
	if w.attempts < c.readMaxAttempts && c.clock != nil {
		w.retryScheduled = true
		w.generation++
		w.retryTimer = c.clock.AfterFunc(exponentialBackoff(c.readRetryBackoff, w.attempts), func() {
			if !w.active || !w.retryScheduled || c.closed {
				return
			}
			w.retryTimer = nil
			w.skipped = nil
			if startErr := c.startWatch(w, false); startErr != nil {
				c.finishWatch(w, kv.WatchResult{}, startErr)
			}
		})
		return
	}
	c.finishWatch(w, kv.WatchResult{}, err)
}

func (c *Client) deliverWatch(w *WatchHandle, page kv.WatchResult, err error) {
	if !w.active {
		return
	}
	if err != nil {
		c.finishWatch(w, page, err)
		return
	}
	w.cb(page, nil)
}

func (c *Client) finishWatch(w *WatchHandle, page kv.WatchResult, err error) {
	if !w.active {
		return
	}
	cb := w.cb
	w.cancel(false)
	if cb != nil {
		cb(page, err)
	}
}
