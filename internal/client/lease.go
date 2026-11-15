package client

import (
	"errors"

	"github.com/souviks22/continuum/internal/kv"
	"github.com/souviks22/continuum/internal/raftnode"
)

type leaseRequest struct {
	id   uint64
	cb   func(kv.Lease, error)
	done bool
}

// GrantLease proposes a replicated lease grant. The returned ID is the Raft
// log index assigned to the grant. If the result is ambiguous, cb receives
// ErrUnknownResult; callers must not assume a grant did or did not commit.
func (c *Client) GrantLease(ttlTicks uint64, cb func(kv.Lease, error)) error {
	if c.closed {
		return ErrClosed
	}
	if cb == nil {
		return errors.New("client: nil callback")
	}
	cmd, err := kv.EncodeLeaseGrant(ttlTicks)
	if err != nil {
		return err
	}
	return c.proposeLease(cmd, cb, func(b []byte) (kv.Lease, error) { return kv.DecodeLease(b) })
}

// KeepAlive resets a lease's remaining lifetime to its granted number of
// replicated ticks. The callback runs once when the proposal resolves.
func (c *Client) KeepAlive(leaseID uint64, cb func(kv.Lease, error)) error {
	if c.closed {
		return ErrClosed
	}
	if cb == nil {
		return errors.New("client: nil callback")
	}
	cmd, err := kv.EncodeLeaseKeepAlive(leaseID)
	if err != nil {
		return err
	}
	return c.proposeLease(cmd, cb, func(b []byte) (kv.Lease, error) { return kv.DecodeLease(b) })
}

// RevokeLease removes a lease and its attached keys. Closing the client while
// the proposal is unresolved reports ErrUnknownResult because revocation may
// already have committed.
func (c *Client) RevokeLease(leaseID uint64, cb func(error)) error {
	if c.closed {
		return ErrClosed
	}
	if cb == nil {
		return errors.New("client: nil callback")
	}
	cmd, err := kv.EncodeLeaseRevoke(leaseID)
	if err != nil {
		return err
	}
	return c.proposeLease(cmd, func(_ kv.Lease, err error) { cb(err) }, func(b []byte) (kv.Lease, error) {
		_, err := kv.DecodeResult(b)
		return kv.Lease{}, err
	})
}

// TickLeases proposes one replicated TTL tick. A process that owns lease
// scheduling can call this on its configured cadence; the state machine itself
// never reads a local clock.
func (c *Client) TickLeases(cb func(error)) error {
	if c.closed {
		return ErrClosed
	}
	if cb == nil {
		return errors.New("client: nil callback")
	}
	return c.proposeLease(kv.EncodeLeaseTick(), func(_ kv.Lease, err error) { cb(err) }, func(b []byte) (kv.Lease, error) {
		_, err := kv.DecodeResult(b)
		return kv.Lease{}, err
	})
}

func (c *Client) proposeLease(cmd []byte, cb func(kv.Lease, error), decode func([]byte) (kv.Lease, error)) error {
	if c.closed {
		return ErrClosed
	}
	if cb == nil {
		return errors.New("client: nil callback")
	}
	c.nextLeaseOpID++
	req := &leaseRequest{id: c.nextLeaseOpID, cb: cb}
	c.leaseOps[req.id] = req
	err := c.propose(cmd, func(b []byte, err error) {
		if req.done {
			return
		}
		var result kv.Lease
		if err != nil {
			if errors.Is(err, raftnode.ErrResultUnknown) {
				err = ErrUnknownResult
			}
		} else {
			result, err = decode(b)
		}
		c.finishLeaseOp(req, result, err)
	})
	if err != nil {
		req.done = true
		delete(c.leaseOps, req.id)
		return err
	}
	return nil
}

func (c *Client) finishLeaseOp(req *leaseRequest, lease kv.Lease, err error) {
	if req.done {
		return
	}
	req.done = true
	delete(c.leaseOps, req.id)
	cb := req.cb
	req.cb = nil
	cb(lease, err)
}
