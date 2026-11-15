package client

import (
	"errors"

	"github.com/souviks22/continuum/internal/kv"
)

// Compact proposes replicated MVCC history compaction through revision. A
// repeated target is idempotent, so callers may safely retry after an unknown
// result. Use a revision observed from a linearizable Range result.
func (c *Client) Compact(revision uint64, cb func(error)) error {
	if c.closed {
		return ErrClosed
	}
	if cb == nil {
		return errors.New("client: nil callback")
	}
	return c.proposeLease(kv.EncodeCompact(revision), func(_ kv.Lease, err error) { cb(err) }, func(b []byte) (kv.Lease, error) {
		_, err := kv.DecodeResult(b)
		return kv.Lease{}, err
	})
}
