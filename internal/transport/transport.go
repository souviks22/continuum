// Package transport is the seam between RPC logic and message delivery, so node
// logic is written once and runs unchanged against the deterministic simulator
// and a real network.
package transport

// ReceiveHandler is called for each delivered message. Ownership of payload
// passes to the handler.
type ReceiveHandler func(from string, payload []byte)

// Transport is the entire surface RPC code needs. One instance is bound to one
// local node identity.
//
// Delivery is unreliable, unordered, and may duplicate: nothing here promises
// otherwise, and layers above must not assume it. A nil error from Send means
// only "accepted for delivery"; drops (partition, crashed peer, loss) are
// silent, as on a real network.
type Transport interface {
	// Send queues payload for delivery to node `to`. Ownership of payload passes
	// to the transport: the caller must not modify it afterwards. It returns an
	// error only when the call itself is invalid (e.g. unknown destination).
	Send(to string, payload []byte) error

	// SetReceiveHandler installs the handler for inbound messages. It must be
	// called before any traffic can arrive.
	SetReceiveHandler(h ReceiveHandler)
}
