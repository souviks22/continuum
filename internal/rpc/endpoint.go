package rpc

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/souviks22/continuum/internal/clock"
	"github.com/souviks22/continuum/internal/transport"
)

var (
	// ErrTimeout is delivered to a call's callback when no reply arrived in time.
	ErrTimeout = errors.New("rpc: timeout")
	// ErrUnknownMethod means the peer is reachable but does not implement the
	// method, typically a version skew during a rolling upgrade. Unlike a
	// timeout it is definitive: the request was not executed.
	ErrUnknownMethod = errors.New("rpc: unknown method")
	ErrClosed        = errors.New("rpc: endpoint closed")
)

// RemoteError is delivered when the peer's handler returned an error. The
// request reached the handler, so the caller must not assume it had no effect.
type RemoteError struct{ Message string }

func (e *RemoteError) Error() string { return "rpc: remote error: " + e.Message }

// Handler serves one method. It runs synchronously on the node's serial
// context and returns the reply payload or an error.
type Handler func(from string, payload []byte) ([]byte, error)

// AsyncHandler serves a request whose result is available later. The handler
// must invoke reply at most once and on the endpoint's serial context; if it
// never replies, the caller eventually times out.
type AsyncHandler func(from string, payload []byte, reply func([]byte, error))

// NotifyHandler serves a one-way method. There is no reply and no error path
// back to the sender.
type NotifyHandler func(from string, payload []byte)

// ReplyFunc receives the outcome of a Call: the reply payload, or an error
// (ErrTimeout, ErrUnknownMethod, or *RemoteError).
type ReplyFunc func(payload []byte, err error)

// Options configures an Endpoint.
type Options struct {
	// IDBase is the first request ID. IDs only need to be unique per sender,
	// but a restarted node that reused IDs could match a stale reply from its
	// previous life to a new request. Production should pick a random base at
	// process start; simulations derive it from the seed.
	IDBase uint64
	// OnError, if set, is told about inbound messages that cannot be decoded
	// (including version mismatches) and replies that cannot be sent.
	OnError func(from string, err error)
}

type pending struct {
	to    string
	cb    ReplyFunc
	timer clock.Timer
}

// Endpoint is one per node. It adds request/reply semantics, correlation IDs,
// and timeouts on top of a Transport.
//
// Not safe for concurrent use: everything runs on the node's serial context
// (see clock.Clock). Delivery below is at-least-once-ish and unordered, so
// handlers can see duplicate requests; the RPC layer does not deduplicate.
type Endpoint struct {
	tr      transport.Transport
	clk     clock.Clock
	opts    Options
	methods map[string]Handler
	async   map[string]AsyncHandler
	notifys map[string]NotifyHandler
	pending map[uint64]*pending
	nextID  uint64
	closed  bool
}

// NewEndpoint binds an endpoint to tr, taking over its receive handler.
func NewEndpoint(tr transport.Transport, clk clock.Clock, opts Options) *Endpoint {
	e := &Endpoint{
		tr:      tr,
		clk:     clk,
		opts:    opts,
		methods: make(map[string]Handler),
		async:   make(map[string]AsyncHandler),
		notifys: make(map[string]NotifyHandler),
		pending: make(map[uint64]*pending),
		nextID:  opts.IDBase,
	}
	tr.SetReceiveHandler(e.onReceive)
	return e
}

// Register installs the handler for method.
func (e *Endpoint) Register(method string, h Handler) error {
	if e.closed {
		return ErrClosed
	}
	if method == "" || len(method) > MaxMethodLen {
		return fmt.Errorf("rpc: method name must be 1..%d bytes", MaxMethodLen)
	}
	if _, ok := e.methods[method]; ok || e.async[method] != nil || e.notifys[method] != nil {
		return fmt.Errorf("rpc: method %q already registered", method)
	}
	if h == nil {
		return errors.New("rpc: handler is nil")
	}
	e.methods[method] = h
	return nil
}

// RegisterAsync installs a handler that can reply after asynchronous work,
// such as a Raft proposal becoming committed. The reply callback must be
// invoked at most once on the endpoint's serial context.
func (e *Endpoint) RegisterAsync(method string, h AsyncHandler) error {
	if e.closed {
		return ErrClosed
	}
	if method == "" || len(method) > MaxMethodLen {
		return fmt.Errorf("rpc: method name must be 1..%d bytes", MaxMethodLen)
	}
	if _, ok := e.methods[method]; ok || e.async[method] != nil || e.notifys[method] != nil {
		return fmt.Errorf("rpc: method %q already registered", method)
	}
	if h == nil {
		return errors.New("rpc: async handler is nil")
	}
	e.async[method] = h
	return nil
}

// RegisterNotify installs the handler for a one-way method.
func (e *Endpoint) RegisterNotify(method string, h NotifyHandler) error {
	if e.closed {
		return ErrClosed
	}
	if method == "" || len(method) > MaxMethodLen {
		return fmt.Errorf("rpc: method name must be 1..%d bytes", MaxMethodLen)
	}
	if _, ok := e.notifys[method]; ok || e.methods[method] != nil || e.async[method] != nil {
		return fmt.Errorf("rpc: notify method %q already registered", method)
	}
	if h == nil {
		return errors.New("rpc: notify handler is nil")
	}
	e.notifys[method] = h
	return nil
}

// Notify sends a one-way message to `to`. A nil error means only that it was
// handed to the transport; delivery is not confirmed. An error means the call
// itself was invalid (bad method, unknown destination).
func (e *Endpoint) Notify(to, method string, payload []byte) error {
	if e.closed {
		return ErrClosed
	}
	b, err := Message{Kind: KindNotify, Method: method, Payload: payload}.Encode()
	if err != nil {
		return err
	}
	return e.tr.Send(to, b)
}

// PendingCalls returns the number of calls awaiting a reply or timeout. Tests
// use it to catch leaks.
func (e *Endpoint) PendingCalls() int { return len(e.pending) }

// Close cancels outstanding calls and prevents further endpoint use. Call it
// on the endpoint's serial context; callbacks are completed in request-ID
// order with ErrClosed.
func (e *Endpoint) Close() {
	if e.closed {
		return
	}
	e.closed = true
	ids := make([]uint64, 0, len(e.pending))
	for id := range e.pending {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	pending := make([]*pending, 0, len(ids))
	for _, id := range ids {
		p := e.pending[id]
		delete(e.pending, id)
		p.timer.Stop()
		pending = append(pending, p)
	}
	for _, p := range pending {
		p.cb(nil, ErrClosed)
	}
}

// Call sends a request to `to`. If it returns nil, cb runs exactly once, later,
// with the reply or an error; a reply that arrives after the timeout is
// ignored, so the caller is never invoked twice. If Call returns an error the
// request was not sent and cb never runs.
func (e *Endpoint) Call(to, method string, payload []byte, timeout time.Duration, cb ReplyFunc) error {
	if e.closed {
		return ErrClosed
	}
	if timeout <= 0 {
		return fmt.Errorf("rpc: timeout must be positive, got %v", timeout)
	}
	id := e.nextID
	req := Message{Kind: KindRequest, ID: id, Method: method, Payload: payload}
	b, err := req.Encode()
	if err != nil {
		return err
	}
	e.nextID++

	// Register before sending so a transport that delivers synchronously
	// (loopback) still finds the pending entry.
	p := &pending{to: to, cb: cb}
	p.timer = e.clk.AfterFunc(timeout, func() { e.onTimeout(id) })
	e.pending[id] = p

	if err := e.tr.Send(to, b); err != nil {
		p.timer.Stop()
		delete(e.pending, id)
		return err
	}
	return nil
}

func (e *Endpoint) onTimeout(id uint64) {
	p, ok := e.pending[id]
	if !ok {
		return // reply already arrived
	}
	delete(e.pending, id)
	p.cb(nil, ErrTimeout)
}

func (e *Endpoint) onReceive(from string, payload []byte) {
	if e.closed {
		return
	}
	m, err := Decode(payload)
	if err != nil {
		e.report(from, err)
		return
	}
	switch m.Kind {
	case KindRequest:
		e.handleRequest(from, m)
	case KindReply:
		e.handleReply(from, m)
	case KindNotify:
		h, ok := e.notifys[m.Method]
		if !ok {
			// Nobody to tell: a notify has no reply. Count it via OnError (a
			// peer on a newer version sending a method we lack).
			e.report(from, fmt.Errorf("rpc: no handler for notify method %q", m.Method))
			return
		}
		h(from, m.Payload)
	}
}

func (e *Endpoint) handleRequest(from string, m Message) {
	if h, ok := e.async[m.Method]; ok {
		replied := false
		h(from, m.Payload, func(payload []byte, err error) {
			if replied {
				return
			}
			replied = true
			e.sendReply(from, m.ID, payload, err)
		})
		return
	}
	reply := Message{Kind: KindReply, ID: m.ID}
	if h, ok := e.methods[m.Method]; !ok {
		// Definitive answer rather than silence: with mixed versions in a
		// rolling upgrade, "not implemented" must be distinguishable from
		// "network down".
		reply.Status = StatusUnknownMethod
	} else if out, err := h(from, m.Payload); err != nil {
		reply.Status, reply.Payload = StatusError, []byte(err.Error())
	} else {
		reply.Payload = out
	}
	e.sendMessage(from, reply)
}

func (e *Endpoint) sendReply(to string, id uint64, payload []byte, err error) {
	reply := Message{Kind: KindReply, ID: id}
	if err != nil {
		reply.Status, reply.Payload = StatusError, []byte(err.Error())
	} else {
		reply.Payload = payload
	}
	e.sendMessage(to, reply)
}

func (e *Endpoint) sendMessage(to string, reply Message) {
	b, err := reply.Encode()
	if err != nil {
		e.report(to, err)
		return
	}
	if err := e.tr.Send(to, b); err != nil {
		e.report(to, err)
	}
}

func (e *Endpoint) handleReply(from string, m Message) {
	p, ok := e.pending[m.ID]
	// A reply is only valid from the peer the request went to. Without this
	// check any node could complete another node's call by guessing an ID.
	if !ok || p.to != from {
		return // timed out already, duplicate, or stray
	}
	delete(e.pending, m.ID)
	p.timer.Stop()
	switch m.Status {
	case StatusOK:
		p.cb(m.Payload, nil)
	case StatusUnknownMethod:
		p.cb(nil, ErrUnknownMethod)
	default: // StatusError; Decode rejects anything else
		p.cb(nil, &RemoteError{Message: string(m.Payload)})
	}
}

func (e *Endpoint) report(from string, err error) {
	if e.opts.OnError != nil {
		e.opts.OnError(from, err)
	}
}
