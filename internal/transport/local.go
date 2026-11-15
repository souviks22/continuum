package transport

import (
	"errors"
	"fmt"
	"net"
	"sync"
)

// LocalNetwork connects node transports in one process. It is intended for
// embedded deployments where several Raft members share an address space.
type LocalNetwork struct {
	mu    sync.RWMutex
	nodes map[string]*LocalTransport
}

func NewLocalNetwork() *LocalNetwork {
	return &LocalNetwork{nodes: make(map[string]*LocalTransport)}
}

type LocalOptions struct {
	Name          string
	Peers         map[string]struct{}
	Network       *LocalNetwork
	Dispatch      func(func()) error
	QueueSize     int
	MaxFrameBytes int
	OnError       func(error)
}

// LocalTransport implements Transport using bounded outbound queues and
// delivery onto each receiver's serial event loop.
type LocalTransport struct {
	opts    LocalOptions
	queues  map[string]chan []byte
	done    chan struct{}
	mu      sync.Mutex
	closed  bool
	handler ReceiveHandler
	wg      sync.WaitGroup
	addr    localAddr
}

type localAddr string

func (a localAddr) Network() string { return "continuum-inproc" }
func (a localAddr) String() string  { return string(a) }

func NewLocalTransport(opts LocalOptions) (*LocalTransport, error) {
	if opts.Name == "" || len(opts.Name) > maxNodeName {
		return nil, fmt.Errorf("transport: node name must be 1..%d bytes", maxNodeName)
	}
	if opts.Network == nil || opts.Dispatch == nil {
		return nil, errors.New("transport: local network and Dispatch are required")
	}
	if _, ok := opts.Peers[opts.Name]; !ok {
		return nil, errors.New("transport: Peers must include the local node name")
	}
	if opts.QueueSize == 0 {
		opts.QueueSize = defaultQueue
	}
	if opts.MaxFrameBytes == 0 {
		opts.MaxFrameBytes = defaultMaxFrame
	}
	if opts.QueueSize < 1 || opts.MaxFrameBytes < 1 {
		return nil, errors.New("transport: local queue and frame limit must be positive")
	}
	t := &LocalTransport{opts: opts, queues: make(map[string]chan []byte), done: make(chan struct{}), addr: localAddr(opts.Name)}
	for peer := range opts.Peers {
		if peer == "" || len(peer) > maxNodeName {
			return nil, fmt.Errorf("transport: peer name must be 1..%d bytes", maxNodeName)
		}
		if peer != opts.Name {
			t.queues[peer] = make(chan []byte, opts.QueueSize)
		}
	}
	if err := opts.Network.register(t); err != nil {
		return nil, err
	}
	for peer, queue := range t.queues {
		t.wg.Add(1)
		go t.writeLoop(peer, queue)
	}
	return t, nil
}

func (t *LocalTransport) Addr() net.Addr { return t.addr }

func (t *LocalTransport) SetReceiveHandler(h ReceiveHandler) {
	t.mu.Lock()
	if t.handler == nil {
		t.handler = h
	}
	t.mu.Unlock()
}

func (t *LocalTransport) Send(to string, payload []byte) error {
	queue, ok := t.queues[to]
	if !ok {
		return fmt.Errorf("transport: unknown peer %q", to)
	}
	if len(payload) > t.opts.MaxFrameBytes {
		return fmt.Errorf("transport: frame exceeds %d bytes", t.opts.MaxFrameBytes)
	}
	body := append([]byte(nil), payload...)
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return ErrClosed
	}
	select {
	case queue <- body:
		return nil
	default:
		return ErrBackpressure
	}
}

func (t *LocalTransport) writeLoop(peer string, queue <-chan []byte) {
	defer t.wg.Done()
	for {
		select {
		case <-t.done:
			return
		case payload := <-queue:
			t.opts.Network.deliver(t.opts.Name, peer, payload)
		}
	}
}

func (t *LocalTransport) receive(from string, payload []byte) {
	if _, authorized := t.opts.Peers[from]; !authorized || from == t.opts.Name {
		if t.opts.OnError != nil {
			t.opts.OnError(fmt.Errorf("transport: sender identity %q is not authorized", from))
		}
		return
	}
	err := t.opts.Dispatch(func() {
		t.mu.Lock()
		h, closed := t.handler, t.closed
		t.mu.Unlock()
		if !closed && h != nil {
			h(from, payload)
		}
	})
	if err != nil && t.opts.OnError != nil {
		t.opts.OnError(fmt.Errorf("transport: dispatch from %q: %w", from, err))
	}
}

func (t *LocalTransport) Close() error {
	t.mu.Lock()
	if !t.closed {
		t.closed = true
		close(t.done)
	}
	t.mu.Unlock()
	t.opts.Network.unregister(t)
	t.wg.Wait()
	return nil
}

func (n *LocalNetwork) register(t *LocalTransport) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.nodes == nil {
		n.nodes = make(map[string]*LocalTransport)
	}
	if _, exists := n.nodes[t.opts.Name]; exists {
		return fmt.Errorf("transport: local node %q is already registered", t.opts.Name)
	}
	n.nodes[t.opts.Name] = t
	return nil
}

func (n *LocalNetwork) unregister(t *LocalTransport) {
	n.mu.Lock()
	if n.nodes[t.opts.Name] == t {
		delete(n.nodes, t.opts.Name)
	}
	n.mu.Unlock()
}

func (n *LocalNetwork) deliver(from, to string, payload []byte) {
	n.mu.RLock()
	receiver := n.nodes[to]
	n.mu.RUnlock()
	if receiver != nil {
		receiver.receive(from, payload)
	}
}

var _ Transport = (*LocalTransport)(nil)
