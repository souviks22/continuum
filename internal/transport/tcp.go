package transport

import (
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

var (
	ErrClosed       = errors.New("transport: closed")
	ErrBackpressure = errors.New("transport: outbound queue is full")
)

const (
	defaultMaxFrame = 1 << 20
	defaultQueue    = 256
	maxNodeName     = 255
)

// TCPOptions configures a bounded TCP transport. Peers maps stable node names
// to addresses; Name must be the local node's stable name. Dispatch must post
// callbacks onto the receiver's serial event loop.
//
// TLS is enabled only when all three TLS fields are supplied. PeerName maps a
// verified client certificate to its stable node name. InsecureLocal must be
// explicitly set for plaintext links, typically in tests or a trusted local
// development network.
type TCPOptions struct {
	Name              string
	ListenAddr        string
	Peers             map[string]string
	Dispatch          func(func())
	MaxFrameBytes     int
	QueueSize         int
	MaxConnections    int
	MaxInboundPending int
	DialTimeout       time.Duration
	WriteTimeout      time.Duration
	TLSClient         func(peer string) *tls.Config
	TLSServer         *tls.Config
	PeerName          func(tls.ConnectionState) (string, error)
	InsecureLocal     bool
	// OnError receives asynchronous dial, write, accept and malformed-frame
	// failures. It may run concurrently and must be quick and thread-safe.
	OnError func(error)
}

// TCPTransport implements Transport with one bounded outbound queue and writer
// per peer. Frames are length-prefixed and bounded before allocation. Send
// succeeds when a frame is enqueued; TCP delivery is not acknowledged.
type TCPTransport struct {
	opts         TCPOptions
	listener     net.Listener
	peers        map[string]string
	queues       map[string]chan []byte
	peerReset    map[string]chan struct{}
	ready        chan struct{}
	done         chan struct{}
	handler      ReceiveHandler
	mu           sync.Mutex
	closed       bool
	conns        map[net.Conn]struct{}
	inboundSlots chan struct{}
	closeOnce    sync.Once
	closeErr     error
	wg           sync.WaitGroup
}

// NewTCPTransport binds the listener and starts its bounded writer workers.
// Call SetReceiveHandler (normally through rpc.NewEndpoint) before peers begin
// sending application traffic.
func NewTCPTransport(opts TCPOptions) (*TCPTransport, error) {
	if len(opts.Name) == 0 || len(opts.Name) > maxNodeName {
		return nil, fmt.Errorf("transport: node name must be 1..%d bytes", maxNodeName)
	}
	if opts.ListenAddr == "" || opts.Dispatch == nil {
		return nil, errors.New("transport: ListenAddr and Dispatch are required")
	}
	if len(opts.Peers) == 0 {
		return nil, errors.New("transport: at least one peer is required")
	}
	if _, ok := opts.Peers[opts.Name]; !ok {
		return nil, errors.New("transport: Peers must include the local node name")
	}
	for name, addr := range opts.Peers {
		if len(name) == 0 || len(name) > maxNodeName || addr == "" {
			return nil, errors.New("transport: peer names and addresses must be non-empty and bounded")
		}
	}
	secure := opts.TLSClient != nil && opts.TLSServer != nil && opts.PeerName != nil
	if (opts.TLSClient != nil || opts.TLSServer != nil || opts.PeerName != nil) && !secure {
		return nil, errors.New("transport: TLSClient, TLSServer and PeerName must be configured together")
	}
	if !secure && !opts.InsecureLocal {
		return nil, errors.New("transport: plaintext requires explicit InsecureLocal")
	}
	if secure && (opts.TLSServer.ClientAuth != tls.RequireAndVerifyClientCert || opts.TLSServer.ClientCAs == nil) {
		return nil, errors.New("transport: TLS server must require verified client certificates with ClientCAs configured")
	}
	if opts.MaxFrameBytes == 0 {
		opts.MaxFrameBytes = defaultMaxFrame
	}
	if opts.QueueSize == 0 {
		opts.QueueSize = defaultQueue
	}
	if opts.MaxConnections == 0 {
		opts.MaxConnections = max(16, len(opts.Peers)*4)
	}
	if opts.MaxInboundPending == 0 {
		opts.MaxInboundPending = opts.QueueSize
	}
	if opts.DialTimeout == 0 {
		opts.DialTimeout = 2 * time.Second
	}
	if opts.WriteTimeout == 0 {
		opts.WriteTimeout = 2 * time.Second
	}
	if opts.MaxFrameBytes < 2+len(opts.Name) || opts.QueueSize < 1 || opts.MaxConnections < len(opts.Peers)-1 || opts.MaxInboundPending < 1 || opts.DialTimeout <= 0 || opts.WriteTimeout <= 0 {
		return nil, errors.New("transport: invalid frame, queue or timeout limit")
	}
	if uint64(opts.MaxFrameBytes) > uint64(^uint32(0)) {
		return nil, errors.New("transport: MaxFrameBytes exceeds the frame format limit")
	}
	listener, err := net.Listen("tcp", opts.ListenAddr)
	if err != nil {
		return nil, fmt.Errorf("transport: listen: %w", err)
	}
	if secure {
		serverTLS := opts.TLSServer.Clone()
		if serverTLS.MinVersion < tls.VersionTLS12 {
			serverTLS.MinVersion = tls.VersionTLS12
		}
		listener = tls.NewListener(listener, serverTLS)
	}
	t := &TCPTransport{
		opts: opts, listener: listener,
		peers:  make(map[string]string, len(opts.Peers)),
		queues: make(map[string]chan []byte, len(opts.Peers)-1), peerReset: make(map[string]chan struct{}, len(opts.Peers)-1),
		ready: make(chan struct{}), done: make(chan struct{}),
		conns: make(map[net.Conn]struct{}), inboundSlots: make(chan struct{}, opts.MaxInboundPending),
	}
	for name, addr := range opts.Peers {
		t.peers[name] = addr
		if name != opts.Name {
			t.queues[name] = make(chan []byte, opts.QueueSize)
			t.peerReset[name] = make(chan struct{}, 1)
		}
	}
	t.wg.Add(1 + len(t.queues))
	go t.acceptLoop()
	for peer, q := range t.queues {
		go t.writeLoop(peer, q)
	}
	return t, nil
}

// Addr returns the bound listener address (useful when ListenAddr used :0).
func (t *TCPTransport) Addr() net.Addr { return t.listener.Addr() }

// SetReceiveHandler installs the sole inbound handler. It is safe to call once
// after construction; already accepted connections wait until it is installed.
func (t *TCPTransport) SetReceiveHandler(h ReceiveHandler) {
	t.mu.Lock()
	if t.handler == nil {
		t.handler = h
		close(t.ready)
	}
	t.mu.Unlock()
}

// Send copies and queues payload for the named peer. A full queue returns
// ErrBackpressure rather than growing memory without bound.
func (t *TCPTransport) Send(to string, payload []byte) error {
	q, ok := t.queues[to]
	if !ok {
		return fmt.Errorf("transport: unknown peer %q", to)
	}
	if len(payload)+2+len(t.opts.Name) > t.opts.MaxFrameBytes {
		return fmt.Errorf("transport: frame exceeds %d bytes", t.opts.MaxFrameBytes)
	}
	body := make([]byte, 2+len(t.opts.Name)+len(payload))
	binary.BigEndian.PutUint16(body[:2], uint16(len(t.opts.Name)))
	copy(body[2:], t.opts.Name)
	copy(body[2+len(t.opts.Name):], payload)
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return ErrClosed
	}
	select {
	case q <- body:
		return nil
	default:
		return ErrBackpressure
	}
}

func (t *TCPTransport) acceptLoop() {
	defer t.wg.Done()
	for {
		conn, err := t.listener.Accept()
		if err != nil {
			select {
			case <-t.done:
				return
			default:
			}
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				t.report(fmt.Errorf("transport: temporary accept failure: %w", err))
				time.Sleep(10 * time.Millisecond)
				continue
			}
			t.report(fmt.Errorf("transport: accept: %w", err))
			return
		}
		t.mu.Lock()
		if t.closed || len(t.conns) >= t.opts.MaxConnections {
			t.mu.Unlock()
			conn.Close()
			return
		}
		t.conns[conn] = struct{}{}
		t.wg.Add(1)
		t.mu.Unlock()
		go t.readLoop(conn)
	}
}

func (t *TCPTransport) writeLoop(peer string, q <-chan []byte) {
	defer t.wg.Done()
	var conn net.Conn
	defer func() {
		if conn != nil {
			t.removeConn(conn)
			conn.Close()
		}
	}()
	for {
		select {
		case <-t.done:
			return
		case <-t.peerReset[peer]:
			if conn != nil {
				t.removeConn(conn)
				_ = conn.Close()
				conn = nil
			}
		case body := <-q:
			if conn == nil {
				var err error
				conn, err = t.dial(peer)
				if err != nil {
					t.report(fmt.Errorf("transport: dial peer %q: %w", peer, err))
					continue // Raft will retransmit on a later heartbeat.
				}
			}
			_ = conn.SetWriteDeadline(time.Now().Add(t.opts.WriteTimeout))
			if err := writeFrame(conn, body); err != nil {
				t.report(fmt.Errorf("transport: write peer %q: %w", peer, err))
				t.removeConn(conn)
				conn.Close()
				conn = nil // this frame may have been lost; later Raft messages retry.
			}
		}
	}
}

func (t *TCPTransport) dial(peer string) (net.Conn, error) {
	addr := t.peers[peer]
	d := net.Dialer{Timeout: t.opts.DialTimeout}
	var conn net.Conn
	var err error
	if t.opts.TLSClient != nil {
		cfg := t.opts.TLSClient(peer)
		if cfg == nil || cfg.InsecureSkipVerify {
			return nil, errors.New("transport: TLSClient must return a config with certificate verification enabled")
		}
		clientTLS := cfg.Clone()
		if clientTLS.MinVersion < tls.VersionTLS12 {
			clientTLS.MinVersion = tls.VersionTLS12
		}
		conn, err = tls.DialWithDialer(&d, "tcp", addr, clientTLS)
	} else {
		conn, err = d.Dial("tcp", addr)
	}
	if err != nil {
		return nil, err
	}
	t.mu.Lock()
	closed := t.closed
	if !closed {
		t.conns[conn] = struct{}{}
	}
	t.mu.Unlock()
	if closed {
		conn.Close()
		return nil, ErrClosed
	}
	return conn, nil
}

func (t *TCPTransport) readLoop(conn net.Conn) {
	defer t.wg.Done()
	peer := ""
	defer func() {
		t.removeConn(conn)
		_ = conn.Close()
		if peer != "" {
			// The other process may have restarted while our writer still holds
			// its old outbound TCP connection. Force that writer to reconnect
			// before it sends a reply to the peer's next request.
			select {
			case t.peerReset[peer] <- struct{}{}:
			default:
			}
		}
	}()
	var authenticated string
	if tlsConn, ok := conn.(*tls.Conn); ok {
		_ = tlsConn.SetDeadline(time.Now().Add(t.opts.DialTimeout))
		if err := tlsConn.Handshake(); err != nil {
			t.report(fmt.Errorf("transport: TLS handshake: %w", err))
			return
		}
		_ = tlsConn.SetDeadline(time.Time{})
		var err error
		authenticated, err = t.opts.PeerName(tlsConn.ConnectionState())
		if err != nil || t.peers[authenticated] == "" || authenticated == t.opts.Name {
			if err == nil {
				err = errors.New("certificate identity is not a configured remote peer")
			}
			t.report(fmt.Errorf("transport: authenticate peer: %w", err))
			return
		}
	}
	for {
		body, err := readFrame(conn, t.opts.MaxFrameBytes)
		if err != nil {
			t.report(fmt.Errorf("transport: read frame: %w", err))
			return
		}
		if len(body) < 2 {
			t.report(errors.New("transport: short sender header"))
			return
		}
		nameLen := int(binary.BigEndian.Uint16(body[:2]))
		if nameLen == 0 || nameLen > maxNodeName || 2+nameLen > len(body) {
			t.report(errors.New("transport: invalid sender name length"))
			return
		}
		from := string(body[2 : 2+nameLen])
		if t.peers[from] == "" || from == t.opts.Name || (authenticated != "" && authenticated != from) {
			t.report(fmt.Errorf("transport: sender identity %q is not authorized", from))
			return
		}
		if peer != "" && peer != from {
			t.report(errors.New("transport: peer identity changed on one connection"))
			return
		}
		peer = from
		payload := append([]byte(nil), body[2+nameLen:]...)
		select {
		case <-t.done:
			return
		case <-t.ready:
		}
		t.mu.Lock()
		h := t.handler
		closed := t.closed
		t.mu.Unlock()
		if closed || h == nil {
			return
		}
		select {
		case t.inboundSlots <- struct{}{}:
		case <-t.done:
			return
		}
		t.opts.Dispatch(func() {
			defer func() { <-t.inboundSlots }()
			t.mu.Lock()
			closed := t.closed
			h := t.handler
			t.mu.Unlock()
			if !closed && h != nil {
				h(from, payload)
			}
		})
	}
}

// Close stops accepting connections, interrupts active sockets and waits for
// all transport workers to exit. It is idempotent.
func (t *TCPTransport) Close() error {
	t.closeOnce.Do(func() {
		t.mu.Lock()
		t.closed = true
		close(t.done)
		t.closeErr = t.listener.Close()
		for conn := range t.conns {
			_ = conn.Close()
		}
		t.mu.Unlock()
		t.wg.Wait()
	})
	return t.closeErr
}

func (t *TCPTransport) addConn(conn net.Conn) {
	t.mu.Lock()
	if !t.closed {
		t.conns[conn] = struct{}{}
	}
	t.mu.Unlock()
}

func (t *TCPTransport) removeConn(conn net.Conn) {
	t.mu.Lock()
	delete(t.conns, conn)
	t.mu.Unlock()
}

func (t *TCPTransport) report(err error) {
	if t.opts.OnError != nil {
		select {
		case <-t.done:
			return
		default:
			t.opts.OnError(err)
		}
	}
}

func writeFrame(w io.Writer, body []byte) error {
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(body)))
	if err := writeFull(w, header[:]); err != nil {
		return err
	}
	return writeFull(w, body)
}

func writeFull(w io.Writer, b []byte) error {
	for len(b) > 0 {
		n, err := w.Write(b)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		b = b[n:]
	}
	return nil
}

func readFrame(r io.Reader, max int) ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(header[:])
	if n < 2 || n > uint32(max) {
		return nil, fmt.Errorf("transport: invalid frame length %d", n)
	}
	body := make([]byte, int(n))
	_, err := io.ReadFull(r, body)
	return body, err
}

var _ Transport = (*TCPTransport)(nil)
