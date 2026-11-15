package transport_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/souviks22/continuum/internal/rpc"
	"github.com/souviks22/continuum/internal/sim"
	"github.com/souviks22/continuum/internal/transport"
)

func TestTCPTransportCarriesRPCBetweenProcesses(t *testing.T) {
	addrA, addrB := freeAddr(t), freeAddr(t)
	peers := map[string]string{"a": addrA, "b": addrB}
	dispatch := func(f func()) { f() } // production dispatchers enqueue on a serial event loop
	a, err := transport.NewTCPTransport(transport.TCPOptions{
		Name: "a", ListenAddr: addrA, Peers: peers, Dispatch: dispatch, InsecureLocal: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	b, err := transport.NewTCPTransport(transport.TCPOptions{
		Name: "b", ListenAddr: addrB, Peers: peers, Dispatch: dispatch, InsecureLocal: true,
	})
	if err != nil {
		a.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { a.Close(); b.Close() })

	clockA := sim.NewScheduler()
	clockB := sim.NewScheduler()
	eA := rpc.NewEndpoint(a, clockA, rpc.Options{IDBase: 10})
	eB := rpc.NewEndpoint(b, clockB, rpc.Options{IDBase: 20})
	if err := eB.Register("echo", func(from string, payload []byte) ([]byte, error) {
		if from != "a" {
			return nil, errors.New("unexpected caller")
		}
		return append([]byte("reply:"), payload...), nil
	}); err != nil {
		t.Fatal(err)
	}
	replies := make(chan []byte, 1)
	if err := eA.Call("b", "echo", []byte("hello"), time.Second, func(payload []byte, err error) {
		if err != nil {
			replies <- []byte("error:" + err.Error())
			return
		}
		replies <- payload
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-replies:
		if string(got) != "reply:hello" {
			t.Fatalf("RPC reply %q", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for TCP RPC reply")
	}
}

func TestTCPTransportReconnectsAfterPeerProcessRestart(t *testing.T) {
	addrA, addrB := freeAddr(t), freeAddr(t)
	peers := map[string]string{"a": addrA, "b": addrB}
	dispatch := func(f func()) { f() }
	peerClosed := make(chan struct{}, 4)
	b, err := transport.NewTCPTransport(transport.TCPOptions{
		Name: "b", ListenAddr: addrB, Peers: peers, Dispatch: dispatch, InsecureLocal: true,
		OnError: func(err error) {
			if errors.Is(err, io.EOF) || strings.Contains(err.Error(), "read frame: EOF") {
				select {
				case peerClosed <- struct{}{}:
				default:
				}
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	eB := rpc.NewEndpoint(b, sim.NewScheduler(), rpc.Options{IDBase: 30})
	if err := eB.Register("echo", func(_ string, payload []byte) ([]byte, error) {
		return append([]byte("reply:"), payload...), nil
	}); err != nil {
		t.Fatal(err)
	}

	openA := func(id uint64) (*transport.TCPTransport, *rpc.Endpoint) {
		t.Helper()
		a, err := transport.NewTCPTransport(transport.TCPOptions{
			Name: "a", ListenAddr: addrA, Peers: peers, Dispatch: dispatch, InsecureLocal: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		return a, rpc.NewEndpoint(a, sim.NewScheduler(), rpc.Options{IDBase: id})
	}
	call := func(endpoint *rpc.Endpoint, payload string) {
		t.Helper()
		reply := make(chan string, 1)
		if err := endpoint.Call("b", "echo", []byte(payload), time.Second, func(b []byte, err error) {
			if err != nil {
				reply <- "error: " + err.Error()
				return
			}
			reply <- string(b)
		}); err != nil {
			t.Fatal(err)
		}
		select {
		case got := <-reply:
			if got != "reply:"+payload {
				t.Fatalf("RPC reply %q", got)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for TCP RPC reply")
		}
	}

	a1, eA1 := openA(40)
	call(eA1, "first")
	if err := a1.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-peerClosed:
	case <-time.After(time.Second):
		t.Fatal("server did not observe the first client process exit")
	}
	// Give the server's peer writer time to consume its disconnect signal before
	// the restarted process sends a request that needs an RPC reply.
	time.Sleep(20 * time.Millisecond)
	a2, eA2 := openA(50)
	defer a2.Close()
	call(eA2, "after-restart")
}

func TestTCPTransportRequiresExplicitSecurityModeAndBoundsFrames(t *testing.T) {
	addr := freeAddr(t)
	peers := map[string]string{"a": addr, "b": freeAddr(t)}
	base := transport.TCPOptions{Name: "a", ListenAddr: addr, Peers: peers, Dispatch: func(func()) {}}
	if _, err := transport.NewTCPTransport(base); err == nil {
		t.Fatal("plaintext transport was enabled without explicit opt-in")
	}
	base.InsecureLocal = true
	base.MaxFrameBytes = 64
	tr, err := transport.NewTCPTransport(base)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	if err := tr.Send("missing", []byte("x")); err == nil {
		t.Fatal("unknown peer accepted")
	}
	if err := tr.Send("b", make([]byte, 64)); err == nil {
		t.Fatal("oversized frame accepted")
	}
	if err := tr.Send("b", []byte("ok")); err != nil {
		t.Fatalf("valid frame rejected: %v", err)
	}
	if err := tr.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tr.Send("b", []byte("after-close")); !errors.Is(err, transport.ErrClosed) {
		t.Fatalf("Send after Close: %v", err)
	}
}

func TestTCPTransportAuthenticatesMutualTLSPeers(t *testing.T) {
	addrA, addrB := freeAddr(t), freeAddr(t)
	peers := map[string]string{"a": addrA, "b": addrB}
	root, certA, certB := testCertificates(t)
	dispatch := func(f func()) { f() }
	newNode := func(name, addr string, cert tls.Certificate) *transport.TCPTransport {
		tr, err := transport.NewTCPTransport(transport.TCPOptions{
			Name: name, ListenAddr: addr, Peers: peers, Dispatch: dispatch,
			TLSClient: func(string) *tls.Config {
				return &tls.Config{Certificates: []tls.Certificate{cert}, RootCAs: root, ServerName: "localhost"}
			},
			TLSServer: &tls.Config{
				Certificates: []tls.Certificate{cert}, ClientAuth: tls.RequireAndVerifyClientCert,
				ClientCAs: root,
			},
			PeerName: func(state tls.ConnectionState) (string, error) {
				if len(state.PeerCertificates) != 1 {
					return "", errors.New("peer certificate missing")
				}
				return state.PeerCertificates[0].Subject.CommonName, nil
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		return tr
	}
	a, b := newNode("a", addrA, certA), newNode("b", addrB, certB)
	t.Cleanup(func() { a.Close(); b.Close() })
	eA := rpc.NewEndpoint(a, sim.NewScheduler(), rpc.Options{})
	eB := rpc.NewEndpoint(b, sim.NewScheduler(), rpc.Options{})
	if err := eB.Register("secure", func(from string, payload []byte) ([]byte, error) {
		if from != "a" {
			return nil, errors.New("certificate identity did not match sender")
		}
		return payload, nil
	}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	if err := eA.Call("b", "secure", []byte("verified"), time.Second, func(payload []byte, err error) {
		if err == nil && string(payload) != "verified" {
			err = errors.New("unexpected secure reply")
		}
		done <- err
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for mutual TLS RPC reply")
	}
}

func TestTCPTransportBoundsQueuedInboundDispatch(t *testing.T) {
	addrA, addrB := freeAddr(t), freeAddr(t)
	peers := map[string]string{"a": addrA, "b": addrB}
	dispatches := make(chan func(), 4)
	a, err := transport.NewTCPTransport(transport.TCPOptions{
		Name: "a", ListenAddr: addrA, Peers: peers, Dispatch: func(f func()) { f() }, InsecureLocal: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	b, err := transport.NewTCPTransport(transport.TCPOptions{
		Name: "b", ListenAddr: addrB, Peers: peers, Dispatch: func(f func()) { dispatches <- f },
		InsecureLocal: true, MaxInboundPending: 1,
	})
	if err != nil {
		a.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { a.Close(); b.Close() })
	received := make(chan string, 2)
	b.SetReceiveHandler(func(_ string, payload []byte) { received <- string(payload) })
	if err := a.Send("b", []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := a.Send("b", []byte("two")); err != nil {
		t.Fatal(err)
	}
	var first func()
	select {
	case first = <-dispatches:
	case <-time.After(2 * time.Second):
		t.Fatal("first inbound dispatch was not queued")
	}
	select {
	case <-dispatches:
		t.Fatal("more inbound work was queued than MaxInboundPending")
	case <-time.After(50 * time.Millisecond):
	}
	first()
	select {
	case second := <-dispatches:
		second()
	case <-time.After(2 * time.Second):
		t.Fatal("second inbound dispatch did not resume after capacity freed")
	}
	for _, want := range []string{"one", "two"} {
		select {
		case got := <-received:
			if got != want {
				t.Fatalf("got %q, want %q", got, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("handler did not receive %q", want)
		}
	}
}

func testCertificates(t *testing.T) (*x509.CertPool, tls.Certificate, tls.Certificate) {
	t.Helper()
	now := time.Now()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "continuum test CA"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true,
		BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	makeLeaf := func(serial int64, name string) tls.Certificate {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		template := &x509.Certificate{
			SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: name},
			NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
			DNSNames: []string{"localhost"}, KeyUsage: x509.KeyUsageDigitalSignature,
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		}
		der, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
		if err != nil {
			t.Fatal(err)
		}
		keyDER, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			t.Fatal(err)
		}
		cert, err := tls.X509KeyPair(
			pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
			pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
		)
		if err != nil {
			t.Fatal(err)
		}
		return cert
	}
	return roots, makeLeaf(2, "a"), makeLeaf(3, "b")
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}
