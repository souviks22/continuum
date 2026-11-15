package rpc_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"testing"

	"github.com/souviks22/continuum-go/internal/rpc"
)

func mustEncode(t *testing.T, m rpc.Message) []byte {
	t.Helper()
	b, err := m.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return b
}

func sameMessage(a, b rpc.Message) bool {
	return a.Kind == b.Kind && a.ID == b.ID && a.Method == b.Method &&
		a.Status == b.Status && bytes.Equal(a.Payload, b.Payload)
}

func TestMessageRoundTrips(t *testing.T) {
	cases := []rpc.Message{
		{Kind: rpc.KindRequest, ID: 7, Method: "Ping", Payload: []byte("hi")},
		{Kind: rpc.KindRequest, ID: 0, Method: "M", Payload: nil},
		{Kind: rpc.KindRequest, ID: ^uint64(0), Method: strings.Repeat("m", rpc.MaxMethodLen), Payload: []byte{0, 255, 0}},
		{Kind: rpc.KindReply, ID: 300, Status: rpc.StatusOK, Payload: bytes.Repeat([]byte{9}, 70000)},
		{Kind: rpc.KindReply, ID: 1, Status: rpc.StatusUnknownMethod},
		{Kind: rpc.KindReply, ID: 2, Status: rpc.StatusError, Payload: []byte("boom")},
		{Kind: rpc.KindNotify, Method: "raft", Payload: []byte("msg")},
	}
	for _, m := range cases {
		got, err := rpc.Decode(mustEncode(t, m))
		if err != nil {
			t.Fatalf("decode %+v: %v", m, err)
		}
		if !sameMessage(got, m) {
			t.Fatalf("round trip mismatch: got %+v, want %+v", got, m)
		}
	}
}

func TestEncodeRejectsInvalidMessages(t *testing.T) {
	bad := []rpc.Message{
		{Kind: 0, Method: "M"},
		{Kind: 9, Method: "M"},
		{Kind: rpc.KindRequest, Method: ""},
		{Kind: rpc.KindRequest, Method: strings.Repeat("m", rpc.MaxMethodLen+1)},
		{Kind: rpc.KindRequest, Method: "M", Status: rpc.StatusError},
		{Kind: rpc.KindReply, Method: "M"},
		{Kind: rpc.KindReply, Status: 77},
		{Kind: rpc.KindNotify, Method: ""},
		{Kind: rpc.KindNotify, Method: "M", Status: rpc.StatusError},
	}
	for _, m := range bad {
		if _, err := m.Encode(); !errors.Is(err, rpc.ErrMalformed) {
			t.Fatalf("%+v: err=%v, want ErrMalformed", m, err)
		}
	}
}

func TestDecodeRejectsUnsupportedVersion(t *testing.T) {
	b := mustEncode(t, rpc.Message{Kind: rpc.KindRequest, ID: 1, Method: "M"})
	b[0] = rpc.WireVersion + 1
	if _, err := rpc.Decode(b); !errors.Is(err, rpc.ErrUnsupportedVersion) {
		t.Fatalf("err=%v, want ErrUnsupportedVersion", err)
	}
}

func TestDecodeRejectsEveryTruncation(t *testing.T) {
	full := mustEncode(t, rpc.Message{Kind: rpc.KindRequest, ID: 12345, Method: "AppendEntries", Payload: []byte("payload-bytes")})
	for n := 0; n < len(full); n++ {
		if _, err := rpc.Decode(full[:n]); err == nil {
			t.Fatalf("truncation to %d/%d bytes decoded successfully", n, len(full))
		}
	}
}

func TestDecodeRejectsTrailingBytes(t *testing.T) {
	b := mustEncode(t, rpc.Message{Kind: rpc.KindRequest, ID: 1, Method: "M"})
	if _, err := rpc.Decode(append(b, 0)); !errors.Is(err, rpc.ErrMalformed) {
		t.Fatalf("err=%v, want ErrMalformed", err)
	}
}

func TestDecodeRejectsHugeLengthPrefixWithoutAllocating(t *testing.T) {
	// header, id=1, method length = 2^62 with no bytes behind it.
	b := []byte{rpc.WireVersion, byte(rpc.KindRequest), 0, 1}
	b = binary.AppendUvarint(b, 1<<62)
	if _, err := rpc.Decode(b); !errors.Is(err, rpc.ErrMalformed) {
		t.Fatalf("err=%v, want ErrMalformed", err)
	}
	// same for payload length
	b = []byte{rpc.WireVersion, byte(rpc.KindRequest), 0, 1, 1, 'M'}
	b = binary.AppendUvarint(b, 1<<62)
	if _, err := rpc.Decode(b); !errors.Is(err, rpc.ErrMalformed) {
		t.Fatalf("err=%v, want ErrMalformed", err)
	}
}

func TestDecodeRejectsStructurallyInvalidMessages(t *testing.T) {
	// A request with an empty method, and a reply that carries a method, are
	// well-framed but invalid; Decode must apply the same rules as Encode.
	emptyMethodReq := []byte{rpc.WireVersion, byte(rpc.KindRequest), 0, 1, 0, 0}
	replyWithMethod := []byte{rpc.WireVersion, byte(rpc.KindReply), 0, 1, 1, 'M', 0}
	for _, b := range [][]byte{emptyMethodReq, replyWithMethod} {
		if _, err := rpc.Decode(b); !errors.Is(err, rpc.ErrMalformed) {
			t.Fatalf("% x: err=%v, want ErrMalformed", b, err)
		}
	}
}

// FuzzDecode: Decode must never panic, and anything it accepts must survive an
// encode/decode round trip. (Re-encoding need not be byte-identical because a
// varint may arrive in a non-minimal form.)
func FuzzDecode(f *testing.F) {
	f.Add(mustEncodeSeed(rpc.Message{Kind: rpc.KindRequest, ID: 1, Method: "M", Payload: []byte("x")}))
	f.Add(mustEncodeSeed(rpc.Message{Kind: rpc.KindReply, ID: 9, Status: rpc.StatusError, Payload: []byte("e")}))
	f.Add([]byte{})
	f.Add([]byte{1, 1, 0, 0x80, 0x00, 1, 'M', 0})
	f.Fuzz(func(t *testing.T, data []byte) {
		m, err := rpc.Decode(data)
		if err != nil {
			return
		}
		again, err := rpc.Decode(mustEncode(t, m))
		if err != nil || !sameMessage(m, again) {
			t.Fatalf("accepted message does not round trip: %+v -> %+v (%v)", m, again, err)
		}
	})
}

func mustEncodeSeed(m rpc.Message) []byte {
	b, err := m.Encode()
	if err != nil {
		panic(err)
	}
	return b
}
