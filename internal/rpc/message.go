// Package rpc is request/reply RPC over a transport.Transport, with
// clock-driven timeouts and a versioned binary wire envelope.
package rpc

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// WireVersion is the envelope version this build speaks. Decode rejects every
// other version rather than guessing, so rolling upgrades fail loudly (and are
// counted by the caller's OnError hook) instead of corrupting state. Any change
// to the layout below must bump it.
const WireVersion = 1

// MaxMethodLen bounds method names so a hostile length prefix cannot force a
// large allocation.
const MaxMethodLen = 255

type Kind uint8

const (
	KindRequest Kind = 1
	KindReply   Kind = 2
	// KindNotify is a one-way message: no reply is sent and none is awaited.
	// It carries a method and payload like a request. Raft traffic uses it,
	// because Raft already tolerates loss and needs no per-message reply.
	// (Added while the format is still pre-release; any change after the first
	// deployment must bump WireVersion.)
	KindNotify Kind = 3
)

// Status is carried on replies. Requests always carry StatusOK.
type Status uint8

const (
	StatusOK            Status = 0
	StatusUnknownMethod Status = 1 // peer does not implement the method (e.g. older version)
	StatusError         Status = 2 // handler returned an error; Payload holds its text
)

// Message is one RPC envelope. Payload is opaque: each method owns its codec
// (protobuf, hand-rolled, ...), so the envelope can stay tiny and stable.
//
// Layout (all integers after the header are unsigned varints):
//
//	version u8 | kind u8 | status u8 | id | len(method) | method | len(payload) | payload
type Message struct {
	Kind    Kind
	ID      uint64
	Method  string // required on requests, empty on replies
	Status  Status
	Payload []byte
}

var (
	ErrUnsupportedVersion = errors.New("rpc: unsupported wire version")
	ErrMalformed          = errors.New("rpc: malformed message")
)

func (m Message) validate() error {
	switch m.Kind {
	case KindRequest, KindNotify:
		if m.Method == "" || len(m.Method) > MaxMethodLen {
			return fmt.Errorf("%w: request method must be 1..%d bytes", ErrMalformed, MaxMethodLen)
		}
		if m.Status != StatusOK {
			return fmt.Errorf("%w: request must have StatusOK", ErrMalformed)
		}
	case KindReply:
		if m.Method != "" {
			return fmt.Errorf("%w: reply must not carry a method", ErrMalformed)
		}
		if m.Status > StatusError {
			return fmt.Errorf("%w: unknown status %d", ErrMalformed, m.Status)
		}
	default:
		return fmt.Errorf("%w: unknown kind %d", ErrMalformed, m.Kind)
	}
	return nil
}

// Encode serializes the message, or reports why it is not a valid message.
func (m Message) Encode() ([]byte, error) {
	if err := m.validate(); err != nil {
		return nil, err
	}
	b := make([]byte, 0, 3+3*binary.MaxVarintLen64+len(m.Method)+len(m.Payload))
	b = append(b, WireVersion, byte(m.Kind), byte(m.Status))
	b = binary.AppendUvarint(b, m.ID)
	b = binary.AppendUvarint(b, uint64(len(m.Method)))
	b = append(b, m.Method...)
	b = binary.AppendUvarint(b, uint64(len(m.Payload)))
	b = append(b, m.Payload...)
	return b, nil
}

// Decode parses and validates a message. The returned Payload aliases b; the
// caller owns b and must not modify it while the Message is in use. Decode never
// panics and never allocates based on an unvalidated length prefix.
func Decode(b []byte) (Message, error) {
	if len(b) < 3 {
		return Message{}, fmt.Errorf("%w: short header", ErrMalformed)
	}
	if b[0] != WireVersion {
		return Message{}, fmt.Errorf("%w: got %d, want %d", ErrUnsupportedVersion, b[0], WireVersion)
	}
	m := Message{Kind: Kind(b[1]), Status: Status(b[2])}
	rest := b[3:]

	id, n := binary.Uvarint(rest)
	if n <= 0 {
		return Message{}, fmt.Errorf("%w: bad id", ErrMalformed)
	}
	m.ID, rest = id, rest[n:]

	method, rest, err := takeBytes(rest)
	if err != nil {
		return Message{}, fmt.Errorf("method: %w", err)
	}
	m.Method = string(method)

	m.Payload, rest, err = takeBytes(rest)
	if err != nil {
		return Message{}, fmt.Errorf("payload: %w", err)
	}
	if len(rest) != 0 {
		return Message{}, fmt.Errorf("%w: %d trailing bytes", ErrMalformed, len(rest))
	}
	if err := m.validate(); err != nil {
		return Message{}, err
	}
	return m, nil
}

// takeBytes reads a uvarint length then that many bytes, checking the length
// against what is actually left in b before slicing.
func takeBytes(b []byte) (val, rest []byte, err error) {
	l, n := binary.Uvarint(b)
	if n <= 0 {
		return nil, nil, fmt.Errorf("%w: bad length", ErrMalformed)
	}
	b = b[n:]
	if l > uint64(len(b)) {
		return nil, nil, fmt.Errorf("%w: length %d exceeds remaining %d", ErrMalformed, l, len(b))
	}
	return b[:l], b[l:], nil
}
