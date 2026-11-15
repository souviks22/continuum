package client

import (
	"encoding/binary"
	"errors"
	"hash/crc32"

	"github.com/souviks22/continuum/internal/kv"
)

var errSessionState = errors.New("client: invalid session state")

var sessionStateCRC = crc32.MakeTable(crc32.Castagnoli)

const sessionStateVersion byte = 1

// ExportSessionState returns an opaque, versioned checkpoint of the session
// and any unresolved transaction. Callers should persist it when a transaction
// returns ErrUnknownResult, then restore it after recreating the client and use
// Retry. The checkpoint cannot make an application-level persistence operation
// atomic with a Raft proposal; applications requiring crash recovery must save
// the returned bytes before treating the operation as externally complete.
func (c *Client) ExportSessionState() ([]byte, error) {
	if c.closed {
		return nil, ErrClosed
	}
	if c.session == 0 {
		return nil, ErrNoSession
	}
	if c.busy && len(c.pending) == 0 {
		return nil, ErrBusy // session registration is unresolved
	}
	return marshalSessionState(c.session, c.seq, c.pending)
}

// RestoreSessionState installs a previously exported checkpoint into a fresh
// client. A pending transaction becomes eligible for Retry; no proposal is
// issued until the caller explicitly retries it.
func (c *Client) RestoreSessionState(data []byte) error {
	if c.closed {
		return ErrClosed
	}
	if c.busy || c.session != 0 || c.seq != 0 || len(c.pending) != 0 {
		return ErrBusy
	}
	session, seq, pending, err := unmarshalSessionState(data)
	if err != nil {
		return err
	}
	c.session, c.seq, c.pending = session, seq, pending
	if len(pending) != 0 {
		c.busy = true
		c.unknown = true
	}
	return nil
}

func marshalSessionState(session, seq uint64, pending []byte) ([]byte, error) {
	if session == 0 || seq == ^uint64(0) || (len(pending) != 0 && kv.ValidateSessionCommand(pending, session, seq+1) != nil) {
		return nil, errSessionState
	}
	b := make([]byte, 0, 1+8+8+4+len(pending)+4)
	b = append(b, sessionStateVersion)
	b = binary.AppendUvarint(b, session)
	b = binary.AppendUvarint(b, seq)
	b = binary.AppendUvarint(b, uint64(len(pending)))
	b = append(b, pending...)
	b = binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, sessionStateCRC))
	return b, nil
}

func unmarshalSessionState(data []byte) (uint64, uint64, []byte, error) {
	if len(data) < 1+1+1+1+4 || data[0] != sessionStateVersion {
		return 0, 0, nil, errSessionState
	}
	body := data[:len(data)-4]
	if crc32.Checksum(body, sessionStateCRC) != binary.LittleEndian.Uint32(data[len(data)-4:]) {
		return 0, 0, nil, errSessionState
	}
	data = body[1:]
	session, n := binary.Uvarint(data)
	if n <= 0 {
		return 0, 0, nil, errSessionState
	}
	data = data[n:]
	seq, n := binary.Uvarint(data)
	if n <= 0 {
		return 0, 0, nil, errSessionState
	}
	data = data[n:]
	size, n := binary.Uvarint(data)
	if n <= 0 {
		return 0, 0, nil, errSessionState
	}
	data = data[n:]
	if session == 0 || seq == ^uint64(0) || size != uint64(len(data)) {
		return 0, 0, nil, errSessionState
	}
	pending := append([]byte(nil), data...)
	if len(pending) != 0 && kv.ValidateSessionCommand(pending, session, seq+1) != nil {
		return 0, 0, nil, errSessionState
	}
	return session, seq, pending, nil
}
