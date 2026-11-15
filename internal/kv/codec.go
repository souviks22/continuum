package kv

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Wire format (version 1). Integers are unsigned varints; byte strings are a
// varint length then the bytes.
//
//	command: version u8 | kind u8 | body
//	  txn:     nCompares {target u8, result u8, key, (value | int)} nSuccess ops nFailure ops
//	  compact: revision
//	  register: (empty)
//	  sessiontxn: session seq (both non-zero) then the txn body above
//	op: type u8, then
//	  put:    key value prevKV u8
//	  delete: key rangeEnd prevKV u8
//	  range:  key rangeEnd limit revision flags u8 (bit0 keysOnly, bit1 countOnly)
//
//	result: version u8 | status u8 | (status != ok: message) |
//	        succeeded u8 revision nResults {type u8, body}
const (
	wireVersion = 1

	kindTxn                   byte = 1
	kindCompact               byte = 2
	kindRegister              byte = 3
	kindSessionTxn            byte = 4
	kindLeaseGrant            byte = 5
	kindLeaseKeep             byte = 6
	kindLeaseDrop             byte = 7
	kindLeaseTick             byte = 8
	kindActivateCompatibility byte = 9

	statusOK                byte = 0
	statusInvalid           byte = 1
	statusCompacted         byte = 2
	statusFutureRev         byte = 3
	statusSession           byte = 4
	statusStale             byte = 5
	statusNotCached         byte = 6
	statusLease             byte = 7
	statusLeaseLimit        byte = 8
	statusFeatureInactive   byte = 9
	statusMembershipChanged byte = 10
	maxStatus                    = statusMembershipChanged
)

type enc struct{ b []byte }

func (e *enc) u8(v byte)      { e.b = append(e.b, v) }
func (e *enc) uv(v uint64)    { e.b = binary.AppendUvarint(e.b, v) }
func (e *enc) bytes(p []byte) { e.uv(uint64(len(p))); e.b = append(e.b, p...) }
func (e *enc) bool(v bool) {
	if v {
		e.u8(1)
	} else {
		e.u8(0)
	}
}

// dec reads with a sticky error and never allocates from an unchecked length.
type dec struct {
	b   []byte
	err error
}

func (d *dec) fail(format string, a ...any) {
	if d.err == nil {
		d.err = invalid(format, a...)
	}
}

func (d *dec) u8() byte {
	if d.err != nil {
		return 0
	}
	if len(d.b) == 0 {
		d.fail("truncated")
		return 0
	}
	v := d.b[0]
	d.b = d.b[1:]
	return v
}

func (d *dec) uv() uint64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Uvarint(d.b)
	if n <= 0 {
		d.fail("bad varint")
		return 0
	}
	d.b = d.b[n:]
	return v
}

// bytes returns a slice aliasing the input.
func (d *dec) bytes(max int) []byte {
	n := d.uv()
	if d.err != nil {
		return nil
	}
	if n > uint64(max) || n > uint64(len(d.b)) {
		d.fail("byte string of %d exceeds limit %d or remaining %d", n, max, len(d.b))
		return nil
	}
	v := d.b[:n]
	d.b = d.b[n:]
	return v
}

func (d *dec) flag() bool {
	v := d.u8()
	if v > 1 {
		d.fail("flag byte %d", v)
	}
	return v == 1
}

// count reads a list length and checks it against a limit and against the bytes
// left (every element takes at least one byte), so no allocation is attacker-sized.
func (d *dec) count(max int) int {
	n := d.uv()
	if d.err != nil {
		return 0
	}
	if n > uint64(max) || n > uint64(len(d.b)) {
		d.fail("list of %d exceeds limit %d or remaining %d", n, max, len(d.b))
		return 0
	}
	return int(n)
}

// command is a decoded replicated command. kindTxn and kindSessionTxn set txn
// (the latter also session and seq); kindCompact sets compact; kindRegister
// sets nothing.
type command struct {
	kind               byte
	txn                *Txn
	compact            uint64
	session            uint64
	seq                uint64
	leaseID            uint64
	leaseTTL           uint64
	compatibilityLevel byte
	membershipDigest   [32]byte
}

// Encode serializes the transaction as a command, enforcing lim (the zero Limits
// means the consensus limits). Use it before proposing so an oversized or
// malformed request is refused locally instead of replicated.
//
// A command encoded this way is not deduplicated: if it is proposed twice it
// applies twice. Writes that may be retried should use EncodeSession.
func (t Txn) Encode(lim Limits) ([]byte, error) {
	return t.encode(&enc{b: []byte{wireVersion, kindTxn}}, lim)
}

// EncodeSession serializes the transaction as request seq of a session obtained
// by registering (see EncodeRegister). Proposing the same bytes again, for
// instance after a timeout, returns the first attempt's result instead of
// applying again. seq must be non-zero and greater than any earlier request of
// the session.
func (t Txn) EncodeSession(session, seq uint64, lim Limits) ([]byte, error) {
	if session == 0 || seq == 0 {
		return nil, invalid("session and sequence number must be non-zero")
	}
	e := &enc{b: []byte{wireVersion, kindSessionTxn}}
	e.uv(session)
	e.uv(seq)
	return t.encode(e, lim)
}

// EncodeRegister serializes a command that opens a session. Its result decodes
// with DecodeSession.
func EncodeRegister() []byte { return []byte{wireVersion, kindRegister} }

func (t Txn) encode(e *enc, lim Limits) ([]byte, error) {
	lim = lim.orDefault()
	e.uv(uint64(len(t.Compares)))
	for _, c := range t.Compares {
		e.u8(byte(c.Target))
		e.u8(byte(c.Result))
		e.bytes(c.Key)
		if c.Target == CompareValue {
			e.bytes(c.Value)
		} else {
			e.uv(c.Int)
		}
	}
	for _, ops := range [][]Op{t.Success, t.Failure} {
		e.uv(uint64(len(ops)))
		for _, op := range ops {
			e.u8(byte(op.Type))
			switch op.Type {
			case OpPut, OpPutLease:
				e.bytes(op.Key)
				e.bytes(op.Value)
				e.bool(op.PrevKV)
				if op.Type == OpPutLease {
					if op.LeaseID == 0 {
						return nil, invalid("put lease ID must be non-zero")
					}
					e.uv(op.LeaseID)
				}
			case OpDeleteRange:
				e.bytes(op.Key)
				e.bytes(op.RangeEnd)
				e.bool(op.PrevKV)
			case OpRange:
				e.bytes(op.Key)
				e.bytes(op.RangeEnd)
				e.uv(op.Limit)
				e.uv(op.Revision)
				var flags byte
				if op.KeysOnly {
					flags |= 1
				}
				if op.CountOnly {
					flags |= 2
				}
				e.u8(flags)
			default:
				return nil, invalid("unknown op type %d", op.Type)
			}
		}
	}
	if err := checkCommand(e.b, lim); err != nil {
		return nil, err
	}
	return e.b, nil
}

// EncodeCompact serializes a compaction command.
func EncodeCompact(revision uint64) []byte {
	e := &enc{b: []byte{wireVersion, kindCompact}}
	e.uv(revision)
	return e.b
}

// ValidateSessionCommand checks that b is a complete session transaction for
// the given session and sequence. It is used when restoring client retry state
// so an opaque checkpoint cannot replay an unrelated command.
func ValidateSessionCommand(b []byte, session, seq uint64) error {
	cmd, err := decodeCommand(b)
	if err != nil {
		return err
	}
	if cmd.kind != kindSessionTxn || cmd.session != session || cmd.seq != seq {
		return invalid("session checkpoint command does not match session %d sequence %d", session, seq)
	}
	return nil
}

// EncodeLeaseGrant creates a lease whose ID will be the grant command's Raft
// log index. TTL is a count of replicated LeaseTick commands.
func EncodeLeaseGrant(ttl uint64) ([]byte, error) {
	if ttl == 0 || ttl > MaxLeaseTTL {
		return nil, invalid("lease TTL must be in [1,%d] ticks", MaxLeaseTTL)
	}
	e := &enc{b: []byte{wireVersion, kindLeaseGrant}}
	e.uv(ttl)
	return e.b, nil
}

// EncodeLeaseKeepAlive resets a lease's remaining lifetime to its granted TTL.
func EncodeLeaseKeepAlive(id uint64) ([]byte, error) { return encodeLeaseID(kindLeaseKeep, id) }

// EncodeLeaseRevoke removes a lease and all keys attached to it.
func EncodeLeaseRevoke(id uint64) ([]byte, error) { return encodeLeaseID(kindLeaseDrop, id) }

func encodeLeaseID(kind byte, id uint64) ([]byte, error) {
	if id == 0 {
		return nil, invalid("lease ID must be non-zero")
	}
	e := &enc{b: []byte{wireVersion, kind}}
	e.uv(id)
	return e.b, nil
}

// EncodeLeaseTick advances every active lease by one replicated time unit.
func EncodeLeaseTick() []byte { return []byte{wireVersion, kindLeaseTick} }

// EncodeActivateCompatibility creates a replicated feature activation command
// bound to the exact Raft membership checked by the caller.
func EncodeActivateCompatibility(level uint8, digest [32]byte) ([]byte, error) {
	if level < CompatibilityLevelBase || level > MaxCompatibilityLevel {
		return nil, invalid("compatibility level %d is unsupported", level)
	}
	e := &enc{b: []byte{wireVersion, kindActivateCompatibility, byte(level)}}
	e.b = append(e.b, digest[:]...)
	return e.b, nil
}

// checkCommand decodes b fully under lim; it is the single definition of a valid command.
func checkCommand(b []byte, lim Limits) error {
	_, err := decodeCommandLimited(b, lim)
	return err
}

func decodeCommand(b []byte) (*command, error) { return decodeCommandLimited(b, Limits{}) }

func decodeCommandLimited(b []byte, lim Limits) (*command, error) {
	lim = lim.orDefault()
	if len(b) > lim.MaxRequestSize {
		return nil, invalid("request of %d bytes exceeds %d", len(b), lim.MaxRequestSize)
	}
	d := &dec{b: b}
	if v := d.u8(); d.err == nil && v != wireVersion {
		return nil, invalid("unsupported command version %d", v)
	}
	cmd := &command{kind: d.u8()}
	switch cmd.kind {
	case kindCompact:
		cmd.compact = d.uv()
	case kindTxn:
		cmd.txn = decodeTxn(d, lim)
	case kindRegister:
		// no body
	case kindSessionTxn:
		cmd.session, cmd.seq = d.uv(), d.uv()
		if d.err == nil && (cmd.session == 0 || cmd.seq == 0) {
			d.fail("session and sequence number must be non-zero")
		}
		cmd.txn = decodeTxn(d, lim)
	case kindLeaseGrant:
		cmd.leaseTTL = d.uv()
		if d.err == nil && (cmd.leaseTTL == 0 || cmd.leaseTTL > MaxLeaseTTL) {
			d.fail("lease TTL must be in [1,%d] ticks", MaxLeaseTTL)
		}
	case kindLeaseKeep, kindLeaseDrop:
		cmd.leaseID = d.uv()
		if d.err == nil && cmd.leaseID == 0 {
			d.fail("lease ID must be non-zero")
		}
	case kindLeaseTick:
	case kindActivateCompatibility:
		cmd.compatibilityLevel = d.u8()
		if d.err == nil && (cmd.compatibilityLevel < CompatibilityLevelBase || cmd.compatibilityLevel > MaxCompatibilityLevel) {
			d.fail("compatibility level %d is unsupported", cmd.compatibilityLevel)
		}
		if d.err == nil {
			if len(d.b) != len(cmd.membershipDigest) {
				d.fail("membership fingerprint must be %d bytes", len(cmd.membershipDigest))
			} else {
				copy(cmd.membershipDigest[:], d.b)
				d.b = d.b[len(cmd.membershipDigest):]
			}
		}
	default:
		if d.err == nil {
			d.fail("unknown command kind %d", cmd.kind)
		}
	}
	if d.err == nil && len(d.b) != 0 {
		d.fail("%d trailing bytes", len(d.b))
	}
	if d.err != nil {
		return nil, d.err
	}
	return cmd, nil
}

func decodeTxn(d *dec, lim Limits) *Txn {
	t := &Txn{}
	n := d.count(lim.MaxTxnOps)
	for i := 0; i < n && d.err == nil; i++ {
		c := Compare{Target: CompareTarget(d.u8()), Result: CompareResult(d.u8())}
		c.Key = d.bytes(lim.MaxKeySize)
		if len(c.Key) == 0 && d.err == nil {
			d.fail("compare with empty key")
		}
		switch c.Target {
		case CompareVersion, CompareCreate, CompareMod:
			c.Int = d.uv()
		case CompareValue:
			c.Value = d.bytes(lim.MaxValueSize)
		default:
			d.fail("unknown compare target %d", c.Target)
		}
		if c.Result < Equal || c.Result > Less {
			d.fail("unknown compare result %d", c.Result)
		}
		t.Compares = append(t.Compares, c)
	}
	for _, dst := range []*[]Op{&t.Success, &t.Failure} {
		n := d.count(lim.MaxTxnOps)
		for i := 0; i < n && d.err == nil; i++ {
			op := Op{Type: OpType(d.u8())}
			switch op.Type {
			case OpPut, OpPutLease:
				op.Key = d.bytes(lim.MaxKeySize)
				op.Value = d.bytes(lim.MaxValueSize)
				op.PrevKV = d.flag()
				if op.Type == OpPutLease {
					op.LeaseID = d.uv()
					if op.LeaseID == 0 && d.err == nil {
						d.fail("put lease ID must be non-zero")
					}
				}
				if len(op.Key) == 0 && d.err == nil {
					d.fail("put with empty key")
				}
			case OpDeleteRange:
				op.Key = d.bytes(lim.MaxKeySize)
				op.RangeEnd = d.bytes(lim.MaxKeySize)
				op.PrevKV = d.flag()
				if len(op.Key) == 0 && d.err == nil {
					d.fail("delete with empty key")
				}
			case OpRange:
				op.Key = d.bytes(lim.MaxKeySize)
				op.RangeEnd = d.bytes(lim.MaxKeySize)
				op.Limit = d.uv()
				op.Revision = d.uv()
				flags := d.u8()
				if flags > 3 && d.err == nil {
					d.fail("range flags %d", flags)
				}
				op.KeysOnly, op.CountOnly = flags&1 != 0, flags&2 != 0
				if len(op.Key) == 0 && d.err == nil {
					d.fail("range with empty key")
				}
			default:
				d.fail("unknown op type %d", op.Type)
			}
			*dst = append(*dst, op)
		}
	}
	return t
}

// -- results ----------------------------------------------------------------

func encodeKV(e *enc, kv KeyValue, keysOnly bool) {
	e.bytes(kv.Key)
	if keysOnly {
		e.bytes(nil)
	} else {
		e.bytes(kv.Value)
	}
	e.uv(kv.CreateRevision)
	e.uv(kv.ModRevision)
	e.uv(kv.Version)
}

func decodeKV(d *dec) KeyValue {
	return KeyValue{
		Key:            bytesClone(d.bytes(MaxKeySize)),
		Value:          bytesClone(d.bytes(MaxValueSize)),
		CreateRevision: d.uv(), ModRevision: d.uv(), Version: d.uv(),
	}
}

func bytesClone(b []byte) []byte {
	if b == nil {
		return nil
	}
	return append([]byte{}, b...)
}

func encodeResult(r TxnResult) []byte {
	e := &enc{b: []byte{wireVersion, statusOK}}
	e.bool(r.Succeeded)
	e.uv(r.Revision)
	e.uv(uint64(len(r.Results)))
	for _, o := range r.Results {
		e.u8(byte(o.Type))
		switch o.Type {
		case OpPut:
			e.bool(o.PrevKV != nil)
			if o.PrevKV != nil {
				encodeKV(e, *o.PrevKV, false)
			}
		case OpDeleteRange:
			e.uv(o.Deleted)
			e.uv(uint64(len(o.PrevKVs)))
			for _, kv := range o.PrevKVs {
				encodeKV(e, kv, false)
			}
		case OpRange:
			e.uv(o.Count)
			e.bool(o.More)
			e.uv(uint64(len(o.KVs)))
			for _, kv := range o.KVs {
				encodeKV(e, kv, false)
			}
		}
	}
	return e.b
}

func encodeError(err error) []byte {
	status := statusInvalid
	switch {
	case errors.Is(err, ErrCompacted):
		status = statusCompacted
	case errors.Is(err, ErrFutureRevision):
		status = statusFutureRev
	case errors.Is(err, ErrSessionExpired):
		status = statusSession
	case errors.Is(err, ErrStaleRequest):
		status = statusStale
	case errors.Is(err, ErrResultNotCached):
		status = statusNotCached
	case errors.Is(err, ErrLeaseNotFound), errors.Is(err, ErrTooManyLeases):
		status = statusLease
		if errors.Is(err, ErrTooManyLeases) {
			status = statusLeaseLimit
		}
	case errors.Is(err, ErrFeatureInactive):
		status = statusFeatureInactive
	case errors.Is(err, ErrMembershipChanged):
		status = statusMembershipChanged
	}
	e := &enc{b: []byte{wireVersion, status}}
	e.bytes([]byte(err.Error()))
	return e.b
}

// decodeStatus reads the result header. It returns nil for an OK result (the
// body follows in d) and otherwise the error the state machine reported.
func decodeStatus(d *dec) error {
	if v := d.u8(); d.err == nil && v != wireVersion {
		return fmt.Errorf("kv: unsupported result version %d", v)
	}
	status := d.u8()
	if d.err != nil {
		return d.err
	}
	if status > maxStatus {
		return fmt.Errorf("kv: unknown result status %d", status)
	}
	if status == statusOK {
		return nil
	}
	msg := string(d.bytes(1 << 20))
	if d.err != nil {
		return d.err
	}
	sentinel := ErrInvalidCommand
	switch status {
	case statusCompacted:
		sentinel = ErrCompacted
	case statusFutureRev:
		sentinel = ErrFutureRevision
	case statusSession:
		sentinel = ErrSessionExpired
	case statusStale:
		sentinel = ErrStaleRequest
	case statusNotCached:
		sentinel = ErrResultNotCached
	case statusLease:
		sentinel = ErrLeaseNotFound
	case statusLeaseLimit:
		sentinel = ErrTooManyLeases
	case statusFeatureInactive:
		sentinel = ErrFeatureInactive
	case statusMembershipChanged:
		sentinel = ErrMembershipChanged
	}
	return fmt.Errorf("%w: %s", sentinel, msg)
}

func encodeSessionResult(id uint64) []byte {
	e := &enc{b: []byte{wireVersion, statusOK}}
	e.uv(id)
	return e.b
}

func encodeLeaseResult(l Lease) []byte {
	e := &enc{b: []byte{wireVersion, statusOK}}
	e.uv(l.ID)
	e.uv(l.TTL)
	e.uv(l.Remaining)
	return e.b
}

// DecodeLease parses the result of a lease grant or keepalive command.
func DecodeLease(b []byte) (Lease, error) {
	d := &dec{b: b}
	if err := decodeStatus(d); err != nil {
		return Lease{}, err
	}
	l := Lease{ID: d.uv(), TTL: d.uv(), Remaining: d.uv()}
	if d.err == nil && (l.ID == 0 || l.TTL == 0 || l.TTL > MaxLeaseTTL || l.Remaining == 0 || l.Remaining > l.TTL) {
		d.fail("invalid lease result")
	}
	if d.err == nil && len(d.b) != 0 {
		d.fail("%d trailing bytes", len(d.b))
	}
	if d.err != nil {
		return Lease{}, d.err
	}
	return l, nil
}

// DecodeSession parses what Apply returned for a register command: the new
// session's ID.
func DecodeSession(b []byte) (uint64, error) {
	d := &dec{b: b}
	if err := decodeStatus(d); err != nil {
		return 0, err
	}
	id := d.uv()
	if d.err == nil && len(d.b) != 0 {
		d.fail("%d trailing bytes", len(d.b))
	}
	if d.err == nil && id == 0 {
		d.fail("session ID zero")
	}
	if d.err != nil {
		return 0, d.err
	}
	return id, nil
}

// DecodeResult parses what Apply returned for a command. Errors that the state
// machine reported (invalid command, compacted or future revision) come back as
// the matching sentinel error.
func DecodeResult(b []byte) (TxnResult, error) {
	d := &dec{b: b}
	if err := decodeStatus(d); err != nil {
		return TxnResult{}, err
	}
	r := TxnResult{Succeeded: d.flag(), Revision: d.uv()}
	n := d.count(MaxTxnOps)
	for i := 0; i < n && d.err == nil; i++ {
		o := OpResult{Type: OpType(d.u8())}
		switch o.Type {
		case OpPut:
			if d.flag() {
				kv := decodeKV(d)
				o.PrevKV = &kv
			}
		case OpDeleteRange:
			o.Deleted = d.uv()
			for m := d.count(len(d.b)); m > 0 && d.err == nil; m-- {
				o.PrevKVs = append(o.PrevKVs, decodeKV(d))
			}
		case OpRange:
			o.Count = d.uv()
			o.More = d.flag()
			for m := d.count(len(d.b)); m > 0 && d.err == nil; m-- {
				o.KVs = append(o.KVs, decodeKV(d))
			}
		default:
			d.fail("unknown result op type %d", o.Type)
		}
		r.Results = append(r.Results, o)
	}
	if d.err == nil && len(d.b) != 0 {
		d.fail("%d trailing bytes", len(d.b))
	}
	if d.err != nil {
		return TxnResult{}, d.err
	}
	return r, nil
}
