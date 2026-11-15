package kv

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"sort"
)

// session is one client's dedup record. Everything in it is a function of the
// log, so every replica holds the same table.
type session struct {
	lastUsed uint64 // log index of the last command that touched the session
	seq      uint64 // highest request sequence number applied (0 = none yet)
	res      []byte // encoded result of request seq, if retained
	dropped  bool   // request seq applied but its result was too large to keep
}

type lease struct {
	ttl       uint64
	remaining uint64
}

// StateMachine adapts a Store to the replicated log. It implements
// raftnode.StateMachine: every committed command arrives through Apply, in log
// order, identically on every replica. It also owns the session table that
// makes retried writes safe (see the package comment).
type StateMachine struct {
	store              *Store
	sessions           map[uint64]*session
	leases             map[uint64]*lease
	compatibilityLevel uint8
}

// NewStateMachine returns an empty state machine.
func NewStateMachine() *StateMachine {
	return &StateMachine{store: NewStore(), sessions: map[uint64]*session{}, leases: map[uint64]*lease{}, compatibilityLevel: MaxCompatibilityLevel}
}

// CompatibilityLevel is the highest KV feature level enabled by committed
// compatibility activation. Fresh clusters start at the latest supported
// level; legacy snapshots restore at the level their format represents.
func (m *StateMachine) CompatibilityLevel() uint8 { return m.compatibilityLevel }

// CompatibilityFormats reports this state machine's read/write formats and
// supported feature level for compatibility inventory.
func (m *StateMachine) CompatibilityFormats() (commandVersion, snapshotWriteVersion, maxCommandKind, supportedLevel, activeLevel byte, snapshotReadVersions []byte) {
	switch m.compatibilityLevel {
	case CompatibilityLevelBase:
		snapshotWriteVersion = 1
	case CompatibilityLevelSessions:
		snapshotWriteVersion = 2
	default:
		snapshotWriteVersion = smSnapshotVersion
	}
	return wireVersion, snapshotWriteVersion, kindActivateCompatibility, MaxCompatibilityLevel, m.compatibilityLevel, []byte{1, 2, snapshotVersion, smSnapshotVersion}
}

// Store returns the underlying store for reads. Callers must be on the same
// serial context as Apply, and must not mutate it.
func (m *StateMachine) Store() *Store { return m.store }

// Sessions returns how many sessions are currently registered.
func (m *StateMachine) Sessions() int { return len(m.sessions) }

// LeaseCount returns the number of active replicated leases.
func (m *StateMachine) LeaseCount() int { return len(m.leases) }

// LeaseTickCommand returns the command raftnode may propose on its configured
// leader-only lease cadence.
func (m *StateMachine) LeaseTickCommand() []byte { return EncodeLeaseTick() }

// Apply executes one committed command and returns its encoded result (decode
// with DecodeResult, or DecodeSession for a register command). It never panics
// and never returns an error: a command that is malformed, over a limit, or
// reads an unavailable revision yields an error result, the same one on every
// replica, and changes nothing.
func (m *StateMachine) Apply(index uint64, cmd []byte) []byte {
	c, err := decodeCommand(cmd)
	if err != nil {
		return encodeError(err)
	}
	if c.kind == kindActivateCompatibility {
		return encodeError(ErrMembershipChanged)
	}
	return m.applyDecoded(index, c)
}

// ApplyWithMembership applies a committed command with the configuration that
// was active immediately before this entry. Compatibility activation is
// rejected if membership no longer matches the preflight fingerprint.
func (m *StateMachine) ApplyWithMembership(index uint64, cmd []byte, voters, learners, votersOutgoing, learnersNext []uint64, autoLeave bool) []byte {
	c, err := decodeCommand(cmd)
	if err != nil {
		return encodeError(err)
	}
	if c.kind == kindActivateCompatibility {
		got := MembershipDigest(voters, learners, votersOutgoing, learnersNext, autoLeave)
		if got != c.membershipDigest {
			return encodeError(fmt.Errorf("%w: activation fingerprint %x does not match applied configuration %x", ErrMembershipChanged, c.membershipDigest, got))
		}
		if c.compatibilityLevel < m.compatibilityLevel {
			return encodeError(invalid("compatibility level cannot move backwards from %d to %d", m.compatibilityLevel, c.compatibilityLevel))
		}
		m.compatibilityLevel = c.compatibilityLevel
		return encodeResult(TxnResult{Succeeded: true, Revision: m.store.rev})
	}
	return m.applyDecoded(index, c)
}

// ValidateCommand rejects feature use before it can enter the Raft log while
// the corresponding compatibility level is inactive.
func (m *StateMachine) ValidateCommand(command []byte) error {
	c, err := decodeCommand(command)
	if err != nil {
		return err
	}
	return m.validateLevel(c)
}

func (m *StateMachine) applyDecoded(index uint64, c *command) []byte {
	if err := m.validateLevel(c); err != nil {
		return encodeError(err)
	}
	switch c.kind {
	case kindCompact:
		if index <= m.store.rev {
			return encodeError(invalid("revision %d is not after the store's %d", index, m.store.rev))
		}
		// A compaction request may be retried after its first result was lost.
		// Since compaction is monotonic, repeating an already-applied target is
		// an idempotent success and can be safely proposed again by a client.
		if c.compact != 0 && c.compact <= m.store.compact {
			return encodeResult(TxnResult{Succeeded: true, Revision: m.store.rev})
		}
		if err := m.store.Compact(c.compact); err != nil {
			return encodeError(err)
		}
		return encodeResult(TxnResult{Succeeded: true, Revision: m.store.rev})
	case kindRegister:
		return m.register(index)
	case kindSessionTxn:
		return m.applySession(index, c)
	case kindLeaseGrant:
		return m.grantLease(index, c.leaseTTL)
	case kindLeaseKeep:
		return m.keepLease(c.leaseID)
	case kindLeaseDrop:
		return m.revokeLease(index, c.leaseID)
	case kindLeaseTick:
		return m.tickLeases(index)
	case kindActivateCompatibility:
		return encodeError(ErrMembershipChanged)
	default: // kindTxn; decodeCommand admits nothing else
		return m.applyTxn(index, c.txn)
	}
}

func (m *StateMachine) validateLevel(c *command) error {
	required := CompatibilityLevelBase
	switch c.kind {
	case kindRegister, kindSessionTxn:
		required = CompatibilityLevelSessions
	case kindLeaseGrant, kindLeaseKeep, kindLeaseDrop, kindLeaseTick:
		required = CompatibilityLevelLeases
	case kindTxn:
		for _, ops := range [][]Op{c.txn.Success, c.txn.Failure} {
			for _, op := range ops {
				if op.Type == OpPutLease {
					required = CompatibilityLevelLeases
				}
			}
		}
	}
	if m.compatibilityLevel < required {
		return ErrFeatureInactive
	}
	return nil
}

func (m *StateMachine) applyTxn(index uint64, t *Txn) []byte {
	ops := t.Success
	for _, c := range t.Compares {
		if !m.store.evalCompare(c) {
			ops = t.Failure
			break
		}
	}
	for _, op := range ops {
		if op.Type == OpPutLease && m.leases[op.LeaseID] == nil {
			return encodeError(fmt.Errorf("%w: lease %d", ErrLeaseNotFound, op.LeaseID))
		}
	}
	res, err := m.store.ApplyTxn(index, t)
	if err != nil {
		return encodeError(err)
	}
	return encodeResult(res)
}

func (m *StateMachine) grantLease(index, ttl uint64) []byte {
	if index == 0 || m.leases[index] != nil {
		return encodeError(invalid("lease ID %d is unavailable", index))
	}
	if len(m.leases) >= MaxLeases {
		return encodeError(ErrTooManyLeases)
	}
	m.leases[index] = &lease{ttl: ttl, remaining: ttl}
	return encodeLeaseResult(Lease{ID: index, TTL: ttl, Remaining: ttl})
}

func (m *StateMachine) keepLease(id uint64) []byte {
	l := m.leases[id]
	if l == nil {
		return encodeError(fmt.Errorf("%w: lease %d", ErrLeaseNotFound, id))
	}
	l.remaining = l.ttl
	return encodeLeaseResult(Lease{ID: id, TTL: l.ttl, Remaining: l.remaining})
}

func (m *StateMachine) revokeLease(index, id uint64) []byte {
	if m.leases[id] == nil {
		return encodeError(fmt.Errorf("%w: lease %d", ErrLeaseNotFound, id))
	}
	delete(m.leases, id)
	m.store.deleteLease(index, id)
	return encodeResult(TxnResult{Succeeded: true, Revision: m.store.rev})
}

func (m *StateMachine) tickLeases(index uint64) []byte {
	ids := make([]uint64, 0, len(m.leases))
	for id := range m.leases {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	var expired []uint64
	for _, id := range ids {
		l := m.leases[id]
		if l.remaining <= 1 {
			expired = append(expired, id)
		} else {
			l.remaining--
		}
	}
	for _, id := range expired {
		delete(m.leases, id)
		m.store.deleteLease(index, id)
	}
	return encodeResult(TxnResult{Succeeded: true, Revision: m.store.rev})
}

// register opens a session whose ID is the log index of this command. If the
// table is full the least recently used session is evicted first; the order is
// by log index, so every replica evicts the same one.
func (m *StateMachine) register(index uint64) []byte {
	if _, dup := m.sessions[index]; dup {
		return encodeError(invalid("log index %d already registered a session", index))
	}
	if len(m.sessions) >= MaxSessions {
		var oldest uint64
		for id, s := range m.sessions {
			if oldest == 0 || s.lastUsed < m.sessions[oldest].lastUsed {
				oldest = id
			}
		}
		delete(m.sessions, oldest)
	}
	m.sessions[index] = &session{lastUsed: index}
	return encodeSessionResult(index)
}

func (m *StateMachine) applySession(index uint64, c *command) []byte {
	s := m.sessions[c.session]
	if s == nil {
		return encodeError(fmt.Errorf("%w: session %d", ErrSessionExpired, c.session))
	}
	s.lastUsed = index
	switch {
	case c.seq < s.seq:
		return encodeError(fmt.Errorf("%w: request %d, session %d is at %d", ErrStaleRequest, c.seq, c.session, s.seq))
	case c.seq == s.seq:
		if s.dropped {
			return encodeError(fmt.Errorf("%w: request %d of session %d", ErrResultNotCached, c.seq, c.session))
		}
		return bytes.Clone(s.res) // the exact bytes the first attempt returned
	}
	out := m.applyTxn(index, c.txn)
	s.seq = c.seq
	if len(out) <= MaxCachedResult {
		s.res, s.dropped = bytes.Clone(out), false
	} else {
		s.res, s.dropped = nil, true
	}
	return out
}

// -- snapshot ----------------------------------------------------------------

// smSnapshotVersion is the state machine snapshot format: store, sessions and
// leases. Older state-machine and bare-store snapshots remain readable.
//
//	version u8 (=4) | store snapshot | sessions | leases {id ttl remaining}
//
// Sessions are written in ascending ID order so equal states give equal bytes.
const smSnapshotVersion = 4

// Snapshot serializes the state as of the last Apply.
func (m *StateMachine) Snapshot() ([]byte, error) {
	if m.compatibilityLevel == CompatibilityLevelBase {
		return m.store.snapshotV1()
	}
	version := byte(smSnapshotVersion)
	includeLeases := true
	storeData := m.store.Snapshot()
	if m.compatibilityLevel == CompatibilityLevelSessions {
		version = 2
		includeLeases = false
		var err error
		storeData, err = m.store.snapshotV1()
		if err != nil {
			return nil, err
		}
	}
	return m.snapshotEnvelope(version, storeData, includeLeases), nil
}

func (m *StateMachine) snapshotEnvelope(version byte, storeData []byte, includeLeases bool) []byte {
	e := &enc{b: []byte{version}}
	e.bytes(storeData)
	ids := make([]uint64, 0, len(m.sessions))
	for id := range m.sessions {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	e.uv(uint64(len(ids)))
	for _, id := range ids {
		s := m.sessions[id]
		e.uv(id)
		e.uv(s.lastUsed)
		e.uv(s.seq)
		switch {
		case s.seq == 0:
			e.u8(0)
		case s.dropped:
			e.u8(2)
		default:
			e.u8(1)
			e.bytes(s.res)
		}
	}
	if includeLeases {
		ids = ids[:0]
		for id := range m.leases {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		e.uv(uint64(len(ids)))
		for _, id := range ids {
			l := m.leases[id]
			e.uv(id)
			e.uv(l.ttl)
			e.uv(l.remaining)
		}
	}
	return e.b
}

// Hash is a digest of everything Apply can influence (store and sessions), for
// comparing replicas.
func (m *StateMachine) Hash() [32]byte {
	b, _ := m.Snapshot()
	return sha256.Sum256(b)
}

// Restore replaces the state with a snapshot; empty data means the empty state.
// The snapshot is validated completely first: on any error nothing changes.
func (m *StateMachine) Restore(data []byte) error {
	if len(data) == 0 {
		m.store.replaceFromQuiet(NewStore())
		m.sessions = map[uint64]*session{}
		m.leases = map[uint64]*lease{}
		m.store.notifyWatchers()
		return nil
	}
	if data[0] == 1 || data[0] == snapshotVersion { // legacy: a bare store snapshot, no sessions
		ns := NewStore()
		if err := ns.Restore(data); err != nil {
			return fmt.Errorf("kv: %w", err)
		}
		if err := validateLeaseRefs(ns, nil); err != nil {
			return fmt.Errorf("kv: bad snapshot: %w", err)
		}
		m.store.replaceFromQuiet(ns)
		m.sessions = map[uint64]*session{}
		m.leases = map[uint64]*lease{}
		m.compatibilityLevel = CompatibilityLevelBase
		m.store.notifyWatchers()
		return nil
	}
	d := &dec{b: data}
	v := d.u8()
	if v != 2 && v != smSnapshotVersion {
		return fmt.Errorf("kv: unsupported snapshot version %d", v)
	}
	storeBytes := d.bytes(len(d.b))
	sessions := decodeSessions(d)
	leases := map[uint64]*lease{}
	if v == smSnapshotVersion {
		leases = decodeLeases(d)
	}
	if d.err == nil && len(d.b) != 0 {
		d.fail("%d trailing bytes", len(d.b))
	}
	if d.err != nil {
		return fmt.Errorf("kv: bad snapshot: %w", d.err)
	}
	ns := NewStore()
	if err := ns.Restore(storeBytes); err != nil {
		return fmt.Errorf("kv: %w", err)
	}
	if err := validateLeaseRefs(ns, leases); err != nil {
		return fmt.Errorf("kv: bad snapshot: %w", err)
	}
	m.store.replaceFromQuiet(ns)
	m.sessions = sessions
	m.leases = leases
	if v == 2 {
		m.compatibilityLevel = CompatibilityLevelSessions
	} else {
		m.compatibilityLevel = CompatibilityLevelLeases
	}
	m.store.notifyWatchers()
	return nil
}

func decodeLeases(d *dec) map[uint64]*lease {
	n := d.count(MaxLeases)
	out := make(map[uint64]*lease, n)
	var prev uint64
	for i := 0; i < n && d.err == nil; i++ {
		id := d.uv()
		l := &lease{ttl: d.uv(), remaining: d.uv()}
		switch {
		case id == 0 || id <= prev:
			d.fail("lease IDs not strictly ascending at %d", i)
		case l.ttl == 0 || l.ttl > MaxLeaseTTL || l.remaining == 0 || l.remaining > l.ttl:
			d.fail("lease %d has invalid TTL state", id)
		}
		prev = id
		out[id] = l
	}
	return out
}

func validateLeaseRefs(s *Store, leases map[uint64]*lease) error {
	for _, h := range s.keys {
		if r := h.at(latest); r != nil && !r.tomb && r.leaseID != 0 && leases[r.leaseID] == nil {
			return invalid("key %q refers to missing lease %d", h.key, r.leaseID)
		}
	}
	return nil
}

// wellFormedResult reports whether b is exactly one encoded result: either a
// full OK result or an error status with its message and nothing after it. A
// snapshot may not smuggle in a "cached result" that a client would later be
// handed as if a replica had produced it.
func wellFormedResult(b []byte) bool {
	if len(b) < 2 || b[0] != wireVersion || b[1] > maxStatus {
		return false
	}
	if b[1] == statusOK {
		_, err := DecodeResult(b)
		return err == nil
	}
	d := &dec{b: b}
	_ = decodeStatus(d)
	return d.err == nil && len(d.b) == 0
}

func decodeSessions(d *dec) map[uint64]*session {
	n := d.count(MaxSessions)
	out := make(map[uint64]*session, n)
	var prev uint64
	for i := 0; i < n && d.err == nil; i++ {
		id := d.uv()
		s := &session{lastUsed: d.uv(), seq: d.uv()}
		state := d.u8()
		if d.err != nil {
			break
		}
		switch {
		case id == 0 || id <= prev:
			d.fail("session IDs not strictly ascending at %d", i)
		case s.lastUsed < id:
			d.fail("session %d used at %d, before it was registered", id, s.lastUsed)
		case state == 0 && s.seq != 0, state != 0 && s.seq == 0:
			d.fail("session %d: result state %d inconsistent with seq %d", id, state, s.seq)
		case state == 1:
			s.res = bytes.Clone(d.bytes(MaxCachedResult))
			if d.err == nil && !wellFormedResult(s.res) {
				d.fail("session %d: retained result is not a result", id)
			}
		case state == 2:
			s.dropped = true
		case state != 0:
			d.fail("session %d: unknown result state %d", id, state)
		}
		prev = id
		out[id] = s
	}
	return out
}
