// Package kv is the replicated key-value state machine: a multi-version store
// with etcd-style transactions, driven by the Raft log.
//
// # Model
//
// Every write is stamped with a revision, which is the Raft log index at which
// it was applied. Revisions therefore increase monotonically and are identical
// on every replica by construction, with no separate counter to keep consistent.
// They have gaps (leader no-ops and membership changes consume log indexes);
// that is harmless. Each key keeps its history until compaction discards it, so
// reads can be served as of an older revision and watchers can resume.
//
// # Transactions
//
// Every mutation is a Txn: a list of compares, and two op lists. All compares
// are evaluated against the state before the transaction; if all hold the
// Success ops run, otherwise the Failure ops. Ops run in order and each sees the
// effects of the ones before it. Everything that could fail is validated before
// the first op runs, so a transaction either applies completely or not at all.
// Put, Delete and compare-and-set are just Txns.
//
// # Sessions and exactly-once writes
//
// A client whose write times out cannot tell whether it applied. If it simply
// resends, the write may apply twice. To make a retry safe, a client first
// registers a session (the session ID is the Raft log index of the registration,
// so it is identical on every replica), then sends each write as a numbered
// request in that session. The state machine remembers, per session, the highest
// sequence number applied and its result: a repeat of that number returns the
// stored result without applying anything, and a lower number is refused. Both
// the table and its eviction are deterministic (driven by log indexes, never by
// wall-clock time), and the table is part of the snapshot.
//
// The table is bounded (MaxSessions, MaxCachedResult). When it is full the
// session unused for the longest is evicted. A request for an unknown or evicted
// session is refused (ErrSessionExpired) and applies nothing, because applying it
// could duplicate a write that already ran. The client must then report the
// outcome as unknown, which is the truth.
//
// # Determinism
//
// Apply must produce the same result on every replica forever, including when
// replaying an old log. So nothing about it may depend on local configuration:
// the size limits below are consensus-level constants. Changing one is a
// breaking change to the replicated state machine, not a tuning knob. A node may
// enforce stricter limits before proposing (see Limits), never looser or
// different ones when applying.
package kv

import (
	"bytes"
	"errors"
	"fmt"
)

// Consensus-level limits. See the package comment before touching these.
const (
	MaxKeySize     = 4 << 10
	MaxValueSize   = 1 << 20
	MaxTxnOps      = 128 // per list: compares, success ops, failure ops
	MaxRequestSize = 1536 << 10

	// MaxSessions bounds the session table; beyond it the least recently used
	// session is evicted. MaxCachedResult bounds the result remembered per
	// session, so the table's worst case is MaxSessions x MaxCachedResult (40 MiB).
	MaxSessions     = 10000
	MaxCachedResult = 4 << 10
	// MaxLeases bounds consensus-visible lease state on every replica.
	MaxLeases = 10000
	// MaxLeaseTTL bounds lease lifetime in replicated ticks.
	MaxLeaseTTL = 1_000_000
)

var (
	// ErrInvalidCommand means a command was malformed or broke a limit. It is
	// deterministic: every replica rejects it identically and applies nothing.
	ErrInvalidCommand = errors.New("kv: invalid command")
	// ErrCompacted means a read asked for a revision that has been compacted away.
	ErrCompacted = errors.New("kv: requested revision has been compacted")
	// ErrFutureRevision means a read asked for a revision that does not exist yet.
	ErrFutureRevision = errors.New("kv: requested revision is in the future")
	// ErrSessionExpired means the session is unknown or was evicted. The request
	// was NOT applied. Whether an earlier attempt of the same request applied
	// is unknowable from here, so a caller retrying a write must treat the
	// outcome as unknown.
	ErrSessionExpired = errors.New("kv: session unknown or expired")
	// ErrStaleRequest means the sequence number is lower than one already applied
	// in the session. The request was not applied; the client has moved on.
	ErrStaleRequest = errors.New("kv: stale request sequence number")
	// ErrResultNotCached means the request is a repeat of the session's latest,
	// and it DID apply, but its result was too large to be remembered.
	ErrResultNotCached = errors.New("kv: request applied but its result was not retained")
	ErrLeaseNotFound   = errors.New("kv: lease not found")
	ErrTooManyLeases   = errors.New("kv: active lease limit reached")
)

func invalid(format string, a ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidCommand, fmt.Sprintf(format, a...))
}

// KeyValue is one key at one revision.
type KeyValue struct {
	Key   []byte
	Value []byte
	// CreateRevision is the revision at which this key came into existence in its
	// current life (a delete followed by a put starts a new life).
	CreateRevision uint64
	// ModRevision is the revision of the last write to the key.
	ModRevision uint64
	// Version counts writes to the key in its current life, starting at 1.
	Version uint64
}

func (kv KeyValue) clone() KeyValue {
	kv.Key, kv.Value = bytes.Clone(kv.Key), bytes.Clone(kv.Value)
	return kv
}

type CompareTarget uint8

const (
	CompareVersion CompareTarget = 1
	CompareCreate  CompareTarget = 2
	CompareMod     CompareTarget = 3
	CompareValue   CompareTarget = 4
)

type CompareResult uint8

const (
	Equal    CompareResult = 1
	NotEqual CompareResult = 2
	Greater  CompareResult = 3 // the stored value is greater than the given one
	Less     CompareResult = 4
)

// Compare tests one key. A key that does not exist compares as version 0,
// create revision 0 and mod revision 0, so {CompareMod, Equal, key, Int: 0}
// means "the key does not exist". A value comparison against a missing key is
// false.
type Compare struct {
	Target CompareTarget
	Result CompareResult
	Key    []byte
	Int    uint64 // for Version, Create and Mod
	Value  []byte // for Value
}

type OpType uint8

const (
	OpPut         OpType = 1
	OpDeleteRange OpType = 2
	OpRange       OpType = 3
	// OpPutLease is a put attached to a replicated lease. LeaseID zero is invalid.
	OpPutLease OpType = 4
)

// Op is one operation inside a Txn.
//
// RangeEnd selects keys: empty means exactly Key; a single zero byte means every
// key >= Key; otherwise the half-open range [Key, RangeEnd). See PrefixEnd.
type Op struct {
	Type     OpType
	Key      []byte
	RangeEnd []byte
	Value    []byte // Put
	PrevKV   bool   // Put, DeleteRange: return what was there before
	LeaseID  uint64 // OpPutLease: lease granted by the state machine

	Limit     uint64 // Range: at most this many results (0 = all)
	Revision  uint64 // Range: read as of this revision (0 = now)
	KeysOnly  bool   // Range
	CountOnly bool   // Range
}

// PutWithLease returns a transaction that sets key to value and attaches the
// key's lifetime to leaseID. The lease must exist when the transaction applies.
func PutWithLease(key, value []byte, leaseID uint64) Txn {
	return Txn{Success: []Op{{Type: OpPutLease, Key: key, Value: value, LeaseID: leaseID}}}
}

// Lease describes one replicated lease. TTL and Remaining are measured in
// committed LeaseTick commands, not local wall-clock time.
type Lease struct {
	ID        uint64
	TTL       uint64
	Remaining uint64
}

// Txn is the only mutation.
type Txn struct {
	Compares []Compare
	Success  []Op
	Failure  []Op
}

// Put returns a transaction that sets key to value.
func Put(key, value []byte) Txn {
	return Txn{Success: []Op{{Type: OpPut, Key: key, Value: value}}}
}

// Delete returns a transaction that deletes key.
func Delete(key []byte) Txn {
	return Txn{Success: []Op{{Type: OpDeleteRange, Key: key}}}
}

// PrefixEnd returns the RangeEnd that selects every key starting with prefix.
func PrefixEnd(prefix []byte) []byte {
	end := bytes.Clone(prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return []byte{0} // the prefix is all 0xff: everything from it onward
}

// OpResult is the outcome of one Op, in the same order as the Ops that ran.
type OpResult struct {
	Type OpType
	// Put: PrevKV holds the previous value if requested and the key existed.
	// DeleteRange: Deleted counts keys removed, PrevKVs lists them if requested.
	// Range: KVs, the total Count matching, and whether More remain beyond Limit.
	PrevKV  *KeyValue
	Deleted uint64
	PrevKVs []KeyValue
	KVs     []KeyValue
	Count   uint64
	More    bool
}

// TxnResult is what a committed Txn returns to its proposer.
type TxnResult struct {
	// Succeeded reports whether the compares held (so the Success ops ran).
	Succeeded bool
	// Revision is the store's revision after the transaction: the transaction's
	// own revision if it wrote anything, otherwise unchanged.
	Revision uint64
	Results  []OpResult
}

// Limits are stricter-than-consensus limits a node may enforce before proposing.
// The zero value means the consensus limits.
type Limits struct {
	MaxKeySize, MaxValueSize, MaxTxnOps, MaxRequestSize int
}

func (l Limits) orDefault() Limits {
	pick := func(v, def int) int {
		if v <= 0 || v > def {
			return def
		}
		return v
	}
	return Limits{pick(l.MaxKeySize, MaxKeySize), pick(l.MaxValueSize, MaxValueSize), pick(l.MaxTxnOps, MaxTxnOps), pick(l.MaxRequestSize, MaxRequestSize)}
}
