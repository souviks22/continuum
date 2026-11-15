// Package raftnode runs one member of a Raft cluster on top of etcd-io/raft's
// RawNode, with durable state in this repository's WAL, atomic-file and snapshot
// packages and messaging over the rpc package.
//
// # Execution model
//
// A Node is single-threaded: Open, Propose, Stop and every callback run on the
// serial context of its clock.Clock (see that package). Nothing here spawns a
// goroutine or takes a lock. That is what makes it deterministic under the
// simulator; the production runtime will provide the serial context.
//
// # Durability
//
// Each Ready is handled in this order, and the order is the safety argument:
//
//  1. an incoming snapshot is committed to the snapshot store, and the WAL is reset;
//  2. new log entries are appended to the WAL and fsynced;
//  3. term/vote, if changed, are written atomically and durably;
//  4. only then are messages sent and entries applied.
//
// A node therefore never acknowledges, votes on, or applies anything it could
// forget after a power cut. Commit index is deliberately not persisted on every
// change: it is re-learned from the leader after a restart, and the state machine
// is rebuilt from the newest snapshot plus the log.
//
// # Reads
//
// A linearizable read (Node.LinearizableRead) uses Raft's ReadIndex in its safe
// mode: the leader records its commit index, confirms with a heartbeat round that
// a quorum still recognizes it as leader, and the requesting node waits until it
// has applied up to that index. Leader leases are deliberately not used: they
// would make correctness depend on bounded clock drift. A leader that has been
// deposed or partitioned cannot gather the quorum, so it never answers; the read
// times out instead of returning stale data. Any member can serve a read (a
// follower asks the leader for the index), and the callback runs on the node's
// serial context with the state machine applied at least as far as the index.
//
// # Bootstrap
//
// A brand-new cluster is started by installing, on every member, a synthetic
// snapshot at (index 1, term 1) whose configuration is the voter set. It is one
// atomic write, idempotent if the node crashes midway, and avoids the window in
// which every node holds membership entries that no one can commit.
package raftnode

import (
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	"go.etcd.io/raft/v3"

	"github.com/souviks22/continuum/internal/clock"
	"github.com/souviks22/continuum/internal/rpc"
	"github.com/souviks22/continuum/internal/vfs"
)

var (
	// ErrStopped is returned for operations on a stopped or failed node.
	ErrStopped = errors.New("raftnode: stopped")
	// ErrNotLeader matches *NotLeaderError: the proposal went to a node that is
	// not the leader.
	ErrNotLeader = errors.New("raftnode: not the leader")
	// ErrResultUnknown is delivered to a proposal's callback when its outcome is
	// not known: it may still commit later. It is never reported as plain failure.
	ErrResultUnknown = errors.New("raftnode: result unknown, the proposal may still commit")
	// ErrLogGap means the write-ahead log no longer reaches back to the newest
	// usable snapshot, so local state cannot be reconstructed. The node must
	// rejoin the cluster from scratch.
	ErrLogGap = errors.New("raftnode: log does not connect to the newest usable snapshot")
)

var (
	// ErrReadTimeout means a linearizable read was not confirmed in time. A read
	// has no effect, so unlike ErrResultUnknown this is always safe to retry.
	ErrReadTimeout = errors.New("raftnode: read not confirmed in time")
	// ErrLeaderNotReady means this node is the leader but has not yet committed an
	// entry in its own term, so it cannot yet prove what has been committed.
	// This lasts about one round trip after an election; retry shortly.
	ErrLeaderNotReady = errors.New("raftnode: leader has not committed an entry in its term yet")
)

// NotLeaderError is returned by Propose on a node that is not the leader. Leader
// is the Raft ID of the leader this node knows of, or 0 if it knows of none.
type NotLeaderError struct{ Leader uint64 }

func (e *NotLeaderError) Error() string {
	if e.Leader == 0 {
		return "raftnode: not the leader, and no leader is known"
	}
	return fmt.Sprintf("raftnode: not the leader; try node %d", e.Leader)
}

// Is makes errors.Is(err, ErrNotLeader) true.
func (e *NotLeaderError) Is(target error) bool { return target == ErrNotLeader }

// StateMachine is the replicated application state. Raft guarantees every
// replica applies the same commands in the same order, so implementations must
// be deterministic: no clocks, randomness, or map-order dependence.
type StateMachine interface {
	// Apply applies a committed command and returns a result for the proposer.
	// index is the command's position in the Raft log.
	Apply(index uint64, cmd []byte) []byte
	// Snapshot serializes the state as of the last Apply.
	Snapshot() ([]byte, error)
	// Restore replaces the state with a snapshot. Empty data means the initial
	// (empty) state.
	Restore(data []byte) error
}

// AppliedMembership is the Raft configuration that was active immediately
// before a committed normal entry was applied. Slices are owned by the caller
// and must be treated as read-only.
type AppliedMembership struct {
	Voters         []uint64
	Learners       []uint64
	VotersOutgoing []uint64
	LearnersNext   []uint64
	AutoLeave      bool
}

// MembershipAwareStateMachine is optional. It lets a state machine bind a
// replicated control command to the configuration that applies it, closing
// races between a client-side membership read and command application.
type MembershipAwareStateMachine interface {
	ApplyWithMembership(index uint64, cmd []byte, membership AppliedMembership) []byte
}

// Config configures a Node. Zero values select defaults where noted.
type Config struct {
	// ID is this member's Raft ID (non-zero).
	ID uint64
	// Peers maps every configured transport identity, including this node, to its
	// transport node name. It may include learners or future members.
	Peers map[uint64]string
	// InitialVoters selects the voter set used by Bootstrap. Nil preserves the
	// legacy behavior of making every Peers entry a voter.
	InitialVoters []uint64
	// Bootstrap makes a node with no durable state at all start a new cluster
	// whose voters are InitialVoters (or every ID in Peers when nil). Every
	// initial member must set it.
	Bootstrap bool
	// BootstrapLearner initializes a new, non-voting member with the supplied
	// InitialVoters and this node as its learner. Use this when admitting a fresh
	// node with AddLearner; it must not campaign before it catches up.
	BootstrapLearner bool

	Endpoint     *rpc.Endpoint
	Clock        clock.Clock
	FS           vfs.FS
	Dir          string
	StateMachine StateMachine

	// TickInterval is the Raft logical clock period (default 100ms).
	TickInterval time.Duration
	// LeaseTickInterval periodically proposes the state's deterministic lease
	// tick while this node is leader and active leases exist. Zero disables it.
	LeaseTickInterval time.Duration
	// KVCompactionInterval periodically requests a replicated compaction command
	// from a compaction-aware state machine. Zero disables automatic compaction.
	KVCompactionInterval time.Duration
	// KVCompactionRetain is the number of recent data revisions to keep when
	// automatic compaction runs. It must be positive when the interval is set.
	KVCompactionRetain uint64
	// ElectionTick and HeartbeatTick are in ticks (defaults 10 and 1). The
	// election timeout is randomized in [ElectionTick, 2*ElectionTick).
	ElectionTick  int
	HeartbeatTick int

	// SnapshotEvery is how many applied entries trigger a snapshot (default 10000).
	SnapshotEvery uint64
	// SnapshotTrailing is how many entries before the snapshot stay in the log
	// so a slightly slow follower can catch up without a snapshot (default 1000).
	SnapshotTrailing uint64
	// WALSegmentSize is the WAL's soft segment size (default 64 MiB).
	WALSegmentSize int64

	// RequestIDBase must differ between process incarnations of the same
	// member, so a result for a previous life's proposal cannot be delivered to
	// a new caller. Zero means "pick randomly" (production); simulations set it.
	RequestIDBase uint64

	// Logger receives raft's internal logging (default: discarded).
	Logger raft.Logger
	// Observer receives lifecycle events on the serial context. It must be
	// non-blocking and must not call back into this node.
	Observer Observer
	// OnFatal is called once if the node fails (e.g. a storage error). The node
	// has already stopped by then; the host should restart or exit.
	OnFatal func(error)
}

func (c Config) withDefaults() (Config, error) {
	if c.ID == 0 {
		return c, errors.New("raftnode: ID must be non-zero")
	}
	if _, ok := c.Peers[c.ID]; !ok {
		return c, fmt.Errorf("raftnode: Peers must include this node (ID %d)", c.ID)
	}
	if c.Bootstrap && c.BootstrapLearner {
		return c, errors.New("raftnode: Bootstrap and BootstrapLearner are mutually exclusive")
	}
	for id, name := range c.Peers {
		if id == 0 || name == "" {
			return c, errors.New("raftnode: Peers must map non-zero IDs to non-empty names")
		}
	}
	if c.InitialVoters != nil {
		seen := make(map[uint64]bool, len(c.InitialVoters))
		for _, id := range c.InitialVoters {
			if id == 0 || c.Peers[id] == "" || seen[id] {
				return c, errors.New("raftnode: InitialVoters must contain unique configured non-zero IDs")
			}
			seen[id] = true
		}
		if c.Bootstrap && !seen[c.ID] {
			return c, errors.New("raftnode: InitialVoters must include this node")
		}
		if c.BootstrapLearner && (len(seen) == 0 || seen[c.ID]) {
			return c, errors.New("raftnode: BootstrapLearner requires existing voters and must not include this node")
		}
	} else if c.BootstrapLearner {
		return c, errors.New("raftnode: BootstrapLearner requires InitialVoters")
	}
	if c.Endpoint == nil || c.Clock == nil || c.FS == nil || c.StateMachine == nil || c.Dir == "" {
		return c, errors.New("raftnode: Endpoint, Clock, FS, StateMachine and Dir are required")
	}
	if c.TickInterval == 0 {
		c.TickInterval = 100 * time.Millisecond
	}
	if c.ElectionTick == 0 {
		c.ElectionTick = 10
	}
	if c.HeartbeatTick == 0 {
		c.HeartbeatTick = 1
	}
	if c.SnapshotEvery == 0 {
		c.SnapshotEvery = 10000
	}
	if c.SnapshotTrailing == 0 {
		c.SnapshotTrailing = 1000
	}
	if c.ElectionTick <= c.HeartbeatTick || c.TickInterval < 0 || c.LeaseTickInterval < 0 || (c.LeaseTickInterval > 0 && c.LeaseTickInterval < c.TickInterval) || c.KVCompactionInterval < 0 || (c.KVCompactionInterval > 0 && c.KVCompactionInterval < c.TickInterval) {
		return c, errors.New("raftnode: need ElectionTick > HeartbeatTick, positive TickInterval, and lease/compaction intervals of zero or at least TickInterval")
	}
	if (c.KVCompactionInterval == 0) != (c.KVCompactionRetain == 0) {
		return c, errors.New("raftnode: KVCompactionInterval and positive KVCompactionRetain must be configured together")
	}
	if c.Logger == nil {
		c.Logger = &raft.DefaultLogger{Logger: log.New(io.Discard, "", 0)}
	}
	return c, nil
}
