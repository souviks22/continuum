package raftnode

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"path"
	"sort"
	"time"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/souviks22/continuum/internal/atomicfile"
	"github.com/souviks22/continuum/internal/clock"
	"github.com/souviks22/continuum/internal/snapshot"
	"github.com/souviks22/continuum/internal/vfs"
	"github.com/souviks22/continuum/internal/wal"
)

// reader is a linearizable read waiting for its read index and then for the
// state machine to catch up to it.
type reader struct {
	cb        func(error)
	timer     clock.Timer
	started   time.Duration
	confirmed bool   // raft has answered with a read index
	index     uint64 // the index to wait for, valid once confirmed
}

type waiter struct {
	cb      func(result []byte, err error)
	timer   clock.Timer
	started time.Duration
}

// leaseTickStateMachine is implemented by state machines that support
// deterministic lease expiry. Node owns only scheduling; lease semantics stay
// in the replicated state machine.
type leaseTickStateMachine interface {
	LeaseCount() int
	LeaseTickCommand() []byte
}

// kvCompactionStateMachine supplies a deterministic command when its retained
// data-revision window has been exceeded. The command is still committed by
// Raft before any replica drops MVCC history.
type kvCompactionStateMachine interface {
	KVCompactionCommand(retain uint64) []byte
}

// Node is one Raft member. See the package comment for its execution model.
type Node struct {
	cfg   Config
	rn    *raft.RawNode
	ms    *raft.MemoryStorage
	log   *wal.Log
	snaps *snapshot.Store
	sm    StateMachine

	tick             clock.Timer
	leaseTick        clock.Timer
	leasePending     bool
	kvCompactTick    clock.Timer
	kvCompactPending bool
	stopped          bool
	processing       bool
	fatalErr         error

	applied       uint64
	confState     *pb.ConfState
	lastSnapIndex uint64

	persistedTerm, persistedVote uint64

	nextReq     uint64
	waiters     map[uint64]*waiter
	reads       map[uint64]*reader
	confWaiters map[uint64]*waiter
	confPending bool
	confIndex   uint64
}

// Open recovers the node from durable state (or bootstraps it) and starts it.
func Open(cfg Config) (*Node, error) {
	cfg, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	if cfg.LeaseTickInterval > 0 {
		if _, ok := cfg.StateMachine.(leaseTickStateMachine); !ok {
			return nil, errors.New("raftnode: LeaseTickInterval requires a lease-aware state machine")
		}
	}
	if cfg.KVCompactionInterval > 0 {
		if _, ok := cfg.StateMachine.(kvCompactionStateMachine); !ok {
			return nil, errors.New("raftnode: KVCompactionInterval requires a compaction-aware state machine")
		}
	}
	n := &Node{cfg: cfg, sm: cfg.StateMachine, waiters: make(map[uint64]*waiter), reads: make(map[uint64]*reader), confWaiters: make(map[uint64]*waiter)}
	if cfg.RequestIDBase != 0 {
		n.nextReq = cfg.RequestIDBase
	} else {
		var b [8]byte
		if _, err := rand.Read(b[:]); err != nil {
			return nil, err
		}
		n.nextReq = binary.LittleEndian.Uint64(b[:])
	}
	if err := n.recover(); err != nil {
		if n.log != nil {
			n.log.Close()
		}
		return nil, err
	}
	if err := cfg.Endpoint.RegisterNotify(raftMethod, n.onMessage); err != nil {
		n.log.Close()
		return nil, err
	}
	if cfg.LeaseTickInterval > 0 {
		n.scheduleLeaseTick()
	}
	if cfg.KVCompactionInterval > 0 {
		n.scheduleKVCompaction()
	}
	n.scheduleTick()
	n.process()
	return n, nil
}

func (n *Node) scheduleLeaseTick() {
	if n.stopped || n.cfg.LeaseTickInterval <= 0 {
		return
	}
	n.leaseTick = n.cfg.Clock.AfterFunc(n.cfg.LeaseTickInterval, func() {
		n.leaseTick = nil
		if n.stopped {
			return
		}
		if n.IsLeader() && !n.leasePending {
			if leases, ok := n.sm.(leaseTickStateMachine); ok && leases.LeaseCount() > 0 {
				n.leasePending = true
				err := n.Propose(leases.LeaseTickCommand(), n.cfg.LeaseTickInterval, func(_ []byte, _ error) {
					n.leasePending = false
				})
				if err != nil {
					n.leasePending = false
				}
			}
		}
		n.scheduleLeaseTick()
	})
}

func (n *Node) scheduleKVCompaction() {
	if n.stopped || n.cfg.KVCompactionInterval <= 0 {
		return
	}
	n.kvCompactTick = n.cfg.Clock.AfterFunc(n.cfg.KVCompactionInterval, func() {
		n.kvCompactTick = nil
		if n.stopped {
			return
		}
		if n.IsLeader() && !n.kvCompactPending {
			if sm, ok := n.sm.(kvCompactionStateMachine); ok {
				if command := sm.KVCompactionCommand(n.cfg.KVCompactionRetain); len(command) != 0 {
					n.kvCompactPending = true
					err := n.Propose(command, n.cfg.KVCompactionInterval, func(_ []byte, _ error) {
						n.kvCompactPending = false
					})
					if err != nil {
						n.kvCompactPending = false
					}
				}
			}
		}
		n.scheduleKVCompaction()
	})
}

// recover rebuilds in-memory state from disk. Every branch here corresponds to
// a crash window the tests exercise.
func (n *Node) recover() error {
	cfg := n.cfg
	var err error
	if n.snaps, err = snapshot.Open(cfg.FS, path.Join(cfg.Dir, snapDirName)); err != nil {
		return err
	}

	// Newest snapshot that verifies. A newer corrupt one is ignored here; the
	// log-gap check below decides whether falling back is actually safe.
	entries, err := n.snaps.Scan()
	if err != nil {
		return err
	}
	var snap *snapshot.Snapshot
	var snapKind byte
	snapCS := new(pb.ConfState)
	for i := len(entries) - 1; i >= 0 && snap == nil; i-- {
		if entries[i].Err != nil {
			continue
		}
		s, err := n.snaps.Load(entries[i].Index)
		if err != nil {
			continue
		}
		kind, cs, err := decodeExtra(s.Meta.Extra)
		if err != nil {
			continue
		}
		snap, snapKind, snapCS = &s, kind, cs
	}
	var snapIndex, snapTerm uint64
	if snap != nil {
		snapIndex, snapTerm = snap.Meta.Index, snap.Meta.Term
	}

	// Term and vote. Guessing them could double-vote, so an unreadable file is fatal.
	hs := new(pb.HardState)
	hsBytes, err := atomicfile.Read(cfg.FS, path.Join(cfg.Dir, hardStateFile))
	hasHS := false
	switch {
	case err == nil:
		if err := proto.Unmarshal(hsBytes, hs); err != nil {
			return fmt.Errorf("raftnode: hard state undecodable: %w", err)
		}
		hasHS = true
	case errors.Is(err, vfs.ErrNotExist):
	default:
		return fmt.Errorf("raftnode: hard state unreadable, refusing to guess term and vote: %w", err)
	}
	n.persistedTerm, n.persistedVote = hs.GetTerm(), hs.GetVote()

	n.log, err = wal.Open(cfg.FS, path.Join(cfg.Dir, walDirName), wal.Options{
		FirstSeq: snapIndex + 1, SegmentSize: cfg.WALSegmentSize,
	})
	if err != nil {
		return err
	}

	// Reconcile the log with the snapshot.
	need := snapIndex + 1
	if n.log.FirstSeq() > need {
		return fmt.Errorf("%w: log starts at %d but the newest usable snapshot ends at %d", ErrLogGap, n.log.FirstSeq(), snapIndex)
	}
	logEmpty := n.log.LastSeq() < n.log.FirstSeq()
	switch {
	case n.log.LastSeq()+1 < need:
		// The log stops short of the snapshot: it is superseded.
		if err := n.log.Reset(need); err != nil {
			return err
		}
	case snap != nil && snapKind == snapKindInstalled && n.log.FirstSeq() < need:
		// A crash between committing an installed snapshot and resetting the
		// log. Old entries above the snapshot are not part of the history the
		// snapshot came from and must not be resurrected.
		if err := n.log.Reset(need); err != nil {
			return err
		}
	}

	// A node with no durable state at all either starts a new cluster or waits
	// to be sent a snapshot.
	fresh := snap == nil && !hasHS && logEmpty
	if fresh && (cfg.Bootstrap || cfg.BootstrapLearner) {
		ids := append([]uint64(nil), cfg.InitialVoters...)
		if cfg.InitialVoters == nil && cfg.Bootstrap {
			ids = make([]uint64, 0, len(cfg.Peers))
			for id := range cfg.Peers {
				ids = append(ids, id)
			}
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		snapCS = &pb.ConfState{Voters: ids}
		if cfg.BootstrapLearner {
			snapCS.Learners = []uint64{cfg.ID}
		}
		extra, err := encodeExtra(snapKindLocal, snapCS)
		if err != nil {
			return err
		}
		w, err := n.snaps.Create(snapshot.Meta{Index: 1, Term: 1, Extra: extra})
		if err != nil {
			return err
		}
		if err := w.Commit(); err != nil {
			return err
		}
		if err := n.log.Reset(2); err != nil {
			return err
		}
		snap = &snapshot.Snapshot{Meta: snapshot.Meta{Index: 1, Term: 1, Extra: extra}}
		snapIndex, snapTerm = 1, 1
	}

	n.ms = raft.NewMemoryStorage()
	if snap != nil {
		err := n.ms.ApplySnapshot(&pb.Snapshot{
			Data: snap.Data,
			Metadata: &pb.SnapshotMetadata{
				Index: new(snapIndex), Term: new(snapTerm), ConfState: snapCS,
			},
		})
		if err != nil {
			return err
		}
		if err := n.sm.Restore(snap.Data); err != nil {
			return fmt.Errorf("raftnode: restoring snapshot %d: %w", snapIndex, err)
		}
	}
	n.applied, n.lastSnapIndex, n.confState = snapIndex, snapIndex, snapCS

	var ents []*pb.Entry
	if from := snapIndex + 1; n.log.LastSeq() >= from {
		prevTerm := snapTerm
		err := n.log.Read(from, func(seq uint64, payload []byte) error {
			e := new(pb.Entry)
			if err := proto.Unmarshal(payload, e); err != nil {
				return err
			}
			if e.GetIndex() != seq || e.GetTerm() < prevTerm {
				return fmt.Errorf("raftnode: log entry at seq %d is inconsistent (index %d, term %d after term %d)", seq, e.GetIndex(), e.GetTerm(), prevTerm)
			}
			prevTerm = e.GetTerm()
			ents = append(ents, e)
			return nil
		})
		if err != nil {
			return err
		}
	}
	if len(ents) > 0 {
		if err := n.ms.Append(ents); err != nil {
			return err
		}
	}

	// Term can never be below the snapshot's. Commit is clamped into what the
	// log can actually back; it is re-learned from the leader anyway.
	term, vote, commit := hs.GetTerm(), hs.GetVote(), hs.GetCommit()
	if term < snapTerm {
		term, vote = snapTerm, 0
	}
	last, _ := n.ms.LastIndex()
	commit = min(max(commit, snapIndex), last)
	if err := n.ms.SetHardState(&pb.HardState{Term: new(term), Vote: new(vote), Commit: new(commit)}); err != nil {
		return err
	}

	n.rn, err = raft.NewRawNode(&raft.Config{
		ID:                        cfg.ID,
		ElectionTick:              cfg.ElectionTick,
		HeartbeatTick:             cfg.HeartbeatTick,
		Storage:                   n.ms,
		Applied:                   snapIndex,
		MaxSizePerMsg:             1 << 20,
		MaxCommittedSizePerReady:  1 << 20,
		MaxUncommittedEntriesSize: 64 << 20,
		MaxInflightMsgs:           256,
		CheckQuorum:               true,
		// Safe ReadIndex: the leader proves leadership with a heartbeat quorum.
		// Lease-based reads would depend on bounded clock drift.
		ReadOnlyOption: raft.ReadOnlySafe,
		PreVote:        true,
		// Raft's follower-to-leader forwarding (MsgProp) is the one raft message
		// that is not idempotent: a duplicated or retried forward appends the
		// command twice. Proposals are therefore accepted only on the leader.
		DisableProposalForwarding: true,
		Logger:                    cfg.Logger,
	})
	return err
}

// -- driving raft ------------------------------------------------------------

func (n *Node) scheduleTick() {
	n.tick = n.cfg.Clock.AfterFunc(n.cfg.TickInterval, func() {
		if n.stopped {
			return
		}
		n.rn.Tick()
		n.process()
		if !n.stopped {
			n.scheduleTick()
		}
	})
}

func (n *Node) onMessage(from string, payload []byte) {
	if n.stopped {
		return
	}
	m := new(pb.Message)
	if proto.Unmarshal(payload, m) != nil || m.GetTo() != n.cfg.ID {
		return
	}
	// The sender must be who it claims, or any node could speak as another.
	if n.cfg.Peers[m.GetFrom()] != from {
		return
	}
	if n.rn.Step(m) != nil { // local-only messages and unknown peers
		return
	}
	n.process()
}

// process drains raft until it has nothing more to do. It is not re-entrant:
// a callback that proposes while a Ready is being handled just queues the
// proposal, and this loop picks it up.
func (n *Node) process() {
	if n.processing {
		return
	}
	n.processing = true
	defer func() { n.processing = false }()
	for !n.stopped && n.rn.HasReady() {
		rd := n.rn.Ready()
		if err := n.handleReady(&rd); err != nil {
			n.fatal(err)
			return
		}
		n.rn.Advance(rd)
	}
}

func (n *Node) handleReady(rd *raft.Ready) error {
	hasSnap := !raft.IsEmptySnap(rd.Snapshot)

	// 1-3: make everything durable before anything leaves this process.
	if hasSnap {
		if err := n.persistInstalledSnapshot(rd.Snapshot); err != nil {
			return err
		}
	}
	if len(rd.Entries) > 0 {
		if err := n.persistEntries(rd.Entries); err != nil {
			return err
		}
	}
	if rd.HardState != nil {
		if err := n.persistHardState(rd.HardState); err != nil {
			return err
		}
	}

	// Mirror into raft's in-memory storage, now that the data is durable.
	if hasSnap {
		if err := n.ms.ApplySnapshot(rd.Snapshot); err != nil {
			return err
		}
	}
	if len(rd.Entries) > 0 {
		if err := n.ms.Append(rd.Entries); err != nil {
			return err
		}
	}
	if rd.HardState != nil {
		if err := n.ms.SetHardState(rd.HardState); err != nil {
			return err
		}
	}

	// 4: send, then apply.
	if err := n.send(rd.Messages); err != nil {
		return err
	}
	if hasSnap {
		meta := rd.Snapshot.GetMetadata()
		if err := n.sm.Restore(rd.Snapshot.Data); err != nil {
			return fmt.Errorf("raftnode: restoring installed snapshot %d: %w", meta.GetIndex(), err)
		}
		n.applied, n.lastSnapIndex = meta.GetIndex(), meta.GetIndex()
		n.confState = proto.Clone(pb.EnsureConfState(meta.GetConfState())).(*pb.ConfState)
	}
	for _, rs := range rd.ReadStates {
		if len(rs.RequestCtx) != 8 {
			continue
		}
		// An answer for an unknown or already answered request is ignored; a
		// duplicated forward can produce a second answer for the same read.
		if r := n.reads[binary.LittleEndian.Uint64(rs.RequestCtx)]; r != nil && !r.confirmed {
			r.confirmed, r.index = true, rs.Index
		}
	}
	for _, e := range rd.CommittedEntries {
		if err := n.apply(e); err != nil {
			return err
		}
	}
	n.releaseReads()
	return n.maybeSnapshot()
}

func (n *Node) persistInstalledSnapshot(s *pb.Snapshot) error {
	meta := s.GetMetadata()
	extra, err := encodeExtra(snapKindInstalled, meta.GetConfState())
	if err != nil {
		return err
	}
	w, err := n.snaps.Create(snapshot.Meta{Index: meta.GetIndex(), Term: meta.GetTerm(), Extra: extra})
	if err != nil {
		return err
	}
	if _, err := w.Write(s.Data); err != nil {
		return err
	}
	if err := w.Commit(); err != nil {
		return err
	}
	// The snapshot is durable; only now is the older log superseded.
	if err := n.log.Reset(meta.GetIndex() + 1); err != nil {
		return err
	}
	return n.snaps.Prune(2)
}

func (n *Node) persistEntries(ents []*pb.Entry) error {
	first := ents[0].GetIndex()
	if first <= n.log.LastSeq() {
		// Raft is overwriting a conflicting uncommitted suffix.
		invalidateConf := n.confPending && first <= n.confIndex
		if err := n.log.TruncateTo(first - 1); err != nil {
			return err
		}
		if invalidateConf {
			n.invalidateConfChange()
		}
	}
	if first != n.log.LastSeq()+1 {
		return fmt.Errorf("raftnode: entries start at %d but the log ends at %d", first, n.log.LastSeq())
	}
	for _, e := range ents {
		b, err := proto.Marshal(e)
		if err != nil {
			return err
		}
		seq, err := n.log.Append(b)
		if err != nil {
			return err
		}
		if seq != e.GetIndex() {
			return fmt.Errorf("raftnode: entry %d landed at log position %d", e.GetIndex(), seq)
		}
	}
	return n.log.Sync()
}

func (n *Node) persistHardState(hs *pb.HardState) error {
	if hs.GetTerm() == n.persistedTerm && hs.GetVote() == n.persistedVote {
		return nil // only a commit-index change; that is re-learned after a restart
	}
	b, err := proto.Marshal(hs)
	if err != nil {
		return err
	}
	if err := atomicfile.Write(n.cfg.FS, path.Join(n.cfg.Dir, hardStateFile), b); err != nil {
		return err
	}
	n.persistedTerm, n.persistedVote = hs.GetTerm(), hs.GetVote()
	return nil
}

func (n *Node) send(msgs []*pb.Message) error {
	for _, m := range msgs {
		to := m.GetTo()
		addr, ok := n.cfg.Peers[to]
		if !ok {
			continue
		}
		b, err := proto.Marshal(m)
		if err != nil {
			return err
		}
		if err := n.cfg.Endpoint.Notify(addr, raftMethod, b); err != nil {
			n.observe(EventTransportError, 0, 0, err)
			n.rn.ReportUnreachable(to)
			continue
		}
		if m.GetType() == pb.MsgSnap {
			// Delivery is not confirmed. If it was lost, the follower keeps
			// rejecting appends and the leader sends another snapshot.
			n.rn.ReportSnapshot(to, raft.SnapshotFinish)
		}
	}
	return nil
}

func (n *Node) apply(e *pb.Entry) error {
	idx := e.GetIndex()
	// Raft hands over committed entries exactly once and in order. Skipping a
	// replay silently would hide a bug, so any deviation stops the node.
	if idx != n.applied+1 {
		return fmt.Errorf("raftnode: committed entry %d delivered after %d", idx, n.applied)
	}
	switch e.GetType() {
	case pb.EntryNormal:
		if len(e.Data) > 0 { // an empty entry is the leader's no-op at the start of its term
			proposer, req, cmd, err := decodeFrame(e.Data)
			if err != nil {
				return err
			}
			var res []byte
			if aware, ok := n.sm.(MembershipAwareStateMachine); ok {
				conf := n.confState
				res = aware.ApplyWithMembership(idx, cmd,
					append([]uint64(nil), conf.GetVoters()...),
					append([]uint64(nil), conf.GetLearners()...),
					append([]uint64(nil), conf.GetVotersOutgoing()...),
					append([]uint64(nil), conf.GetLearnersNext()...),
					conf.GetAutoLeave())
			} else {
				res = n.sm.Apply(idx, cmd)
			}
			if proposer == n.cfg.ID {
				if w, ok := n.waiters[req]; ok {
					delete(n.waiters, req)
					w.timer.Stop()
					n.observeRequest(EventProposalApplied, req, idx, n.cfg.Clock.Now()-w.started, nil)
					w.cb(res, nil)
				}
			}
		}
	case pb.EntryConfChange:
		cc := new(pb.ConfChange)
		if err := proto.Unmarshal(e.Data, cc); err != nil {
			return err
		}
		n.confState = proto.Clone(n.rn.ApplyConfChange(cc)).(*pb.ConfState)
		n.resolveConfChange(nil, idx)
	case pb.EntryConfChangeV2:
		cc := new(pb.ConfChangeV2)
		if err := proto.Unmarshal(e.Data, cc); err != nil {
			return err
		}
		n.confState = proto.Clone(n.rn.ApplyConfChange(cc)).(*pb.ConfState)
		n.resolveConfChange(cc.Context, idx)
	}
	n.applied = idx
	return nil
}

// maybeSnapshot takes a local snapshot every SnapshotEvery applied entries. The
// order is the contract: snapshot durable first, only then is log discarded.
func (n *Node) maybeSnapshot() error {
	if n.applied-n.lastSnapIndex < n.cfg.SnapshotEvery {
		return nil
	}
	data, err := n.sm.Snapshot()
	if err != nil {
		return fmt.Errorf("raftnode: taking snapshot: %w", err)
	}
	term, err := n.ms.Term(n.applied)
	if err != nil {
		return err
	}
	extra, err := encodeExtra(snapKindLocal, n.confState)
	if err != nil {
		return err
	}
	w, err := n.snaps.Create(snapshot.Meta{Index: n.applied, Term: term, Extra: extra})
	if err != nil {
		return err
	}
	if _, err := w.Write(data); err != nil {
		return err
	}
	if err := w.Commit(); err != nil {
		return err
	}
	if _, err := n.ms.CreateSnapshot(n.applied, n.confState, data); err != nil && !errors.Is(err, raft.ErrSnapOutOfDate) {
		return err
	}
	n.lastSnapIndex = n.applied
	n.observe(EventSnapshotCreated, n.applied, 0, nil)
	if n.applied > n.cfg.SnapshotTrailing {
		keep := n.applied - n.cfg.SnapshotTrailing
		if err := n.ms.Compact(keep); err != nil && !errors.Is(err, raft.ErrCompacted) {
			return err
		}
		if err := n.log.DiscardBefore(keep + 1); err != nil {
			return err
		}
	}
	return n.snaps.Prune(2)
}

// -- proposals and status ----------------------------------------------------

// Propose submits a command, accepted only on the leader: any other node
// returns *NotLeaderError carrying the leader it knows of, for the caller to
// redirect to.
//
// If it returns nil, cb runs exactly once: with the state machine's result when
// the command is applied on this node, or with ErrResultUnknown if that has not
// happened within timeout. Unknown means the command may still commit later
// (for example if this node was deposed with the entry in flight); a caller that
// retries must make the command idempotent. A command is applied at most once
// per Propose call, whatever the network does. An error return means the
// proposal was not accepted and cb never runs.
func (n *Node) Propose(cmd []byte, timeout time.Duration, cb func(result []byte, err error)) error {
	if n.stopped {
		return ErrStopped
	}
	if !n.IsLeader() {
		return &NotLeaderError{Leader: n.Leader()}
	}
	if validator, ok := n.sm.(CommandValidator); ok {
		if err := validator.ValidateCommand(cmd); err != nil {
			return err
		}
	}
	if timeout <= 0 {
		return fmt.Errorf("raftnode: timeout must be positive, got %v", timeout)
	}
	req := n.nextReq
	n.nextReq++
	w := &waiter{cb: cb, started: n.cfg.Clock.Now()}
	w.timer = n.cfg.Clock.AfterFunc(timeout, func() {
		if got, ok := n.waiters[req]; ok && got == w {
			delete(n.waiters, req)
			n.observeRequest(EventProposalUnknown, req, 0, n.cfg.Clock.Now()-w.started, ErrResultUnknown)
			cb(nil, ErrResultUnknown)
		}
	})
	n.waiters[req] = w
	if err := n.rn.Propose(encodeFrame(n.cfg.ID, req, cmd)); err != nil {
		w.timer.Stop()
		delete(n.waiters, req)
		return err
	}
	n.observeRequest(EventProposalAccepted, req, 0, 0, nil)
	n.process()
	return nil
}

// LinearizableRead calls cb once this node's state machine reflects every write
// that completed before the call began. cb runs on the node's serial context, so
// it may read the state machine directly; the state is at least that fresh, and
// may be newer. A nil error is the only case in which the state may be trusted.
//
// If it returns nil, cb runs exactly once, with nil, or with ErrReadTimeout if
// the read was not confirmed within timeout (a leader that lost its quorum never
// confirms), or with ErrStopped. Reads have no side effects, so every error is
// safe to retry, on this or another node. An error return means nothing was
// started and cb never runs: *NotLeaderError when no leader is known, or
// ErrLeaderNotReady.
func (n *Node) LinearizableRead(timeout time.Duration, cb func(error)) error {
	if n.stopped {
		return ErrStopped
	}
	if timeout <= 0 {
		return fmt.Errorf("raftnode: timeout must be positive, got %v", timeout)
	}
	if n.IsLeader() {
		if !n.committedInOwnTerm() {
			return ErrLeaderNotReady
		}
	} else if n.Leader() == 0 {
		return &NotLeaderError{}
	}
	id := n.nextReq
	n.nextReq++
	r := &reader{cb: cb, started: n.cfg.Clock.Now()}
	r.timer = n.cfg.Clock.AfterFunc(timeout, func() {
		if got, ok := n.reads[id]; ok && got == r {
			delete(n.reads, id)
			n.observeRequest(EventReadCompleted, id, 0, n.cfg.Clock.Now()-r.started, ErrReadTimeout)
			cb(ErrReadTimeout)
		}
	})
	n.reads[id] = r
	var ctx [8]byte
	binary.LittleEndian.PutUint64(ctx[:], id)
	n.rn.ReadIndex(ctx[:])
	n.process()
	return nil
}

// committedInOwnTerm reports whether the commit index lies in this node's
// current term. Raft silently drops a leader's read requests until it does,
// because before that it cannot know what the previous leader committed.
func (n *Node) committedInOwnTerm() bool {
	st := n.rn.BasicStatus()
	t, err := n.ms.Term(st.GetCommit())
	return err == nil && t == st.GetTerm()
}

// releaseReads completes every confirmed read whose index has been applied, in
// request order so that simulations replay identically.
func (n *Node) releaseReads() {
	var ready []uint64
	for id, r := range n.reads {
		if r.confirmed && r.index <= n.applied {
			ready = append(ready, id)
		}
	}
	sort.Slice(ready, func(i, j int) bool { return ready[i] < ready[j] })
	for _, id := range ready {
		r := n.reads[id]
		delete(n.reads, id)
		r.timer.Stop()
		n.observeRequest(EventReadCompleted, id, r.index, n.cfg.Clock.Now()-r.started, nil)
		r.cb(nil)
	}
}

// Stop shuts the node down gracefully, flushing the log. Accepted but unresolved
// writes and membership changes complete with ErrResultUnknown because they may
// still commit after this process stops; reads complete with ErrStopped.
func (n *Node) Stop() error {
	if n.stopped {
		return nil
	}
	n.shutdown()
	return n.log.Close()
}

func (n *Node) shutdown() {
	n.stopped = true
	if n.tick != nil {
		n.tick.Stop()
	}
	if n.leaseTick != nil {
		n.leaseTick.Stop()
		n.leaseTick = nil
	}
	if n.kvCompactTick != nil {
		n.kvCompactTick.Stop()
		n.kvCompactTick = nil
	}
	ws := make([]uint64, 0, len(n.waiters))
	for req := range n.waiters {
		ws = append(ws, req)
	}
	sort.Slice(ws, func(i, j int) bool { return ws[i] < ws[j] })
	for _, req := range ws {
		w := n.waiters[req]
		delete(n.waiters, req)
		w.timer.Stop()
		n.observeRequest(EventProposalUnknown, req, 0, n.cfg.Clock.Now()-w.started, ErrResultUnknown)
		w.cb(nil, ErrResultUnknown)
	}
	cs := make([]uint64, 0, len(n.confWaiters))
	for req := range n.confWaiters {
		cs = append(cs, req)
	}
	sort.Slice(cs, func(i, j int) bool { return cs[i] < cs[j] })
	for _, req := range cs {
		w := n.confWaiters[req]
		delete(n.confWaiters, req)
		w.timer.Stop()
		n.observeRequest(EventProposalUnknown, req, n.confIndex, n.cfg.Clock.Now()-w.started, ErrResultUnknown)
		w.cb(nil, ErrResultUnknown)
	}
	n.confPending, n.confIndex = false, 0
	readErr := ErrStopped
	if n.fatalErr != nil {
		readErr = fmt.Errorf("%w: node failed: %v", ErrStopped, n.fatalErr)
	}
	rs := make([]uint64, 0, len(n.reads))
	for id := range n.reads {
		rs = append(rs, id)
	}
	sort.Slice(rs, func(i, j int) bool { return rs[i] < rs[j] })
	for _, id := range rs {
		r := n.reads[id]
		delete(n.reads, id)
		r.timer.Stop()
		n.observeRequest(EventReadCompleted, id, r.index, n.cfg.Clock.Now()-r.started, readErr)
		r.cb(readErr)
	}
}

func (n *Node) fatal(err error) {
	if n.fatalErr != nil {
		return
	}
	n.fatalErr = err
	n.observe(EventNodeFatal, n.applied, 0, err)
	n.shutdown()
	n.log.Close()
	if n.cfg.OnFatal != nil {
		n.cfg.OnFatal(err)
	}
}

// Err returns the error that stopped the node, if it failed.
func (n *Node) Err() error { return n.fatalErr }

// ID returns this member's Raft ID.
func (n *Node) ID() uint64 { return n.cfg.ID }

// Applied returns the index of the last entry applied to the state machine.
func (n *Node) Applied() uint64 { return n.applied }

// Leader returns the Raft ID of the leader this node knows of, or 0.
func (n *Node) Leader() uint64 { return n.rn.BasicStatus().Lead }

// IsLeader reports whether this node currently believes it is the leader.
func (n *Node) IsLeader() bool { return n.rn.BasicStatus().RaftState == raft.StateLeader }

// Term returns the node's current term.
func (n *Node) Term() uint64 { return n.rn.BasicStatus().GetTerm() }

// LastSnapshotIndex returns the index of the newest snapshot known to this node.
func (n *Node) LastSnapshotIndex() uint64 { return n.lastSnapIndex }
