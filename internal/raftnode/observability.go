package raftnode

import (
	"time"

	"go.etcd.io/raft/v3"
)

const (
	EventProposalAccepted  = "proposal.accepted"
	EventProposalApplied   = "proposal.applied"
	EventProposalUnknown   = "proposal.unknown"
	EventReadCompleted     = "read.completed"
	EventSnapshotCreated   = "snapshot.created"
	EventBackupCreated     = "backup.created"
	EventMembershipApplied = "membership.applied"
	EventTransportError    = "transport.error"
	EventNodeFatal         = "node.fatal"
)

// Event is a low-cardinality node lifecycle observation. Observer callbacks
// run on the node's serial context and must be fast and must not call back into
// the same Node. Adapters can translate events into counters, structured logs,
// or tracing spans without imposing a metrics dependency on raftnode.
type Event struct {
	Name      string
	NodeID    uint64
	RequestID uint64
	Term      uint64
	Index     uint64
	Duration  time.Duration
	Err       error
}

// Observer receives node events synchronously on the serial context.
type Observer interface{ Observe(Event) }

// Health is an instantaneous, read-only node health and readiness view.
type Health struct {
	ID                uint64
	Term              uint64
	Leader            uint64
	Commit            uint64
	Applied           uint64
	LastIndex         uint64
	LastSnapshotIndex uint64
	IsLeader          bool
	Healthy           bool
	Ready             bool
	Err               error
}

// Health returns whether this node is running and caught up to its known
// commit index. A leader is not ready until it has committed an entry in its
// current term; followers are ready only while they know a leader.
func (n *Node) Health() Health {
	status := n.rn.BasicStatus()
	last, _ := n.ms.LastIndex()
	leader := status.Lead
	healthy := !n.stopped && n.fatalErr == nil
	ready := healthy && leader != 0 && n.applied >= status.GetCommit()
	isLeader := status.RaftState == raft.StateLeader
	if ready && isLeader && !n.committedInOwnTerm() {
		ready = false
	}
	return Health{ID: n.cfg.ID, Term: status.GetTerm(), Leader: leader, Commit: status.GetCommit(), Applied: n.applied,
		LastIndex: last, LastSnapshotIndex: n.lastSnapIndex, IsLeader: isLeader, Healthy: healthy, Ready: ready, Err: n.fatalErr}
}

func (n *Node) observe(name string, index uint64, duration time.Duration, err error) {
	n.observeRequest(name, 0, index, duration, err)
}

func (n *Node) observeRequest(name string, requestID, index uint64, duration time.Duration, err error) {
	if n.cfg.Observer == nil {
		return
	}
	n.cfg.Observer.Observe(Event{Name: name, NodeID: n.cfg.ID, RequestID: requestID, Term: n.Term(), Index: index, Duration: duration, Err: err})
}
