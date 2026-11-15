// Package metrics provides a small Prometheus text exporter for raftnode
// lifecycle events. It has no external dependencies and does not own an HTTP
// server; mount Exporter on the application's metrics mux.
package metrics

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"sync"

	"github.com/souviks22/continuum-go/internal/raftnode"
)

type eventKey struct {
	node          uint64
	name, outcome string
}
type durationKey struct {
	node uint64
	name string
}

type durationStat struct {
	count   uint64
	seconds float64
}

// Exporter is safe for concurrent Observe, SetHealth and scrape calls.
// Labels are limited to configured node IDs, a fixed event vocabulary and a
// fixed outcome vocabulary; error strings and request IDs are never labels.
type Exporter struct {
	mu        sync.RWMutex
	events    map[eventKey]uint64
	durations map[durationKey]durationStat
	health    map[uint64]raftnode.Health
}

func New() *Exporter {
	return &Exporter{events: make(map[eventKey]uint64), durations: make(map[durationKey]durationStat), health: make(map[uint64]raftnode.Health)}
}

func (e *Exporter) Observe(event raftnode.Event) {
	name := knownEvent(event.Name)
	outcome := errorClass(event.Err)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.events[eventKey{node: event.NodeID, name: name, outcome: outcome}]++
	if event.Duration > 0 {
		key := durationKey{node: event.NodeID, name: name}
		stat := e.durations[key]
		stat.count++
		stat.seconds += event.Duration.Seconds()
		e.durations[key] = stat
	}
}

// SetHealth publishes a snapshot obtained by polling Node.Health on that
// node's serial context. The exporter never calls into a Node while scraping.
func (e *Exporter) SetHealth(health raftnode.Health) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.health[health.ID] = health
}

func (e *Exporter) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	if err := e.WritePrometheus(w); err != nil {
		return
	}
}

// WritePrometheus writes a stable text exposition. It is also useful when an
// application uses a custom scrape handler.
func (e *Exporter) WritePrometheus(w io.Writer) error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if _, err := fmt.Fprintln(w, "# HELP continuum_events_total Lifecycle events observed by raft nodes."); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "# TYPE continuum_events_total counter"); err != nil {
		return err
	}
	keys := make([]eventKey, 0, len(e.events))
	for key := range e.events {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
		if a.node != b.node {
			return a.node < b.node
		}
		if a.name != b.name {
			return a.name < b.name
		}
		return a.outcome < b.outcome
	})
	for _, key := range keys {
		if _, err := fmt.Fprintf(w, "continuum_events_total{node_id=%q,event=%q,outcome=%q} %d\n", strconv.FormatUint(key.node, 10), key.name, key.outcome, e.events[key]); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintln(w, "# HELP continuum_event_duration_seconds_total Total observed operation duration."); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "# TYPE continuum_event_duration_seconds_total counter"); err != nil {
		return err
	}
	dkeys := make([]durationKey, 0, len(e.durations))
	for key := range e.durations {
		dkeys = append(dkeys, key)
	}
	sort.Slice(dkeys, func(i, j int) bool {
		a, b := dkeys[i], dkeys[j]
		if a.node != b.node {
			return a.node < b.node
		}
		return a.name < b.name
	})
	for _, key := range dkeys {
		if _, err := fmt.Fprintf(w, "continuum_event_duration_seconds_total{node_id=%q,event=%q} %g\n", strconv.FormatUint(key.node, 10), key.name, e.durations[key].seconds); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintln(w, "# HELP continuum_event_duration_observations_total Number of non-zero duration observations."); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "# TYPE continuum_event_duration_observations_total counter"); err != nil {
		return err
	}
	for _, key := range dkeys {
		if _, err := fmt.Fprintf(w, "continuum_event_duration_observations_total{node_id=%q,event=%q} %d\n", strconv.FormatUint(key.node, 10), key.name, e.durations[key].count); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintln(w, "# HELP continuum_node_health Node is running without a fatal error."); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "# TYPE continuum_node_health gauge"); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "# HELP continuum_node_ready Node is caught up and can serve its role."); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "# TYPE continuum_node_ready gauge"); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "# HELP continuum_node_leader_id Currently known Raft leader ID."); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "# TYPE continuum_node_leader_id gauge"); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "# HELP continuum_node_commit_index Latest commit index known by the node."); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "# TYPE continuum_node_commit_index gauge"); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "# HELP continuum_node_applied_index Latest state machine index applied by the node."); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "# TYPE continuum_node_applied_index gauge"); err != nil {
		return err
	}
	ids := make([]uint64, 0, len(e.health))
	for id := range e.health {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		h := e.health[id]
		label := strconv.FormatUint(id, 10)
		healthy := 0
		if h.Healthy {
			healthy = 1
		}
		ready := 0
		if h.Ready {
			ready = 1
		}
		for _, metric := range []struct {
			name  string
			value any
		}{{"continuum_node_health", healthy}, {"continuum_node_ready", ready}, {"continuum_node_leader_id", h.Leader}, {"continuum_node_commit_index", h.Commit}, {"continuum_node_applied_index", h.Applied}} {
			if _, err := fmt.Fprintf(w, "%s{node_id=%q} %v\n", metric.name, label, metric.value); err != nil {
				return err
			}
		}
	}
	return nil
}

func knownEvent(name string) string {
	switch name {
	case raftnode.EventProposalAccepted, raftnode.EventProposalApplied, raftnode.EventProposalUnknown, raftnode.EventReadCompleted, raftnode.EventSnapshotCreated, raftnode.EventBackupCreated, raftnode.EventMembershipApplied, raftnode.EventTransportError, raftnode.EventNodeFatal:
		return name
	default:
		return "other"
	}
}
func errorClass(err error) string {
	if err == nil {
		return "none"
	}
	switch {
	case errors.Is(err, raftnode.ErrResultUnknown):
		return "unknown"
	case errors.Is(err, raftnode.ErrReadTimeout):
		return "timeout"
	case errors.Is(err, raftnode.ErrStopped):
		return "stopped"
	case errors.Is(err, raftnode.ErrNotLeader):
		return "not_leader"
	default:
		return "error"
	}
}
