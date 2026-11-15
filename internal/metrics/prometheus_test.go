package metrics_test

import (
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/souviks22/continuum-go/internal/metrics"
	"github.com/souviks22/continuum-go/internal/raftnode"
)

func TestPrometheusExporterUsesBoundedLabelsAndPublishesHealth(t *testing.T) {
	exporter := metrics.New()
	exporter.Observe(raftnode.Event{Name: raftnode.EventProposalUnknown, NodeID: 2, RequestID: 987654, Duration: 500000000, Err: fmt.Errorf("secret-value: %w", raftnode.ErrResultUnknown)})
	exporter.Observe(raftnode.Event{Name: raftnode.EventReadCompleted, NodeID: 2, Err: raftnode.ErrReadTimeout})
	exporter.Observe(raftnode.Event{Name: "attacker-controlled-name", NodeID: 2, Err: fmt.Errorf("secret-value")})
	exporter.SetHealth(raftnode.Health{ID: 2, Leader: 1, Commit: 10, Applied: 9, Healthy: true, Ready: false})
	r := httptest.NewRecorder()
	exporter.ServeHTTP(r, httptest.NewRequest("GET", "/metrics", nil))
	body := r.Body.String()
	for _, want := range []string{
		`continuum_events_total{node_id="2",event="proposal.unknown",outcome="unknown"} 1`,
		`continuum_events_total{node_id="2",event="read.completed",outcome="timeout"} 1`,
		`continuum_events_total{node_id="2",event="other",outcome="error"} 1`,
		`continuum_node_health{node_id="2"} 1`,
		`continuum_node_ready{node_id="2"} 0`,
		`continuum_node_leader_id{node_id="2"} 1`,
		`continuum_node_commit_index{node_id="2"} 10`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics missing %q:\n%s", want, body)
		}
	}
	if strings.Contains(body, "secret-value") || strings.Contains(body, "987654") || strings.Contains(body, "attacker-controlled-name") {
		t.Fatalf("unbounded data leaked into labels:\n%s", body)
	}
	if got := r.Header().Get("Content-Type"); !strings.HasPrefix(got, "text/plain; version=0.0.4") {
		t.Fatalf("content type=%q", got)
	}
}
