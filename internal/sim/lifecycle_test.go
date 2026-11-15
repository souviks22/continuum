package sim_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/souviks22/continuum/internal/sim"
)

func TestNodeStartsUpByDefault(t *testing.T) {
	_, n := makeNet(t, 0)
	register(t, n, "a", noop)
	if !n.IsUp("a") {
		t.Fatal("node should start up")
	}
}

func TestCrashNowMarksNodeDownAndRestartNowUp(t *testing.T) {
	_, n := makeNet(t, 0)
	register(t, n, "a", noop)
	mustNoErr(t, n.CrashNow("a"))
	if n.IsUp("a") {
		t.Fatal("node should be down")
	}
	mustNoErr(t, n.RestartNow("a", nil))
	if !n.IsUp("a") {
		t.Fatal("node should be up")
	}
}

func TestLifecycleOnUnknownNodeErrors(t *testing.T) {
	_, n := makeNet(t, 0)
	if n.CrashNow("ghost") == nil || n.RestartNow("ghost", nil) == nil {
		t.Fatal("expected errors for unknown node")
	}
	if _, err := n.CrashAt(1, "ghost"); err == nil {
		t.Fatal("CrashAt should validate eagerly")
	}
	if _, err := n.RestartAt(1, "ghost", nil); err == nil {
		t.Fatal("RestartAt should validate eagerly")
	}
}

func TestMessageSentByCrashedNodeNeverDelivered(t *testing.T) {
	s, n := makeNet(t, 0)
	count := 0
	register(t, n, "a", noop)
	register(t, n, "b", func(string, []byte) { count++ })
	mustNoErr(t, n.CrashNow("a"))

	mustNoErr(t, n.Send("a", "b", []byte("x")))
	drain(t, s)

	if count != 0 {
		t.Fatalf("delivered %d", count)
	}
}

func TestMessageToNodeThatCrashesBeforeDeliveryIsLost(t *testing.T) {
	s, n := makeNet(t, 0)
	count := 0
	register(t, n, "a", noop)
	register(t, n, "b", func(string, []byte) { count++ })
	mustNoErr(t, n.SetDefaultLink(sim.LinkConfig{MinDelay: 10 * ms, MaxDelay: 10 * ms}))

	mustNoErr(t, n.Send("a", "b", []byte("in_flight")))
	_, err := n.CrashAt(3*ms, "b") // well before arrival at t=10ms
	mustNoErr(t, err)
	drain(t, s)

	if count != 0 {
		t.Fatalf("delivered %d", count)
	}
}

func TestMessageArrivesAfterReceiverRestarts(t *testing.T) {
	s, n := makeNet(t, 0)
	var got []string
	register(t, n, "a", noop)
	register(t, n, "b", func(_ string, p []byte) { got = append(got, string(p)) })
	mustNoErr(t, n.SetDefaultLink(sim.LinkConfig{MinDelay: 10 * ms, MaxDelay: 10 * ms}))

	mustNoErr(t, n.CrashNow("b"))
	mustNoErr(t, n.Send("a", "b", []byte("sent_while_down"))) // a is up, so the send succeeds
	_, err := n.RestartAt(5*ms, "b", nil)
	mustNoErr(t, err)
	drain(t, s)

	if !reflect.DeepEqual(got, []string{"sent_while_down"}) {
		t.Fatalf("got %v", got)
	}
}

func TestOnRestartRunsBeforeNodeMarkedUp(t *testing.T) {
	s, n := makeNet(t, 0)
	var upDuringRecovery []bool
	register(t, n, "a", noop)
	mustNoErr(t, n.CrashNow("a"))
	_, err := n.RestartAt(5*ms, "a", func() { upDuringRecovery = append(upDuringRecovery, n.IsUp("a")) })
	mustNoErr(t, err)
	drain(t, s)

	if !reflect.DeepEqual(upDuringRecovery, []bool{false}) {
		t.Fatalf("node must still be down during recovery, got %v", upDuringRecovery)
	}
	if !n.IsUp("a") {
		t.Fatal("node should be up after restart")
	}
}

func TestCrashAtSchedulesCrashInTheFuture(t *testing.T) {
	s, n := makeNet(t, 0)
	var at []time.Duration
	register(t, n, "a", noop)
	register(t, n, "b", func(string, []byte) { at = append(at, s.Now()) })
	mustNoErr(t, n.SetDefaultLink(sim.LinkConfig{MinDelay: 1 * ms, MaxDelay: 1 * ms}))

	_, err := n.CrashAt(100*ms, "b")
	mustNoErr(t, err)
	mustNoErr(t, n.Send("a", "b", []byte("before_crash")))
	drain(t, s)

	if !reflect.DeepEqual(at, []time.Duration{1 * ms}) || n.IsUp("b") {
		t.Fatalf("at=%v up=%v", at, n.IsUp("b"))
	}
}

func TestRepeatedCrashRestartCycle(t *testing.T) {
	s, n := makeNet(t, 0)
	register(t, n, "a", noop)
	for _, step := range []struct {
		crash bool
		at    time.Duration
	}{{true, 10 * ms}, {false, 20 * ms}, {true, 30 * ms}, {false, 40 * ms}} {
		var err error
		if step.crash {
			_, err = n.CrashAt(step.at, "a")
		} else {
			_, err = n.RestartAt(step.at, "a", nil)
		}
		mustNoErr(t, err)
	}

	for _, c := range []struct {
		until time.Duration
		up    bool
	}{{15 * ms, false}, {25 * ms, true}, {35 * ms, false}, {45 * ms, true}} {
		s.RunUntil(c.until)
		if n.IsUp("a") != c.up {
			t.Fatalf("at %v: up=%v, want %v", c.until, n.IsUp("a"), c.up)
		}
	}
}
