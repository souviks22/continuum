package sim_test

import (
	"reflect"
	"testing"

	"github.com/souviks22/continuum-go/internal/sim"
)

func TestTransportDeliversRawPayloadWithSender(t *testing.T) {
	s, n := makeNet(t, 0)
	a, err := sim.NewTransport("a", n)
	mustNoErr(t, err)
	b, err := sim.NewTransport("b", n)
	mustNoErr(t, err)
	type rx struct{ from, payload string }
	var got []rx
	b.SetReceiveHandler(func(from string, p []byte) { got = append(got, rx{from, string(p)}) })

	mustNoErr(t, a.Send("b", []byte("hello")))
	drain(t, s)

	if want := []rx{{"a", "hello"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestTransportSendToUnknownNodeErrors(t *testing.T) {
	_, n := makeNet(t, 0)
	a, err := sim.NewTransport("a", n)
	mustNoErr(t, err)
	if a.Send("ghost", []byte("x")) == nil {
		t.Fatal("expected error")
	}
}

func TestTransportDuplicateNodeIDErrors(t *testing.T) {
	_, n := makeNet(t, 0)
	_, err := sim.NewTransport("a", n)
	mustNoErr(t, err)
	if _, err := sim.NewTransport("a", n); err == nil {
		t.Fatal("expected error on duplicate id")
	}
}

func TestTransportMessageBeforeHandlerIsAWiringBugAndPanics(t *testing.T) {
	s, n := makeNet(t, 0)
	a, _ := sim.NewTransport("a", n)
	_, _ = sim.NewTransport("b", n) // no handler set
	mustNoErr(t, a.Send("b", []byte("x")))
	mustPanic(t, func() { drain(t, s) })
}

func TestSchedulerAfterFuncTimerStopSemantics(t *testing.T) {
	s := sim.NewScheduler()
	fired := 0
	tm := s.AfterFunc(5*ms, func() { fired++ })
	if !tm.Stop() {
		t.Fatal("Stop on a pending timer must return true")
	}
	if tm.Stop() {
		t.Fatal("second Stop must return false")
	}
	tm2 := s.AfterFunc(5*ms, func() { fired++ })
	drain(t, s)
	if fired != 1 {
		t.Fatalf("fired=%d, want 1 (only the unstopped timer)", fired)
	}
	if tm2.Stop() {
		t.Fatal("Stop after firing must return false")
	}
}

func TestProcessClockKillSilencesPendingAndFutureTimersOnlyForThatProcess(t *testing.T) {
	s := sim.NewScheduler()
	a, b := s.NewProcessClock(), s.NewProcessClock()
	var fired []string
	a.AfterFunc(5*ms, func() { fired = append(fired, "a-pending") })
	b.AfterFunc(5*ms, func() { fired = append(fired, "b") })
	if s.Pending() != 2 {
		t.Fatalf("pending=%d", s.Pending())
	}
	a.Kill()
	if s.Pending() != 1 {
		t.Fatalf("Kill must cancel the process's queued events, pending=%d", s.Pending())
	}
	tm := a.AfterFunc(1*ms, func() { fired = append(fired, "a-after-kill") })
	if tm.Stop() {
		t.Fatal("a timer from a dead process must be inert")
	}
	drain(t, s)
	if !reflect.DeepEqual(fired, []string{"b"}) {
		t.Fatalf("fired=%v, want only the surviving process's timer", fired)
	}
	if b.Now() != 5*ms {
		t.Fatalf("clocks share virtual time: %v", b.Now())
	}
}
