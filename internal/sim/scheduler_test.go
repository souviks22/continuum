package sim_test

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/souviks22/continuum/internal/sim"
)

const ms = time.Millisecond

func TestEventsFireInTimeOrder(t *testing.T) {
	s := sim.NewScheduler()
	var order []string
	s.ScheduleAfter(30*ms, func() { order = append(order, "c") })
	s.ScheduleAfter(10*ms, func() { order = append(order, "a") })
	s.ScheduleAfter(20*ms, func() { order = append(order, "b") })
	_, err := s.RunUntilIdle(sim.Unbounded)
	mustNoErr(t, err)
	if want := []string{"a", "b", "c"}; !reflect.DeepEqual(order, want) {
		t.Fatalf("got %v, want %v", order, want)
	}
}

func TestTiesBrokenByInsertionOrder(t *testing.T) {
	s := sim.NewScheduler()
	var order []string
	for _, name := range []string{"first", "second", "third"} {
		name := name
		s.ScheduleAt(10*ms, func() { order = append(order, name) })
	}
	_, err := s.RunUntilIdle(sim.Unbounded)
	mustNoErr(t, err)
	if want := []string{"first", "second", "third"}; !reflect.DeepEqual(order, want) {
		t.Fatalf("got %v, want %v", order, want)
	}
}

func TestClockAdvancesToEventTime(t *testing.T) {
	s := sim.NewScheduler()
	var seen []time.Duration
	s.ScheduleAfter(15*ms, func() { seen = append(seen, s.Now()) })
	_, err := s.RunUntilIdle(sim.Unbounded)
	mustNoErr(t, err)
	if !reflect.DeepEqual(seen, []time.Duration{15 * ms}) || s.Now() != 15*ms {
		t.Fatalf("seen=%v now=%v", seen, s.Now())
	}
}

func TestRunUntilStopsAtBoundaryAndStillAdvancesClock(t *testing.T) {
	s := sim.NewScheduler()
	var fired []int
	s.ScheduleAfter(5*ms, func() { fired = append(fired, 5) })
	s.ScheduleAfter(50*ms, func() { fired = append(fired, 50) })
	s.RunUntil(10 * ms)
	if !reflect.DeepEqual(fired, []int{5}) {
		t.Fatalf("fired=%v", fired)
	}
	if s.Now() != 10*ms {
		t.Fatalf("clock should advance to boundary, got %v", s.Now())
	}
	if s.Pending() != 1 {
		t.Fatalf("pending=%d, want 1", s.Pending())
	}
}

func TestRunUntilBeforeNowIsNoop(t *testing.T) {
	s := sim.NewScheduler()
	s.RunUntil(100 * ms)
	s.RunUntil(50 * ms)
	if s.Now() != 100*ms {
		t.Fatalf("clock moved: %v", s.Now())
	}
}

func TestCancelledEventDoesNotFire(t *testing.T) {
	s := sim.NewScheduler()
	fired := false
	h := s.ScheduleAfter(10*ms, func() { fired = true })
	h.Cancel()
	if s.Pending() != 0 {
		t.Fatalf("pending=%d after cancel", s.Pending())
	}
	_, err := s.RunUntilIdle(sim.Unbounded)
	mustNoErr(t, err)
	if fired {
		t.Fatal("cancelled event fired")
	}
}

func TestCancelIsIdempotentAndNoopAfterFire(t *testing.T) {
	s := sim.NewScheduler()
	h1 := s.ScheduleAfter(1*ms, func() {})
	h2 := s.ScheduleAfter(2*ms, func() {})
	h1.Cancel()
	h1.Cancel() // must not double-decrement Pending
	if s.Pending() != 1 {
		t.Fatalf("pending=%d, want 1", s.Pending())
	}
	_, err := s.RunUntilIdle(sim.Unbounded)
	mustNoErr(t, err)
	h2.Cancel() // already fired
	if s.Pending() != 0 {
		t.Fatalf("pending=%d, want 0", s.Pending())
	}
}

func TestEventCancellingItselfIsHarmless(t *testing.T) {
	s := sim.NewScheduler()
	var h *sim.Handle
	h = s.ScheduleAfter(1*ms, func() { h.Cancel() })
	_, err := s.RunUntilIdle(sim.Unbounded)
	mustNoErr(t, err)
	if s.Pending() != 0 {
		t.Fatalf("pending=%d, want 0", s.Pending())
	}
}

func TestCannotScheduleInThePast(t *testing.T) {
	s := sim.NewScheduler()
	s.RunUntil(100 * ms)
	mustPanic(t, func() { s.ScheduleAt(50*ms, func() {}) })
}

func TestNegativeDelayRejected(t *testing.T) {
	s := sim.NewScheduler()
	mustPanic(t, func() { s.ScheduleAfter(-1, func() {}) })
}

func TestRunUntilIdleGuardsAgainstInfiniteRetryLoop(t *testing.T) {
	s := sim.NewScheduler()
	var retry func()
	retry = func() { s.ScheduleAfter(1*ms, retry) }
	s.ScheduleAfter(1*ms, retry)
	steps, err := s.RunUntilIdle(1000)
	if !errors.Is(err, sim.ErrStepLimit) {
		t.Fatalf("err=%v, want ErrStepLimit", err)
	}
	if steps != 1000 {
		t.Fatalf("steps=%d, want 1000", steps)
	}
}

func TestStepLimitIgnoresCancelledEvents(t *testing.T) {
	s := sim.NewScheduler()
	ran := 0
	s.ScheduleAfter(1*ms, func() { ran++ })
	for i := 0; i < 10; i++ {
		s.ScheduleAfter(2*ms, func() {}).Cancel()
	}
	steps, err := s.RunUntilIdle(1)
	mustNoErr(t, err)
	if steps != 1 || ran != 1 {
		t.Fatalf("steps=%d ran=%d", steps, ran)
	}
}

func TestCallbackSchedulingNewEventIsProcessedInOrder(t *testing.T) {
	s := sim.NewScheduler()
	var order []string
	s.ScheduleAfter(1*ms, func() {
		order = append(order, "one")
		s.ScheduleAfter(5*ms, func() { order = append(order, "two") })
	})
	_, err := s.RunUntilIdle(sim.Unbounded)
	mustNoErr(t, err)
	if want := []string{"one", "two"}; !reflect.DeepEqual(order, want) {
		t.Fatalf("got %v, want %v", order, want)
	}
}
