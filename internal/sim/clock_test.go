package sim_test

import (
	"testing"
	"time"

	"github.com/souviks22/continuum/internal/sim"
)

func TestClockStartsAtGivenTime(t *testing.T) {
	if got := sim.NewClock(0).Now(); got != 0 {
		t.Fatalf("default start: got %v", got)
	}
	if got := sim.NewClock(100).Now(); got != 100 {
		t.Fatalf("start=100: got %v", got)
	}
}

func TestClockAdvanceToMovesForward(t *testing.T) {
	c := sim.NewClock(0)
	c.AdvanceTo(50 * time.Millisecond)
	if c.Now() != 50*time.Millisecond {
		t.Fatalf("got %v", c.Now())
	}
}

func TestClockAdvanceToSameTimeIsNoop(t *testing.T) {
	c := sim.NewClock(10)
	c.AdvanceTo(10)
	if c.Now() != 10 {
		t.Fatalf("got %v", c.Now())
	}
}

func TestClockCannotMoveBackward(t *testing.T) {
	c := sim.NewClock(10)
	mustPanic(t, func() { c.AdvanceTo(5) })
}

func TestClockNegativeStartRejected(t *testing.T) {
	mustPanic(t, func() { sim.NewClock(-1) })
}
