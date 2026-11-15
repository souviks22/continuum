package sim

import (
	"fmt"
	"time"
)

// Clock is monotonic virtual time, advanced only by the Scheduler.
//
// Time is a time.Duration measured from the start of the simulation. An
// integer type is used deliberately: floating-point drift would make two runs
// of the same schedule diverge for reasons unrelated to the failure under
// test. time.Duration (rather than a bare int64) means the production Clock
// abstraction can share the same types without conversion.
type Clock struct {
	now time.Duration
}

// NewClock returns a clock starting at start. It panics if start is negative.
func NewClock(start time.Duration) *Clock {
	if start < 0 {
		panic(fmt.Sprintf("sim: negative clock start %v", start))
	}
	return &Clock{now: start}
}

// Now returns the current virtual time.
func (c *Clock) Now() time.Duration { return c.now }

// AdvanceTo moves the clock forward to t. Advancing to the current time is a
// no-op. It panics if t is in the past: time never moves backward.
func (c *Clock) AdvanceTo(t time.Duration) {
	if t < c.now {
		panic(fmt.Sprintf("sim: clock cannot move backward: now=%v, requested=%v", c.now, t))
	}
	c.now = t
}
