package sim

import (
	"time"

	"github.com/souviks22/continuum-go/internal/clock"
)

var _ clock.Clock = (*ProcessClock)(nil)

// ProcessClock is the clock.Clock of one simulated process. Kill makes every
// timer it ever handed out, and any it would hand out later, inert. That is what
// "the process died" means for code that schedules callbacks: without it, a
// crashed node's old timers would keep firing into a restarted node.
type ProcessClock struct {
	s       *Scheduler
	dead    bool
	handles []*Handle
}

// NewProcessClock returns a live clock for a new simulated process.
func (s *Scheduler) NewProcessClock() *ProcessClock { return &ProcessClock{s: s} }

// Now implements clock.Clock.
func (c *ProcessClock) Now() time.Duration { return c.s.Now() }

type deadTimer struct{}

func (deadTimer) Stop() bool { return false }

// AfterFunc implements clock.Clock. After Kill it returns a timer that never fires.
func (c *ProcessClock) AfterFunc(d time.Duration, f func()) clock.Timer {
	if c.dead {
		return deadTimer{}
	}
	h := c.s.ScheduleAfter(d, func() {
		if !c.dead {
			f()
		}
	})
	if len(c.handles) >= 1024 { // drop finished handles so long runs stay bounded
		live := c.handles[:0]
		for _, old := range c.handles {
			if !old.ev.fired && !old.ev.cancelled {
				live = append(live, old)
			}
		}
		c.handles = live
	}
	c.handles = append(c.handles, h)
	return timer{h}
}

// Kill marks the process dead and cancels its pending timers.
func (c *ProcessClock) Kill() {
	c.dead = true
	for _, h := range c.handles {
		h.Cancel()
	}
	c.handles = nil
}
