package sim

import (
	"container/heap"
	"errors"
	"fmt"
	"time"

	"github.com/souviks22/continuum-go/internal/clock"
)

// Unbounded passed as maxSteps to RunUntilIdle disables the step limit.
const Unbounded = -1

// ErrStepLimit is returned (wrapped) by RunUntilIdle when the step limit is
// hit. It almost always means the code under test schedules an unbounded chain
// of events, e.g. a retry loop with no backoff cap.
var ErrStepLimit = errors.New("sim: step limit exceeded")

type event struct {
	at        time.Duration
	seq       uint64 // insertion order; breaks ties between equal times
	fn        func()
	cancelled bool
	fired     bool
}

// eventHeap orders events by (at, seq). Ties are broken by insertion order,
// never by anything arbitrary, so equal-time events always fire in the order
// they were scheduled.
type eventHeap []*event

func (h eventHeap) Len() int { return len(h) }
func (h eventHeap) Less(i, j int) bool {
	if h[i].at != h[j].at {
		return h[i].at < h[j].at
	}
	return h[i].seq < h[j].seq
}
func (h eventHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *eventHeap) Push(x any)   { *h = append(*h, x.(*event)) }
func (h *eventHeap) Pop() any {
	old := *h
	n := len(old)
	ev := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return ev
}

// Handle lets the caller cancel a pending event.
type Handle struct {
	s  *Scheduler
	ev *event
}

// Cancel prevents the event from firing. It is a no-op if the event has
// already fired or been cancelled.
func (h *Handle) Cancel() {
	if h.ev.cancelled || h.ev.fired {
		return
	}
	h.ev.cancelled = true
	h.s.pending--
}

// Scheduler is a deterministic discrete-event scheduler. Every piece of
// simulated behavior (message delivery, timers, crash/restart) is an event
// scheduled here, so a run is reproducible given the same seed and the same
// sequence of Schedule calls.
type Scheduler struct {
	clock   *Clock
	queue   eventHeap
	seq     uint64
	pending int // live (not fired, not cancelled) events, kept for O(1) Pending
}

// NewScheduler returns a scheduler whose virtual clock starts at zero.
func NewScheduler() *Scheduler {
	return &Scheduler{clock: NewClock(0)}
}

// Now returns the current virtual time.
func (s *Scheduler) Now() time.Duration { return s.clock.Now() }

// ScheduleAt runs fn at virtual time at. It panics if at is in the past.
func (s *Scheduler) ScheduleAt(at time.Duration, fn func()) *Handle {
	if at < s.clock.Now() {
		panic(fmt.Sprintf("sim: cannot schedule an event in the past: now=%v, requested=%v", s.clock.Now(), at))
	}
	ev := &event{at: at, seq: s.seq, fn: fn}
	s.seq++
	heap.Push(&s.queue, ev)
	s.pending++
	return &Handle{s: s, ev: ev}
}

// ScheduleAfter runs fn after delay. It panics if delay is negative.
func (s *Scheduler) ScheduleAfter(delay time.Duration, fn func()) *Handle {
	if delay < 0 {
		panic(fmt.Sprintf("sim: negative delay %v", delay))
	}
	return s.ScheduleAt(s.clock.Now()+delay, fn)
}

// Pending returns the number of events that have neither fired nor been
// cancelled.
func (s *Scheduler) Pending() int { return s.pending }

// run pops and executes ev. The caller has already checked it is live.
func (s *Scheduler) run(ev *event) {
	s.clock.AdvanceTo(ev.at)
	// Mark fired before running so a callback that cancels its own handle
	// is a no-op and Pending stays correct.
	ev.fired = true
	s.pending--
	ev.fn()
}

// RunUntil processes every event with time <= end, then advances the clock to
// end even if no event landed exactly on it. If end is before Now, no events
// run and the clock is unchanged.
func (s *Scheduler) RunUntil(end time.Duration) {
	for len(s.queue) > 0 && s.queue[0].at <= end {
		ev := heap.Pop(&s.queue).(*event)
		if ev.cancelled {
			continue
		}
		s.run(ev)
	}
	if s.clock.Now() < end {
		s.clock.AdvanceTo(end)
	}
}

// RunUntilIdle processes events until none remain. If maxSteps is >= 0 and
// that many callbacks have run while live events remain, it returns
// ErrStepLimit (wrapped); pass Unbounded to disable the limit. It returns the
// number of callbacks executed. Cancelled events never count as steps.
func (s *Scheduler) RunUntilIdle(maxSteps int) (int, error) {
	steps := 0
	for len(s.queue) > 0 {
		ev := s.queue[0]
		if ev.cancelled {
			heap.Pop(&s.queue)
			continue
		}
		if maxSteps >= 0 && steps >= maxSteps {
			return steps, fmt.Errorf("%w: maxSteps=%d; likely an unbounded retry/resend loop in the code under test", ErrStepLimit, maxSteps)
		}
		heap.Pop(&s.queue)
		s.run(ev)
		steps++
	}
	return steps, nil
}

var _ clock.Clock = (*Scheduler)(nil)

type timer struct{ h *Handle }

func (t timer) Stop() bool {
	if t.h.ev.cancelled || t.h.ev.fired {
		return false
	}
	t.h.Cancel()
	return true
}

// AfterFunc implements clock.Clock. Callbacks run on the simulation goroutine,
// which satisfies the Clock serialization contract by construction.
func (s *Scheduler) AfterFunc(d time.Duration, f func()) clock.Timer {
	return timer{s.ScheduleAfter(d, f)}
}
