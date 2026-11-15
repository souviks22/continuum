// Package clock is the time seam: node logic asks a Clock for "now" and for
// timers, and never touches package time directly. In simulation the Clock is
// the deterministic sim.Scheduler; in production it is a real clock wrapped by
// the node runtime.
package clock

import "time"

// Clock provides time and timers.
//
// Concurrency contract: callbacks passed to AfterFunc run serialized with every
// other callback of the same node (timers, transport receives). Code under test
// is therefore single-threaded and needs no locks. The simulator satisfies this
// trivially; the production runtime must satisfy it by dispatching timer and
// receive events onto the node's single event-loop goroutine. A bare
// time.AfterFunc does NOT satisfy it, which is why no "real" Clock lives here.
type Clock interface {
	// Now returns monotonic time since an arbitrary fixed origin.
	Now() time.Duration
	// AfterFunc runs f once after d. d must be >= 0.
	AfterFunc(d time.Duration, f func()) Timer
}

// Timer is a pending AfterFunc.
type Timer interface {
	// Stop prevents f from running. It reports whether it did so: false means f
	// already ran or the timer was already stopped.
	Stop() bool
}
