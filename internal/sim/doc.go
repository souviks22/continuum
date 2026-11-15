// Package sim is a deterministic discrete-event simulator: a virtual clock, an
// event scheduler, and a fault-injecting network.
//
// Rules that make runs reproducible, and that code under test must respect:
//
//   - Single-threaded. Everything runs on the goroutine that calls
//     Scheduler.RunUntil / RunUntilIdle. Nothing here is safe for concurrent
//     use, deliberately: locks would add nondeterminism and hide races that the
//     simulator is meant to expose through ordering, not scheduling luck.
//   - No wall-clock time. "Now" is Scheduler.Now(); time.Now and time.Sleep
//     must never be reached from simulated code.
//   - One seeded RNG per Network (math/rand/v2 PCG, whose output stream is
//     documented as stable). The global RNG is never used.
//   - No behavior depends on Go map iteration order. Maps here are only ever
//     looked up by key.
//
// Misuse that can only be a programming error (moving the clock backward,
// scheduling in the past) panics. Conditions a caller could plausibly hit with
// bad input (invalid LinkConfig, unknown node ID) return errors.
package sim
