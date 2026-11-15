package clock

import (
	"errors"
	"time"
)

// Dispatch posts callbacks onto a caller-owned serial event loop.
type Dispatch func(func()) error

// Serial is a production clock whose timers preserve the Clock serialization
// contract by dispatching fired callbacks onto the node's event loop.
type Serial struct {
	start    time.Time
	dispatch Dispatch
}

func NewSerial(dispatch Dispatch) (*Serial, error) {
	if dispatch == nil {
		return nil, errors.New("clock: serial clock requires a dispatch function")
	}
	return &Serial{start: time.Now(), dispatch: dispatch}, nil
}

func (c *Serial) Now() time.Duration { return time.Since(c.start) }

func (c *Serial) AfterFunc(delay time.Duration, f func()) Timer {
	if f == nil {
		panic("clock: nil timer callback")
	}
	return &serialTimer{clock: c, timer: time.AfterFunc(delay, func() { _ = c.dispatch(f) })}
}

type serialTimer struct {
	clock *Serial
	timer *time.Timer
}

func (t *serialTimer) Stop() bool { return t.timer.Stop() }

var _ Clock = (*Serial)(nil)
