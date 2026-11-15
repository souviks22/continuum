package runtime

import (
	"context"
	"errors"
	"sync"
)

var (
	ErrClosed       = errors.New("runtime: event loop closed")
	ErrBackpressure = errors.New("runtime: event loop queue is full")
)

type task func()

// Loop serializes callbacks on one goroutine. Queue is bounded; Post provides
// backpressure instead of allowing producers to grow memory without limit.
type Loop struct {
	queue    chan task
	space    chan struct{}
	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once
	mu       sync.Mutex
	closed   bool
}

func NewLoop(queueSize int) (*Loop, error) {
	if queueSize < 1 {
		return nil, errors.New("runtime: event loop queue must be positive")
	}
	return &Loop{queue: make(chan task, queueSize), space: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{})}, nil
}

func (l *Loop) Run() {
	defer close(l.done)
	for {
		select {
		case f := <-l.queue:
			l.signalSpace()
			f()
		case <-l.stop:
			for {
				select {
				case f := <-l.queue:
					l.signalSpace()
					f()
				default:
					return
				}
			}
		}
	}
}

func (l *Loop) Post(ctx context.Context, f func()) error {
	if ctx == nil {
		return errors.New("runtime: nil event loop context")
	}
	if f == nil {
		return errors.New("runtime: nil event loop task")
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		l.mu.Lock()
		if l.closed {
			l.mu.Unlock()
			return ErrClosed
		}
		select {
		case l.queue <- f:
			l.mu.Unlock()
			return nil
		default:
			l.mu.Unlock()
		}
		select {
		case <-l.stop:
			return ErrClosed
		case <-ctx.Done():
			return ctx.Err()
		case <-l.space:
		}
	}
}

// TryPost enqueues a task without waiting for queue capacity. It is useful when
// two serialized runtimes dispatch to each other and blocking could form a
// backpressure cycle.
func (l *Loop) TryPost(f func()) error {
	if f == nil {
		return errors.New("runtime: nil event loop task")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	select {
	case l.queue <- f:
		return nil
	default:
		return ErrBackpressure
	}
}

// Dispatch posts a callback and waits under bounded backpressure. It is used
// by timer and transport adapters; closing the loop releases blocked dispatches.
func (l *Loop) Dispatch(f func()) error { return l.Post(context.Background(), f) }

func (l *Loop) Close() {
	l.stopOnce.Do(func() { l.mu.Lock(); l.closed = true; close(l.stop); l.mu.Unlock() })
	<-l.done
}
func (l *Loop) Done() <-chan struct{} { return l.done }

func (l *Loop) signalSpace() {
	select {
	case l.space <- struct{}{}:
	default:
	}
}
