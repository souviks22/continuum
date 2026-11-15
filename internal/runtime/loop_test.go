package runtime

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestPostBackpressureAndCloseDrain(t *testing.T) {
	loop, err := NewLoop(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := loop.Post(nil, func() {}); err == nil {
		t.Fatal("Post accepted a nil context")
	}
	go loop.Run()

	entered := make(chan struct{})
	release := make(chan struct{})
	var ran atomic.Int32
	if err := loop.Post(context.Background(), func() {
		close(entered)
		<-release
		ran.Add(1)
	}); err != nil {
		t.Fatal(err)
	}
	<-entered
	if err := loop.Post(context.Background(), func() { ran.Add(1) }); err != nil {
		t.Fatal(err)
	}
	if err := loop.TryPost(func() { t.Error("backpressured task ran") }); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("TryPost error = %v, want ErrBackpressure", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := loop.Post(ctx, func() { t.Error("canceled task ran") }); !errors.Is(err, context.Canceled) {
		t.Fatalf("Post error = %v, want context.Canceled", err)
	}

	closed := make(chan struct{})
	go func() { loop.Close(); close(closed) }()
	close(release)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close did not drain accepted tasks")
	}
	if got := ran.Load(); got != 2 {
		t.Fatalf("ran %d accepted tasks, want 2", got)
	}
	if err := loop.Dispatch(func() {}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Dispatch after close = %v, want ErrClosed", err)
	}
}
