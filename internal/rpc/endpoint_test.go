package rpc_test

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/souviks22/continuum-go/internal/rpc"
	"github.com/souviks22/continuum-go/internal/sim"
)

const ms = time.Millisecond

type outcome struct {
	at      time.Duration
	payload string
	err     error
}

type cluster struct {
	t     *testing.T
	sched *sim.Scheduler
	net   *sim.Network
	eps   map[string]*rpc.Endpoint
	tps   map[string]*sim.Transport
}

func newCluster(t *testing.T, seed uint64, ids ...string) *cluster {
	t.Helper()
	s := sim.NewScheduler()
	n := sim.NewNetwork(s, seed)
	if err := n.SetDefaultLink(sim.LinkConfig{MinDelay: 1 * ms, MaxDelay: 1 * ms}); err != nil {
		t.Fatal(err)
	}
	c := &cluster{t: t, sched: s, net: n, eps: map[string]*rpc.Endpoint{}, tps: map[string]*sim.Transport{}}
	for _, id := range ids {
		tp, err := sim.NewTransport(id, n)
		if err != nil {
			t.Fatal(err)
		}
		c.tps[id] = tp
		c.eps[id] = rpc.NewEndpoint(tp, s, rpc.Options{})
	}
	return c
}

func (c *cluster) drain() {
	c.t.Helper()
	if _, err := c.sched.RunUntilIdle(sim.Unbounded); err != nil {
		c.t.Fatal(err)
	}
}

func (c *cluster) register(id, method string, h rpc.Handler) {
	c.t.Helper()
	if err := c.eps[id].Register(method, h); err != nil {
		c.t.Fatal(err)
	}
}

// call issues a call and appends its outcome to out when the callback runs.
func (c *cluster) call(from, to, method, payload string, timeout time.Duration, out *[]outcome) {
	c.t.Helper()
	err := c.eps[from].Call(to, method, []byte(payload), timeout, func(p []byte, err error) {
		*out = append(*out, outcome{c.sched.Now(), string(p), err})
	})
	if err != nil {
		c.t.Fatalf("call: %v", err)
	}
}

func echo(_ string, p []byte) ([]byte, error) { return p, nil }

func TestSuccessfulCallDeliversReplyPayload(t *testing.T) {
	c := newCluster(t, 0, "a", "b")
	c.register("b", "Echo", echo)
	var got []outcome
	c.call("a", "b", "Echo", "hi", 100*ms, &got)
	c.drain()

	want := []outcome{{at: 2 * ms, payload: "hi"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	if c.eps["a"].PendingCalls() != 0 {
		t.Fatal("pending call leaked")
	}
}

func TestUnknownMethodIsADefinitiveError_NotATimeout(t *testing.T) {
	c := newCluster(t, 0, "a", "b") // b registers nothing
	var got []outcome
	c.call("a", "b", "DoesNotExist", "", 100*ms, &got)
	c.drain()

	if len(got) != 1 || !errors.Is(got[0].err, rpc.ErrUnknownMethod) {
		t.Fatalf("got %+v, want one ErrUnknownMethod", got)
	}
	if got[0].at != 2*ms {
		t.Fatalf("answered at %v; a definitive error should arrive in one round trip, not at the timeout", got[0].at)
	}
}

func TestHandlerErrorBecomesRemoteError(t *testing.T) {
	c := newCluster(t, 0, "a", "b")
	c.register("b", "Fail", func(string, []byte) ([]byte, error) { return nil, errors.New("disk full") })
	var got []outcome
	c.call("a", "b", "Fail", "", 100*ms, &got)
	c.drain()

	var re *rpc.RemoteError
	if len(got) != 1 || !errors.As(got[0].err, &re) || re.Message != "disk full" {
		t.Fatalf("got %+v, want RemoteError(disk full)", got)
	}
}

func TestCallAcrossPartitionTimesOut(t *testing.T) {
	c := newCluster(t, 0, "a", "b")
	c.register("b", "Echo", echo)
	c.net.Partition([]string{"a"}, []string{"b"})
	var got []outcome
	c.call("a", "b", "Echo", "hi", 10*ms, &got)
	c.drain()

	if len(got) != 1 || !errors.Is(got[0].err, rpc.ErrTimeout) || got[0].at != 10*ms {
		t.Fatalf("got %+v, want one ErrTimeout at 10ms", got)
	}
	if c.eps["a"].PendingCalls() != 0 {
		t.Fatal("pending call leaked after timeout")
	}
}

func TestCloseCompletesPendingCallsInRequestOrder(t *testing.T) {
	c := newCluster(t, 1, "a", "b")
	var got []outcome
	for _, payload := range []string{"first", "second", "third"} {
		c.call("a", "b", "missing", payload, time.Second, &got)
	}
	if c.eps["a"].PendingCalls() != 3 {
		t.Fatalf("pending=%d, want 3", c.eps["a"].PendingCalls())
	}
	c.eps["a"].Close()
	if len(got) != 3 || c.eps["a"].PendingCalls() != 0 {
		t.Fatalf("close callbacks=%+v pending=%d", got, c.eps["a"].PendingCalls())
	}
	for i, outcome := range got {
		if !errors.Is(outcome.err, rpc.ErrClosed) || outcome.payload != "" || outcome.at != 0 {
			t.Fatalf("callback %d: %+v", i, outcome)
		}
	}
	if err := c.eps["a"].Call("b", "missing", nil, time.Second, func([]byte, error) {}); !errors.Is(err, rpc.ErrClosed) {
		t.Fatalf("Call after Close: %v", err)
	}
}

func TestLateReplyAfterTimeoutIsIgnoredNotDoubleDelivered(t *testing.T) {
	c := newCluster(t, 0, "a", "b")
	if err := c.net.SetLink("b", "a", sim.LinkConfig{MinDelay: 20 * ms, MaxDelay: 20 * ms}); err != nil {
		t.Fatal(err)
	}
	c.register("b", "Echo", echo)
	var got []outcome
	c.call("a", "b", "Echo", "hi", 5*ms, &got)
	c.drain()

	if len(got) != 1 || !errors.Is(got[0].err, rpc.ErrTimeout) {
		t.Fatalf("got %+v, want exactly one ErrTimeout", got)
	}
}

func TestDuplicatedReplyInvokesCallbackOnce(t *testing.T) {
	c := newCluster(t, 0, "a", "b")
	if err := c.net.SetLink("b", "a", sim.LinkConfig{MinDelay: 1 * ms, MaxDelay: 1 * ms, DuplicateRate: 1.0}); err != nil {
		t.Fatal(err)
	}
	c.register("b", "Echo", echo)
	var got []outcome
	c.call("a", "b", "Echo", "hi", 100*ms, &got)
	c.drain()

	if len(got) != 1 || got[0].err != nil {
		t.Fatalf("got %+v, want exactly one success", got)
	}
}

func TestConcurrentPendingRequestsCorrelateIndependently(t *testing.T) {
	c := newCluster(t, 0, "a", "b")
	c.register("b", "Upper", func(_ string, p []byte) ([]byte, error) {
		return append([]byte("re:"), p...), nil
	})
	results := map[string]string{}
	for _, in := range []string{"1", "2", "3"} {
		in := in
		err := c.eps["a"].Call("b", "Upper", []byte(in), 100*ms, func(p []byte, err error) {
			if err != nil {
				t.Errorf("call %s: %v", in, err)
			}
			results[in] = string(p)
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	c.drain()

	want := map[string]string{"1": "re:1", "2": "re:2", "3": "re:3"}
	if !reflect.DeepEqual(results, want) {
		t.Fatalf("got %v, want %v", results, want)
	}
}

func TestTimeoutStillFiresIfDestinationCrashesMidFlight(t *testing.T) {
	c := newCluster(t, 0, "a", "b")
	c.register("b", "Echo", echo)
	if err := c.net.SetDefaultLink(sim.LinkConfig{MinDelay: 5 * ms, MaxDelay: 5 * ms}); err != nil {
		t.Fatal(err)
	}
	var got []outcome
	c.call("a", "b", "Echo", "hi", 20*ms, &got)
	if _, err := c.net.CrashAt(1*ms, "b"); err != nil {
		t.Fatal(err)
	}
	c.drain()

	if len(got) != 1 || !errors.Is(got[0].err, rpc.ErrTimeout) {
		t.Fatalf("got %+v, want one ErrTimeout", got)
	}
}

func TestHandlerSeesRequestingNode(t *testing.T) {
	c := newCluster(t, 0, "a", "b")
	var seen []string
	c.register("b", "Ping", func(from string, _ []byte) ([]byte, error) {
		seen = append(seen, from)
		return nil, nil
	})
	var got []outcome
	c.call("a", "b", "Ping", "", 100*ms, &got)
	c.drain()

	if !reflect.DeepEqual(seen, []string{"a"}) {
		t.Fatalf("seen=%v", seen)
	}
}

func TestReplyCancelsTheTimeoutTimer(t *testing.T) {
	c := newCluster(t, 0, "a", "b")
	c.register("b", "Echo", echo)
	var got []outcome
	c.call("a", "b", "Echo", "hi", 1000*ms, &got)
	c.sched.RunUntil(3 * ms) // reply has arrived, timeout (at 1000ms) has not fired

	if len(got) != 1 {
		t.Fatalf("reply not delivered: %+v", got)
	}
	if c.sched.Pending() != 0 {
		t.Fatalf("%d events still scheduled: the timeout timer leaked after the reply", c.sched.Pending())
	}
}

func TestAsyncHandlerCanReplyLaterExactlyOnce(t *testing.T) {
	c := newCluster(t, 0, "a", "b")
	var finish func([]byte, error)
	if err := c.eps["b"].RegisterAsync("Commit", func(from string, payload []byte, reply func([]byte, error)) {
		if from != "a" || string(payload) != "proposal" {
			t.Errorf("async request from=%q payload=%q", from, payload)
		}
		finish = reply
	}); err != nil {
		t.Fatal(err)
	}
	var got []outcome
	c.call("a", "b", "Commit", "proposal", 100*ms, &got)
	c.sched.RunUntil(1 * ms) // request accepted; commit result is not available yet
	if len(got) != 0 || finish == nil {
		t.Fatalf("async handler completed too early: got=%+v finish=%v", got, finish != nil)
	}
	finish([]byte("committed"), nil)
	finish([]byte("duplicate"), nil)
	c.drain()
	if len(got) != 1 || got[0].payload != "committed" || got[0].err != nil || got[0].at != 2*ms {
		t.Fatalf("async outcome=%+v", got)
	}
	if c.eps["a"].PendingCalls() != 0 {
		t.Fatal("async reply left a pending call")
	}
}

func TestAsyncHandlerErrorsBecomeRemoteErrorsAndRegistrationIsUnique(t *testing.T) {
	c := newCluster(t, 0, "a", "b")
	if err := c.eps["b"].RegisterAsync("Commit", func(_ string, _ []byte, reply func([]byte, error)) {
		reply(nil, errors.New("not leader"))
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.eps["b"].Register("Commit", echo); err == nil {
		t.Fatal("sync handler replaced async handler")
	}
	if err := c.eps["b"].RegisterNotify("Commit", func(string, []byte) {}); err == nil {
		t.Fatal("notify handler reused request method")
	}
	var got []outcome
	c.call("a", "b", "Commit", "proposal", 100*ms, &got)
	c.drain()
	var remote *rpc.RemoteError
	if len(got) != 1 || !errors.As(got[0].err, &remote) || remote.Message != "not leader" {
		t.Fatalf("async error outcome=%+v", got)
	}
}

func TestReplyFromTheWrongPeerIsIgnored(t *testing.T) {
	s := sim.NewScheduler()
	n := sim.NewNetwork(s, 0)
	if err := n.SetDefaultLink(sim.LinkConfig{MinDelay: 5 * ms, MaxDelay: 5 * ms}); err != nil {
		t.Fatal(err)
	}
	mk := func(id string) (*sim.Transport, *rpc.Endpoint) {
		tp, err := sim.NewTransport(id, n)
		if err != nil {
			t.Fatal(err)
		}
		return tp, rpc.NewEndpoint(tp, s, rpc.Options{IDBase: 1000})
	}
	_, a := mk("a")
	_, b := mk("b")
	tc, _ := mk("c")
	if err := b.Register("Echo", echo); err != nil {
		t.Fatal(err)
	}

	var got []outcome
	if err := a.Call("b", "Echo", []byte("real"), 100*ms, func(p []byte, err error) {
		got = append(got, outcome{s.Now(), string(p), err})
	}); err != nil {
		t.Fatal(err)
	}
	// c forges a reply with the ID a just used (1000), arriving before b's real one.
	forged, err := rpc.Message{Kind: rpc.KindReply, ID: 1000, Payload: []byte("forged")}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if err := n.SetLink("c", "a", sim.LinkConfig{MinDelay: 1 * ms, MaxDelay: 1 * ms}); err != nil {
		t.Fatal(err)
	}
	if err := tc.Send("a", forged); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RunUntilIdle(sim.Unbounded); err != nil {
		t.Fatal(err)
	}

	if len(got) != 1 || got[0].payload != "real" || got[0].err != nil {
		t.Fatalf("got %+v, want only the real reply", got)
	}
}

func TestUndecodableInboundMessageIsReportedNotFatal(t *testing.T) {
	s := sim.NewScheduler()
	n := sim.NewNetwork(s, 0)
	ta, _ := sim.NewTransport("a", n)
	tb, _ := sim.NewTransport("b", n)
	var errs []error
	rpc.NewEndpoint(ta, s, rpc.Options{OnError: func(_ string, err error) { errs = append(errs, err) }})

	garbage := []byte{99, 1, 0, 0, 0, 0} // wrong version
	if err := tb.Send("a", garbage); err != nil {
		t.Fatal(err)
	}
	if err := tb.Send("a", []byte{1}); err != nil { // truncated
		t.Fatal(err)
	}
	if _, err := s.RunUntilIdle(sim.Unbounded); err != nil {
		t.Fatal(err)
	}

	if len(errs) != 2 || !errors.Is(errs[0], rpc.ErrUnsupportedVersion) || !errors.Is(errs[1], rpc.ErrMalformed) {
		t.Fatalf("errs=%v", errs)
	}
}

func TestCallToUnknownNodeFailsSynchronouslyAndNeverCallsBack(t *testing.T) {
	c := newCluster(t, 0, "a")
	called := false
	err := c.eps["a"].Call("ghost", "Echo", nil, 10*ms, func([]byte, error) { called = true })
	if err == nil {
		t.Fatal("expected error for unknown destination")
	}
	c.drain()
	if called {
		t.Fatal("callback ran even though Call returned an error")
	}
	if c.eps["a"].PendingCalls() != 0 || c.sched.Pending() != 0 {
		t.Fatalf("leak: pending=%d events=%d", c.eps["a"].PendingCalls(), c.sched.Pending())
	}
}

func TestCallValidatesArguments(t *testing.T) {
	c := newCluster(t, 0, "a", "b")
	cb := func([]byte, error) { t.Error("callback must not run") }
	if c.eps["a"].Call("b", "Echo", nil, 0, cb) == nil {
		t.Fatal("zero timeout accepted")
	}
	if c.eps["a"].Call("b", "", nil, 10*ms, cb) == nil {
		t.Fatal("empty method accepted")
	}
	c.drain()
}

func TestRegisterRejectsDuplicatesAndBadNames(t *testing.T) {
	c := newCluster(t, 0, "a")
	c.register("a", "M", echo)
	if c.eps["a"].Register("M", echo) == nil {
		t.Fatal("duplicate method accepted")
	}
	if c.eps["a"].Register("", echo) == nil {
		t.Fatal("empty method accepted")
	}
}

func TestLossyRunsAreDeterministicPerSeed(t *testing.T) {
	run := func(seed uint64) []outcome {
		s := sim.NewScheduler()
		n := sim.NewNetwork(s, seed)
		if err := n.SetDefaultLink(sim.LinkConfig{MinDelay: 1 * ms, MaxDelay: 10 * ms, LossRate: 0.3, DuplicateRate: 0.2}); err != nil {
			t.Fatal(err)
		}
		ta, _ := sim.NewTransport("a", n)
		tb, _ := sim.NewTransport("b", n)
		a := rpc.NewEndpoint(ta, s, rpc.Options{IDBase: seed})
		b := rpc.NewEndpoint(tb, s, rpc.Options{})
		if err := b.Register("Echo", echo); err != nil {
			t.Fatal(err)
		}
		var got []outcome
		for i := 0; i < 50; i++ {
			if err := a.Call("b", "Echo", []byte{byte(i)}, 50*ms, func(p []byte, err error) {
				got = append(got, outcome{s.Now(), string(p), err})
			}); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := s.RunUntilIdle(sim.Unbounded); err != nil {
			t.Fatal(err)
		}
		return got
	}
	x, y := run(7), run(7)
	if len(x) != 50 {
		t.Fatalf("every call must complete exactly once, got %d outcomes", len(x))
	}
	if !reflect.DeepEqual(x, y) {
		t.Fatal("same seed produced different runs")
	}
}

func TestNotifyIsDeliveredOneWayWithNoReply(t *testing.T) {
	c := newCluster(t, 0, "a", "b")
	var got []string
	if err := c.eps["b"].RegisterNotify("Hint", func(from string, p []byte) { got = append(got, from+":"+string(p)) }); err != nil {
		t.Fatal(err)
	}
	if err := c.eps["a"].Notify("b", "Hint", []byte("x")); err != nil {
		t.Fatal(err)
	}
	c.drain()
	if !reflect.DeepEqual(got, []string{"a:x"}) {
		t.Fatalf("got %v", got)
	}
	if c.eps["a"].PendingCalls() != 0 {
		t.Fatal("a notify must not create a pending call")
	}
	if n := c.sched.Pending(); n != 0 {
		t.Fatalf("%d events left: nothing should have been sent back", n)
	}
}

func TestNotifyToAnUnregisteredMethodIsReportedNotAnswered(t *testing.T) {
	s := sim.NewScheduler()
	n := sim.NewNetwork(s, 0)
	ta, _ := sim.NewTransport("a", n)
	tb, _ := sim.NewTransport("b", n)
	ea := rpc.NewEndpoint(ta, s, rpc.Options{})
	var errs []error
	rpc.NewEndpoint(tb, s, rpc.Options{OnError: func(_ string, err error) { errs = append(errs, err) }})
	if err := ea.Notify("b", "Nope", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RunUntilIdle(sim.Unbounded); err != nil {
		t.Fatal(err)
	}
	if len(errs) != 1 {
		t.Fatalf("errs=%v", errs)
	}
}

func TestNotifyValidatesArgumentsAndRegistration(t *testing.T) {
	c := newCluster(t, 0, "a")
	if c.eps["a"].Notify("ghost", "M", nil) == nil {
		t.Fatal("unknown destination accepted")
	}
	if c.eps["a"].Notify("a", "", nil) == nil {
		t.Fatal("empty method accepted")
	}
	h := func(string, []byte) {}
	if err := c.eps["a"].RegisterNotify("M", h); err != nil {
		t.Fatal(err)
	}
	if c.eps["a"].RegisterNotify("M", h) == nil || c.eps["a"].RegisterNotify("", h) == nil {
		t.Fatal("duplicate or empty notify registration accepted")
	}
}
