package client

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/souviks22/continuum-go/internal/kv"
	"github.com/souviks22/continuum-go/internal/raftnode"
	"github.com/souviks22/continuum-go/internal/rpc"
	"github.com/souviks22/continuum-go/internal/sim"
)

type fakePeer struct {
	id             uint64
	leader         uint64
	command        [][]byte
	callback       []func([]byte, error)
	readCallbacks  []func(error)
	readStartError error
	proposeError   error
}

type fakeMembershipNode struct {
	redirect   uint64
	lastAction byte
	lastID     uint64
	callback   func(error)
}

type fakeMembershipReadNode struct {
	startErr error
}

func (n *fakeMembershipReadNode) LinearizableRead(_ time.Duration, cb func(error)) error {
	if n.startErr != nil {
		return n.startErr
	}
	cb(nil)
	return nil
}

func (n *fakeMembershipNode) change(action byte, id uint64, cb func(error)) error {
	if n.redirect != 0 {
		return &raftnode.NotLeaderError{Leader: n.redirect}
	}
	n.lastAction, n.lastID, n.callback = action, id, cb
	return nil
}
func (n *fakeMembershipNode) AddLearner(id uint64, _ time.Duration, cb func(error)) error {
	return n.change(membershipAddLearner, id, cb)
}
func (n *fakeMembershipNode) PromoteLearner(id uint64, _ time.Duration, cb func(error)) error {
	return n.change(membershipPromoteLearner, id, cb)
}
func (n *fakeMembershipNode) RemoveMember(id uint64, _ time.Duration, cb func(error)) error {
	return n.change(membershipRemove, id, cb)
}

func (p *fakePeer) ID() uint64     { return p.id }
func (p *fakePeer) Leader() uint64 { return p.leader }
func (p *fakePeer) IsLeader() bool { return p.leader == p.id }
func (p *fakePeer) Propose(cmd []byte, _ time.Duration, cb func([]byte, error)) error {
	if p.proposeError != nil {
		return p.proposeError
	}
	p.command = append(p.command, append([]byte(nil), cmd...))
	p.callback = append(p.callback, cb)
	return nil
}

func (p *fakePeer) reply(i int, b []byte, err error) { p.callback[i](b, err) }

func (p *fakePeer) LinearizableRead(_ time.Duration, cb func(error)) error {
	if p.readStartError != nil {
		return p.readStartError
	}
	p.readCallbacks = append(p.readCallbacks, cb)
	return nil
}

func TestRetryPreservesSessionCommandAndDeduplicates(t *testing.T) {
	p := &fakePeer{id: 1, leader: 1}
	c, err := New([]Peer{p}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	sm := kv.NewStateMachine()
	var openErr error
	if err := c.OpenSession(func(err error) { openErr = err }); err != nil {
		t.Fatal(err)
	}
	p.reply(0, sm.Apply(10, p.command[0]), nil)
	if openErr != nil || c.Session() != 10 {
		t.Fatalf("open: session=%d err=%v", c.Session(), openErr)
	}

	var firstErr error
	if err := c.Txn(kv.Put([]byte("k"), []byte("v")), func(_ kv.TxnResult, err error) { firstErr = err }); err != nil {
		t.Fatal(err)
	}
	if err := c.Retry(func(kv.TxnResult, error) {}); !errors.Is(err, ErrRetryNotReady) {
		t.Fatalf("retry before ambiguous completion: %v", err)
	}
	firstCommand := append([]byte(nil), p.command[1]...)
	// The first attempt applies, but its reply times out.
	firstResult := sm.Apply(11, p.command[1])
	p.reply(1, nil, raftnode.ErrResultUnknown)
	if !errors.Is(firstErr, ErrUnknownResult) || !c.busy {
		t.Fatalf("first attempt: err=%v busy=%v", firstErr, c.busy)
	}

	var retryResult kv.TxnResult
	var retryErr error
	if err := c.Retry(func(r kv.TxnResult, err error) { retryResult, retryErr = r, err }); err != nil {
		t.Fatal(err)
	}
	if string(p.command[2]) != string(firstCommand) {
		t.Fatal("retry changed the replicated command bytes")
	}
	// The state machine recognizes the same session sequence and returns the
	// exact original result without applying the write at index 12.
	p.reply(2, sm.Apply(12, p.command[2]), nil)
	decodedFirst, err := kv.DecodeResult(firstResult)
	if err != nil {
		t.Fatal(err)
	}
	if retryErr != nil || retryResult.Revision != decodedFirst.Revision || c.NextSequence() != 2 || c.busy {
		t.Fatalf("retry: result=%+v err=%v next=%d busy=%v", retryResult, retryErr, c.NextSequence(), c.busy)
	}
	got, err := sm.Store().Range([]byte("k"), nil, kv.RangeOptions{})
	if err != nil || got.Revision != 11 || len(got.KVs) != 1 || string(got.KVs[0].Value) != "v" {
		t.Fatalf("write applied more than once or missing: %+v err=%v", got, err)
	}
}

func TestSessionStateRestoresExactPendingCommand(t *testing.T) {
	p := &fakePeer{id: 1, leader: 1}
	old, err := New([]Peer{p}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	sm := kv.NewStateMachine()
	if err := old.OpenSession(func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}); err != nil {
		t.Fatal(err)
	}
	p.reply(0, sm.Apply(10, p.command[0]), nil)
	if err := old.Txn(kv.Put([]byte("key"), []byte("value")), func(kv.TxnResult, error) {}); err != nil {
		t.Fatal(err)
	}
	want := append([]byte(nil), p.command[1]...)
	first := sm.Apply(11, want) // commit, then lose the reply and checkpoint
	state, err := old.ExportSessionState()
	if err != nil {
		t.Fatal(err)
	}

	newPeer := &fakePeer{id: 1, leader: 1}
	restored, err := New([]Peer{newPeer}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.RestoreSessionState(state); err != nil {
		t.Fatal(err)
	}
	if restored.Session() != 10 || restored.NextSequence() != 1 {
		t.Fatalf("restored session=%d next=%d", restored.Session(), restored.NextSequence())
	}
	var got kv.TxnResult
	var gotErr error
	if err := restored.Retry(func(r kv.TxnResult, err error) { got, gotErr = r, err }); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(newPeer.command[0], want) {
		t.Fatal("restored retry changed command bytes")
	}
	newPeer.reply(0, sm.Apply(12, newPeer.command[0]), nil)
	firstResult, err := kv.DecodeResult(first)
	if err != nil {
		t.Fatal(err)
	}
	if gotErr != nil || got.Revision != firstResult.Revision {
		t.Fatalf("retry result=%+v err=%v", got, gotErr)
	}

	for i := range state {
		bad := append([]byte(nil), state...)
		bad[i] ^= 0x80
		candidate, _ := New([]Peer{&fakePeer{id: 2}}, time.Second)
		if err := candidate.RestoreSessionState(bad); err == nil {
			t.Fatalf("accepted corrupted checkpoint at byte %d", i)
		}
	}
}

func TestClientRejectsConcurrentOrInvalidCalls(t *testing.T) {
	if _, err := New(nil, time.Second); !errors.Is(err, ErrNoPeers) {
		t.Fatalf("New(nil): %v", err)
	}
	if _, err := New([]Peer{nil}, time.Second); err == nil {
		t.Fatal("New accepted a nil peer")
	}
	p := &fakePeer{id: 1, leader: 1}
	c, err := New([]Peer{p}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Txn(kv.Txn{}, func(kv.TxnResult, error) {}); !errors.Is(err, ErrNoSession) {
		t.Fatalf("Txn before open: %v", err)
	}
	if err := c.OpenSession(func(error) {}); err != nil {
		t.Fatal(err)
	}
	if err := c.OpenSession(func(error) {}); !errors.Is(err, ErrBusy) {
		t.Fatalf("concurrent open: %v", err)
	}
}

func TestRangeReadsOnlyAfterLinearizableBarrier(t *testing.T) {
	p := &fakePeer{id: 1, leader: 1}
	sm := kv.NewStateMachine()
	put, err := kv.Put([]byte("service/a"), []byte("10.0.0.1")).Encode(kv.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	sm.Apply(5, put)
	bound, err := WithStore(p, sm.Store())
	if err != nil {
		t.Fatal(err)
	}
	c, err := New([]Peer{bound}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var result kv.RangeResult
	var readErr error
	called := false
	if err := c.Range([]byte("service/"), kv.PrefixEnd([]byte("service/")), kv.RangeOptions{}, func(r kv.RangeResult, err error) {
		called, result, readErr = true, r, err
	}); err != nil {
		t.Fatal(err)
	}
	if called {
		t.Fatal("range ran before ReadIndex completed")
	}
	if len(p.readCallbacks) != 1 {
		t.Fatalf("read barriers=%d, want 1", len(p.readCallbacks))
	}
	p.readCallbacks[0](nil)
	if !called || readErr != nil || result.Revision != 5 || len(result.KVs) != 1 || string(result.KVs[0].Value) != "10.0.0.1" {
		t.Fatalf("range result=%+v err=%v called=%v", result, readErr, called)
	}
}

func TestRangeCanFallBackWhenPeerCannotStartRead(t *testing.T) {
	first := &fakePeer{id: 1, leader: 0}
	second := &fakePeer{id: 2, leader: 2}
	firstBound, err := WithStore(first, kv.NewStore())
	if err != nil {
		t.Fatal(err)
	}
	secondBound, err := WithStore(second, kv.NewStore())
	if err != nil {
		t.Fatal(err)
	}
	// Make the first peer reject synchronously; the second starts the read.
	first.readStartError = &raftnode.NotLeaderError{}
	c, err := New([]Peer{firstBound, secondBound}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	called := false
	if err := c.Range([]byte("k"), nil, kv.RangeOptions{}, func(_ kv.RangeResult, err error) {
		called = true
		if err != nil {
			t.Errorf("range callback: %v", err)
		}
	}); err != nil {
		t.Fatal(err)
	}
	if len(first.readCallbacks) != 0 || len(second.readCallbacks) != 1 || called {
		t.Fatalf("first reads=%d second reads=%d called=%v", len(first.readCallbacks), len(second.readCallbacks), called)
	}
	second.readCallbacks[0](nil)
	if !called {
		t.Fatal("second peer callback did not complete the range")
	}
}

func TestAutomaticRetryUsesBackoffAndIdenticalSessionCommand(t *testing.T) {
	p := &fakePeer{id: 1, leader: 1}
	sched := sim.NewScheduler()
	c, err := NewWithOptions([]Peer{p}, Options{
		Timeout: time.Second, Clock: sched, MaxAttempts: 2, RetryBackoff: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	sm := kv.NewStateMachine()
	if err := c.OpenSession(func(err error) {
		if err != nil {
			t.Errorf("open session: %v", err)
		}
	}); err != nil {
		t.Fatal(err)
	}
	p.reply(0, sm.Apply(10, p.command[0]), nil)

	callbackCalls := 0
	var result kv.TxnResult
	var txnErr error
	if err := c.Txn(kv.Put([]byte("k"), []byte("v")), func(r kv.TxnResult, err error) {
		callbackCalls++
		result, txnErr = r, err
	}); err != nil {
		t.Fatal(err)
	}
	cmd := append([]byte(nil), p.command[1]...)
	firstResult := sm.Apply(11, cmd) // committed, but the reply is lost
	p.reply(1, nil, raftnode.ErrResultUnknown)
	if callbackCalls != 0 {
		t.Fatal("client reported an unknown result before its configured retry")
	}
	sched.RunUntil(9 * time.Millisecond)
	if len(p.command) != 2 {
		t.Fatalf("retry fired before backoff: proposals=%d", len(p.command)-1)
	}
	sched.RunUntil(10 * time.Millisecond)
	if len(p.command) != 3 || string(p.command[2]) != string(cmd) {
		t.Fatal("automatic retry did not send the same command after the backoff")
	}
	// Retry returns the exact result cached by the first application.
	p.reply(2, sm.Apply(12, p.command[2]), nil)
	first, err := kv.DecodeResult(firstResult)
	if err != nil {
		t.Fatal(err)
	}
	if callbackCalls != 1 || txnErr != nil || result.Revision != first.Revision || c.NextSequence() != 2 {
		t.Fatalf("calls=%d result=%+v err=%v next=%d", callbackCalls, result, txnErr, c.NextSequence())
	}
}

func TestExhaustedAutomaticRetriesRemainExplicitlyRetryable(t *testing.T) {
	p := &fakePeer{id: 1, leader: 1}
	sched := sim.NewScheduler()
	c, err := NewWithOptions([]Peer{p}, Options{
		Timeout: time.Second, Clock: sched, MaxAttempts: 2, RetryBackoff: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	sm := kv.NewStateMachine()
	if err := c.OpenSession(func(error) {}); err != nil {
		t.Fatal(err)
	}
	p.reply(0, sm.Apply(10, p.command[0]), nil)
	calls := 0
	var got error
	if err := c.Txn(kv.Put([]byte("k"), []byte("v")), func(_ kv.TxnResult, err error) { calls++; got = err }); err != nil {
		t.Fatal(err)
	}
	p.reply(1, nil, raftnode.ErrResultUnknown)
	sched.RunUntil(time.Millisecond)
	p.reply(2, nil, raftnode.ErrResultUnknown)
	if calls != 1 || !errors.Is(got, ErrUnknownResult) || !c.busy || !c.unknown {
		t.Fatalf("calls=%d err=%v busy=%v unknown=%v", calls, got, c.busy, c.unknown)
	}
	// The callback that received ErrUnknownResult is never called again. A
	// caller can explicitly supply a new callback to retry the retained bytes.
	var retryResult kv.TxnResult
	var retryErr error
	if err := c.Retry(func(r kv.TxnResult, err error) { retryResult, retryErr = r, err }); err != nil {
		t.Fatal(err)
	}
	p.reply(3, sm.Apply(11, p.command[3]), nil)
	if retryErr != nil || retryResult.Revision != 11 || c.busy {
		t.Fatalf("explicit retry result=%+v err=%v busy=%v", retryResult, retryErr, c.busy)
	}
}

func TestCloseCancelsRetryAndIgnoresLateProposalCompletion(t *testing.T) {
	p := &fakePeer{id: 1, leader: 1}
	sched := sim.NewScheduler()
	c, err := NewWithOptions([]Peer{p}, Options{
		Timeout: time.Second, Clock: sched, MaxAttempts: 2, RetryBackoff: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	sm := kv.NewStateMachine()
	if err := c.OpenSession(func(error) {}); err != nil {
		t.Fatal(err)
	}
	p.reply(0, sm.Apply(10, p.command[0]), nil)
	calls := 0
	var got error
	if err := c.Txn(kv.Put([]byte("k"), []byte("v")), func(_ kv.TxnResult, err error) { calls++; got = err }); err != nil {
		t.Fatal(err)
	}
	p.reply(1, nil, raftnode.ErrResultUnknown)
	if calls != 0 {
		t.Fatal("retry was reported before its timer or Close")
	}
	c.Close()
	if calls != 1 || !errors.Is(got, ErrUnknownResult) {
		t.Fatalf("Close callback calls=%d err=%v", calls, got)
	}
	// Late completion from the original attempt and the cancelled timer must
	// not call the user again or launch another proposal.
	p.reply(1, []byte("late"), nil)
	sched.RunUntil(10 * time.Millisecond)
	if calls != 1 || len(p.command) != 2 {
		t.Fatalf("calls=%d proposals=%d after close", calls, len(p.command)-1)
	}
	if err := c.Range([]byte("k"), nil, kv.RangeOptions{}, func(kv.RangeResult, error) {}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Range after Close: %v", err)
	}
}

func TestSynchronousRetryErrorDoesNotInvokeItsCallbackLater(t *testing.T) {
	p := &fakePeer{id: 1, leader: 1}
	c, err := New([]Peer{p}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	sm := kv.NewStateMachine()
	if err := c.OpenSession(func(error) {}); err != nil {
		t.Fatal(err)
	}
	p.reply(0, sm.Apply(10, p.command[0]), nil)
	if err := c.Txn(kv.Put([]byte("k"), []byte("v")), func(kv.TxnResult, error) {}); err != nil {
		t.Fatal(err)
	}
	p.reply(1, nil, raftnode.ErrResultUnknown)
	if !c.unknown {
		t.Fatal("one-attempt client did not report an unknown outcome")
	}

	startErr := errors.New("leader unavailable")
	p.proposeError = startErr
	callbackCalls := 0
	if err := c.Retry(func(kv.TxnResult, error) { callbackCalls++ }); !errors.Is(err, startErr) {
		t.Fatalf("Retry error=%v, want %v", err, startErr)
	}
	c.Close()
	if callbackCalls != 0 {
		t.Fatalf("synchronous Retry error callback ran %d times", callbackCalls)
	}
}

func TestRangeRetriesFailedReadIndexWithBoundedBackoff(t *testing.T) {
	p := &fakePeer{id: 1, leader: 1}
	sched := sim.NewScheduler()
	sm := kv.NewStateMachine()
	cmd, err := kv.Put([]byte("k"), []byte("v")).Encode(kv.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	sm.Apply(5, cmd)
	bound, err := WithStore(p, sm.Store())
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewWithOptions([]Peer{bound}, Options{
		Timeout: time.Second, Clock: sched, ReadMaxAttempts: 2, ReadRetryBackoff: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	var result kv.RangeResult
	var readErr error
	if err := c.Range([]byte("k"), nil, kv.RangeOptions{}, func(r kv.RangeResult, err error) {
		calls++
		result, readErr = r, err
	}); err != nil {
		t.Fatal(err)
	}
	p.readCallbacks[0](raftnode.ErrReadTimeout)
	if calls != 0 {
		t.Fatal("read failure was reported before configured retry")
	}
	sched.RunUntil(9 * time.Millisecond)
	if len(p.readCallbacks) != 1 {
		t.Fatal("read retry fired before backoff")
	}
	sched.RunUntil(10 * time.Millisecond)
	if len(p.readCallbacks) != 2 {
		t.Fatal("read retry did not fire at backoff boundary")
	}
	p.readCallbacks[1](nil)
	if calls != 1 || readErr != nil || len(result.KVs) != 1 || string(result.KVs[0].Value) != "v" {
		t.Fatalf("calls=%d result=%+v err=%v", calls, result, readErr)
	}
	// A successful ReadIndex resolves the operation, so a duplicate timeout
	// callback from that round cannot trigger a second user callback.
	p.readCallbacks[1](raftnode.ErrReadTimeout)
	if calls != 1 {
		t.Fatalf("read callback ran %d times", calls)
	}
}

func TestCloseCompletesPendingRangeAndCancelsReadRetry(t *testing.T) {
	p := &fakePeer{id: 1, leader: 1}
	sched := sim.NewScheduler()
	bound, err := WithStore(p, kv.NewStore())
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewWithOptions([]Peer{bound}, Options{
		Timeout: time.Second, Clock: sched, ReadMaxAttempts: 2, ReadRetryBackoff: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	var got error
	if err := c.Range([]byte("k"), nil, kv.RangeOptions{}, func(_ kv.RangeResult, err error) { calls++; got = err }); err != nil {
		t.Fatal(err)
	}
	p.readCallbacks[0](raftnode.ErrReadTimeout)
	c.Close()
	if calls != 1 || !errors.Is(got, ErrClosed) {
		t.Fatalf("Close callback calls=%d err=%v", calls, got)
	}
	p.readCallbacks[0](nil)
	sched.RunUntil(10 * time.Millisecond)
	if calls != 1 || len(p.readCallbacks) != 1 {
		t.Fatalf("calls=%d reads=%d after Close", calls, len(p.readCallbacks))
	}
}

func TestRangeRetryAttemptsAreBounded(t *testing.T) {
	p := &fakePeer{id: 1, leader: 1}
	sched := sim.NewScheduler()
	bound, err := WithStore(p, kv.NewStore())
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewWithOptions([]Peer{bound}, Options{
		Timeout: time.Second, Clock: sched, ReadMaxAttempts: 2, ReadRetryBackoff: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	var got error
	if err := c.Range([]byte("k"), nil, kv.RangeOptions{}, func(_ kv.RangeResult, err error) { calls++; got = err }); err != nil {
		t.Fatal(err)
	}
	p.readCallbacks[0](raftnode.ErrReadTimeout)
	sched.RunUntil(time.Millisecond)
	if len(p.readCallbacks) != 2 {
		t.Fatalf("attempts=%d, want 2", len(p.readCallbacks))
	}
	p.readCallbacks[1](raftnode.ErrReadTimeout)
	p.readCallbacks[1](nil) // a duplicate completion cannot turn failure into success
	if calls != 1 || !errors.Is(got, raftnode.ErrReadTimeout) || len(p.readCallbacks) != 2 {
		t.Fatalf("calls=%d err=%v attempts=%d", calls, got, len(p.readCallbacks))
	}
}

func TestClientWatchUsesReadBarrierThenDeliversFutureEvents(t *testing.T) {
	p := &fakePeer{id: 1, leader: 1}
	sm := kv.NewStateMachine()
	bound, err := WithStore(p, sm.Store())
	if err != nil {
		t.Fatal(err)
	}
	c, err := New([]Peer{bound}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	var got kv.WatchResult
	var watchErr error
	h, err := c.Watch([]byte("svc/"), kv.PrefixEnd([]byte("svc/")), kv.WatchOptions{}, func(page kv.WatchResult, err error) {
		calls++
		got, watchErr = page, err
	})
	if err != nil {
		t.Fatal(err)
	}
	if !h.Active() || h.Ready() || calls != 0 {
		t.Fatalf("before ReadIndex: active=%v ready=%v callbacks=%d", h.Active(), h.Ready(), calls)
	}
	p.readCallbacks[0](nil)
	if !h.Ready() || calls != 0 {
		t.Fatalf("after empty setup: ready=%v callbacks=%d", h.Ready(), calls)
	}
	cmd, err := kv.Put([]byte("svc/a"), []byte("10.0.0.9")).Encode(kv.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	sm.Apply(5, cmd)
	if calls != 1 || watchErr != nil || len(got.Events) != 1 || string(got.Events[0].KV.Value) != "10.0.0.9" {
		t.Fatalf("watch calls=%d result=%+v err=%v", calls, got, watchErr)
	}
	if !h.Cancel() || h.Active() {
		t.Fatal("watch cancellation failed")
	}
	cmd, err = kv.Put([]byte("svc/b"), []byte("10.0.0.10")).Encode(kv.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	sm.Apply(6, cmd)
	if calls != 1 {
		t.Fatalf("cancelled watch received %d callbacks", calls)
	}
}

func TestClientWatchResumesRetainedEventsAndCloseCompletesPendingSetup(t *testing.T) {
	p := &fakePeer{id: 1, leader: 1}
	sm := kv.NewStateMachine()
	cmd, err := kv.Put([]byte("k"), []byte("v")).Encode(kv.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	sm.Apply(5, cmd)
	bound, err := WithStore(p, sm.Store())
	if err != nil {
		t.Fatal(err)
	}
	c, err := New([]Peer{bound}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	var result kv.WatchResult
	var watchErr error
	readyInCallback := false
	var h *WatchHandle
	h, err = c.Watch([]byte("k"), nil, kv.WatchOptions{Cursor: kv.WatchCursor{Revision: 5}}, func(page kv.WatchResult, err error) {
		calls++
		result, watchErr = page, err
		readyInCallback = h.Ready()
	})
	if err != nil {
		t.Fatal(err)
	}
	p.readCallbacks[0](nil)
	if calls != 1 || watchErr != nil || len(result.Events) != 1 || string(result.Events[0].KV.Value) != "v" || !h.Ready() || !readyInCallback {
		t.Fatalf("resume calls=%d result=%+v err=%v ready=%v readyInCallback=%v", calls, result, watchErr, h.Ready(), readyInCallback)
	}

	// A second watch is still waiting for ReadIndex when Close is called.
	closeCalls := 0
	h2, err := c.Watch([]byte("k"), nil, kv.WatchOptions{}, func(_ kv.WatchResult, err error) { closeCalls++; watchErr = err })
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
	if closeCalls != 1 || !errors.Is(watchErr, ErrClosed) || h.Active() || h2.Active() {
		t.Fatalf("close callbacks=%d err=%v active=(%v,%v)", closeCalls, watchErr, h.Active(), h2.Active())
	}
	p.readCallbacks[1](nil)
	if closeCalls != 1 {
		t.Fatalf("late ReadIndex completion invoked watch callback again: %d", closeCalls)
	}
}

func TestClientLeaseLifecycleAndUnknownOutcomes(t *testing.T) {
	p := &fakePeer{id: 1, leader: 1}
	c, err := New([]Peer{p}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	sm := kv.NewStateMachine()
	var granted kv.Lease
	var callbackErr error
	if err := c.GrantLease(3, func(l kv.Lease, err error) { granted, callbackErr = l, err }); err != nil {
		t.Fatal(err)
	}
	p.reply(0, sm.Apply(10, p.command[0]), nil)
	if callbackErr != nil || granted.ID != 10 || granted.Remaining != 3 {
		t.Fatalf("grant=%+v err=%v", granted, callbackErr)
	}

	var kept kv.Lease
	if err := c.KeepAlive(granted.ID, func(l kv.Lease, err error) { kept, callbackErr = l, err }); err != nil {
		t.Fatal(err)
	}
	p.reply(1, sm.Apply(11, p.command[1]), nil)
	if callbackErr != nil || kept.ID != granted.ID || kept.Remaining != 3 {
		t.Fatalf("keepalive=%+v err=%v", kept, callbackErr)
	}

	var ticked bool
	if err := c.TickLeases(func(err error) { callbackErr, ticked = err, true }); err != nil {
		t.Fatal(err)
	}
	p.reply(2, sm.Apply(12, p.command[2]), nil)
	if callbackErr != nil || !ticked {
		t.Fatalf("tick err=%v called=%v", callbackErr, ticked)
	}

	put, _ := kv.PutWithLease([]byte("ephemeral"), []byte("v"), granted.ID).Encode(kv.Limits{})
	sm.Apply(13, put)
	var revoked bool
	if err := c.RevokeLease(granted.ID, func(err error) { callbackErr, revoked = err, true }); err != nil {
		t.Fatal(err)
	}
	p.reply(3, sm.Apply(14, p.command[3]), nil)
	if callbackErr != nil || !revoked || sm.LeaseCount() != 0 {
		t.Fatalf("revoke err=%v called=%v leases=%d", callbackErr, revoked, sm.LeaseCount())
	}
	if rr, err := sm.Store().Range([]byte("ephemeral"), nil, kv.RangeOptions{}); err != nil || len(rr.KVs) != 0 {
		t.Fatalf("revoke left attached key: %+v err=%v", rr, err)
	}

	unknownCalls := 0
	if err := c.GrantLease(2, func(_ kv.Lease, err error) {
		unknownCalls++
		callbackErr = err
	}); err != nil {
		t.Fatal(err)
	}
	p.reply(4, nil, raftnode.ErrResultUnknown)
	if unknownCalls != 1 || !errors.Is(callbackErr, ErrUnknownResult) {
		t.Fatalf("ambiguous grant calls=%d err=%v", unknownCalls, callbackErr)
	}

	closeCalls := 0
	if err := c.KeepAlive(granted.ID, func(_ kv.Lease, err error) { closeCalls++; callbackErr = err }); err != nil {
		t.Fatal(err)
	}
	c.Close()
	if closeCalls != 1 || !errors.Is(callbackErr, ErrUnknownResult) {
		t.Fatalf("close calls=%d err=%v", closeCalls, callbackErr)
	}
	p.reply(5, nil, raftnode.ErrResultUnknown)
	if closeCalls != 1 {
		t.Fatalf("late callback repeated after close: %d", closeCalls)
	}
}

func TestNetworkClientRedirectsProposalAndReturnsCommittedResult(t *testing.T) {
	sched := sim.NewScheduler()
	network := sim.NewNetwork(sched, 17)
	if err := network.SetDefaultLink(sim.LinkConfig{MinDelay: time.Millisecond, MaxDelay: time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	newEndpoint := func(name string, id uint64) *rpc.Endpoint {
		tp, err := sim.NewTransport(name, network)
		if err != nil {
			t.Fatal(err)
		}
		return rpc.NewEndpoint(tp, sched, rpc.Options{IDBase: id})
	}
	clientEndpoint := newEndpoint("client", 100)
	server1, server2 := newEndpoint("n1", 200), newEndpoint("n2", 300)
	follower := &fakePeer{id: 1, leader: 2, proposeError: &raftnode.NotLeaderError{Leader: 2}}
	leader := &fakePeer{id: 2, leader: 2}
	if err := RegisterNodeRPC(server1, follower, time.Second); err != nil {
		t.Fatal(err)
	}
	if err := RegisterNodeRPC(server2, leader, time.Second); err != nil {
		t.Fatal(err)
	}
	r1, err := NewRemotePeer(1, "n1", clientEndpoint, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := NewRemotePeer(2, "n2", clientEndpoint, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	c, err := New([]Peer{r1, r2}, time.Second)
	if err != nil {
		t.Fatal(err)
	}

	var sessionErr error
	if err := c.OpenSession(func(err error) { sessionErr = err }); err != nil {
		t.Fatal(err)
	}
	sched.RunUntil(3 * time.Millisecond) // follower redirects, leader accepts
	if len(leader.command) != 1 || len(follower.command) != 0 {
		t.Fatalf("proposal routing: follower=%d leader=%d", len(follower.command), len(leader.command))
	}
	sm := kv.NewStateMachine()
	openResult := sm.Apply(20, leader.command[0])
	leader.reply(0, openResult, nil)
	sched.RunUntil(4 * time.Millisecond)
	if sessionErr != nil || c.Session() != 20 {
		t.Fatalf("network session=%d err=%v", c.Session(), sessionErr)
	}

	var result kv.TxnResult
	var txnErr error
	if err := c.Txn(kv.Put([]byte("remote/key"), []byte("v")), func(r kv.TxnResult, err error) { result, txnErr = r, err }); err != nil {
		t.Fatal(err)
	}
	sched.RunUntil(5 * time.Millisecond)
	if len(leader.command) != 2 {
		t.Fatalf("leader received %d proposals, want registration and transaction", len(leader.command))
	}
	txnResult := sm.Apply(21, leader.command[1])
	leader.reply(1, txnResult, nil)
	sched.RunUntil(6 * time.Millisecond)
	if txnErr != nil || result.Revision != 21 || c.NextSequence() != 2 {
		t.Fatalf("network txn=%+v err=%v next=%d", result, txnErr, c.NextSequence())
	}

	var lease kv.Lease
	var leaseErr error
	if err := c.GrantLease(5, func(got kv.Lease, err error) { lease, leaseErr = got, err }); err != nil {
		t.Fatal(err)
	}
	sched.RunUntil(7 * time.Millisecond)
	if len(leader.command) != 3 {
		t.Fatalf("network grant reached %d proposals, want 3", len(leader.command))
	}
	leader.reply(2, sm.Apply(22, leader.command[2]), nil)
	sched.RunUntil(8 * time.Millisecond)
	if leaseErr != nil || lease.ID != 22 || lease.TTL != 5 {
		t.Fatalf("network grant=%+v err=%v", lease, leaseErr)
	}

	var kept kv.Lease
	if err := c.KeepAlive(lease.ID, func(got kv.Lease, err error) { kept, leaseErr = got, err }); err != nil {
		t.Fatal(err)
	}
	sched.RunUntil(9 * time.Millisecond)
	leader.reply(3, sm.Apply(23, leader.command[3]), nil)
	sched.RunUntil(10 * time.Millisecond)
	if leaseErr != nil || kept.ID != lease.ID || kept.Remaining != lease.TTL {
		t.Fatalf("network keepalive=%+v err=%v", kept, leaseErr)
	}

	if err := c.Txn(kv.PutWithLease([]byte("remote/ephemeral"), []byte("leased"), lease.ID), func(_ kv.TxnResult, err error) { leaseErr = err }); err != nil {
		t.Fatal(err)
	}
	sched.RunUntil(11 * time.Millisecond)
	leader.reply(4, sm.Apply(24, leader.command[4]), nil)
	sched.RunUntil(12 * time.Millisecond)
	if leaseErr != nil {
		t.Fatalf("network leased transaction: %v", leaseErr)
	}

	if err := c.RevokeLease(lease.ID, func(err error) { leaseErr = err }); err != nil {
		t.Fatal(err)
	}
	sched.RunUntil(13 * time.Millisecond)
	leader.reply(5, sm.Apply(25, leader.command[5]), nil)
	sched.RunUntil(14 * time.Millisecond)
	if leaseErr != nil || sm.LeaseCount() != 0 {
		t.Fatalf("network revoke err=%v leases=%d", leaseErr, sm.LeaseCount())
	}
	if got, err := sm.Store().Range([]byte("remote/ephemeral"), nil, kv.RangeOptions{}); err != nil || len(got.KVs) != 0 {
		t.Fatalf("revoked network key remains: %+v err=%v", got, err)
	}
}

func TestNetworkRangeWaitsForReadBarrierAndReturnsReplicaData(t *testing.T) {
	sched := sim.NewScheduler()
	network := sim.NewNetwork(sched, 19)
	if err := network.SetDefaultLink(sim.LinkConfig{MinDelay: time.Millisecond, MaxDelay: time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	newEndpoint := func(name string, id uint64) *rpc.Endpoint {
		tp, err := sim.NewTransport(name, network)
		if err != nil {
			t.Fatal(err)
		}
		return rpc.NewEndpoint(tp, sched, rpc.Options{IDBase: id})
	}
	clientEndpoint, server := newEndpoint("client-range", 500), newEndpoint("node-range", 600)
	peer := &fakePeer{id: 1, leader: 1}
	store := kv.NewStore()
	if _, err := store.ApplyTxn(7, &kv.Txn{Success: []kv.Op{{Type: kv.OpPut, Key: []byte("k"), Value: []byte("v")}}}); err != nil {
		t.Fatal(err)
	}
	if err := RegisterRangeRPC(server, peer, store, time.Second); err != nil {
		t.Fatal(err)
	}
	remote, err := NewRemotePeer(1, "node-range", clientEndpoint, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	c, err := New([]Peer{remote}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var got kv.RangeResult
	var gotErr error
	if err := c.Range([]byte("k"), nil, kv.RangeOptions{}, func(r kv.RangeResult, err error) { got, gotErr = r, err }); err != nil {
		t.Fatal(err)
	}
	sched.RunUntil(time.Millisecond)
	if len(peer.readCallbacks) != 1 || gotErr != nil || len(got.KVs) != 0 {
		t.Fatalf("range returned before ReadIndex: reads=%d result=%+v err=%v", len(peer.readCallbacks), got, gotErr)
	}
	peer.readCallbacks[0](nil)
	sched.RunUntil(2 * time.Millisecond)
	if gotErr != nil || len(got.KVs) != 1 || string(got.KVs[0].Value) != "v" || got.Revision != 7 {
		t.Fatalf("remote range=%+v err=%v", got, gotErr)
	}
}

func TestNetworkWatchStreamsChangesAndCancels(t *testing.T) {
	sched := sim.NewScheduler()
	network := sim.NewNetwork(sched, 29)
	if err := network.SetDefaultLink(sim.LinkConfig{MinDelay: time.Millisecond, MaxDelay: time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	newEndpoint := func(name string, id uint64) *rpc.Endpoint {
		tp, err := sim.NewTransport(name, network)
		if err != nil {
			t.Fatal(err)
		}
		return rpc.NewEndpoint(tp, sched, rpc.Options{IDBase: id})
	}
	clientEndpoint, server := newEndpoint("client-watch", 700), newEndpoint("node-watch", 800)
	peer := &fakePeer{id: 1, leader: 1}
	store := kv.NewStore()
	if err := RegisterWatchRPC(server, peer, store, sched, time.Second); err != nil {
		t.Fatal(err)
	}
	remote, err := NewRemotePeer(1, "node-watch", clientEndpoint, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	c, err := New([]Peer{remote}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var pages []kv.WatchResult
	h, err := c.Watch([]byte("watched"), nil, kv.WatchOptions{}, func(page kv.WatchResult, err error) {
		if err != nil {
			t.Errorf("watch callback: %v", err)
			return
		}
		pages = append(pages, page)
	})
	if err != nil {
		t.Fatal(err)
	}
	sched.RunUntil(time.Millisecond)
	if len(peer.readCallbacks) != 1 || !h.Active() {
		t.Fatalf("watch setup: callbacks=%d active=%v", len(peer.readCallbacks), h.Active())
	}
	peer.readCallbacks[0](nil)
	sched.RunUntil(2 * time.Millisecond)
	if len(pages) != 0 {
		t.Fatalf("watch returned before a change: %+v", pages)
	}
	if _, err := store.ApplyTxn(8, &kv.Txn{Success: []kv.Op{{Type: kv.OpPut, Key: []byte("watched"), Value: []byte("value")}}}); err != nil {
		t.Fatal(err)
	}
	sched.RunUntil(3 * time.Millisecond)
	if len(pages) != 1 || len(pages[0].Events) != 1 || string(pages[0].Events[0].KV.Value) != "value" || !h.Active() {
		t.Fatalf("remote watch pages=%+v active=%v", pages, h.Active())
	}
	if !h.Cancel() || h.Active() {
		t.Fatal("remote watch did not cancel")
	}
	sched.RunUntil(4 * time.Millisecond)
}

func TestWatchRPCCodecRoundTrip(t *testing.T) {
	in := kv.WatchResult{Revision: 8, Next: kv.WatchCursor{Revision: 9}, Events: []kv.Event{{Type: kv.EventPut, Revision: 8, KV: kv.KeyValue{Key: []byte("watched"), Value: []byte("value"), CreateRevision: 8, ModRevision: 8, Version: 1}}}}
	wire := encodeWatchResponse(in, nil)
	out, err := decodeWatchResponse(wire)
	if err != nil || len(out.Events) != 1 || !bytes.Equal(out.Events[0].KV.Value, []byte("value")) {
		t.Fatalf("wire=% x decoded=%+v err=%v", wire, out, err)
	}
}

func TestMembershipRPCFollowsRedirectAndWaitsForApply(t *testing.T) {
	sched := sim.NewScheduler()
	network := sim.NewNetwork(sched, 31)
	if err := network.SetDefaultLink(sim.LinkConfig{MinDelay: time.Millisecond, MaxDelay: time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	newEndpoint := func(name string, id uint64) *rpc.Endpoint {
		tp, err := sim.NewTransport(name, network)
		if err != nil {
			t.Fatal(err)
		}
		return rpc.NewEndpoint(tp, sched, rpc.Options{IDBase: id})
	}
	clientEndpoint := newEndpoint("membership-client", 900)
	server1, server2 := newEndpoint("member-1", 1000), newEndpoint("member-2", 1100)
	follower := &fakeMembershipNode{redirect: 2}
	leader := &fakeMembershipNode{}
	if err := RegisterMembershipRPC(server1, follower, time.Second); err != nil {
		t.Fatal(err)
	}
	if err := RegisterMembershipRPC(server2, leader, time.Second); err != nil {
		t.Fatal(err)
	}
	p1, err := NewRemotePeer(1, "member-1", clientEndpoint, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	p2, err := NewRemotePeer(2, "member-2", clientEndpoint, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewMembershipClient([]*RemotePeer{p1, p2}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var calls int
	var resultErr error
	if err := manager.AddLearner(4, func(err error) { calls++; resultErr = err }); err != nil {
		t.Fatal(err)
	}
	sched.RunUntil(3 * time.Millisecond)
	if follower.callback != nil || leader.callback == nil || leader.lastAction != membershipAddLearner || leader.lastID != 4 {
		t.Fatalf("follower=%+v leader=%+v", follower, leader)
	}
	if calls != 0 {
		t.Fatal("membership RPC completed before Raft apply")
	}
	if err := manager.RemoveMember(3, func(error) {}); !errors.Is(err, ErrBusy) {
		t.Fatalf("concurrent admin operation returned %v", err)
	}
	leader.callback(nil)
	sched.RunUntil(4 * time.Millisecond)
	if calls != 1 || resultErr != nil {
		t.Fatalf("callback calls=%d err=%v", calls, resultErr)
	}
	var unknown error
	if err := manager.RemoveMember(3, func(err error) { unknown = err }); err != nil {
		t.Fatal(err)
	}
	sched.RunUntil(6 * time.Millisecond)
	if leader.callback == nil {
		t.Fatal("leader did not accept remove request")
	}
	leader.callback(raftnode.ErrResultUnknown)
	sched.RunUntil(7 * time.Millisecond)
	if !errors.Is(unknown, raftnode.ErrResultUnknown) {
		t.Fatalf("ambiguous admin result=%v", unknown)
	}
}

func TestMembershipStateRPCFollowsLeaderRedirect(t *testing.T) {
	sched := sim.NewScheduler()
	network := sim.NewNetwork(sched, 42)
	if err := network.SetDefaultLink(sim.LinkConfig{MinDelay: time.Millisecond, MaxDelay: time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	newEndpoint := func(name string, id uint64) *rpc.Endpoint {
		tp, err := sim.NewTransport(name, network)
		if err != nil {
			t.Fatal(err)
		}
		return rpc.NewEndpoint(tp, sched, rpc.Options{IDBase: id})
	}
	clientEndpoint := newEndpoint("membership-state-client", 1200)
	followerEndpoint, leaderEndpoint := newEndpoint("membership-state-1", 1300), newEndpoint("membership-state-2", 1400)
	follower := &fakeMembershipReadNode{startErr: &raftnode.NotLeaderError{Leader: 2}}
	leader := &fakeMembershipReadNode{}
	if err := RegisterMembershipStateRPC(followerEndpoint, follower, func() MembershipSnapshot { return MembershipSnapshot{} }, time.Second); err != nil {
		t.Fatal(err)
	}
	want := MembershipSnapshot{Voters: []uint64{1, 2}, Learners: []uint64{3}, VotersOutgoing: []uint64{}, LearnersNext: []uint64{}}
	if err := RegisterMembershipStateRPC(leaderEndpoint, leader, func() MembershipSnapshot { return want }, time.Second); err != nil {
		t.Fatal(err)
	}
	p1, err := NewRemotePeer(1, "membership-state-1", clientEndpoint, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	p2, err := NewRemotePeer(2, "membership-state-2", clientEndpoint, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewMembershipClient([]*RemotePeer{p1, p2}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var got MembershipSnapshot
	var gotErr error
	if err := manager.State(func(state MembershipSnapshot, err error) { got, gotErr = state, err }); err != nil {
		t.Fatal(err)
	}
	sched.RunUntil(5 * time.Millisecond)
	if gotErr != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("membership state=%+v err=%v, want %+v", got, gotErr, want)
	}
}
