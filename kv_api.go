package continuum

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/souviks22/continuum-go/internal/client"
	"github.com/souviks22/continuum-go/internal/kv"
)

// KVStateMachine is Continuum's built-in deterministic MVCC state machine.
// Use it as Config.StateMachine to enable Node's Txn, Range and Watch methods.
type KVStateMachine struct{ machine *kv.StateMachine }

func NewKVStateMachine() *KVStateMachine { return &KVStateMachine{machine: kv.NewStateMachine()} }

func (s *KVStateMachine) Apply(index uint64, command []byte) []byte {
	return s.machine.Apply(index, command)
}
func (s *KVStateMachine) Snapshot() ([]byte, error) { return s.machine.Snapshot() }
func (s *KVStateMachine) Restore(data []byte) error { return s.machine.Restore(data) }
func (s *KVStateMachine) LeaseCount() int           { return s.machine.LeaseCount() }
func (s *KVStateMachine) LeaseTickCommand() []byte  { return s.machine.LeaseTickCommand() }

// KVCompactionCommand returns a replicated compaction request only when the
// latest data revision exceeds the configured retained revision window. The
// Raft runtime invokes this while deciding whether its leader should compact.
func (s *KVStateMachine) KVCompactionCommand(retain uint64) []byte {
	if s == nil || s.machine == nil || retain == 0 {
		return nil
	}
	store := s.machine.Store()
	revision, compacted := store.Revision(), store.CompactRevision()
	if revision <= compacted || revision-compacted <= retain {
		return nil
	}
	return kv.EncodeCompact(revision - retain)
}

type (
	KeyValue      = kv.KeyValue
	Compare       = kv.Compare
	CompareTarget = kv.CompareTarget
	CompareResult = kv.CompareResult
	Op            = kv.Op
	OpType        = kv.OpType
	OpResult      = kv.OpResult
	Txn           = kv.Txn
	TxnResult     = kv.TxnResult
	RangeOptions  = kv.RangeOptions
	RangeResult   = kv.RangeResult
	Lease         = kv.Lease
	WatchEvent    = kv.Event
	WatchCursor   = kv.WatchCursor
	WatchOptions  = kv.WatchOptions
	WatchResult   = kv.WatchResult
	WatchCallback = kv.WatchCallback
)

const (
	CompareVersion = kv.CompareVersion
	CompareCreate  = kv.CompareCreate
	CompareMod     = kv.CompareMod
	CompareValue   = kv.CompareValue
	Equal          = kv.Equal
	NotEqual       = kv.NotEqual
	Greater        = kv.Greater
	Less           = kv.Less
	OpPut          = kv.OpPut
	OpDeleteRange  = kv.OpDeleteRange
	OpRange        = kv.OpRange
	OpPutLease     = kv.OpPutLease
	EventPut       = kv.EventPut
	EventDelete    = kv.EventDelete
)

var (
	ErrKVUnavailable    = errors.New("continuum: built-in KV state machine is not enabled")
	ErrKVBusy           = client.ErrBusy
	ErrKVRetryNotReady  = client.ErrRetryNotReady
	ErrKVSessionUnknown = errors.New("continuum: client session registration outcome is unknown; transaction was not submitted")
	ErrInvalidCommand   = kv.ErrInvalidCommand
	ErrCompacted        = kv.ErrCompacted
	ErrFutureRevision   = kv.ErrFutureRevision
	ErrSessionExpired   = kv.ErrSessionExpired
	ErrStaleRequest     = kv.ErrStaleRequest
	ErrResultNotCached  = kv.ErrResultNotCached
	ErrLeaseNotFound    = kv.ErrLeaseNotFound
	ErrTooManyLeases    = kv.ErrTooManyLeases
	ErrTooManyWatchers  = kv.ErrTooManyWatchers
)

func Put(key, value []byte) Txn                     { return kv.Put(key, value) }
func Delete(key []byte) Txn                         { return kv.Delete(key) }
func PutWithLease(key, value []byte, id uint64) Txn { return kv.PutWithLease(key, value, id) }
func PrefixEnd(prefix []byte) []byte                { return kv.PrefixEnd(prefix) }

type txnOutcome struct {
	result TxnResult
	err    error
}

// Txn submits a deduplicated transaction through this node. The first write
// lazily opens a client session. A context timeout after proposal acceptance
// returns ErrResultUnknown; call RetryTxn to resolve that same transaction.
func (n *Node) Txn(ctx context.Context, txn Txn) (TxnResult, error) {
	if ctx == nil {
		return TxnResult{}, errors.New("continuum: nil context")
	}
	if n.closing.Load() {
		return TxnResult{}, ErrStopped
	}
	if n.kvClient == nil {
		return TxnResult{}, ErrKVUnavailable
	}
	// The call may wait behind a busy loop and return on context cancellation
	// before that task runs, so snapshot every caller-owned slice now.
	txn = cloneTxn(txn)
	state := &writeState{}
	started := make(chan error, 1)
	result := make(chan txnOutcome, 1)
	var startOnce sync.Once
	signalStart := func(err error) { startOnce.Do(func() { started <- err }) }
	submit := func() {
		state.mu.Lock()
		if state.cancelled || ctx.Err() != nil {
			state.mu.Unlock()
			signalStart(ctx.Err())
			return
		}
		err := n.kvClient.Txn(txn, func(r kv.TxnResult, err error) {
			result <- txnOutcome{result: r, err: publicKVError(err)}
		})
		state.accepted = err == nil
		state.mu.Unlock()
		signalStart(err)
	}
	openAttempts := 0
	var openSession func()
	openSession = func() {
		if ctx.Err() != nil {
			signalStart(ctx.Err())
			return
		}
		state.mu.Lock()
		cancelled := state.cancelled
		state.mu.Unlock()
		if cancelled {
			signalStart(ctx.Err())
			return
		}
		openAttempts++
		err := n.kvClient.OpenSession(func(err error) {
			if errors.Is(err, ErrResultUnknown) && openAttempts < 3 && ctx.Err() == nil {
				openSession()
				return
			}
			if errors.Is(err, ErrResultUnknown) {
				signalStart(ErrKVSessionUnknown)
				return
			}
			if err != nil {
				signalStart(publicKVError(err))
				return
			}
			submit()
		})
		if err != nil {
			signalStart(err)
		}
	}
	if err := n.loop.Post(ctx, func() {
		if n.closing.Load() {
			signalStart(ErrStopped)
			return
		}
		if err := ctx.Err(); err != nil {
			signalStart(err)
			return
		}
		if n.kvClient.Session() != 0 {
			submit()
			return
		}
		openSession()
	}); err != nil {
		return TxnResult{}, err
	}
	select {
	case err := <-started:
		if err != nil {
			return TxnResult{}, err
		}
	case <-ctx.Done():
		state.mu.Lock()
		if state.accepted {
			state.mu.Unlock()
			return TxnResult{}, unknownContext(ctx.Err())
		}
		state.cancelled = true
		state.mu.Unlock()
		select {
		case err := <-started:
			if err == nil {
				return TxnResult{}, unknownContext(ctx.Err())
			}
			return TxnResult{}, err
		default:
			return TxnResult{}, ctx.Err()
		}
	}
	select {
	case out := <-result:
		return out.result, out.err
	case <-ctx.Done():
		select {
		case out := <-result:
			return out.result, out.err
		default:
			return TxnResult{}, unknownContext(ctx.Err())
		}
	}
}

type writeState struct {
	mu        sync.Mutex
	cancelled bool
	accepted  bool
}

// RetryTxn retries the last transaction after its result became unknown. The
// built-in client reuses the exact session sequence and command bytes.
func (n *Node) RetryTxn(ctx context.Context) (TxnResult, error) {
	if ctx == nil {
		return TxnResult{}, errors.New("continuum: nil context")
	}
	if n.closing.Load() {
		return TxnResult{}, ErrStopped
	}
	if n.kvClient == nil {
		return TxnResult{}, ErrKVUnavailable
	}
	return n.runKVWrite(ctx, func(cb func(kv.TxnResult, error)) error {
		return n.kvClient.Retry(func(r kv.TxnResult, err error) { cb(r, publicKVError(err)) })
	})
}

// ExportSessionState returns an opaque checkpoint of the built-in KV client's
// deduplication session and any unresolved transaction. Persist the returned
// bytes when Txn reports ErrResultUnknown; after recreating the client, call
// RestoreSessionState and RetryTxn to resolve that exact request. This
// checkpoint is not atomic with proposing a transaction, so applications that
// need crash recovery must persist it before acknowledging their own operation.
func (n *Node) ExportSessionState(ctx context.Context) ([]byte, error) {
	if ctx == nil {
		return nil, errors.New("continuum: nil context")
	}
	if n.closing.Load() {
		return nil, ErrStopped
	}
	if n.kvClient == nil {
		return nil, ErrKVUnavailable
	}
	type outcome struct {
		data []byte
		err  error
	}
	done := make(chan outcome, 1)
	if err := n.loop.Post(ctx, func() {
		if err := ctx.Err(); err != nil {
			done <- outcome{err: err}
			return
		}
		data, err := n.kvClient.ExportSessionState()
		done <- outcome{data: data, err: err}
	}); err != nil {
		return nil, err
	}
	select {
	case out := <-done:
		return out.data, out.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// RestoreSessionState installs a checkpoint into a fresh KV client. If the
// checkpoint contains an unresolved transaction, it can be resumed only by
// calling RetryTxn; no write is sent during restore.
func (n *Node) RestoreSessionState(ctx context.Context, state []byte) error {
	if ctx == nil {
		return errors.New("continuum: nil context")
	}
	if n.closing.Load() {
		return ErrStopped
	}
	if n.kvClient == nil {
		return ErrKVUnavailable
	}
	state = cloneBytes(state)
	done := make(chan error, 1)
	if err := n.loop.Post(ctx, func() {
		if err := ctx.Err(); err != nil {
			done <- err
			return
		}
		done <- n.kvClient.RestoreSessionState(state)
	}); err != nil {
		return err
	}
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// GrantLease creates a replicated lease. Its ID is derived from the applied
// log index. A timed out grant returns ErrResultUnknown; it may have committed.
func (n *Node) GrantLease(ctx context.Context, ttlTicks uint64) (Lease, error) {
	return n.runKVLease(ctx, func(cb func(Lease, error)) error { return n.kvClient.GrantLease(ttlTicks, cb) })
}

// KeepAlive resets the remaining lifetime of an existing lease.
func (n *Node) KeepAlive(ctx context.Context, leaseID uint64) (Lease, error) {
	return n.runKVLease(ctx, func(cb func(Lease, error)) error { return n.kvClient.KeepAlive(leaseID, cb) })
}

// RevokeLease removes the lease and its attached keys.
func (n *Node) RevokeLease(ctx context.Context, leaseID uint64) error {
	_, err := n.runKVLease(ctx, func(cb func(Lease, error)) error {
		return n.kvClient.RevokeLease(leaseID, func(err error) { cb(Lease{}, err) })
	})
	return err
}

// Compact removes MVCC history through revision. Choose a revision returned by
// a linearizable Range. Repeating the same target is safe if the result was
// unknown; reads and watches below the compacted revision return ErrCompacted.
func (n *Node) Compact(ctx context.Context, revision uint64) error {
	_, err := n.runKVLease(ctx, func(cb func(Lease, error)) error {
		return n.kvClient.Compact(revision, func(err error) { cb(Lease{}, err) })
	})
	return err
}

func (n *Node) runKVLease(ctx context.Context, start func(func(Lease, error)) error) (Lease, error) {
	if ctx == nil {
		return Lease{}, errors.New("continuum: nil context")
	}
	if n.closing.Load() {
		return Lease{}, ErrStopped
	}
	if n.kvClient == nil {
		return Lease{}, ErrKVUnavailable
	}
	state := &writeState{}
	started := make(chan error, 1)
	result := make(chan struct {
		lease Lease
		err   error
	}, 1)
	if err := n.loop.Post(ctx, func() {
		state.mu.Lock()
		if state.cancelled || ctx.Err() != nil || n.closing.Load() {
			state.mu.Unlock()
			err := ctx.Err()
			if err == nil {
				err = ErrStopped
			}
			started <- err
			return
		}
		err := start(func(lease Lease, err error) {
			result <- struct {
				lease Lease
				err   error
			}{lease: lease, err: publicKVError(err)}
		})
		state.accepted = err == nil
		state.mu.Unlock()
		started <- err
	}); err != nil {
		return Lease{}, err
	}
	select {
	case err := <-started:
		if err != nil {
			return Lease{}, err
		}
	case <-ctx.Done():
		state.mu.Lock()
		if state.accepted {
			state.mu.Unlock()
			return Lease{}, unknownContext(ctx.Err())
		}
		state.cancelled = true
		state.mu.Unlock()
		select {
		case err := <-started:
			if err == nil {
				return Lease{}, unknownContext(ctx.Err())
			}
			return Lease{}, err
		default:
			return Lease{}, ctx.Err()
		}
	}
	select {
	case out := <-result:
		return out.lease, out.err
	case <-ctx.Done():
		select {
		case out := <-result:
			return out.lease, out.err
		default:
			return Lease{}, unknownContext(ctx.Err())
		}
	}
}

func (n *Node) runKVWrite(ctx context.Context, start func(func(kv.TxnResult, error)) error) (TxnResult, error) {
	state := &writeState{}
	started := make(chan error, 1)
	result := make(chan txnOutcome, 1)
	if err := n.loop.Post(ctx, func() {
		state.mu.Lock()
		if state.cancelled || ctx.Err() != nil || n.closing.Load() {
			state.mu.Unlock()
			signalErr := ctx.Err()
			if signalErr == nil {
				signalErr = ErrStopped
			}
			started <- signalErr
			return
		}
		err := start(func(r kv.TxnResult, err error) { result <- txnOutcome{result: r, err: err} })
		state.accepted = err == nil
		state.mu.Unlock()
		started <- err
	}); err != nil {
		return TxnResult{}, err
	}
	select {
	case err := <-started:
		if err != nil {
			return TxnResult{}, err
		}
	case <-ctx.Done():
		state.mu.Lock()
		if state.accepted {
			state.mu.Unlock()
			return TxnResult{}, unknownContext(ctx.Err())
		}
		state.cancelled = true
		state.mu.Unlock()
		select {
		case err := <-started:
			if err == nil {
				return TxnResult{}, unknownContext(ctx.Err())
			}
			return TxnResult{}, err
		default:
			return TxnResult{}, ctx.Err()
		}
	}
	select {
	case out := <-result:
		return out.result, out.err
	case <-ctx.Done():
		select {
		case out := <-result:
			return out.result, out.err
		default:
			return TxnResult{}, unknownContext(ctx.Err())
		}
	}
}

// Range performs a linearizable range read on this node after a ReadIndex
// barrier. Read errors are safe to retry on another Node.
func (n *Node) Range(ctx context.Context, key, end []byte, opts RangeOptions) (RangeResult, error) {
	if ctx == nil {
		return RangeResult{}, errors.New("continuum: nil context")
	}
	if n.closing.Load() {
		return RangeResult{}, ErrStopped
	}
	if n.kvClient == nil {
		return RangeResult{}, ErrKVUnavailable
	}
	// The event loop can run the read after this method returns on cancellation.
	key, end = cloneBytes(key), cloneBytes(end)
	result := make(chan struct {
		rangeResult kv.RangeResult
		err         error
	}, 1)
	if err := n.loop.Post(ctx, func() {
		if n.closing.Load() {
			result <- struct {
				rangeResult kv.RangeResult
				err         error
			}{err: ErrStopped}
			return
		}
		err := n.kvClient.Range(key, end, opts, func(r kv.RangeResult, err error) {
			result <- struct {
				rangeResult kv.RangeResult
				err         error
			}{rangeResult: r, err: publicKVError(err)}
		})
		if err != nil {
			result <- struct {
				rangeResult kv.RangeResult
				err         error
			}{err: publicKVError(err)}
		}
	}); err != nil {
		return RangeResult{}, err
	}
	select {
	case out := <-result:
		return out.rangeResult, out.err
	case <-ctx.Done():
		select {
		case out := <-result:
			return out.rangeResult, out.err
		default:
			return RangeResult{}, ctx.Err()
		}
	}
}

// Watch installs a resumable linearizable watch. The callback runs on this
// node's serialized event loop and must return quickly. Cancel is safe from any
// goroutine and suppresses callbacks once cancellation is requested.
func (n *Node) Watch(ctx context.Context, key, end []byte, opts WatchOptions, cb WatchCallback) (*WatchHandle, error) {
	if ctx == nil {
		return nil, errors.New("continuum: nil context")
	}
	if cb == nil {
		return nil, errors.New("continuum: nil watch callback")
	}
	if n.closing.Load() {
		return nil, ErrStopped
	}
	if n.kvClient == nil {
		return nil, ErrKVUnavailable
	}
	// Watch setup is asynchronous, so it must not retain caller-owned buffers.
	key, end = cloneBytes(key), cloneBytes(end)
	handle := &WatchHandle{node: n, done: make(chan struct{})}
	created := make(chan error, 1)
	if err := n.loop.Post(ctx, func() {
		if n.closing.Load() {
			created <- ErrStopped
			return
		}
		inner, err := n.kvClient.Watch(key, end, opts, func(page kv.WatchResult, err error) {
			if handle.cancelled.Load() {
				return
			}
			handle.ready.Store(true)
			cb(page, publicKVError(err))
			if err != nil {
				handle.finish()
			}
		})
		if err != nil {
			created <- err
			return
		}
		handle.inner = inner
		created <- nil
	}); err != nil {
		return nil, err
	}
	select {
	case err := <-created:
		if err != nil {
			return nil, publicKVError(err)
		}
	case <-ctx.Done():
		handle.Cancel()
		select {
		case err := <-created:
			if err == nil {
				return nil, ctx.Err()
			}
		default:
			return nil, ctx.Err()
		}
	}
	go func() {
		select {
		case <-ctx.Done():
			handle.Cancel()
		case <-handle.done:
		}
	}()
	return handle, nil
}

type WatchHandle struct {
	node      *Node
	inner     *client.WatchHandle // accessed only on node.loop
	cancelled atomic.Bool
	ready     atomic.Bool
	once      sync.Once
	done      chan struct{}
}

func (w *WatchHandle) Cancel() bool {
	if w == nil || !w.cancelled.CompareAndSwap(false, true) {
		return false
	}
	w.finish()
	if w.node != nil {
		_ = w.node.loop.Post(context.Background(), func() {
			if w.inner != nil {
				w.inner.Cancel()
			}
		})
	}
	return true
}

// Active reports whether the watch can still deliver events.
func (w *WatchHandle) Active() bool {
	if w == nil {
		return false
	}
	select {
	case <-w.done:
		return false
	default:
		return !w.cancelled.Load()
	}
}

// Ready reports whether the watch completed its first linearizable barrier
// and page request. A ready watch may have no events yet.
func (w *WatchHandle) Ready() bool { return w != nil && w.ready.Load() && w.Active() }

func (w *WatchHandle) finish() {
	w.cancelled.Store(true)
	w.once.Do(func() { close(w.done) })
}

func publicKVError(err error) error {
	if errors.Is(err, client.ErrClosed) {
		return ErrStopped
	}
	if errors.Is(err, client.ErrUnknownResult) {
		return ErrResultUnknown
	}
	return err
}

func cloneTxn(txn kv.Txn) kv.Txn {
	out := kv.Txn{
		Compares: make([]kv.Compare, len(txn.Compares)),
		Success:  make([]kv.Op, len(txn.Success)),
		Failure:  make([]kv.Op, len(txn.Failure)),
	}
	for i, compare := range txn.Compares {
		compare.Key, compare.Value = cloneBytes(compare.Key), cloneBytes(compare.Value)
		out.Compares[i] = compare
	}
	cloneOps := func(src, dst []kv.Op) {
		for i, op := range src {
			op.Key, op.RangeEnd, op.Value = cloneBytes(op.Key), cloneBytes(op.RangeEnd), cloneBytes(op.Value)
			dst[i] = op
		}
	}
	cloneOps(txn.Success, out.Success)
	cloneOps(txn.Failure, out.Failure)
	return out
}

func cloneBytes(b []byte) []byte { return append([]byte(nil), b...) }

var _ StateMachine = (*KVStateMachine)(nil)

func (s *KVStateMachine) validate() error {
	if s == nil || s.machine == nil {
		return errors.New("continuum: KVStateMachine must be created with NewKVStateMachine")
	}
	return nil
}
