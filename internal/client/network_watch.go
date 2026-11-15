package client

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/souviks22/continuum/internal/clock"
	"github.com/souviks22/continuum/internal/kv"
	"github.com/souviks22/continuum/internal/raftnode"
	"github.com/souviks22/continuum/internal/rpc"
)

const (
	watchMethod       = "continuum.watch.v1"
	watchCancelMethod = "continuum.watch.cancel.v1"
)

type watchServerKey struct {
	from string
	id   uint64
}
type watchServerCall struct {
	key   watchServerKey
	reply func([]byte, error)
	sub   *kv.WatchSubscription
	timer clock.Timer
	done  bool
}

type remoteWatchReader interface {
	remoteWatch(uint64, []byte, []byte, kv.WatchOptions, time.Duration, func(kv.WatchResult, error)) error
	cancelRemoteWatch(uint64)
}

// AsyncWatchPeer creates one linearizable watch page on its own serial context
// and invokes the callback on the Client's serial context. CancelWatch must
// stop the active local subscription for this client watch ID.
type AsyncWatchPeer interface {
	WatchAsync(uint64, []byte, []byte, kv.WatchOptions, time.Duration, func(kv.WatchResult, error)) error
	CancelWatch(uint64)
}

// RegisterWatchRPC installs a bounded long-poll watch service. Each RPC call
// waits for one page (or a heartbeat), then the client resumes from its cursor.
// The supplied clock and node/store must share the same serial context.
func RegisterWatchRPC(endpoint *rpc.Endpoint, node readNode, store *kv.Store, clk clock.Clock, timeout time.Duration) error {
	if endpoint == nil || node == nil || store == nil || clk == nil || timeout <= 0 {
		return errors.New("client: RegisterWatchRPC requires endpoint, node, store, clock and positive timeout")
	}
	calls := make(map[watchServerKey]*watchServerCall)
	finish := func(call *watchServerCall, page kv.WatchResult, err error) {
		if call.done {
			return
		}
		call.done = true
		if call.timer != nil {
			call.timer.Stop()
			call.timer = nil
		}
		if call.sub != nil {
			call.sub.Cancel()
			call.sub = nil
		}
		if calls[call.key] == call {
			delete(calls, call.key)
		}
		call.reply(encodeWatchResponse(page, err), nil)
	}
	if err := endpoint.RegisterNotify(watchCancelMethod, func(from string, payload []byte) {
		id, err := decodeWatchID(payload)
		if err != nil {
			return
		}
		if call := calls[watchServerKey{from: from, id: id}]; call != nil {
			finish(call, kv.WatchResult{}, nil)
		}
	}); err != nil {
		return err
	}
	return endpoint.RegisterAsync(watchMethod, func(from string, payload []byte, reply func([]byte, error)) {
		id, key, end, opts, err := decodeWatchRequest(payload)
		if err != nil {
			reply(encodeWatchResponse(kv.WatchResult{}, err), nil)
			return
		}
		k := watchServerKey{from: from, id: id}
		if calls[k] == nil && len(calls) >= kv.MaxWatchers {
			reply(encodeWatchResponse(kv.WatchResult{}, kv.ErrTooManyWatchers), nil)
			return
		}
		if old := calls[k]; old != nil {
			finish(old, kv.WatchResult{}, errors.New("client: duplicate active watch poll"))
		}
		call := &watchServerCall{key: k, reply: reply}
		calls[k] = call
		heartbeat := timeout / 2
		if heartbeat <= 0 {
			heartbeat = timeout
		}
		if err := node.LinearizableRead(timeout, func(err error) {
			if call.done {
				return
			}
			if err != nil {
				finish(call, kv.WatchResult{}, err)
				return
			}
			var sub *kv.WatchSubscription
			sub, err = store.Subscribe(key, end, opts, func(page kv.WatchResult, watchErr error) {
				if call.done {
					return
				}
				if watchErr != nil || len(page.Events) != 0 {
					finish(call, page, watchErr)
				}
			})
			if err != nil {
				finish(call, kv.WatchResult{}, err)
				return
			}
			if call.done {
				sub.Cancel()
				return
			}
			call.sub = sub
			// Start the long-poll heartbeat only after the linearizable barrier
			// and subscription are active. An early heartbeat could otherwise
			// claim readiness while the ReadIndex is still unresolved.
			call.timer = clk.AfterFunc(heartbeat, func() {
				if call.done {
					return
				}
				revision := store.Revision()
				cursor := opts.Cursor
				if cursor.Revision == 0 && revision < ^uint64(0) {
					cursor.Revision = revision + 1
				}
				finish(call, kv.WatchResult{Revision: revision, Next: cursor}, nil)
			})
		}); err != nil {
			finish(call, kv.WatchResult{}, err)
		}
	})
}

func (p *RemotePeer) remoteWatch(id uint64, key, end []byte, opts kv.WatchOptions, timeout time.Duration, cb func(kv.WatchResult, error)) error {
	if timeout <= 0 {
		timeout = p.timeout
	}
	return p.endpoint.Call(p.name, watchMethod, encodeWatchRequest(id, key, end, opts), timeout, func(payload []byte, err error) {
		if err != nil {
			if errors.Is(err, rpc.ErrTimeout) {
				err = raftnode.ErrReadTimeout
			}
			cb(kv.WatchResult{}, err)
			return
		}
		page, err := decodeWatchResponse(payload)
		cb(page, err)
	})
}

func (p *RemotePeer) cancelRemoteWatch(id uint64) {
	_ = p.endpoint.Notify(p.name, watchCancelMethod, encodeWatchID(id))
}

func (p *RemotePeer) WatchAsync(id uint64, key, end []byte, opts kv.WatchOptions, timeout time.Duration, cb func(kv.WatchResult, error)) error {
	return p.remoteWatch(id, key, end, opts, timeout, cb)
}

func (p *RemotePeer) CancelWatch(id uint64) { p.cancelRemoteWatch(id) }

func encodeWatchID(id uint64) []byte { b := []byte{1}; return binary.AppendUvarint(b, id) }
func decodeWatchID(b []byte) (uint64, error) {
	if len(b) < 2 || b[0] != 1 {
		return 0, errors.New("client: malformed watch ID")
	}
	id, rest, err := takeVarint(b[1:])
	if err != nil || len(rest) != 0 || id == 0 {
		return 0, errors.New("client: malformed watch ID")
	}
	return id, nil
}

func encodeWatchRequest(id uint64, key, end []byte, o kv.WatchOptions) []byte {
	b := encodeWatchID(id)
	b = binary.AppendUvarint(b, o.Cursor.Revision)
	b = binary.AppendUvarint(b, uint64(o.Cursor.SubRevision))
	b = binary.AppendUvarint(b, o.Limit)
	b = appendBytes(b, key)
	return appendBytes(b, end)
}
func decodeWatchRequest(b []byte) (uint64, []byte, []byte, kv.WatchOptions, error) {
	bad := func() (uint64, []byte, []byte, kv.WatchOptions, error) {
		return 0, nil, nil, kv.WatchOptions{}, errors.New("client: malformed watch request")
	}
	if len(b) < 2 || b[0] != 1 {
		return bad()
	}
	id, b, err := takeVarint(b[1:])
	if err != nil || id == 0 {
		return bad()
	}
	o := kv.WatchOptions{}
	if o.Cursor.Revision, b, err = takeVarint(b); err != nil {
		return bad()
	}
	sub, b, err := takeVarint(b)
	if err != nil || sub > uint64(^uint32(0)) {
		return bad()
	}
	o.Cursor.SubRevision = uint32(sub)
	if o.Limit, b, err = takeVarint(b); err != nil {
		return bad()
	}
	key, b, err := takeBytes(b)
	if err != nil {
		return bad()
	}
	end, b, err := takeBytes(b)
	if err != nil || len(b) != 0 || len(key) > kv.MaxKeySize || len(end) > kv.MaxKeySize {
		return bad()
	}
	return id, key, end, o, nil
}

func encodeWatchResponse(page kv.WatchResult, err error) []byte {
	b := []byte{1}
	if err != nil {
		code := byte(0)
		switch {
		case errors.Is(err, raftnode.ErrReadTimeout):
			code = 1
		case errors.Is(err, raftnode.ErrStopped):
			code = 2
		case errors.Is(err, kv.ErrCompacted):
			code = 3
		case errors.Is(err, kv.ErrFutureRevision):
			code = 4
		case errors.Is(err, kv.ErrInvalidCommand):
			code = 5
		}
		b = append(b, 1, code)
		return appendBytes(b, []byte(err.Error()))
	}
	b = append(b, 0, boolByte(page.More))
	b = binary.AppendUvarint(b, page.Revision)
	b = binary.AppendUvarint(b, page.Next.Revision)
	b = binary.AppendUvarint(b, uint64(page.Next.SubRevision))
	b = binary.AppendUvarint(b, uint64(len(page.Events)))
	for _, event := range page.Events {
		b = append(b, byte(event.Type))
		b = binary.AppendUvarint(b, event.Revision)
		b = binary.AppendUvarint(b, uint64(event.SubRevision))
		b = appendBytes(b, event.KV.Key)
		b = appendBytes(b, event.KV.Value)
		b = binary.AppendUvarint(b, event.KV.CreateRevision)
		b = binary.AppendUvarint(b, event.KV.ModRevision)
		b = binary.AppendUvarint(b, event.KV.Version)
		if event.PrevKV == nil {
			b = append(b, 0)
		} else {
			b = append(b, 1)
			b = appendBytes(b, event.PrevKV.Key)
			b = appendBytes(b, event.PrevKV.Value)
			b = binary.AppendUvarint(b, event.PrevKV.CreateRevision)
			b = binary.AppendUvarint(b, event.PrevKV.ModRevision)
			b = binary.AppendUvarint(b, event.PrevKV.Version)
		}
	}
	return b
}
func decodeWatchResponse(b []byte) (kv.WatchResult, error) {
	bad := func() (kv.WatchResult, error) {
		return kv.WatchResult{}, errors.New("client: malformed remote watch response")
	}
	if len(b) < 2 || b[0] != 1 {
		return bad()
	}
	status := b[1]
	b = b[2:]
	if status == 1 {
		if len(b) < 1 {
			return bad()
		}
		code := b[0]
		msg, rest, e := takeBytes(b[1:])
		if e != nil || len(rest) != 0 {
			return bad()
		}
		switch code {
		case 1:
			return kv.WatchResult{}, raftnode.ErrReadTimeout
		case 2:
			return kv.WatchResult{}, raftnode.ErrStopped
		case 3:
			return kv.WatchResult{}, kv.ErrCompacted
		case 4:
			return kv.WatchResult{}, kv.ErrFutureRevision
		case 5:
			return kv.WatchResult{}, kv.ErrInvalidCommand
		case 0:
			return kv.WatchResult{}, fmt.Errorf("client: remote watch: %s", msg)
		default:
			return bad()
		}
	}
	if status != 0 || len(b) < 1 || b[0] > 1 {
		return bad()
	}
	page := kv.WatchResult{More: b[0] == 1}
	var e error
	if page.Revision, b, e = takeVarint(b[1:]); e != nil {
		return bad()
	}
	if page.Next.Revision, b, e = takeVarint(b); e != nil {
		return bad()
	}
	sub, b, e := takeVarint(b)
	if e != nil || sub > uint64(^uint32(0)) {
		return bad()
	}
	page.Next.SubRevision = uint32(sub)
	n, rest, e := takeVarint(b)
	if e != nil || n > kv.MaxWatchLimit {
		return bad()
	}
	b = rest
	page.Events = make([]kv.Event, 0, int(n))
	readKV := func(buf []byte) (kv.KeyValue, []byte, error) {
		var x kv.KeyValue
		var er error
		if x.Key, buf, er = takeBytes(buf); er != nil || len(x.Key) > kv.MaxKeySize {
			return x, nil, errors.New("bad key")
		}
		if x.Value, buf, er = takeBytes(buf); er != nil || len(x.Value) > kv.MaxValueSize {
			return x, nil, errors.New("bad value")
		}
		if x.CreateRevision, buf, er = takeVarint(buf); er != nil {
			return x, nil, er
		}
		if x.ModRevision, buf, er = takeVarint(buf); er != nil {
			return x, nil, er
		}
		x.Version, buf, er = takeVarint(buf)
		return x, buf, er
	}
	for i := uint64(0); i < n; i++ {
		if len(b) < 1 {
			return bad()
		}
		ev := kv.Event{Type: kv.EventType(b[0])}
		b = b[1:]
		if ev.Type != kv.EventPut && ev.Type != kv.EventDelete {
			return bad()
		}
		if ev.Revision, b, e = takeVarint(b); e != nil {
			return bad()
		}
		var sr uint64
		sr, b, e = takeVarint(b)
		if e != nil || sr > uint64(^uint32(0)) {
			return bad()
		}
		ev.SubRevision = uint32(sr)
		if ev.KV, b, e = readKV(b); e != nil {
			return bad()
		}
		if len(b) < 1 || b[0] > 1 {
			return bad()
		}
		hasPrev := b[0] == 1
		b = b[1:]
		if hasPrev {
			var prev kv.KeyValue
			if prev, b, e = readKV(b); e != nil {
				return bad()
			}
			ev.PrevKV = &prev
		}
		page.Events = append(page.Events, ev)
	}
	if len(b) != 0 {
		return bad()
	}
	return page, nil
}
