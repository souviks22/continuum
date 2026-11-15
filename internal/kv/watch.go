package kv

import (
	"bytes"
	"container/heap"
	"fmt"
	"math"
	"sort"
)

const (
	// DefaultWatchLimit bounds a watch response when the caller leaves Limit zero.
	DefaultWatchLimit = 256
	// MaxWatchLimit is the maximum number of events returned in one response.
	MaxWatchLimit = 4096
	// MaxWatchers bounds active subscriptions on one Store.
	MaxWatchers = 10000
)

var ErrTooManyWatchers = fmt.Errorf("kv: active watch limit %d reached", MaxWatchers)

type EventType uint8

const (
	EventPut EventType = iota + 1
	EventDelete
)

// Event is one key change in deterministic (Revision, SubRevision) order.
// KV describes the new value for a put, or the tombstone metadata for a delete;
// PrevKV is populated when the key had a live value immediately beforehand.
type Event struct {
	Type        EventType
	KV          KeyValue
	PrevKV      *KeyValue
	Revision    uint64
	SubRevision uint32
}

// WatchCursor identifies the next event to read. Revision is inclusive and
// SubRevision is inclusive within that revision. A zero revision means start
// after the store's current revision.
type WatchCursor struct {
	Revision    uint64
	SubRevision uint32
}

// WatchOptions selects a key range and bounds the response. Limit zero selects
// DefaultWatchLimit; larger values than MaxWatchLimit are rejected.
type WatchOptions struct {
	Cursor WatchCursor
	Limit  uint64
}

// WatchResult contains events and a resumable cursor. More means additional
// matching events already exist in the store; Next points to the first event
// not included in this response, even if a response ends mid-transaction.
type WatchResult struct {
	Events   []Event
	Revision uint64 // latest store revision when this response was produced
	Next     WatchCursor
	More     bool
}

// WatchCallback receives one page of changes or a terminal history error.
// Callbacks run synchronously on the Store's serial context.
type WatchCallback func(WatchResult, error)

// WatchSubscription is an ephemeral, cancellable watch. It is not included in
// snapshots; after process restart clients resume from their last WatchCursor.
type WatchSubscription struct {
	store  *Store
	id     uint64
	key    []byte
	end    []byte
	cursor WatchCursor
	limit  uint64
	cb     WatchCallback
	active bool
}

// Subscribe registers for changes after cursor. Retained events are delivered
// immediately in bounded pages; subsequent matching mutations trigger the same
// callback. A zero cursor revision starts after the store's current revision.
func (s *Store) Subscribe(key, end []byte, o WatchOptions, cb WatchCallback) (*WatchSubscription, error) {
	if cb == nil {
		return nil, invalid("nil watch callback")
	}
	initial, err := s.Watch(key, end, o)
	if err != nil {
		return nil, err
	}
	if len(s.watchers) >= MaxWatchers {
		return nil, ErrTooManyWatchers
	}
	if s.nextWatcher == math.MaxUint64 {
		return nil, ErrTooManyWatchers
	}
	if s.watchers == nil {
		s.watchers = make(map[uint64]*WatchSubscription)
	}
	s.nextWatcher++
	w := &WatchSubscription{
		store: s, id: s.nextWatcher, key: bytes.Clone(key), end: bytes.Clone(end),
		cursor: o.Cursor, limit: o.Limit, cb: cb, active: true,
	}
	s.watchers[w.id] = w
	s.deliverWatcher(w, &initial)
	return w, nil
}

// Cancel unregisters the watch. It is safe to call more than once.
func (w *WatchSubscription) Cancel() bool {
	if w == nil || !w.active {
		return false
	}
	w.active = false
	if w.store != nil {
		delete(w.store.watchers, w.id)
	}
	w.cb, w.key, w.end, w.store = nil, nil, nil, nil
	return true
}

// Active reports whether the subscription is still registered.
func (w *WatchSubscription) Active() bool { return w != nil && w.active }

func (s *Store) notifyWatchers() {
	if s.notifying {
		s.notifyAgain = true
		return
	}
	s.notifying = true
	defer func() { s.notifying = false }()
	for {
		s.notifyAgain = false
		ids := make([]uint64, 0, len(s.watchers))
		for id := range s.watchers {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		for _, id := range ids {
			if w := s.watchers[id]; w != nil {
				s.deliverWatcher(w, nil)
			}
		}
		if !s.notifyAgain {
			return
		}
	}
}

func (s *Store) deliverWatcher(w *WatchSubscription, initial *WatchResult) {
	for w.active {
		var page WatchResult
		if initial != nil {
			page, initial = *initial, nil
		} else {
			var err error
			page, err = s.Watch(w.key, w.end, WatchOptions{Cursor: w.cursor, Limit: w.limit})
			if err != nil {
				w.active = false
				delete(s.watchers, w.id)
				cb := w.cb
				w.cb, w.key, w.end, w.store = nil, nil, nil, nil
				cb(WatchResult{}, err)
				return
			}
		}
		w.cursor = page.Next
		if len(page.Events) > 0 {
			w.cb(page, nil)
			if !w.active {
				return
			}
		}
		if !page.More {
			return
		}
	}
}

type watchItem struct {
	h *keyHist
	i int
	r *rev
}

type watchHeap []*watchItem

func (h watchHeap) Len() int { return len(h) }
func (h watchHeap) Less(i, j int) bool {
	if h[i].r.mod != h[j].r.mod {
		return h[i].r.mod < h[j].r.mod
	}
	return h[i].r.sub < h[j].r.sub
}
func (h watchHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *watchHeap) Push(x any)   { *h = append(*h, x.(*watchItem)) }
func (h *watchHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return item
}

// Watch returns retained changes in [key, rangeEnd), ordered by Raft revision
// and transaction sub-revision. An empty rangeEnd selects exactly key; {0}
// selects all keys >= key. The cursor is inclusive. Compaction removes events
// at or below the compact revision, so a cursor there returns ErrCompacted.
// Event history is derived from MVCC versions and therefore survives snapshots
// without a second event log or a separate durability protocol.
func (s *Store) Watch(key, rangeEnd []byte, o WatchOptions) (WatchResult, error) {
	if len(key) == 0 || len(key) > MaxKeySize || len(rangeEnd) > MaxKeySize {
		return WatchResult{}, invalid("watch key must be 1..%d bytes and range end at most %d bytes", MaxKeySize, MaxKeySize)
	}
	limit := o.Limit
	if limit == 0 {
		limit = DefaultWatchLimit
	}
	if limit > MaxWatchLimit {
		return WatchResult{}, invalid("watch limit %d exceeds %d", limit, MaxWatchLimit)
	}
	cursor := o.Cursor
	if cursor.Revision == 0 {
		if cursor.SubRevision != 0 {
			return WatchResult{}, invalid("sub-revision requires a non-zero revision")
		}
		if s.rev == math.MaxUint64 {
			return WatchResult{}, fmt.Errorf("%w: no revision remains after %d", ErrFutureRevision, s.rev)
		}
		cursor.Revision = s.rev + 1
	}
	if cursor.Revision <= s.compact {
		return WatchResult{}, fmt.Errorf("%w: watch cursor %d, compacted through %d", ErrCompacted, cursor.Revision, s.compact)
	}
	if s.rev != math.MaxUint64 && cursor.Revision > s.rev+1 {
		return WatchResult{}, fmt.Errorf("%w: watch cursor %d, latest is %d", ErrFutureRevision, cursor.Revision, s.rev)
	}

	// Each key's versions are already ordered. Merge them with a heap so the
	// response memory is bounded by Limit rather than by the entire history.
	h := &watchHeap{}
	start, _ := s.find(key)
	for i := start; i < len(s.keys); i++ {
		kh := s.keys[i]
		if !inRange(kh.key, key, rangeEnd) {
			break
		}
		j := sort.Search(len(kh.revs), func(j int) bool {
			r := kh.revs[j]
			return r.mod > cursor.Revision || (r.mod == cursor.Revision && r.sub >= cursor.SubRevision)
		})
		if j < len(kh.revs) {
			heap.Push(h, &watchItem{h: kh, i: j, r: &kh.revs[j]})
		}
	}
	result := WatchResult{Revision: s.rev}
	for uint64(len(result.Events)) < limit && h.Len() > 0 {
		item := heap.Pop(h).(*watchItem)
		result.Events = append(result.Events, eventAt(item.h, item.i))
		item.i++
		if item.i < len(item.h.revs) {
			item.r = &item.h.revs[item.i]
			heap.Push(h, item)
		}
	}
	if h.Len() > 0 {
		result.More = true
		last := result.Events[len(result.Events)-1]
		result.Next = WatchCursor{Revision: last.Revision, SubRevision: last.SubRevision + 1}
	} else if len(result.Events) == 0 {
		result.Next = WatchCursor{Revision: cursor.Revision}
		if cursor.Revision <= s.rev {
			result.Next.Revision = s.rev + 1
		}
	} else {
		result.Next = WatchCursor{Revision: s.rev + 1}
	}
	return result, nil
}

func eventAt(h *keyHist, i int) Event {
	r := &h.revs[i]
	typ := EventPut
	kv := h.kvOf(r).clone()
	if r.tomb {
		typ = EventDelete
		kv.Value = nil
	}
	e := Event{Type: typ, KV: kv, Revision: r.mod, SubRevision: r.sub}
	if i > 0 {
		prev := &h.revs[i-1]
		if !prev.tomb {
			pkv := h.kvOf(prev).clone()
			e.PrevKV = &pkv
		}
	}
	return e
}
