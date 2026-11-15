package kv_test

import (
	"errors"
	"testing"

	"github.com/souviks22/continuum-go/internal/kv"
)

func TestWatchReturnsOrderedEventsAndResumesWithinRevision(t *testing.T) {
	s := kv.NewStore()
	txn := kv.Txn{Success: []kv.Op{
		{Type: kv.OpPut, Key: []byte("svc/a"), Value: []byte("one")},
		{Type: kv.OpPut, Key: []byte("svc/b"), Value: []byte("two")},
		{Type: kv.OpPut, Key: []byte("svc/a"), Value: []byte("three")},
		{Type: kv.OpDeleteRange, Key: []byte("svc/b")},
		{Type: kv.OpPut, Key: []byte("other"), Value: []byte("ignored")},
	}}
	if _, err := s.ApplyTxn(7, &txn); err != nil {
		t.Fatal(err)
	}

	first, err := s.Watch([]byte("svc/"), kv.PrefixEnd([]byte("svc/")), kv.WatchOptions{
		Cursor: kv.WatchCursor{Revision: 7}, Limit: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Events) != 2 || !first.More || first.Revision != 7 || first.Next != (kv.WatchCursor{Revision: 7, SubRevision: 2}) {
		t.Fatalf("first page: %+v", first)
	}
	if first.Events[0].Type != kv.EventPut || string(first.Events[0].KV.Key) != "svc/a" || first.Events[0].SubRevision != 0 {
		t.Fatalf("first event: %+v", first.Events[0])
	}
	if first.Events[1].Type != kv.EventPut || string(first.Events[1].KV.Key) != "svc/b" || first.Events[1].SubRevision != 1 {
		t.Fatalf("second event: %+v", first.Events[1])
	}

	second, err := s.Watch([]byte("svc/"), kv.PrefixEnd([]byte("svc/")), kv.WatchOptions{
		Cursor: first.Next, Limit: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Events) != 2 || second.More || second.Next != (kv.WatchCursor{Revision: 8}) {
		t.Fatalf("second page: %+v", second)
	}
	update, deletion := second.Events[0], second.Events[1]
	if update.Type != kv.EventPut || string(update.KV.Value) != "three" || update.SubRevision != 2 || update.PrevKV == nil || string(update.PrevKV.Value) != "one" {
		t.Fatalf("update event: %+v", update)
	}
	if deletion.Type != kv.EventDelete || string(deletion.KV.Key) != "svc/b" || deletion.SubRevision != 3 || deletion.PrevKV == nil || string(deletion.PrevKV.Value) != "two" {
		t.Fatalf("delete event: %+v", deletion)
	}
}

func TestWatchRespectsCompactionAndSnapshotRestore(t *testing.T) {
	s := kv.NewStore()
	for i, value := range []string{"old", "new"} {
		if _, err := s.ApplyTxn(uint64(i+4), &kv.Txn{Success: []kv.Op{{Type: kv.OpPut, Key: []byte("k"), Value: []byte(value)}}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Compact(4); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Watch([]byte("k"), nil, kv.WatchOptions{Cursor: kv.WatchCursor{Revision: 4}}); !errors.Is(err, kv.ErrCompacted) {
		t.Fatalf("watch at compacted revision: %v", err)
	}

	copy := kv.NewStore()
	if err := copy.Restore(s.Snapshot()); err != nil {
		t.Fatal(err)
	}
	got, err := copy.Watch([]byte("k"), nil, kv.WatchOptions{Cursor: kv.WatchCursor{Revision: 5}})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Events) != 1 || string(got.Events[0].KV.Value) != "new" || got.Events[0].Revision != 5 || got.More {
		t.Fatalf("restored watch: %+v", got)
	}
}

func TestWatchValidatesCursorAndBounds(t *testing.T) {
	s := kv.NewStore()
	if _, err := s.Watch(nil, nil, kv.WatchOptions{}); !errors.Is(err, kv.ErrInvalidCommand) {
		t.Fatalf("empty key: %v", err)
	}
	if _, err := s.Watch([]byte("k"), nil, kv.WatchOptions{Cursor: kv.WatchCursor{SubRevision: 1}}); !errors.Is(err, kv.ErrInvalidCommand) {
		t.Fatalf("sub-revision without revision: %v", err)
	}
	if _, err := s.Watch([]byte("k"), nil, kv.WatchOptions{Cursor: kv.WatchCursor{Revision: 2}}); !errors.Is(err, kv.ErrFutureRevision) {
		t.Fatalf("future revision: %v", err)
	}
	if _, err := s.Watch([]byte("k"), nil, kv.WatchOptions{Limit: kv.MaxWatchLimit + 1}); !errors.Is(err, kv.ErrInvalidCommand) {
		t.Fatalf("oversized watch page: %v", err)
	}
}

func TestSubscribeNotifiesInOrderAndCancellationStopsDelivery(t *testing.T) {
	s := kv.NewStore()
	var revisions []uint64
	var values []string
	sub, err := s.Subscribe([]byte("svc/"), kv.PrefixEnd([]byte("svc/")), kv.WatchOptions{}, func(page kv.WatchResult, err error) {
		if err != nil {
			t.Errorf("watch callback: %v", err)
			return
		}
		for _, event := range page.Events {
			revisions = append(revisions, event.Revision)
			values = append(values, string(event.KV.Value))
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if !sub.Active() || len(revisions) != 0 {
		t.Fatalf("new subscription active=%v revisions=%v", sub.Active(), revisions)
	}
	for _, item := range []struct {
		index uint64
		key   string
		value string
	}{{5, "svc/a", "one"}, {9, "svc/b", "two"}} {
		if _, err := s.ApplyTxn(item.index, &kv.Txn{Success: []kv.Op{{Type: kv.OpPut, Key: []byte(item.key), Value: []byte(item.value)}}}); err != nil {
			t.Fatal(err)
		}
	}
	if len(revisions) != 2 || revisions[0] != 5 || revisions[1] != 9 || values[0] != "one" || values[1] != "two" {
		t.Fatalf("delivered revisions=%v values=%v", revisions, values)
	}
	if !sub.Cancel() || sub.Active() || sub.Cancel() {
		t.Fatal("Cancel was not idempotent")
	}
	if _, err := s.ApplyTxn(12, &kv.Txn{Success: []kv.Op{{Type: kv.OpPut, Key: []byte("svc/c"), Value: []byte("three")}}}); err != nil {
		t.Fatal(err)
	}
	if len(revisions) != 2 {
		t.Fatalf("cancelled subscription received another event: %v", revisions)
	}
}

func TestSubscribeDrainsRetainedPagesBeforeWaitingForNewWrites(t *testing.T) {
	s := kv.NewStore()
	for i, value := range []string{"one", "two", "three"} {
		if _, err := s.ApplyTxn(uint64(i+1), &kv.Txn{Success: []kv.Op{{Type: kv.OpPut, Key: []byte("k"), Value: []byte(value)}}}); err != nil {
			t.Fatal(err)
		}
	}
	var values []string
	var pages int
	_, err := s.Subscribe([]byte("k"), nil, kv.WatchOptions{Cursor: kv.WatchCursor{Revision: 1}, Limit: 1}, func(page kv.WatchResult, err error) {
		if err != nil {
			t.Errorf("watch callback: %v", err)
			return
		}
		pages++
		if len(page.Events) != 1 {
			t.Errorf("page has %d events, want 1", len(page.Events))
			return
		}
		values = append(values, string(page.Events[0].KV.Value))
	})
	if err != nil {
		t.Fatal(err)
	}
	if pages != 3 || len(values) != 3 || values[0] != "one" || values[1] != "two" || values[2] != "three" {
		t.Fatalf("pages=%d values=%v", pages, values)
	}
}

func TestStateMachineRestoreKeepsStoreIdentityAndActiveWatch(t *testing.T) {
	sm := kv.NewStateMachine()
	store := sm.Store()
	calls := 0
	_, err := store.Subscribe([]byte("k"), nil, kv.WatchOptions{}, func(page kv.WatchResult, err error) {
		if err != nil {
			t.Errorf("watch callback: %v", err)
			return
		}
		calls += len(page.Events)
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := sm.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := sm.Restore(snapshot); err != nil {
		t.Fatal(err)
	}
	if sm.Store() != store {
		t.Fatal("snapshot restore replaced the store pointer held by its watcher")
	}
	cmd, err := kv.Put([]byte("k"), []byte("v")).Encode(kv.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	sm.Apply(7, cmd)
	if calls != 1 {
		t.Fatalf("active watch received %d events after restore, want 1", calls)
	}
}

func TestSubscribeBoundsActiveWatchers(t *testing.T) {
	s := kv.NewStore()
	for i := 0; i < kv.MaxWatchers; i++ {
		if _, err := s.Subscribe([]byte("k"), nil, kv.WatchOptions{}, func(kv.WatchResult, error) {}); err != nil {
			t.Fatalf("subscribe %d: %v", i, err)
		}
	}
	if _, err := s.Subscribe([]byte("k"), nil, kv.WatchOptions{}, func(kv.WatchResult, error) {}); !errors.Is(err, kv.ErrTooManyWatchers) {
		t.Fatalf("watcher limit: %v", err)
	}
}
