package kv_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/souviks22/continuum-go/internal/kv"
)

func TestPutTracksCreateModAndVersion(t *testing.T) {
	e := newEnv(t)
	e.put("a", "1") // revision 11
	if kvA := e.get("a"); kvA.CreateRevision != 11 || kvA.ModRevision != 11 || kvA.Version != 1 || string(kvA.Value) != "1" {
		t.Fatalf("%+v", kvA)
	}
	e.skip(5)       // log indexes that carried no write
	e.put("a", "2") // revision 17
	if kvA := e.get("a"); kvA.CreateRevision != 11 || kvA.ModRevision != 17 || kvA.Version != 2 || string(kvA.Value) != "2" {
		t.Fatalf("%+v", kvA)
	}
	if e.sm.Store().Revision() != 17 {
		t.Fatalf("revision=%d, want the log index of the last write", e.sm.Store().Revision())
	}
}

func TestDeleteThenRecreateStartsANewLifeAndHistoryRemainsReadable(t *testing.T) {
	e := newEnv(t)
	e.put("a", "1") // 11
	e.put("a", "2") // 12
	e.del("a")      // 13
	if e.get("a") != nil {
		t.Fatal("deleted key still visible")
	}
	e.put("a", "3") // 14
	if k := e.get("a"); k.CreateRevision != 14 || k.Version != 1 {
		t.Fatalf("recreated key must start a new life: %+v", k)
	}
	read := func(rev uint64) []kv.KeyValue {
		r, err := e.sm.Store().Range(b("a"), nil, kv.RangeOptions{Revision: rev})
		if err != nil {
			t.Fatal(err)
		}
		return r.KVs
	}
	if k := read(12); len(k) != 1 || string(k[0].Value) != "2" || k[0].Version != 2 || k[0].CreateRevision != 11 {
		t.Fatalf("as of 12: %s", summary(k))
	}
	if k := read(13); len(k) != 0 {
		t.Fatalf("as of 13 the key was deleted: %s", summary(k))
	}
	if k := read(14); len(k) != 1 || string(k[0].Value) != "3" {
		t.Fatalf("as of 14: %s", summary(k))
	}
}

func TestRangeSelection(t *testing.T) {
	e := newEnv(t)
	for _, k := range []string{"a", "ab", "b", "b/1", "b/2", "c"} {
		e.put(k, "v-"+k)
	}
	keys := func(r kv.RangeResult) string {
		var ks []string
		for _, x := range r.KVs {
			ks = append(ks, string(x.Key))
		}
		return strings.Join(ks, ",")
	}
	cases := []struct {
		name       string
		start, end string
		want       string
	}{
		{"point", "ab", "", "ab"},
		{"missing point", "zz", "", ""},
		{"half open range", "a", "b", "a,ab"},
		{"prefix", "b", string(kv.PrefixEnd(b("b"))), "b,b/1,b/2"},
		{"from key onward", "b", "\x00", "b,b/1,b/2,c"},
		{"everything", "\x00", "\x00", "a,ab,b,b/1,b/2,c"},
	}
	for _, c := range cases {
		if got := keys(e.rangeKVs(c.start, c.end, kv.RangeOptions{})); got != c.want {
			t.Errorf("%s: got %q, want %q", c.name, got, c.want)
		}
	}
	r := e.rangeKVs("a", "\x00", kv.RangeOptions{Limit: 2})
	if keys(r) != "a,ab" || !r.More || r.Count != 6 {
		t.Fatalf("limit: keys=%s more=%v count=%d", keys(r), r.More, r.Count)
	}
	r = e.rangeKVs("a", "\x00", kv.RangeOptions{CountOnly: true})
	if len(r.KVs) != 0 || r.Count != 6 {
		t.Fatalf("count only: %d kvs, count %d", len(r.KVs), r.Count)
	}
	r = e.rangeKVs("a", "b", kv.RangeOptions{KeysOnly: true})
	if len(r.KVs) != 2 || len(r.KVs[0].Value) != 0 || string(r.KVs[1].Key) != "ab" {
		t.Fatalf("keys only: %s", summary(r.KVs))
	}
	if got := string(kv.PrefixEnd(b("a\xff"))); got != "b" {
		t.Fatalf("PrefixEnd(a\\xff) = %q", got)
	}
	if got := kv.PrefixEnd(b("\xff\xff")); len(got) != 1 || got[0] != 0 {
		t.Fatalf("PrefixEnd of an all-0xff prefix = %v", got)
	}
}

func TestReadsReturnCopiesTheCallerCannotUseToCorruptTheStore(t *testing.T) {
	e := newEnv(t)
	e.put("k", "value")
	r := e.rangeKVs("k", "", kv.RangeOptions{})
	r.KVs[0].Value[0] = 'X'
	r.KVs[0].Key[0] = 'X'
	if k := e.get("k"); string(k.Value) != "value" || string(k.Key) != "k" {
		t.Fatalf("store was mutated through a read result: %s", summary([]kv.KeyValue{*k}))
	}
}

func TestStoreCopiesWhatItIsGiven(t *testing.T) {
	// Apply receives a slice of the Raft entry; the store must not alias it.
	e := newEnv(t)
	cmd, _ := kv.Put(b("key"), b("val")).Encode(kv.Limits{})
	e.apply(cmd)
	for i := range cmd {
		cmd[i] = 0xEE
	}
	if k := e.get("key"); k == nil || string(k.Value) != "val" {
		t.Fatalf("store aliased the command buffer: %+v", k)
	}
}

func TestReadRevisionErrors(t *testing.T) {
	e := newEnv(t)
	e.put("a", "1") // 11
	e.put("a", "2") // 12
	e.put("a", "3") // 13
	if _, err := e.sm.Store().Range(b("a"), nil, kv.RangeOptions{Revision: 14}); !errors.Is(err, kv.ErrFutureRevision) {
		t.Fatalf("future: %v", err)
	}
	if err := e.compact(12); err != nil {
		t.Fatal(err)
	}
	if _, err := e.sm.Store().Range(b("a"), nil, kv.RangeOptions{Revision: 11}); !errors.Is(err, kv.ErrCompacted) {
		t.Fatalf("compacted: %v", err)
	}
	if r, err := e.sm.Store().Range(b("a"), nil, kv.RangeOptions{Revision: 12}); err != nil || string(r.KVs[0].Value) != "2" {
		t.Fatalf("the compaction revision itself must stay readable: %v %v", r, err)
	}
}

func TestCompactionKeepsEachKeysStateAsOfTheRevision(t *testing.T) {
	e := newEnv(t)
	e.put("keep", "1") // 11
	e.put("gone", "1") // 12
	e.put("keep", "2") // 13
	e.del("gone")      // 14
	e.put("late", "1") // 15
	e.put("keep", "3") // 16

	if err := e.compact(14); err != nil {
		t.Fatal(err)
	}
	st := e.sm.Store()
	if st.CompactRevision() != 14 {
		t.Fatal("compact revision not recorded")
	}
	at := func(rev uint64) string {
		r, err := st.Range(b("\x01"), b("\x00"), kv.RangeOptions{Revision: rev})
		if err != nil {
			t.Fatalf("read at %d: %v", rev, err)
		}
		return summary(r.KVs)
	}
	if got := at(14); got != "keep=2@c11/m13/v2 " {
		t.Fatalf("as of the compaction revision: %q", got)
	}
	if got := at(0); !strings.Contains(got, "keep=3@c11/m16/v3") || !strings.Contains(got, "late=1") || strings.Contains(got, "gone") {
		t.Fatalf("latest: %q", got)
	}
	if _, err := st.Range(b("keep"), nil, kv.RangeOptions{Revision: 13}); !errors.Is(err, kv.ErrCompacted) {
		t.Fatalf("reads below the compaction revision must fail: %v", err)
	}
	// "gone" was deleted at or before the compaction revision, so nothing of it remains.
	if strings.Contains(string(st.Snapshot()), "gone") {
		t.Fatal("a key deleted before the compaction revision still occupies space")
	}
	if err := e.compact(14); err != nil {
		t.Fatalf("repeating a committed compaction must be idempotent: %v", err)
	}
	if err := e.compact(10); err != nil {
		t.Fatalf("older repeated compaction must be a no-op: %v", err)
	}
	wantErr(t, e.compact(0), kv.ErrCompacted)
	wantErr(t, e.compact(999), kv.ErrFutureRevision)
	if err := e.compact(16); err != nil {
		t.Fatalf("compacting through the latest revision: %v", err)
	}
	if got := at(0); !strings.Contains(got, "keep=3@c11/m16/v3") {
		t.Fatalf("latest after full compaction: %q", got)
	}
}

func TestCompareAndSet(t *testing.T) {
	e := newEnv(t)

	// Create-if-absent: the key must have mod revision 0, i.e. not exist.
	createIfAbsent := func(v string) kv.TxnResult {
		return e.txn(kv.Txn{
			Compares: []kv.Compare{modIs("lock", 0)},
			Success:  []kv.Op{putOp("lock", v)},
			Failure:  []kv.Op{getOp("lock")},
		})
	}
	r := createIfAbsent("owner-1")
	if !r.Succeeded {
		t.Fatal("first create must succeed")
	}
	r = createIfAbsent("owner-2")
	if r.Succeeded || len(r.Results) != 1 || string(r.Results[0].KVs[0].Value) != "owner-1" {
		t.Fatalf("second create must fail and report the holder: %+v", r)
	}

	// Update-if-unchanged.
	cur := e.get("lock").ModRevision
	r = e.txn(kv.Txn{Compares: []kv.Compare{modIs("lock", cur)}, Success: []kv.Op{putOp("lock", "owner-1b")}})
	if !r.Succeeded || e.get("lock").Version != 2 {
		t.Fatal("update at the current revision must succeed")
	}
	r = e.txn(kv.Txn{Compares: []kv.Compare{modIs("lock", cur)}, Success: []kv.Op{putOp("lock", "stale")}})
	if r.Succeeded || string(e.get("lock").Value) != "owner-1b" {
		t.Fatal("update at a stale revision must fail")
	}

	// Other targets and relations.
	e.put("n", "m")
	ver := func(res kv.CompareResult, n uint64) kv.Compare {
		return kv.Compare{Target: kv.CompareVersion, Result: res, Key: b("n"), Int: n}
	}
	val := func(res kv.CompareResult, v string) kv.Compare {
		return kv.Compare{Target: kv.CompareValue, Result: res, Key: b("n"), Value: b(v)}
	}
	checks := []struct {
		name string
		c    kv.Compare
		want bool
	}{
		{"version ==1", ver(kv.Equal, 1), true},
		{"version >0", ver(kv.Greater, 0), true},
		{"version <1", ver(kv.Less, 1), false},
		{"version !=1", ver(kv.NotEqual, 1), false},
		{"value ==", val(kv.Equal, "m"), true},
		{"value > l", val(kv.Greater, "l"), true},
		{"value < n", val(kv.Less, "n"), true},
		{"value > m", val(kv.Greater, "m"), false},
		{"create ==", kv.Compare{Target: kv.CompareCreate, Result: kv.Equal, Key: b("n"), Int: e.get("n").CreateRevision}, true},
		{"missing key version == 0", kv.Compare{Target: kv.CompareVersion, Result: kv.Equal, Key: b("nope"), Int: 0}, true},
		{"missing key create > 0", kv.Compare{Target: kv.CompareCreate, Result: kv.Greater, Key: b("nope"), Int: 0}, false},
		{"missing key value ==", kv.Compare{Target: kv.CompareValue, Result: kv.Equal, Key: b("nope"), Value: nil}, false},
		{"missing key value !=", kv.Compare{Target: kv.CompareValue, Result: kv.NotEqual, Key: b("nope"), Value: b("x")}, false},
	}
	for _, c := range checks {
		if got := e.txn(kv.Txn{Compares: []kv.Compare{c.c}}).Succeeded; got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
	// All compares must hold.
	r = e.txn(kv.Txn{Compares: []kv.Compare{ver(kv.Equal, 1), val(kv.Equal, "WRONG")}})
	if r.Succeeded {
		t.Fatal("a txn succeeds only if every compare holds")
	}
}

func TestOpsRunInOrderAndSeeEarlierWritesButComparesSeeThePast(t *testing.T) {
	e := newEnv(t)
	e.put("x", "old") // 11
	r := e.txn(kv.Txn{
		// This compare is evaluated before any op runs, so it sees "old".
		Compares: []kv.Compare{{Target: kv.CompareValue, Result: kv.Equal, Key: b("x"), Value: b("old")}},
		Success: []kv.Op{
			putOp("x", "one"),
			getOp("x"), // sees the write just above
			putOp("x", "two"),
			getOp("x"),
			{Type: kv.OpDeleteRange, Key: b("x"), PrevKV: true},
			getOp("x"),
		},
	})
	if !r.Succeeded || len(r.Results) != 6 {
		t.Fatalf("%+v", r)
	}
	if string(r.Results[1].KVs[0].Value) != "one" || string(r.Results[3].KVs[0].Value) != "two" {
		t.Fatalf("reads inside a txn must see earlier writes: %v / %v", r.Results[1].KVs, r.Results[3].KVs)
	}
	if r.Results[4].Deleted != 1 || string(r.Results[4].PrevKVs[0].Value) != "two" {
		t.Fatalf("delete result: %+v", r.Results[4])
	}
	if len(r.Results[5].KVs) != 0 || e.get("x") != nil {
		t.Fatal("the key must be gone after the txn")
	}
	// Every write in one txn shares one revision, the log index of the txn.
	if r.Revision != e.idx {
		t.Fatalf("txn revision %d, want its log index %d", r.Revision, e.idx)
	}
	// History within the txn is ordered and readable as of that revision.
	if _, err := e.sm.Store().Range(b("x"), nil, kv.RangeOptions{Revision: e.idx}); err != nil {
		t.Fatal(err)
	}
}

func TestOnlyWritesAdvanceTheRevision(t *testing.T) {
	e := newEnv(t)
	e.put("a", "1") // 11
	base := e.sm.Store().Revision()

	if r := e.txn(kv.Txn{Success: []kv.Op{getOp("a")}}); r.Revision != base {
		t.Fatalf("a read-only txn changed the revision: %d", r.Revision)
	}
	if r := e.txn(kv.Txn{Compares: []kv.Compare{modIs("a", 999)}, Success: []kv.Op{putOp("a", "x")}}); r.Succeeded || r.Revision != base {
		t.Fatalf("a failed compare with no failure ops wrote or advanced: %+v", r)
	}
	if r := e.txn(kv.Delete(b("missing"))); r.Results[0].Deleted != 0 || r.Revision != base {
		t.Fatalf("deleting nothing advanced the revision: %+v", r)
	}
	if e.sm.Store().Revision() != base {
		t.Fatal("store revision moved without a write")
	}
	if r := e.put("a", "2"); r.Revision != e.idx {
		t.Fatalf("a write must stamp its own log index: %d vs %d", r.Revision, e.idx)
	}
}

func TestATxnWithAnUnreadableRevisionAppliesNothing(t *testing.T) {
	e := newEnv(t)
	e.put("a", "1")
	e.put("a", "2")
	if err := e.compact(e.idx); err != nil {
		t.Fatal(err)
	}
	before := e.sm.Store().Hash()

	err := e.txnErr(kv.Txn{Success: []kv.Op{
		putOp("new", "should-not-appear"),
		{Type: kv.OpDeleteRange, Key: b("a")},
		{Type: kv.OpRange, Key: b("a"), Revision: 1}, // compacted
	}})
	wantErr(t, err, kv.ErrCompacted)
	err = e.txnErr(kv.Txn{Success: []kv.Op{putOp("new", "x"), {Type: kv.OpRange, Key: b("a"), Revision: 9999}}})
	wantErr(t, err, kv.ErrFutureRevision)

	if e.sm.Store().Hash() != before || e.get("new") != nil {
		t.Fatal("a failed transaction left partial writes behind: it must be atomic")
	}
}

func TestPrevKV(t *testing.T) {
	e := newEnv(t)
	r := e.txn(kv.Txn{Success: []kv.Op{{Type: kv.OpPut, Key: b("k"), Value: b("1"), PrevKV: true}}})
	if r.Results[0].PrevKV != nil {
		t.Fatal("no previous value on a create")
	}
	r = e.txn(kv.Txn{Success: []kv.Op{{Type: kv.OpPut, Key: b("k"), Value: b("2"), PrevKV: true}}})
	if p := r.Results[0].PrevKV; p == nil || string(p.Value) != "1" || p.Version != 1 {
		t.Fatalf("prev: %+v", p)
	}
	e.put("p/1", "a")
	e.put("p/2", "b")
	r = e.txn(kv.Txn{Success: []kv.Op{{Type: kv.OpDeleteRange, Key: b("p/"), RangeEnd: kv.PrefixEnd(b("p/")), PrevKV: true}}})
	if r.Results[0].Deleted != 2 || len(r.Results[0].PrevKVs) != 2 || string(r.Results[0].PrevKVs[1].Value) != "b" {
		t.Fatalf("%+v", r.Results[0])
	}
}

func TestEmptyValuesAreRealValues(t *testing.T) {
	e := newEnv(t)
	e.put("k", "")
	if k := e.get("k"); k == nil || len(k.Value) != 0 || k.Version != 1 {
		t.Fatalf("%+v", k)
	}
	if r := e.txn(kv.Txn{Compares: []kv.Compare{{Target: kv.CompareValue, Result: kv.Equal, Key: b("k"), Value: nil}}}); !r.Succeeded {
		t.Fatal("an empty value must compare equal to empty")
	}
}

func TestApplyRejectsAnIndexThatDoesNotAdvance(t *testing.T) {
	sm := kv.NewStateMachine()
	cmd, _ := kv.Put(b("a"), b("1")).Encode(kv.Limits{})
	if _, err := kv.DecodeResult(sm.Apply(20, cmd)); err != nil {
		t.Fatal(err)
	}
	for _, idx := range []uint64{20, 19, 1} {
		if _, err := kv.DecodeResult(sm.Apply(idx, cmd)); !errors.Is(err, kv.ErrInvalidCommand) {
			t.Fatalf("index %d after 20: %v", idx, err)
		}
	}
	if sm.Store().Revision() != 20 {
		t.Fatal("a rejected command changed state")
	}
}
