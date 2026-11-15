package kv_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/souviks22/continuum-go/internal/kv"
)

// raw applies pre-encoded bytes at the next log index and returns the raw result.
func (e *env) raw(cmd []byte) []byte {
	e.idx++
	return e.sm.Apply(e.idx, cmd)
}

func (e *env) register() uint64 {
	e.t.Helper()
	id, err := kv.DecodeSession(e.raw(kv.EncodeRegister()))
	if err != nil {
		e.t.Fatalf("register: %v", err)
	}
	return id
}

func sessCmd(t *testing.T, txn kv.Txn, session, seq uint64) []byte {
	t.Helper()
	c, err := txn.EncodeSession(session, seq, kv.Limits{})
	if err != nil {
		t.Fatalf("encode session txn: %v", err)
	}
	return c
}

func TestRegisterIDIsTheLogIndex(t *testing.T) {
	e := newEnv(t)
	a := e.register()
	if a != e.idx {
		t.Fatalf("session %d, log index %d", a, e.idx)
	}
	e.skip(5)
	bID := e.register()
	if bID != e.idx || bID <= a {
		t.Fatalf("second session %d (index %d), first %d", bID, e.idx, a)
	}
	if e.sm.Sessions() != 2 {
		t.Fatalf("sessions = %d", e.sm.Sessions())
	}
	if e.sm.Store().Revision() != 0 {
		t.Fatal("registering must not advance the store revision")
	}
}

func TestRetryReturnsTheFirstResultAndAppliesNothing(t *testing.T) {
	e := newEnv(t)
	s := e.register()
	cmd := sessCmd(t, kv.Txn{Success: []kv.Op{{Type: kv.OpPut, Key: b("k"), Value: b("v1"), PrevKV: true}}}, s, 1)

	first := e.raw(cmd)
	rev := e.sm.Store().Revision()
	hash := e.sm.Store().Hash()
	if _, err := kv.DecodeResult(first); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		e.skip(uint64(i)) // the retries land at arbitrary later indexes
		again := e.raw(cmd)
		if !bytes.Equal(again, first) {
			t.Fatalf("retry %d returned different bytes", i)
		}
	}
	if e.sm.Store().Revision() != rev || e.sm.Store().Hash() != hash {
		t.Fatal("a retry changed the store")
	}
	if kvp := e.get("k"); kvp.Version != 1 || string(kvp.Value) != "v1" {
		t.Fatalf("key applied more than once: %+v", kvp)
	}
}

func TestRetryAfterOtherClientsWroteTheSameKey(t *testing.T) {
	e := newEnv(t)
	s1, s2 := e.register(), e.register()
	c1 := sessCmd(t, kv.Put(b("k"), b("from-1")), s1, 1)
	first := e.raw(c1)
	e.raw(sessCmd(t, kv.Put(b("k"), b("from-2")), s2, 1))
	if !bytes.Equal(e.raw(c1), first) {
		t.Fatal("retry returned a different result")
	}
	if got := e.get("k"); string(got.Value) != "from-2" || got.Version != 2 {
		t.Fatalf("retry must not overwrite the later write: %+v", got)
	}
}

func TestLowerSequenceIsStaleAndAppliesNothing(t *testing.T) {
	e := newEnv(t)
	s := e.register()
	c1 := sessCmd(t, kv.Put(b("a"), b("1")), s, 1)
	e.raw(c1)
	e.raw(sessCmd(t, kv.Put(b("b"), b("2")), s, 2))
	hash := e.sm.Store().Hash()

	_, err := kv.DecodeResult(e.raw(c1))
	wantErr(t, err, kv.ErrStaleRequest)
	if e.sm.Store().Hash() != hash {
		t.Fatal("a stale request changed the store")
	}
}

func TestSequenceNumbersMayHaveGaps(t *testing.T) {
	e := newEnv(t)
	s := e.register()
	e.raw(sessCmd(t, kv.Put(b("a"), b("1")), s, 1))
	if _, err := kv.DecodeResult(e.raw(sessCmd(t, kv.Put(b("b"), b("2")), s, 50))); err != nil {
		t.Fatalf("gap refused: %v", err)
	}
	if e.get("b") == nil {
		t.Fatal("request after a gap not applied")
	}
}

func TestUnknownSessionIsRefusedAndAppliesNothing(t *testing.T) {
	e := newEnv(t)
	before := e.sm.Store().Hash()
	_, err := kv.DecodeResult(e.raw(sessCmd(t, kv.Put(b("k"), b("v")), 424242, 1)))
	wantErr(t, err, kv.ErrSessionExpired)
	if e.sm.Store().Hash() != before || e.get("k") != nil {
		t.Fatal("a request for an unknown session was applied")
	}
}

func TestSessionlessAndSessionTxnsCoexist(t *testing.T) {
	e := newEnv(t)
	s := e.register()
	e.put("plain", "1")
	e.raw(sessCmd(t, kv.Put(b("sess"), b("2")), s, 1))
	if e.get("plain") == nil || e.get("sess") == nil {
		t.Fatal("both kinds should apply")
	}
}

func TestEncodeSessionRefusesZeroIDs(t *testing.T) {
	if _, err := kv.Put(b("k"), b("v")).EncodeSession(0, 1, kv.Limits{}); err == nil {
		t.Fatal("session 0 accepted")
	}
	if _, err := kv.Put(b("k"), b("v")).EncodeSession(1, 0, kv.Limits{}); err == nil {
		t.Fatal("seq 0 accepted")
	}
}

// The retry must get the original answer even when re-executing the request
// now would give a different one.
func TestRetryOfAFailedCompareStaysFailed(t *testing.T) {
	e := newEnv(t)
	s := e.register()
	e.put("lock", "held")
	create := kv.Txn{
		Compares: []kv.Compare{modIs("lock", 0)},
		Success:  []kv.Op{putOp("lock", "mine")},
	}
	cmd := sessCmd(t, create, s, 1)
	first, err := kv.DecodeResult(e.raw(cmd))
	if err != nil || first.Succeeded {
		t.Fatalf("setup: %+v %v", first, err)
	}
	e.del("lock") // now re-executing would succeed
	again, err := kv.DecodeResult(e.raw(cmd))
	if err != nil || again.Succeeded {
		t.Fatalf("retry re-executed: %+v %v", again, err)
	}
	if e.get("lock") != nil {
		t.Fatal("the retry took the lock")
	}
}

func TestRetryOfAnErrorResultStaysTheSameError(t *testing.T) {
	e := newEnv(t)
	s := e.register()
	e.put("a", "1")
	future := e.sm.Store().Revision() + 3
	cmd := sessCmd(t, kv.Txn{Success: []kv.Op{{Type: kv.OpRange, Key: b("a"), Revision: future}}}, s, 1)
	_, err := kv.DecodeResult(e.raw(cmd))
	wantErr(t, err, kv.ErrFutureRevision)
	for i := 0; i < 4; i++ {
		e.put("pad", fmt.Sprint(i)) // the revision becomes reachable
	}
	_, err = kv.DecodeResult(e.raw(cmd))
	wantErr(t, err, kv.ErrFutureRevision)
}

func TestOversizedResultIsAppliedOnceAndReportedNotRetained(t *testing.T) {
	e := newEnv(t)
	s := e.register()
	big := string(bytes.Repeat([]byte("x"), 1024))
	for i := 0; i < 10; i++ {
		e.put(fmt.Sprintf("big/%02d", i), big)
	}
	cmd := sessCmd(t, kv.Txn{Success: []kv.Op{
		putOp("marker", "1"),
		{Type: kv.OpRange, Key: b("big/"), RangeEnd: kv.PrefixEnd(b("big/"))},
	}}, s, 1)
	first := e.raw(cmd)
	if len(first) <= kv.MaxCachedResult {
		t.Fatalf("test setup: result is only %d bytes", len(first))
	}
	if r, err := kv.DecodeResult(first); err != nil || len(r.Results[1].KVs) != 10 {
		t.Fatalf("first attempt: %v", err)
	}
	_, err := kv.DecodeResult(e.raw(cmd))
	wantErr(t, err, kv.ErrResultNotCached)
	if e.get("marker").Version != 1 {
		t.Fatal("marker applied twice")
	}
	// The session carries on, and the dropped marker survives a snapshot.
	snap, _ := e.sm.Snapshot()
	twin := kv.NewStateMachine()
	if err := twin.Restore(snap); err != nil {
		t.Fatal(err)
	}
	_, err = kv.DecodeResult(twin.Apply(e.idx+1, cmd))
	wantErr(t, err, kv.ErrResultNotCached)
	if _, err := kv.DecodeResult(e.raw(sessCmd(t, kv.Put(b("next"), b("1")), s, 2))); err != nil {
		t.Fatalf("session unusable after a dropped result: %v", err)
	}
}

func TestEvictionRemovesTheLeastRecentlyUsedSession(t *testing.T) {
	e := newEnv(t)
	ids := make([]uint64, 0, kv.MaxSessions)
	for i := 0; i < kv.MaxSessions; i++ {
		ids = append(ids, e.register())
	}
	if e.sm.Sessions() != kv.MaxSessions {
		t.Fatalf("sessions = %d", e.sm.Sessions())
	}
	// ids[0] is the oldest, but a retried request counts as use.
	cmd := sessCmd(t, kv.Put(b("k"), b("v")), ids[0], 1)
	e.raw(cmd)
	e.raw(cmd)

	newest := e.register() // evicts ids[1], not ids[0]
	if e.sm.Sessions() != kv.MaxSessions {
		t.Fatalf("table grew past the bound: %d", e.sm.Sessions())
	}
	_, err := kv.DecodeResult(e.raw(sessCmd(t, kv.Put(b("x"), b("1")), ids[1], 1)))
	wantErr(t, err, kv.ErrSessionExpired)
	for _, id := range []uint64{ids[0], ids[2], newest} {
		if _, err := kv.DecodeResult(e.raw(sessCmd(t, kv.Put(b("y"), b("1")), id, 7))); err != nil {
			t.Fatalf("session %d should have survived: %v", id, err)
		}
	}
	if e.get("x") != nil {
		t.Fatal("request for the evicted session was applied")
	}
}

func TestSnapshotCarriesSessionsSoRetriesStayDeduplicated(t *testing.T) {
	a := newEnv(t)
	s := a.register()
	cmd := sessCmd(t, kv.Txn{Success: []kv.Op{{Type: kv.OpPut, Key: b("k"), Value: b("v"), PrevKV: true}}}, s, 1)
	first := a.raw(cmd)
	idle := a.register() // registered, never used

	snap, err := a.sm.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	bsm := kv.NewStateMachine()
	if err := bsm.Restore(snap); err != nil {
		t.Fatal(err)
	}
	if bsm.Hash() != a.sm.Hash() || bsm.Sessions() != 2 {
		t.Fatal("restored state differs")
	}
	if got := bsm.Apply(a.idx+1, cmd); !bytes.Equal(got, first) {
		t.Fatal("retry after restore did not return the first result")
	}
	if r, err := bsm.Store().Range(b("k"), nil, kv.RangeOptions{}); err != nil || r.KVs[0].Version != 1 {
		t.Fatal("retry after restore re-applied")
	}
	if _, err := kv.DecodeResult(bsm.Apply(a.idx+2, sessCmd(t, kv.Put(b("z"), b("1")), idle, 1))); err != nil {
		t.Fatalf("idle session lost: %v", err)
	}
}

func TestLegacyStoreSnapshotStillRestores(t *testing.T) {
	e := newEnv(t)
	e.put("a", "1")
	e.put("a", "2")
	legacy := e.sm.Store().Snapshot() // what a pre-sessions node wrote

	m := kv.NewStateMachine()
	if err := m.Restore(legacy); err != nil {
		t.Fatalf("legacy snapshot refused: %v", err)
	}
	if m.Store().Hash() != e.sm.Store().Hash() || m.Sessions() != 0 {
		t.Fatal("legacy restore lost data")
	}
	if err := m.Restore(nil); err != nil || m.Store().Revision() != 0 || m.Sessions() != 0 {
		t.Fatalf("empty restore: %v", err)
	}
}

func TestRestoreClearsOldSessions(t *testing.T) {
	e := newEnv(t)
	e.register()
	m := kv.NewStateMachine()
	if err := m.Restore(func() []byte { s, _ := e.sm.Snapshot(); return s }()); err != nil {
		t.Fatal(err)
	}
	if err := m.Restore(kv.NewStateMachine().Store().Snapshot()); err != nil { // legacy, empty
		t.Fatal(err)
	}
	if m.Sessions() != 0 {
		t.Fatal("old sessions survived a restore")
	}
}

// The raft layer hands Apply's bytes to a caller; scribbling on them must not
// corrupt what a later retry is given.
func TestReturnedBytesDoNotAliasTheCachedResult(t *testing.T) {
	e := newEnv(t)
	s := e.register()
	cmd := sessCmd(t, kv.Txn{Success: []kv.Op{{Type: kv.OpPut, Key: b("k"), Value: b("v"), PrevKV: true}}}, s, 1)
	first := e.raw(cmd)
	want := bytes.Clone(first)
	for i := range first {
		first[i] ^= 0xff // the first attempt's caller scribbles on its copy
	}
	retry := e.raw(cmd)
	if !bytes.Equal(retry, want) {
		t.Fatal("mutating the first result corrupted the cache")
	}
	for i := range retry {
		retry[i] ^= 0xff
	}
	if !bytes.Equal(e.raw(cmd), want) {
		t.Fatal("mutating a retry's result corrupted the cache")
	}
}
