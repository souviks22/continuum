package kv

import (
	"bytes"
	"errors"
	"testing"
)

func sessionSM(t testing.TB) *StateMachine {
	t.Helper()
	m := NewStateMachine()
	idx := uint64(0)
	next := func(c []byte) []byte { idx++; return m.Apply(idx, c) }
	a, _ := DecodeSession(next(EncodeRegister()))
	bID, _ := DecodeSession(next(EncodeRegister()))
	_, _ = DecodeSession(next(EncodeRegister())) // idle
	for seq, k := range []string{"x", "y"} {
		c, err := Put([]byte(k), []byte("v")).EncodeSession(a, uint64(seq+1), Limits{})
		if err != nil {
			t.Fatal(err)
		}
		next(c)
	}
	c, _ := Txn{Success: []Op{{Type: OpRange, Key: []byte("x"), RangeEnd: []byte{0}}}}.EncodeSession(bID, 1, Limits{})
	next(c)
	return m
}

func TestSessionCommandEveryTruncationAndTrailingByteRejected(t *testing.T) {
	full, err := richTxn().EncodeSession(7, 9, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	for n := 0; n < len(full); n++ {
		if _, err := decodeCommand(full[:n]); !errors.Is(err, ErrInvalidCommand) {
			t.Fatalf("truncation to %d/%d: %v", n, len(full), err)
		}
	}
	if _, err := decodeCommand(append(bytes.Clone(full), 0)); !errors.Is(err, ErrInvalidCommand) {
		t.Fatalf("trailing byte accepted: %v", err)
	}
	if _, err := decodeCommand(append(EncodeRegister(), 0)); !errors.Is(err, ErrInvalidCommand) {
		t.Fatalf("register with a body accepted: %v", err)
	}
	c, err := decodeCommand(full)
	if err != nil || c.kind != kindSessionTxn || c.session != 7 || c.seq != 9 || len(c.txn.Success) != 3 {
		t.Fatalf("round trip: %+v %v", c, err)
	}
}

func TestHandCraftedZeroSessionOrSeqIsRejected(t *testing.T) {
	for name, f := range map[string]func(e *enc){
		"zero session": func(e *enc) { e.uv(0); e.uv(1) },
		"zero seq":     func(e *enc) { e.uv(1); e.uv(0) },
	} {
		e := &enc{b: []byte{wireVersion, kindSessionTxn}}
		f(e)
		e.uv(0)
		e.uv(0)
		e.uv(0) // an empty txn
		if _, err := decodeCommand(e.b); !errors.Is(err, ErrInvalidCommand) {
			t.Errorf("%s accepted: %v", name, err)
		}
		if out := NewStateMachine().Apply(1, e.b); out[1] != statusInvalid {
			t.Errorf("%s: Apply status %d", name, out[1])
		}
	}
}

func TestDecodeSessionRejectsBadResults(t *testing.T) {
	if id, err := DecodeSession(encodeSessionResult(42)); err != nil || id != 42 {
		t.Fatalf("good result: %d %v", id, err)
	}
	for name, b := range map[string][]byte{
		"empty":     nil,
		"trailing":  append(encodeSessionResult(1), 0),
		"zero id":   encodeSessionResult(0),
		"truncated": {wireVersion, statusOK},
		"version":   {9, statusOK, 1},
	} {
		if _, err := DecodeSession(b); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	if _, err := DecodeSession(encodeError(ErrSessionExpired)); !errors.Is(err, ErrSessionExpired) {
		t.Fatalf("error status lost: %v", err)
	}
}

func TestEveryErrorStatusRoundTrips(t *testing.T) {
	for _, target := range []error{ErrInvalidCommand, ErrCompacted, ErrFutureRevision, ErrSessionExpired, ErrStaleRequest, ErrResultNotCached} {
		_, err := DecodeResult(encodeError(target))
		if !errors.Is(err, target) {
			t.Errorf("%v came back as %v", target, err)
		}
	}
}

func TestEverySnapshotTruncationAndTrailingByteIsRejectedAndChangesNothing(t *testing.T) {
	m := sessionSM(t)
	good, _ := m.Snapshot()
	before := m.Hash()
	for n := 1; n < len(good); n++ { // n=0 is the legal empty snapshot
		if err := m.Restore(good[:n]); err == nil {
			t.Fatalf("truncation to %d/%d bytes accepted", n, len(good))
		}
		if m.Hash() != before {
			t.Fatalf("failed restore at %d bytes changed the state", n)
		}
	}
	if err := m.Restore(append(bytes.Clone(good), 0)); err == nil {
		t.Fatal("trailing byte accepted")
	}
	if m.Hash() != before {
		t.Fatal("failed restore changed the state")
	}
}

func TestCraftedSessionTablesAreRejected(t *testing.T) {
	store := NewStore().Snapshot()
	build := func(f func(e *enc)) []byte {
		e := &enc{b: []byte{smSnapshotVersion}}
		e.bytes(store)
		f(e)
		e.uv(0) // no leases
		return e.b
	}
	result := encodeResult(TxnResult{Succeeded: true})
	type sess struct {
		id, used, seq uint64
		state         byte
		res           []byte
	}
	table := func(ss ...sess) []byte {
		return build(func(e *enc) {
			e.uv(uint64(len(ss)))
			for _, s := range ss {
				e.uv(s.id)
				e.uv(s.used)
				e.uv(s.seq)
				e.u8(s.state)
				if s.state == 1 {
					e.bytes(s.res)
				}
			}
		})
	}
	good := table(sess{id: 5, used: 5}, sess{id: 9, used: 12, seq: 2, state: 1, res: result}, sess{id: 11, used: 11, seq: 1, state: 2})
	if err := NewStateMachine().Restore(good); err != nil {
		t.Fatalf("control table rejected: %v", err)
	}
	bad := map[string][]byte{
		"unsorted IDs":               table(sess{id: 9, used: 9}, sess{id: 5, used: 5}),
		"duplicate IDs":              table(sess{id: 5, used: 5}, sess{id: 5, used: 5}),
		"zero ID":                    table(sess{id: 0, used: 1}),
		"used before registered":     table(sess{id: 9, used: 8}),
		"result state without seq":   table(sess{id: 5, used: 5, state: 2}),
		"seq without result state":   table(sess{id: 5, used: 5, seq: 3}),
		"unknown state":              build(func(e *enc) { e.uv(1); e.uv(5); e.uv(5); e.uv(1); e.u8(7) }),
		"retained result is junk":    table(sess{id: 5, used: 5, seq: 1, state: 1, res: []byte{1, 2, 3, 4}}),
		"retained result too short":  table(sess{id: 5, used: 5, seq: 1, state: 1, res: []byte{wireVersion}}),
		"retained result bad status": table(sess{id: 5, used: 5, seq: 1, state: 1, res: []byte{wireVersion, maxStatus + 1}}),
		"retained result too large":  table(sess{id: 5, used: 5, seq: 1, state: 1, res: append(bytes.Clone(result), make([]byte, MaxCachedResult)...)}),
		"too many sessions":          build(func(e *enc) { e.uv(MaxSessions + 1) }),
		"corrupt inner store":        append([]byte{smSnapshotVersion, 3, 9, 9, 9}, 0),
		"unknown version":            {77, 0},
	}
	for name, b := range bad {
		m := sessionSM(t)
		before := m.Hash()
		if err := m.Restore(b); err == nil {
			t.Errorf("%s: accepted", name)
		}
		if m.Hash() != before {
			t.Errorf("%s: failed restore changed the state", name)
		}
	}
}

func TestSnapshotIsCanonicalAcrossRestore(t *testing.T) {
	m := sessionSM(t)
	s1, _ := m.Snapshot()
	again := NewStateMachine()
	if err := again.Restore(s1); err != nil {
		t.Fatal(err)
	}
	s2, _ := again.Snapshot()
	if !bytes.Equal(s1, s2) {
		t.Fatal("snapshot -> restore -> snapshot is not the identity")
	}
}

func FuzzStateMachineRestore(f *testing.F) {
	m := sessionSM(f)
	good, _ := m.Snapshot()
	f.Add(good)
	f.Add([]byte(nil))
	f.Add(NewStore().Snapshot())
	f.Add([]byte{smSnapshotVersion, 0, 0})
	f.Fuzz(func(t *testing.T, data []byte) {
		sm := sessionSM(t)
		before := sm.Hash()
		if err := sm.Restore(data); err != nil {
			if sm.Hash() != before {
				t.Fatal("failed restore changed the state")
			}
			return
		}
		// Whatever was accepted must be stable: it re-serializes to a snapshot
		// that restores to the very same state.
		snap, _ := sm.Snapshot()
		again := NewStateMachine()
		if err := again.Restore(snap); err != nil {
			t.Fatalf("own snapshot rejected: %v", err)
		}
		if again.Hash() != sm.Hash() {
			t.Fatal("snapshot round trip changed the state")
		}
	})
}
