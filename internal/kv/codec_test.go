package kv

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func richTxn() Txn {
	return Txn{
		Compares: []Compare{
			{Target: CompareMod, Result: Equal, Key: []byte("a"), Int: 0},
			{Target: CompareVersion, Result: Greater, Key: []byte("b"), Int: 7},
			{Target: CompareCreate, Result: Less, Key: []byte("c"), Int: 1 << 40},
			{Target: CompareValue, Result: NotEqual, Key: []byte("d"), Value: []byte("v")},
		},
		Success: []Op{
			{Type: OpPut, Key: []byte("k"), Value: []byte("val"), PrevKV: true},
			{Type: OpDeleteRange, Key: []byte("p"), RangeEnd: []byte("q"), PrevKV: true},
			{Type: OpRange, Key: []byte("r"), RangeEnd: []byte{0}, Limit: 5, Revision: 99, KeysOnly: true, CountOnly: true},
		},
		Failure: []Op{{Type: OpPut, Key: []byte("f"), Value: nil}},
	}
}

func TestCommandRoundTrips(t *testing.T) {
	enc1, err := richTxn().Encode(Limits{})
	if err != nil {
		t.Fatal(err)
	}
	c, err := decodeCommand(enc1)
	if err != nil {
		t.Fatal(err)
	}
	enc2, err := c.txn.Encode(Limits{})
	if err != nil || !bytes.Equal(enc1, enc2) {
		t.Fatalf("encode(decode(x)) != x: %v", err)
	}
	if len(c.txn.Compares) != 4 || len(c.txn.Success) != 3 || len(c.txn.Failure) != 1 {
		t.Fatalf("shape lost: %+v", c.txn)
	}
	if got := c.txn.Success[2]; got.Limit != 5 || got.Revision != 99 || !got.KeysOnly || !got.CountOnly {
		t.Fatalf("range op fields lost: %+v", got)
	}
	leased, err := PutWithLease([]byte("ephemeral"), []byte("v"), 71).Encode(Limits{})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeCommand(leased)
	if err != nil || decoded.txn.Success[0].Type != OpPutLease || decoded.txn.Success[0].LeaseID != 71 {
		t.Fatalf("leased put round trip: %+v %v", decoded, err)
	}
	if _, err := PutWithLease([]byte("ephemeral"), []byte("v"), 0).Encode(Limits{}); !errors.Is(err, ErrInvalidCommand) {
		t.Fatalf("zero lease ID accepted: %v", err)
	}
	cc, err := decodeCommand(EncodeCompact(1234))
	if err != nil || cc.kind != kindCompact || cc.compact != 1234 {
		t.Fatalf("compact: %+v %v", cc, err)
	}
}

func TestLeasedPutMetadataSurvivesSnapshotAndLegacySnapshotRestores(t *testing.T) {
	s := NewStore()
	cmd, err := PutWithLease([]byte("ephemeral"), []byte("v"), 71).Encode(Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ApplyTxn(4, decodeTxnForTest(t, cmd)); err != nil {
		t.Fatal(err)
	}
	copy := NewStore()
	if err := copy.Restore(s.Snapshot()); err != nil {
		t.Fatal(err)
	}
	i, found := copy.find([]byte("ephemeral"))
	if !found || copy.keys[i].revs[len(copy.keys[i].revs)-1].leaseID != 71 {
		t.Fatal("leased put metadata was not retained by snapshot")
	}

	// Store snapshot version 1 predates the lease metadata field.
	legacy := &enc{b: []byte{1}}
	legacy.uv(4)
	legacy.uv(0)
	legacy.uv(1)
	legacy.bytes([]byte("old"))
	legacy.uv(1)
	legacy.uv(4)
	legacy.uv(0)
	legacy.uv(4)
	legacy.uv(1)
	legacy.bool(false)
	legacy.bytes([]byte("v"))
	if err := copy.Restore(legacy.b); err != nil {
		t.Fatalf("legacy store snapshot rejected: %v", err)
	}
	if _, found := copy.find([]byte("old")); !found {
		t.Fatal("legacy key missing after restore")
	}
}

func decodeTxnForTest(t *testing.T, b []byte) *Txn {
	t.Helper()
	c, err := decodeCommand(b)
	if err != nil {
		t.Fatal(err)
	}
	return c.txn
}

func TestEveryTruncationAndTrailingByteIsRejected(t *testing.T) {
	full, _ := richTxn().Encode(Limits{})
	for n := 0; n < len(full); n++ {
		if _, err := decodeCommand(full[:n]); !errors.Is(err, ErrInvalidCommand) {
			t.Fatalf("truncation to %d/%d bytes: err=%v", n, len(full), err)
		}
	}
	if _, err := decodeCommand(append(bytes.Clone(full), 0)); !errors.Is(err, ErrInvalidCommand) {
		t.Fatalf("trailing byte accepted: %v", err)
	}
}

func raw(f func(e *enc)) []byte {
	e := &enc{b: []byte{wireVersion, kindTxn}}
	f(e)
	return e.b
}

func TestMalformedCommandsAreRejected(t *testing.T) {
	bad := map[string][]byte{
		"empty":         nil,
		"wrong version": {9, kindTxn, 0, 0, 0},
		"unknown kind":  {wireVersion, 77},
		"unknown compare target": raw(func(e *enc) {
			e.uv(1)
			e.u8(9)
			e.u8(1)
			e.bytes([]byte("k"))
			e.uv(0)
			e.uv(0)
			e.uv(0)
		}),
		"unknown compare result": raw(func(e *enc) {
			e.uv(1)
			e.u8(byte(CompareMod))
			e.u8(9)
			e.bytes([]byte("k"))
			e.uv(0)
			e.uv(0)
			e.uv(0)
		}),
		"compare on empty key": raw(func(e *enc) {
			e.uv(1)
			e.u8(byte(CompareMod))
			e.u8(1)
			e.bytes(nil)
			e.uv(0)
			e.uv(0)
			e.uv(0)
		}),
		"unknown op type": raw(func(e *enc) { e.uv(0); e.uv(1); e.u8(9); e.uv(0) }),
		"put on empty key": raw(func(e *enc) {
			e.uv(0)
			e.uv(1)
			e.u8(byte(OpPut))
			e.bytes(nil)
			e.bytes([]byte("v"))
			e.u8(0)
			e.uv(0)
		}),
		"bad flag byte": raw(func(e *enc) {
			e.uv(0)
			e.uv(1)
			e.u8(byte(OpPut))
			e.bytes([]byte("k"))
			e.bytes(nil)
			e.u8(7)
			e.uv(0)
		}),
		"bad range flags": raw(func(e *enc) {
			e.uv(0)
			e.uv(1)
			e.u8(byte(OpRange))
			e.bytes([]byte("k"))
			e.bytes(nil)
			e.uv(0)
			e.uv(0)
			e.u8(9)
			e.uv(0)
		}),
		"key over the limit": raw(func(e *enc) {
			e.uv(0)
			e.uv(1)
			e.u8(byte(OpPut))
			e.bytes(make([]byte, MaxKeySize+1))
			e.bytes(nil)
			e.u8(0)
			e.uv(0)
		}),
		"value over the limit": raw(func(e *enc) {
			e.uv(0)
			e.uv(1)
			e.u8(byte(OpPut))
			e.bytes([]byte("k"))
			e.bytes(make([]byte, MaxValueSize+1))
			e.u8(0)
			e.uv(0)
		}),
		"too many ops":            raw(func(e *enc) { e.uv(0); e.uv(MaxTxnOps + 1); e.uv(0) }),
		"too many compares":       raw(func(e *enc) { e.uv(MaxTxnOps + 1) }),
		"huge list length":        raw(func(e *enc) { e.uv(1 << 62) }),
		"huge byte string length": raw(func(e *enc) { e.uv(0); e.uv(1); e.u8(byte(OpPut)); e.uv(1 << 62) }),
	}
	for name, b := range bad {
		if _, err := decodeCommand(b); !errors.Is(err, ErrInvalidCommand) {
			t.Errorf("%s: err=%v, want ErrInvalidCommand", name, err)
		}
	}
	over := make([]byte, MaxRequestSize+1)
	copy(over, []byte{wireVersion, kindTxn})
	if _, err := decodeCommand(over); !errors.Is(err, ErrInvalidCommand) {
		t.Errorf("oversize request: %v", err)
	}
}

func TestLimitsCanOnlyTightenTheConsensusLimits(t *testing.T) {
	big := Put([]byte("k"), bytes.Repeat([]byte("x"), 100))
	if _, err := big.Encode(Limits{}); err != nil {
		t.Fatalf("default limits: %v", err)
	}
	if _, err := big.Encode(Limits{MaxValueSize: 99}); !errors.Is(err, ErrInvalidCommand) {
		t.Fatalf("a stricter value limit must reject: %v", err)
	}
	// A "looser" limit is clamped: a command over the consensus limit can never
	// be admitted by configuration, or replicas would disagree about it.
	tooBig := Put([]byte("k"), make([]byte, MaxValueSize+1))
	if _, err := tooBig.Encode(Limits{MaxValueSize: 1 << 30, MaxRequestSize: 1 << 30}); !errors.Is(err, ErrInvalidCommand) {
		t.Fatalf("limits must not loosen the consensus constants: %v", err)
	}
	many := Txn{}
	for i := 0; i < MaxTxnOps+1; i++ {
		many.Success = append(many.Success, Op{Type: OpPut, Key: []byte("k"), Value: nil})
	}
	if _, err := many.Encode(Limits{}); !errors.Is(err, ErrInvalidCommand) {
		t.Fatalf("op count: %v", err)
	}
	if _, err := (Txn{Success: []Op{{Type: 9}}}).Encode(Limits{}); !errors.Is(err, ErrInvalidCommand) {
		t.Fatalf("unknown op type must not encode: %v", err)
	}
}

func TestResultRoundTripsAndErrorsKeepTheirIdentity(t *testing.T) {
	r := TxnResult{Succeeded: true, Revision: 42, Results: []OpResult{
		{Type: OpPut, PrevKV: &KeyValue{Key: []byte("k"), Value: []byte("v"), CreateRevision: 1, ModRevision: 2, Version: 3}},
		{Type: OpPut},
		{Type: OpDeleteRange, Deleted: 2, PrevKVs: []KeyValue{{Key: []byte("a"), Value: []byte("1"), CreateRevision: 1, ModRevision: 1, Version: 1}}},
		{Type: OpRange, Count: 9, More: true, KVs: []KeyValue{{Key: []byte("z"), CreateRevision: 5, ModRevision: 6, Version: 7}}},
	}}
	got, err := DecodeResult(encodeResult(r))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encodeResult(got), encodeResult(r)) || got.Revision != 42 || !got.Succeeded || len(got.Results) != 4 {
		t.Fatalf("round trip changed the result: %+v", got)
	}
	for _, c := range []struct {
		in     error
		target error
	}{
		{invalid("x"), ErrInvalidCommand},
		{ErrCompacted, ErrCompacted},
		{ErrFutureRevision, ErrFutureRevision},
	} {
		_, err := DecodeResult(encodeError(c.in))
		if !errors.Is(err, c.target) {
			t.Errorf("%v decoded as %v", c.in, err)
		}
	}
	full := encodeResult(r)
	for n := 0; n < len(full); n++ {
		if _, err := DecodeResult(full[:n]); err == nil {
			t.Fatalf("truncated result (%d/%d) decoded", n, len(full))
		}
	}
	if _, err := DecodeResult([]byte{wireVersion, 99}); err == nil {
		t.Fatal("unknown status accepted")
	}
}

// Anything the decoder accepts must re-encode to something that decodes to the
// same thing, and nothing may panic or allocate from a hostile length.
func FuzzDecodeCommand(f *testing.F) {
	rich, _ := richTxn().Encode(Limits{})
	f.Add(rich)
	f.Add(EncodeCompact(7))
	leaseGrant, _ := EncodeLeaseGrant(3)
	f.Add(leaseGrant)
	leaseKeep, _ := EncodeLeaseKeepAlive(7)
	f.Add(leaseKeep)
	leaseDrop, _ := EncodeLeaseRevoke(7)
	f.Add(leaseDrop)
	f.Add(EncodeLeaseTick())
	f.Add([]byte{})
	f.Add([]byte{wireVersion, kindTxn, 0xff, 0xff, 0xff, 0xff, 0x0f})
	f.Fuzz(func(t *testing.T, data []byte) {
		c, err := decodeCommand(data)
		if err != nil {
			return
		}
		var re []byte
		if c.kind == kindCompact {
			re = EncodeCompact(c.compact)
		} else if c.kind == kindLeaseGrant {
			re, err = EncodeLeaseGrant(c.leaseTTL)
		} else if c.kind == kindLeaseKeep {
			re, err = EncodeLeaseKeepAlive(c.leaseID)
		} else if c.kind == kindLeaseDrop {
			re, err = EncodeLeaseRevoke(c.leaseID)
		} else if c.kind == kindLeaseTick {
			re = EncodeLeaseTick()
		} else if re, err = c.txn.Encode(Limits{}); err != nil {
			t.Fatalf("accepted a command it cannot re-encode: %v", err)
		}
		if err != nil {
			t.Fatalf("accepted a command it cannot re-encode: %v", err)
		}
		c2, err := decodeCommand(re)
		if err != nil {
			t.Fatalf("re-encoding of an accepted command is rejected: %v", err)
		}
		var re2 []byte
		if c2.kind == kindCompact {
			re2 = EncodeCompact(c2.compact)
		} else if c2.kind == kindLeaseGrant {
			re2, _ = EncodeLeaseGrant(c2.leaseTTL)
		} else if c2.kind == kindLeaseKeep {
			re2, _ = EncodeLeaseKeepAlive(c2.leaseID)
		} else if c2.kind == kindLeaseDrop {
			re2, _ = EncodeLeaseRevoke(c2.leaseID)
		} else if c2.kind == kindLeaseTick {
			re2 = EncodeLeaseTick()
		} else {
			re2, _ = c2.txn.Encode(Limits{})
		}
		if !bytes.Equal(re, re2) {
			t.Fatal("encoding is not stable")
		}
	})
}

func FuzzDecodeResult(f *testing.F) {
	f.Add(encodeResult(TxnResult{Succeeded: true, Revision: 3, Results: []OpResult{{Type: OpRange, Count: 1, KVs: []KeyValue{{Key: []byte("k")}}}}}))
	f.Add(encodeError(ErrCompacted))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) { DecodeResult(data) }) // must not panic
}

func FuzzRestore(f *testing.F) {
	s := NewStore()
	s.ApplyTxn(5, &Txn{Success: []Op{{Type: OpPut, Key: []byte("a"), Value: []byte("1")}, {Type: OpPut, Key: []byte("b"), Value: []byte("2")}}})
	s.ApplyTxn(6, &Txn{Success: []Op{{Type: OpDeleteRange, Key: []byte("a")}}})
	f.Add(s.Snapshot())
	f.Add([]byte{})
	f.Add([]byte{snapshotVersion, 0, 0, 0xff, 0xff, 0xff, 0xff, 0x0f})
	f.Fuzz(func(t *testing.T, data []byte) {
		st := NewStore()
		if err := st.Restore(data); err != nil {
			return
		}
		// A snapshot that restores must itself be a faithful, restorable snapshot.
		again := NewStore()
		if err := again.Restore(st.Snapshot()); err != nil {
			t.Fatalf("a restored store's own snapshot does not restore: %v", err)
		}
		if st.Hash() != again.Hash() {
			t.Fatal("snapshot round trip changed the store")
		}
	})
}

func TestCorruptSnapshotsAreRejectedAndLeaveTheStoreUntouched(t *testing.T) {
	s := NewStore()
	for i := uint64(1); i <= 20; i++ {
		s.ApplyTxn(i+10, &Txn{Success: []Op{
			{Type: OpPut, Key: []byte(strings.Repeat("k", int(i%4)+1)), Value: []byte{byte(i)}},
			{Type: OpPut, Key: []byte{'x', byte(i)}, Value: []byte("v")},
		}})
	}
	s.ApplyTxn(40, &Txn{Success: []Op{{Type: OpDeleteRange, Key: []byte("k")}}})
	good := s.Snapshot()
	before := s.Hash()

	target := NewStore()
	target.ApplyTxn(3, &Txn{Success: []Op{{Type: OpPut, Key: []byte("keep"), Value: []byte("me")}}})
	targetHash := target.Hash()

	for n := 1; n < len(good); n++ { // zero bytes is the documented "reset to empty", tested below
		if err := target.Restore(good[:n]); err == nil {
			t.Fatalf("truncated snapshot (%d/%d bytes) restored", n, len(good))
		}
		if target.Hash() != targetHash {
			t.Fatalf("a failed Restore modified the store (truncation %d)", n)
		}
	}
	if err := target.Restore(append(bytes.Clone(good), 0)); err == nil {
		t.Fatal("trailing byte accepted")
	}
	if target.Hash() != targetHash {
		t.Fatal("failed Restore modified the store")
	}
	if err := target.Restore(good); err != nil || target.Hash() != before {
		t.Fatalf("valid snapshot: %v", err)
	}
	if err := target.Restore(nil); err != nil || target.Revision() != 0 || len(target.Snapshot()) != len(NewStore().Snapshot()) {
		t.Fatalf("an empty snapshot must reset the store to empty: %v", err)
	}
	// Bit flips either fail cleanly or produce a self-consistent store; never a panic.
	for i := range good {
		flipped := bytes.Clone(good)
		flipped[i] ^= 0x40
		st := NewStore()
		if err := st.Restore(flipped); err == nil {
			if err := NewStore().Restore(st.Snapshot()); err != nil {
				t.Fatalf("flip at %d produced an inconsistent store: %v", i, err)
			}
		}
	}
}

type srev struct {
	mod, sub, create, version uint64
	tomb                      bool
	value                     string
	leaseID                   uint64
}

type skey struct {
	key  string
	revs []srev
}

// craftSnapshot builds snapshot bytes directly, so tests can state exactly which
// invariant is broken.
func craftSnapshot(rev, compact uint64, keys ...skey) []byte {
	e := &enc{b: []byte{snapshotVersion}}
	e.uv(rev)
	e.uv(compact)
	e.uv(uint64(len(keys)))
	for _, k := range keys {
		e.bytes([]byte(k.key))
		e.uv(uint64(len(k.revs)))
		for _, r := range k.revs {
			e.uv(r.mod)
			e.uv(r.sub)
			e.uv(r.create)
			e.uv(r.version)
			e.bool(r.tomb)
			e.bytes([]byte(r.value))
			e.uv(r.leaseID)
		}
	}
	return e.b
}

func TestRestoreEnforcesEveryStructuralInvariant(t *testing.T) {
	live := func(mod uint64) srev { return srev{mod: mod, create: mod, version: 1, value: "v"} }
	good := craftSnapshot(10, 0,
		skey{"a", []srev{live(5), {mod: 7, create: 5, version: 2, value: "w"}}},
		skey{"b", []srev{live(6), {mod: 9, create: 6, version: 1, tomb: true}}},
	)
	probe := NewStore()
	if err := probe.Restore(good); err != nil {
		t.Fatalf("the control snapshot must restore: %v", err)
	}

	bad := map[string][]byte{
		"keys out of order":          craftSnapshot(10, 0, skey{"b", []srev{live(5)}}, skey{"a", []srev{live(6)}}),
		"duplicate key":              craftSnapshot(10, 0, skey{"a", []srev{live(5)}}, skey{"a", []srev{live(6)}}),
		"empty key":                  craftSnapshot(10, 0, skey{"", []srev{live(5)}}),
		"key with no history":        craftSnapshot(10, 0, skey{"a", nil}),
		"history out of order":       craftSnapshot(10, 0, skey{"a", []srev{live(7), {mod: 5, create: 5, version: 1, value: "v"}}}),
		"duplicate revision":         craftSnapshot(10, 0, skey{"a", []srev{live(5), {mod: 5, sub: 0, create: 5, version: 2, value: "v"}}}),
		"entry newer than store":     craftSnapshot(10, 0, skey{"a", []srev{live(11)}}),
		"entry at revision zero":     craftSnapshot(10, 0, skey{"a", []srev{{mod: 0, create: 0, version: 1}}}),
		"create revision zero":       craftSnapshot(10, 0, skey{"a", []srev{{mod: 5, create: 0, version: 1}}}),
		"created after modified":     craftSnapshot(10, 0, skey{"a", []srev{{mod: 5, create: 6, version: 1}}}),
		"live entry with version 0":  craftSnapshot(10, 0, skey{"a", []srev{{mod: 5, create: 5, version: 0}}}),
		"tombstone carrying a value": craftSnapshot(10, 0, skey{"a", []srev{{mod: 5, create: 5, version: 1, tomb: true, value: "x"}}}),
		"compacted past the store":   craftSnapshot(10, 11),
		"sub revision overflow":      craftSnapshot(10, 0, skey{"a", []srev{{mod: 5, sub: 1 << 33, create: 5, version: 1}}}),
		"unknown snapshot version":   append([]byte{99}, good[1:]...),
	}
	for name, snap := range bad {
		st := NewStore()
		st.ApplyTxn(3, &Txn{Success: []Op{{Type: OpPut, Key: []byte("keep"), Value: []byte("me")}}})
		before := st.Hash()
		if err := st.Restore(snap); err == nil {
			t.Errorf("%s: restored", name)
		}
		if st.Hash() != before {
			t.Errorf("%s: a rejected snapshot changed the store", name)
		}
	}
}
