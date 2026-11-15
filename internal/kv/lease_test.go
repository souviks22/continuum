package kv_test

import (
	"errors"
	"testing"

	"github.com/souviks22/continuum/internal/kv"
)

func applyLeaseCommand(t *testing.T, sm *kv.StateMachine, index uint64, cmd []byte) []byte {
	t.Helper()
	return sm.Apply(index, cmd)
}

func TestLeaseGrantKeepAliveAndReplicatedExpiry(t *testing.T) {
	sm := kv.NewStateMachine()
	grant, err := kv.EncodeLeaseGrant(2)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := kv.DecodeLease(applyLeaseCommand(t, sm, 1, grant))
	if err != nil || lease != (kv.Lease{ID: 1, TTL: 2, Remaining: 2}) {
		t.Fatalf("grant result=%+v err=%v", lease, err)
	}

	put, err := kv.PutWithLease([]byte("leader"), []byte("node-a"), lease.ID).Encode(kv.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kv.DecodeResult(applyLeaseCommand(t, sm, 2, put)); err != nil {
		t.Fatal(err)
	}

	applyLeaseCommand(t, sm, 3, kv.EncodeLeaseTick())
	keep, err := kv.EncodeLeaseKeepAlive(lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	got, err := kv.DecodeLease(applyLeaseCommand(t, sm, 4, keep))
	if err != nil || got.Remaining != 2 {
		t.Fatalf("keepalive result=%+v err=%v", got, err)
	}
	applyLeaseCommand(t, sm, 5, kv.EncodeLeaseTick())
	applyLeaseCommand(t, sm, 6, kv.EncodeLeaseTick())
	if sm.LeaseCount() != 0 {
		t.Fatalf("lease remains after deterministic TTL: count=%d", sm.LeaseCount())
	}
	rangeResult, err := sm.Store().Range([]byte("leader"), nil, kv.RangeOptions{})
	if err != nil || len(rangeResult.KVs) != 0 || rangeResult.Revision != 6 {
		t.Fatalf("expired key result=%+v err=%v", rangeResult, err)
	}
	if _, err := kv.DecodeLease(applyLeaseCommand(t, sm, 7, keep)); !errors.Is(err, kv.ErrLeaseNotFound) {
		t.Fatalf("expired lease keepalive error=%v", err)
	}
}

func TestLeaseRevokeSnapshotAndLeaseValidation(t *testing.T) {
	sm := kv.NewStateMachine()
	grant, _ := kv.EncodeLeaseGrant(3)
	if _, err := kv.DecodeLease(applyLeaseCommand(t, sm, 1, grant)); err != nil {
		t.Fatal(err)
	}
	put, _ := kv.PutWithLease([]byte("ephemeral"), []byte("x"), 1).Encode(kv.Limits{})
	applyLeaseCommand(t, sm, 2, put)
	applyLeaseCommand(t, sm, 3, kv.EncodeLeaseTick())
	missing, _ := kv.PutWithLease([]byte("bad"), []byte("x"), 99).Encode(kv.Limits{})
	if _, err := kv.DecodeResult(applyLeaseCommand(t, sm, 4, missing)); !errors.Is(err, kv.ErrLeaseNotFound) {
		t.Fatalf("unknown lease put error=%v", err)
	}
	if _, err := sm.Store().Range([]byte("bad"), nil, kv.RangeOptions{}); err != nil {
		t.Fatal(err)
	}

	data, err := sm.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	copy := kv.NewStateMachine()
	if err := copy.Restore(data); err != nil {
		t.Fatal(err)
	}
	applyLeaseCommand(t, copy, 5, kv.EncodeLeaseTick())
	keep, _ := kv.EncodeLeaseKeepAlive(1)
	if got, err := kv.DecodeLease(applyLeaseCommand(t, copy, 6, keep)); err != nil || got.Remaining != 3 {
		t.Fatalf("restored lease=%+v err=%v", got, err)
	}

	revoke, _ := kv.EncodeLeaseRevoke(1)
	if _, err := kv.DecodeResult(applyLeaseCommand(t, copy, 7, revoke)); err != nil {
		t.Fatal(err)
	}
	if copy.LeaseCount() != 0 {
		t.Fatalf("lease remains after revoke: %d", copy.LeaseCount())
	}
	rr, err := copy.Store().Range([]byte("ephemeral"), nil, kv.RangeOptions{})
	if err != nil || len(rr.KVs) != 0 || rr.Revision != 7 {
		t.Fatalf("revoked key result=%+v err=%v", rr, err)
	}
}

func TestLeaseTTLBoundsAndMissingRevoke(t *testing.T) {
	if _, err := kv.EncodeLeaseGrant(0); !errors.Is(err, kv.ErrInvalidCommand) {
		t.Fatalf("zero TTL accepted: %v", err)
	}
	if _, err := kv.EncodeLeaseGrant(kv.MaxLeaseTTL + 1); !errors.Is(err, kv.ErrInvalidCommand) {
		t.Fatalf("oversized TTL accepted: %v", err)
	}
	sm := kv.NewStateMachine()
	revoke, _ := kv.EncodeLeaseRevoke(7)
	if _, err := kv.DecodeResult(applyLeaseCommand(t, sm, 1, revoke)); !errors.Is(err, kv.ErrLeaseNotFound) {
		t.Fatalf("missing revoke error=%v", err)
	}
}
