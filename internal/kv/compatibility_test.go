package kv_test

import (
	"errors"
	"testing"

	"github.com/souviks22/continuum/internal/kv"
)

func TestCompatibilityActivationGatesFeaturesAndPreservesReadableSnapshots(t *testing.T) {
	sm := kv.NewStateMachine()
	if err := sm.Restore(kv.NewStore().Snapshot()); err != nil {
		t.Fatal(err)
	}
	if got := sm.CompatibilityLevel(); got != kv.CompatibilityLevelBase {
		t.Fatalf("restored base compatibility level=%d", got)
	}
	register := kv.EncodeRegister()
	if err := sm.ValidateCommand(register); !errors.Is(err, kv.ErrFeatureInactive) {
		t.Fatalf("register validation error=%v, want feature inactive", err)
	}
	if _, err := kv.DecodeResult(sm.Apply(2, register)); !errors.Is(err, kv.ErrFeatureInactive) {
		t.Fatalf("register apply error=%v, want feature inactive", err)
	}
	baseSnapshot, err := sm.Snapshot()
	if err != nil || len(baseSnapshot) == 0 || baseSnapshot[0] != 1 {
		t.Fatalf("base snapshot version=%v err=%v", baseSnapshot, err)
	}

	voters := []uint64{1, 2, 3}
	digest := kv.MembershipDigest(voters, nil, nil, nil, false)
	activation, err := kv.EncodeActivateCompatibility(kv.CompatibilityLevelSessions, digest)
	if err != nil {
		t.Fatal(err)
	}
	wrong := kv.MembershipDigest([]uint64{1, 2}, nil, nil, nil, false)
	if _, err := kv.DecodeResult(sm.ApplyWithMembership(3, activation, []uint64{1, 2}, nil, nil, nil, false)); !errors.Is(err, kv.ErrMembershipChanged) {
		t.Fatalf("stale activation error=%v, want membership changed", err)
	}
	if sm.CompatibilityLevel() != kv.CompatibilityLevelBase {
		t.Fatal("stale activation changed compatibility level")
	}
	activation, err = kv.EncodeActivateCompatibility(kv.CompatibilityLevelSessions, wrong)
	if err != nil {
		t.Fatal(err)
	}
	// A mismatched fingerprint cannot activate even when the payload is valid.
	if _, err := kv.DecodeResult(sm.ApplyWithMembership(3, activation, voters, nil, nil, nil, false)); !errors.Is(err, kv.ErrMembershipChanged) {
		t.Fatalf("wrong fingerprint error=%v", err)
	}
	activation, err = kv.EncodeActivateCompatibility(kv.CompatibilityLevelSessions, digest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kv.DecodeResult(sm.ApplyWithMembership(3, activation, voters, nil, nil, nil, false)); err != nil {
		t.Fatalf("activation: %v", err)
	}
	if sm.CompatibilityLevel() != kv.CompatibilityLevelSessions {
		t.Fatalf("session compatibility level=%d", sm.CompatibilityLevel())
	}
	if _, err := kv.DecodeSession(sm.Apply(4, register)); err != nil {
		t.Fatalf("register after activation: %v", err)
	}
	sessionSnapshot, err := sm.Snapshot()
	if err != nil || sessionSnapshot[0] != 2 {
		t.Fatalf("session snapshot version=%v err=%v", sessionSnapshot, err)
	}
	restored := kv.NewStateMachine()
	if err := restored.Restore(sessionSnapshot); err != nil || restored.CompatibilityLevel() != kv.CompatibilityLevelSessions || restored.Sessions() != 1 {
		t.Fatalf("session snapshot restore level=%d sessions=%d err=%v", restored.CompatibilityLevel(), restored.Sessions(), err)
	}
	activation, err = kv.EncodeActivateCompatibility(kv.CompatibilityLevelLeases, digest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kv.DecodeResult(restored.ApplyWithMembership(5, activation, voters, nil, nil, nil, false)); err != nil {
		t.Fatalf("lease activation: %v", err)
	}
	grant, err := kv.EncodeLeaseGrant(10)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kv.DecodeLease(restored.Apply(6, grant)); err != nil {
		t.Fatalf("lease grant after activation: %v", err)
	}
	leaseSnapshot, err := restored.Snapshot()
	if err != nil || leaseSnapshot[0] != 4 {
		t.Fatalf("lease snapshot version=%v err=%v", leaseSnapshot, err)
	}
}
