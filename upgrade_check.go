package continuum

import (
	"context"
	"fmt"
	"sort"
)

// CompatibilityRequirements describes the formats a proposed binary writes
// and the commands and snapshots it can read.
type CompatibilityRequirements struct {
	RPCWireVersion         uint8
	KVCommandVersion       uint8
	MaxKVCommandKind       uint8
	KVSnapshotWriteVersion uint8
	KVSnapshotReadVersions []uint8
}

// CompatibilityIssue identifies a current member that does not satisfy a
// compatibility preflight. The check is advisory and does not lock membership.
type CompatibilityIssue struct {
	MemberID uint64
	Reason   string
}

// CompatibilityCheck is a read-only comparison against a cluster membership
// observed before and after querying member capabilities.
type CompatibilityCheck struct {
	Membership MembershipState
	Ready      bool
	Issues     []CompatibilityIssue
}

// CheckCompatibility checks every current voter and learner against target.
// It also checks the target's ability to read each member's snapshot format.
// Membership is read on both sides of the capability queries; if it changed,
// Ready is false. The check does not activate a format or prevent a later
// membership change, so callers must not treat it as an atomic upgrade gate.
func (c *RemoteClient) CheckCompatibility(ctx context.Context, target CompatibilityRequirements) (CompatibilityCheck, error) {
	if target.RPCWireVersion == 0 || target.KVCommandVersion == 0 || target.MaxKVCommandKind == 0 || target.KVSnapshotWriteVersion == 0 || len(target.KVSnapshotReadVersions) == 0 {
		return CompatibilityCheck{}, fmt.Errorf("continuum: incomplete target compatibility requirements")
	}
	before, err := c.Membership(ctx)
	if err != nil {
		return CompatibilityCheck{}, err
	}
	reports, err := c.Compatibility(ctx)
	if err != nil {
		return CompatibilityCheck{}, err
	}
	after, err := c.Membership(ctx)
	if err != nil {
		return CompatibilityCheck{}, err
	}
	check := CompatibilityCheck{Membership: before, Ready: true}
	if !sameMembership(before, after) {
		check.addIssue(0, "membership changed during compatibility check; retry against a stable membership")
	}
	byID := make(map[uint64]PeerCompatibility, len(reports))
	for _, report := range reports {
		byID[report.ID] = report
	}
	for _, id := range memberIDs(before) {
		report, ok := byID[id]
		if !ok {
			check.addIssue(id, "member is not in the remote client's configured peer list")
			continue
		}
		if !report.Known {
			check.addIssue(id, "member does not report compatibility; support is unknown")
			continue
		}
		if report.RPCWireVersion != target.RPCWireVersion {
			check.addIssue(id, fmt.Sprintf("RPC wire version %d does not match target %d", report.RPCWireVersion, target.RPCWireVersion))
		}
		if report.KVCommandVersion != target.KVCommandVersion {
			check.addIssue(id, fmt.Sprintf("KV command version %d does not match target %d", report.KVCommandVersion, target.KVCommandVersion))
		}
		if target.MaxKVCommandKind < report.MaxKVCommandKind {
			check.addIssue(id, fmt.Sprintf("target command support %d is below member requirement %d", target.MaxKVCommandKind, report.MaxKVCommandKind))
		}
		if report.MaxKVCommandKind < target.MaxKVCommandKind {
			check.addIssue(id, fmt.Sprintf("member command support %d is below target requirement %d", report.MaxKVCommandKind, target.MaxKVCommandKind))
		}
		if report.CompatibilityReportVersion < 2 || len(report.KVSnapshotReadVersions) == 0 {
			check.addIssue(id, "member report lacks readable snapshot versions")
			continue
		}
		if !hasVersion(report.KVSnapshotReadVersions, target.KVSnapshotWriteVersion) {
			check.addIssue(id, fmt.Sprintf("member cannot read target snapshot version %d", target.KVSnapshotWriteVersion))
		}
		if !hasVersion(target.KVSnapshotReadVersions, report.KVSnapshotWriteVersion) {
			check.addIssue(id, fmt.Sprintf("target cannot read member snapshot version %d", report.KVSnapshotWriteVersion))
		}
	}
	return check, nil
}

func (c *CompatibilityCheck) addIssue(member uint64, reason string) {
	c.Ready = false
	c.Issues = append(c.Issues, CompatibilityIssue{MemberID: member, Reason: reason})
}

func memberIDs(state MembershipState) []uint64 {
	seen := make(map[uint64]struct{})
	for _, group := range [][]uint64{state.Voters, state.Learners, state.VotersOutgoing, state.LearnersNext} {
		for _, id := range group {
			seen[id] = struct{}{}
		}
	}
	ids := make([]uint64, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func sameMembership(a, b MembershipState) bool {
	return a.AutoLeave == b.AutoLeave && sameIDs(a.Voters, b.Voters) && sameIDs(a.Learners, b.Learners) && sameIDs(a.VotersOutgoing, b.VotersOutgoing) && sameIDs(a.LearnersNext, b.LearnersNext)
}

func sameIDs(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	aCopy, bCopy := append([]uint64(nil), a...), append([]uint64(nil), b...)
	sort.Slice(aCopy, func(i, j int) bool { return aCopy[i] < aCopy[j] })
	sort.Slice(bCopy, func(i, j int) bool { return bCopy[i] < bCopy[j] })
	for i := range aCopy {
		if aCopy[i] != bCopy[i] {
			return false
		}
	}
	return true
}

func hasVersion(versions []uint8, want uint8) bool {
	for _, version := range versions {
		if version == want {
			return true
		}
	}
	return false
}
