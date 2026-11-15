# Rolling upgrade safety

## Current status

Rolling upgrades are not supported yet. Do not mix binaries that have different
Raft RPC envelope, command, or state-machine snapshot support in one cluster.
The RPC envelope currently rejects any version other than its own. The KV
state machine can read several older snapshot formats, but an older binary
cannot necessarily read a snapshot written by a newer one. New command kinds
such as session and lease commands also cannot be assumed to have identical
effects on older binaries.

This is a release blocker for the rolling-upgrade requirement in
[`REQUIREMENTS.md`](REQUIREMENTS.md). The fact that a particular upgrade passes
the current tests is not enough to establish compatibility.

## Required compatibility contract

Before supporting an N to N+1 rolling upgrade, Continuum must define and enforce
compatibility independently for:

- **Transport envelope:** which peers can decode and reply to each wire version.
- **Raft commands:** which command kinds and semantics every voting member can
  apply deterministically.
- **State-machine snapshots:** which readers can restore each snapshot format,
  and whether downgrade is possible after writing it.
- **Durable Raft state:** whether WAL, hard state, membership metadata, and
  internal snapshots remain readable across the upgrade.

Compatibility must be a cluster decision. A node-local setting is insufficient:
different settings on replicas can make the same committed command produce
different state.

## Required rollout protocol

The initial protocol should use an explicit compatibility level with these
phases:

1. **Upgrade binaries while holding the old compatibility level.** New binaries
   must continue to read and write the old command and snapshot formats during
   this phase. New-only commands and formats remain disabled.
2. **Establish a replicated activation barrier.** The cluster records the
   minimum supported compatibility level and the membership configuration at
   which it was established. Activation is allowed only after every current
   voter reports support. Learners do not count until promoted.
3. **Enable the new level by committed entry.** Every replica applies the same
   activation entry before new command kinds or snapshot formats are emitted.
4. **Define downgrade behavior.** Before activation, a binary rollback must be
   safe. After activation, either provide an explicit downgrade conversion or
   reject downgrade before opening durable state; never silently guess.

Membership changes and compatibility activation must be serialized. Adding a
voter during activation must not let a voter without the required support enter
the active configuration.

## Evidence required before closing this gap

- A mixed N/N+1 cluster test covers leader changes, writes, reads, snapshots,
  restart, membership change, and loss of quorum during each rollout phase.
- Tests prove new-only commands are rejected before activation and applied
  identically by every member after activation.
- Snapshot and durable-state fixtures from N are opened by N+1; downgrade tests
  cover both pre-activation and post-activation behavior.
- The operator procedure names the activation point and states when rollback
  stops being safe.

Until those pieces exist, use a full-cluster stop and upgrade only when the
cluster can tolerate downtime and a verified backup is available. This is a
conservative operational recommendation, not a claim that all storage failures
are covered by the existing backup tests.
