# Rolling upgrade safety

## Current status

General mixed-version rolling upgrades are not yet release-supported. The RPC
envelope currently rejects any version other than its own. KV-enabled nodes
report supported and active feature levels, command support, snapshot write
version, and readable snapshot versions through `RemoteClient.Compatibility`.
`RemoteClient.CheckCompatibility` checks every current voter and learner and
detects membership changes across its reads. `RemoteClient.ActivateCompatibility`
then commits a replicated activation command bound to that exact membership;
stale membership is rejected at apply time. New-only commands are rejected
before activation, and snapshot writes stay at the older format until the
matching feature level is active. Older nodes that do not implement report v3
are treated as unknown and fail preflight.

This closes the local activation-barrier gap, but it does not establish general
N to N+1 compatibility. In particular, the current evidence does not cover
mixed-binary transport, durable Raft state, or the complete operational rollout
and rollback procedure. Do not mix binaries in production based only on this
barrier or on the current test suite.

The remaining items below are release blockers for the rolling-upgrade
requirement in [`REQUIREMENTS.md`](REQUIREMENTS.md). Passing the current tests
is not enough to establish compatibility.

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

Compatibility must be a cluster decision. The current activation level is
replicated, and the activation command checks the membership view at apply time.
Membership admission after activation still needs a server-enforced capability
check so an unsupported new member cannot join and apply future commands.

## Required rollout protocol

The initial protocol should use an explicit compatibility level with these
phases:

1. **Upgrade binaries while holding the old compatibility level.** New binaries
   must continue to read and write the old command and snapshot formats during
   this phase. New-only commands and formats remain disabled.
2. **Establish a replicated activation barrier.** Implemented by
   `RemoteClient.ActivateCompatibility`: every current voter and learner must
   report support, and the committed command is rejected if membership changed
   since preflight.
3. **Enable the new level by committed entry.** Every replica applies the same
   activation entry before new command kinds or snapshot formats are emitted.
4. **Define downgrade behavior.** Preflight rejects a target below an already
   active level. A complete release still needs an operator-tested rollback
   procedure covering durable state and snapshot conversion.

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
- Membership admission enforces capability checks after activation, regardless
  of whether membership changes come through the remote client or embedded API.
- The operator procedure names the activation point and states when rollback
  stops being safe.

Until those pieces exist, treat mixed-version rolling upgrades as unsupported.
Use a full-cluster stop and upgrade only when the cluster can tolerate downtime
and a verified backup is available. This is a conservative operational
recommendation, not a claim that all storage failures are covered by the
existing backup tests.
