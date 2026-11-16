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

The test suite now includes a process-level check between historical revision
`49fd507` (the N baseline) and the current tree (N+1). It verifies mixed-binary
TCP operation, rejects preflight while N members are present, forces N to write
snapshots, and recovers N data with N+1 while upgrading members in place. This is
evidence for that revision pair, not a guarantee for future releases. The same
test verifies that N can recover before activation and refuses N recovery after
activation. Do not mix binaries in production based only on this test.

A second process-level check builds the published `v0.1.0-preview.1` tag and
archives the release-candidate application source at commit
`c36fc27c648f6752fd9ffb9ed80cdfed38b088ff`. It writes through a
preview-created snapshot, rolls members to the candidate one at a time, tests
rollback before activation, and activates only after every member reports
compatible support. This is evidence for that exact preview-to-candidate pair
only. Run it with:

```sh
CONTINUUM_RUN_PREVIEW_UPGRADE_TEST=1 go test ./examples/cluster-process -run '^TestPublishedPreviewRollingUpgradeProcess$' -count=1
```

Before a stable release, the released application source and dependency files
must match the tested candidate revision. If any production Go source,
`go.mod`, or `go.sum` changes, rerun the check. Do not infer compatibility with
other release pairs.

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
The embedded API and membership RPC query candidates before add or promotion,
require a current compatibility report and sufficient command, snapshot, and
feature-level support, and recheck against the active level immediately before
proposing the configuration change. Unknown or unreachable candidates fail
closed.

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
4. **Fence downgrade after activation.** Preflight rejects a target below an
   active level. Lease-level activation forces each member to persist snapshot
   format v5, and `ActivateCompatibility` waits until every current voter and
   learner reports that format. N revision `49fd507` rejects v5 during restore.
   Before activation, rolling back a node to N remains supported by the tested
   v4 snapshot and WAL path. After activation, restart only with a binary that
   reads v5 or newer. If activation returns an error after proposal submission,
   treat its outcome as unknown: inspect compatibility reports and continue
   forward if the active level or v5 fence is visible. If the state cannot be
   established, do not start N.

Membership changes and compatibility activation must be serialized. Adding a
voter during activation must not let a voter without the required support enter
the active configuration.

## Evidence required before closing this gap

- A mixed N/N+1 cluster test covers leader changes, writes, reads, snapshots,
  restart, membership change, and loss of quorum during each rollout phase. The
  current Raft simulation exercises this sequence with a legacy state-machine
  interface and upgraded implementation; it does not run separately built
  release binaries or validate their transport envelope.
- An opt-in process test builds revision `49fd507` and the current tree as
  separate binaries, then tests mixed TCP traffic and recovery from N-created
  snapshots and WAL during an in-place upgrade. Run it from a full-history
  checkout with:

  ```sh
  CONTINUUM_RUN_MIXED_REVISION_TEST=1 go test ./examples/cluster-process -run '^TestMixedRevisionRollingUpgradeProcess$' -count=1
  ```

  The test takes about two minutes and is skipped by the default test suite.
- An opt-in process test builds the published preview tag and the current tree
  to verify the preview-to-current release candidate pair. It exercises
  preview-created snapshots, sequential process replacement, pre-activation
  rollback and the activation barrier. Run it before a stable release, then
  repeat it against the exact immutable release candidate tag.
- Tests prove new-only commands are rejected before activation and applied
  identically by every member after activation.
- Downgrade tests cover both pre-activation and post-activation behavior, and
  the historical process test covers both for revision `49fd507`. Repeat it
  for each release pair before supporting that pair.
- The historical process test removes the remaining N voter before activation,
  confirms that excluded N candidate does not block preflight, and confirms it
  cannot rejoin after activation. It then admits and promotes a caught-up N+1
  learner. A deterministic Raft test verifies that promotion without quorum
  returns an ambiguous result and leaves the candidate out of the isolated
  leader's voter set. Another deterministic test changes membership after
  fingerprinting and verifies stale activation is rejected. Repeat this matrix
  for every supported release pair.
- The operator procedure names the activation point and states when rollback
  stops being safe. Before activation, stop N+1 and recover with N. After
  activation, do not start N: it fails on snapshot v5. Recover with N+1 or a
  newer compatible build. Restoring a pre-activation backup after activation
  loses writes committed after that backup and requires an explicit data-loss
  decision. An ambiguous activation error is not evidence that rollback is
  safe; verify the replicated active level and snapshot format first.

The release-pair matrix still needs to be repeated for each supported N/N+1
pair. Until that evidence exists for a pair, treat mixed-version rolling
upgrades for that pair as unsupported. Use a full-cluster stop and upgrade only
when the cluster can tolerate downtime and a verified backup is available.
This is a conservative operational recommendation, not a claim that all storage
failures are covered by the existing backup tests.
