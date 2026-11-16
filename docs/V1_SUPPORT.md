# Proposed v1 support contract

**Status: draft.** This document defines the intended scope for a stable v1
release. It is not a support promise yet; the blockers at the end must be
resolved before Continuum is described as stable.

## Intended workload

Continuum v1 targets small coordination and metadata workloads embedded in Go
services or accessed over TCP. It provides one Raft group with an in-memory
MVCC state machine rebuilt from each member's durable Raft log and snapshots.
It is not a general-purpose, disk-resident database.

The first stable release does not target sharding, online range splitting,
cross-shard transactions, multi-tenancy, or multi-language clients.

## Intended platform and topology

- Go 1.26 or newer, matching the module's `go` directive.
- Linux only. Other operating systems and CPU architectures are not covered by
  the current support evidence.
- One Raft group. Three voters are the reference deployment; other cluster
  sizes need explicit validation before being included in the support claim.
- Embedded transport for nodes in one process, or TCP for separate processes.
  Remote TCP deployments should use TLS/mTLS. Plaintext with
  `InsecureLocal` is for trusted local development and tests only.
- Each node must have exclusive use of its data directory. `Open` obtains a
  non-blocking advisory OS lock and returns `ErrDataDirInUse` if another
  Continuum node already owns that directory. The persistent lock file must not
  be removed manually while a node is running.

## Data and limits

The intended data set is small enough for the complete MVCC state and its
history to reside in memory. Current per-request limits include 4 KiB keys,
1 MiB values, and 128 operations per transaction list. There is no validated
maximum total data set size or production throughput claim. Operators must
configure and test MVCC compaction for their retention needs; compaction is
opt-in.

Backup creates a consistent archive. The application or operator is
responsible for storing that archive in an external location and testing its
restore procedure.

## Compatibility and upgrades

The Go API will be treated as stable only after v1.0.0. Preview versions do
not carry an API or data compatibility promise.

Rolling upgrades are a required project capability, but they are **not
currently generally supported**. A tested historical N/N+1 pair is evidence
only for that pair. Before v1 is called stable, the project must define the
supported upgrade and rollback policy, validate transport, commands, snapshots
and durable Raft state for the initial supported pair, and publish the exact
procedure. Do not infer compatibility for an unlisted pair.

## Blockers before this becomes an effective contract

- Complete and document the initial supported upgrade path and its release-pair
  compatibility evidence.
- Add automated CI for the supported Go version and Linux target, including
  race tests and `go vet`.
- Publish tested dataset, retention, and performance limits, or keep the v1
  claim narrowly scoped to small coordination workloads without numeric service
  guarantees.

Until these are closed, the project remains an experimental preview. See
[requirements](REQUIREMENTS.md), [implementation progress](PROGRESS.md), and
[upgrade safety](UPGRADES.md) for the detailed evidence and known limitations.
