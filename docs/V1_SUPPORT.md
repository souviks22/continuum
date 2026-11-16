# v1 support contract

**Status: effective for v1.0.0.** This contract defines the supported workload,
platform, topology, and compatibility scope for the stable Go API.

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

V1 will make no numeric promise for total keys/bytes, throughput, latency,
failover time, or history-retention duration. Dataset size and performance are
deployment-specific and must be load-tested by the operator. The complete
dataset and MVCC history reside in memory, and snapshots temporarily require
additional memory. MVCC history is retained until an explicit or configured
compaction advances the compact revision; `KVCompactionRetain` counts data
revisions, not elapsed time. Configure the same compaction policy on every
member and test its effect on historical reads and watches.

Backup creates a consistent archive. The application or operator is
responsible for storing that archive in an external location and testing its
restore procedure.

## Automated checks

The GitHub Actions workflow runs formatting checks, `go test -race ./...`, and
`go vet ./...` on Linux with the Go version from `go.mod`. The first hosted run
passed on [`50db426`](https://github.com/souviks22/continuum/actions/runs/43881274712).

## Compatibility and upgrades

The exported Go API is stable for the v1 major version. Preview versions do not
carry an API or data compatibility promise. Data compatibility and rolling
upgrades are supported only for the explicitly tested release pair below; do
not infer support for other pairs.

Rolling upgrades are a required project capability, but they are **not
generally supported across arbitrary releases**. The process matrix now
validates the published `v0.1.0-preview.1` to release-candidate application
source at commit
`c36fc27c648f6752fd9ffb9ed80cdfed38b088ff`, including a preview-created
snapshot, sequential node replacement, rollback before activation, and
post-rollout preflight/activation. This is the only pair established for the
initial v1 path. Future pairs need the same matrix; do not infer compatibility
for an unlisted pair.

## Release basis

The v1.0.0 release uses the same production Go source, `go.mod`, and `go.sum` as
the tested candidate revision above. Re-run the preview upgrade matrix before
release if any of those files change.

Capacity and performance are intentionally qualitative in v1; the release
claim is limited to small coordination datasets with no numeric service-level
guarantees, as stated above.

See [requirements](REQUIREMENTS.md), [implementation progress](PROGRESS.md),
and [upgrade safety](UPGRADES.md) for detailed evidence and known limitations.
