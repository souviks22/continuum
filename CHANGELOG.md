# Changelog

## v1.0.0 — Stable API release

The first stable Go API release of Continuum. The API is stable for the v1
major version within the supported scope in [the v1 support
contract](docs/V1_SUPPORT.md).

### Supported scope

- Go 1.26 or newer on Linux.
- One Raft group for small coordination and metadata datasets held in memory.
- Embedded nodes or separate-process TCP deployments; use TLS/mTLS for remote
  deployments.
- The tested rolling-upgrade path from `v0.1.0-preview.1` to this release.

### Limitations

- Other operating systems, arbitrary cluster sizes, and datasets larger than
  available memory are outside the supported scope.
- There are no numeric capacity, throughput, latency, failover-time, or
  retention guarantees.
- Rolling upgrades between other release pairs are not supported unless that
  exact pair passes the documented compatibility matrix.
- Preview releases do not carry API or data compatibility promises.

### Verification

The release revision passed hosted Linux Go 1.26 CI, including formatting,
`go test -race ./...`, and `go vet ./...`. The pinned preview-to-candidate
process matrix and full local race suite also passed; production Go source and
dependency files match the tested candidate revision.

## v0.1.0-preview.1 — Experimental preview

This is the first public preview of Continuum, an experimental Raft-backed
key-value store and embeddable Go library. It is not a production-ready
database, and this preview does not promise stable APIs or a supported
mixed-version upgrade path.

### Included

- A thread-safe embedded `Open(Config)` API and a remote TCP client.
- A replicated MVCC key-value state machine with transactions, linearizable
  reads, resumable watches, leases, and membership operations.
- Durable Raft logs, snapshots, backup and restore support, and optional mTLS.
- Deterministic simulation and fault-injection tests, plus process-level
  restart and upgrade checks.

### Known limitations

- General N/N+1 rolling upgrades are unsupported. The mixed-binary process test
  covers one historical revision pair only; see [upgrade safety](docs/UPGRADES.md).
- The store keeps its MVCC data in memory and rebuilds it from snapshots and
  the Raft log. Linux is the supported platform.
- Performance targets and production workload limits have not been validated.
- Review [requirements](docs/REQUIREMENTS.md) and
  [implementation progress](docs/PROGRESS.md) before using this preview.

### Verification

The release candidate passed `go test -race ./...`, `go vet ./...`, and the
opt-in mixed-revision TCP process test documented in `docs/UPGRADES.md`.
