# Changelog

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
