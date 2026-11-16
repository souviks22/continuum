# Continuum

Continuum is an experimental Raft-backed key-value store and embeddable Go
library for small coordination and metadata workloads. It replicates an
in-memory MVCC state machine across one Raft group and provides transactions,
linearizable reads, resumable watches, leases, membership changes, and
snapshots. Nodes can communicate in one process or over bounded TCP with
optional mutual TLS.

> **Experimental preview:** the Go API and stored data formats do not yet carry
> a stable compatibility promise. Continuum is not a general-purpose,
> disk-resident database. See the [v1 support draft](docs/V1_SUPPORT.md) and
> [known requirements](docs/REQUIREMENTS.md) before adopting it.

## Try it

Requires Go 1.26 or newer. The restart example writes a value to a durable
single-node cluster, then reads it after the process is restarted:

```sh
go run ./examples/restart write ./continuum-data
# Stop that process, then run:
go run ./examples/restart read ./continuum-data
```

Keep the data directory between runs. Removing it starts a new cluster and
loses the stored data. This example demonstrates a clean single-node restart;
it does not demonstrate failover or recovery from a damaged disk. See the
[restart example](examples/restart/README.md) for details.

## Use as a Go module

Add the preview release to your application:

```sh
go get github.com/souviks22/continuum@v0.1.0-preview.1
```

The root `continuum` package provides `Open` for an embedded node and
`NewKVStateMachine` for the built-in MVCC store. Use `NewClient` to coordinate
embedded nodes in one process, or `NewRemoteClient` to access KV-enabled nodes
over TCP from another Go process. Start with the [remote client guide](docs/REMOTE_CLIENT.md)
and runnable [separate-process example](examples/remote-client/README.md) for
configuration, security, and lifecycle details.

## Scope and operational notes

- The intended first stable release targets Linux, one Raft group, and small
  datasets whose complete state and MVCC history fit in memory. Snapshots
  temporarily need additional memory.
- There is no validated total dataset size, throughput, latency, failover-time,
  or history-retention guarantee. Configure and test compaction for your
  workload; it is opt-in.
- Use TLS/mTLS for remote TCP deployments. Plaintext transport with
  `InsecureLocal` is for trusted local development and tests.
- Each node must have exclusive use of its data directory. `Open` locks it
  until the node closes; do not remove the lock file while a node is running.
- Rolling upgrades are not generally supported across arbitrary releases. The
  documented process test covers only `v0.1.0-preview.1` to candidate source
  commit [`c36fc27`](https://github.com/souviks22/continuum/commit/c36fc27c648f6752fd9ffb9ed80cdfed38b088ff).
  See [upgrade safety](docs/UPGRADES.md) before changing versions.

## Development

Run the repository checks locally with:

```sh
gofmt -w $(git ls-files '*.go')
go test -race ./...
go vet ./...
```

More detail is in [implementation progress](docs/PROGRESS.md),
[requirements](docs/REQUIREMENTS.md), and the [draft v1 support contract](docs/V1_SUPPORT.md).
