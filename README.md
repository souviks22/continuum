# Continuum

Continuum is a Raft-backed key-value store and embeddable Go library for small
coordination and metadata workloads. It replicates an in-memory MVCC state
machine across one Raft group and provides transactions, linearizable reads,
resumable watches, leases, membership changes, and snapshots. Nodes can
communicate in one process or over bounded TCP with optional mutual TLS.

> **v1.0.0 stable API:** supported scope is deliberately narrow: Linux, Go 1.26
> or newer, one Raft group, and small datasets that fit in memory. Continuum is
> not a general-purpose, disk-resident database. Review the [v1 support
> contract](docs/V1_SUPPORT.md) and [known requirements](docs/REQUIREMENTS.md)
> before adopting it.

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

Add v1.0.0 to your application:

```sh
go get github.com/souviks22/continuum@v1.0.0
```

The root `continuum` package provides `Open` for an embedded node and
`NewKVStateMachine` for the built-in MVCC store. Use `NewClient` to coordinate
embedded nodes in one process, or `NewRemoteClient` to access KV-enabled nodes
over TCP from another Go process. Start with the [remote client guide](docs/REMOTE_CLIENT.md)
and runnable [separate-process example](examples/remote-client/README.md) for
configuration, security, and lifecycle details.

## Scope and operational notes

- The supported v1 release targets Linux, one Raft group, and small
  datasets whose complete state and MVCC history fit in memory. Snapshots
  temporarily need additional memory.
- There is no validated total dataset size, throughput, latency, failover-time,
  or history-retention guarantee. Configure and test compaction for your
  workload; it is opt-in.
- Use TLS/mTLS for remote TCP deployments. Plaintext transport with
  `InsecureLocal` is for trusted local development and tests.
- Each node must have exclusive use of its data directory. `Open` locks it
  until the node closes; do not remove the lock file while a node is running.
- Rolling upgrades are not supported across arbitrary releases. The documented
  process test covers only `v0.1.0-preview.1` to the v1.0.0 application source;
  see [upgrade safety](docs/UPGRADES.md) before changing versions.

## Development

Run the repository checks locally with:

```sh
gofmt -w $(git ls-files '*.go')
go test -race ./...
go vet ./...
```

More detail is in [implementation progress](docs/PROGRESS.md),
[requirements](docs/REQUIREMENTS.md), and the [v1 support contract](docs/V1_SUPPORT.md).
