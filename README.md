# Continuum

Continuum is an experimental Raft-backed key-value store and embeddable Go
library. It provides a replicated MVCC state machine with transactions,
linearizable reads, resumable watches, leases, membership operations, snapshots,
and a bounded TCP transport with optional mutual TLS.

The project prioritizes explicit failure behavior and deterministic testing. It
is not yet a production database: see [the requirements](docs/REQUIREMENTS.md)
and [implementation progress](docs/PROGRESS.md) for scope and known gaps. The
[upgrade safety note](docs/UPGRADES.md) describes the current rolling-upgrade
limitation.

## Quick start

Requires Go. Run the repository test suite with:

```sh
go test -race .
```

For a minimal durable single-node example, write a value and then reopen the
same data directory in a new process:

```sh
go run ./examples/restart write ./continuum-data
go run ./examples/restart read ./continuum-data
```

See [the restart example](examples/restart/README.md), [the multi-process
cluster example](examples/cluster-process/README.md), and [the remote client
guide](docs/REMOTE_CLIENT.md) for more deployment examples. The examples use
local settings and are not production deployment recipes.

## Go API

The root `continuum` package exposes `Open(Config)` for an embedded node,
`NewKVStateMachine` for the built-in KV state machine, and
`NewRemoteClient` for applications connecting over TCP. See the package
documentation and examples for configuration details.
