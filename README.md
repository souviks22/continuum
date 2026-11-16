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

Requires Go. Run the full repository test suite with:

```sh
go test -race ./...
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

The root `continuum` package has three entry points:

- `Open(Config)` starts a thread-safe embedded Raft node. `Config` selects the
  state machine, data directory, peers, bootstrap mode, transport security and
  snapshot/compaction settings. A node exposes `Propose`, `ReadIndex`, `Health`,
  and `Close`.
- `NewKVStateMachine()` creates the built-in replicated MVCC state machine.
  KV-enabled nodes expose `Txn`, `RetryTxn`, linearizable `Range`, resumable
  `Watch`, leases, compaction and session checkpoint methods. They also expose
  membership reads and add/promote/remove operations.
- `NewClient(nodes, options)` routes those KV and membership operations across
  embedded nodes in one process. `NewRemoteClient(config, options)` provides the
  same client operations over TCP for applications in separate processes.
  Remote clients can call `Compatibility` to inspect configured servers,
  `CheckCompatibility` to preflight a target, and `ActivateCompatibility` to
  commit a compatibility level after all current voters and learners pass.
  Activation is bound to the observed membership and enables newer command
  and snapshot formats only after the committed barrier. Lease-level activation
  waits for every current member to persist a rollback-fenced snapshot; older
  binaries then fail to restore that state. This API is a rollout primitive;
  mixed-version rolling upgrades still need release-pair validation (see
  [upgrade safety](docs/UPGRADES.md)).

See [the remote client guide](docs/REMOTE_CLIENT.md) and the runnable examples
for configuration and lifecycle details. The Go package documentation lists
the exported types and methods.
