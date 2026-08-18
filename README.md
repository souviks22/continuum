# Continuum

Continuum is a hands-on exploration of distributed-systems design through a
key-value store. It investigates how durable writes, replicated state
machines, leader election, log recovery, snapshots, network failure, and
shard routing compose into a coherent system.

The project favors explicit invariants and deterministic tests over concealing
distributed-systems complexity behind a production API. It is a space for
experimenting with the tradeoffs and failure modes of a sharded, Raft-backed
store, one subsystem at a time.

## What it does today

- Persists key-value mutations in a CRC-protected write-ahead log and recovers
  safely from torn writes.
- Maintains atomic snapshots and compacts old log entries.
- Replicates each shard with Raft: leader election, log matching, quorum
  commits, follower catch-up, persistent term/vote state, and
  `InstallSnapshot`.
- Runs multiple independent Raft groups on each physical node.
- Stores shard ranges and replica placement in a dedicated, replicated
  metadata Raft group.
- Routes callback-based client `get`, `set`, and `delete` requests through
  metadata and follows leader redirects.
- Exercises partitions, delay, loss, duplication, reordering, crashes, and
  restarts using a seeded deterministic simulator.

## Design in one view

```text
Client
  │ route(key)
  ▼
Metadata Raft group ──► shard range + replica placement
  │
  ▼
KV shard Raft group ──► replicated state machine
  │                         │
  └── leader election, log replication, snapshots
                            ▼
                      WAL + snapshot storage
```

Every Raft group uses the same consensus and storage plumbing. The metadata
group differs only in its state machine: it replicates placement information,
while ordinary shards replicate key-value operations.

## Deliberate boundaries

Continuum is currently a simulation-first exploration, not a production database.
It does not yet provide a real RPC server, authentication, operational tooling,
or a stable public client API. Reads are also not linearizable yet: a replica
that has not learned it lost leadership may briefly serve stale data after a
partition heals. Range splitting is represented in metadata, but is not yet a
fully coordinated data-migration protocol.

Those omissions are intentional. Each subsystem is added only after its
correctness conditions can be stated and tested.

## Run the tests

Requires Python 3.11+ and `pytest`.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pytest
```

The test suite is the best executable description of the system's behavior;
it covers both normal operation and adversarial cluster scenarios.

## Project status

Foundations, durable single-node storage, a single Raft group, multi-Raft
sharding, replicated metadata, and client-side routing are complete. Work is
currently moving into an MVCC storage upgrade. See [the progress log](docs/PROGRESS.md)
for the detailed implementation history and planned phases.

## Repository map

- `continuum/sim/` — deterministic virtual clock, scheduler, and unreliable network
- `continuum/rpc/` — message envelopes, transport seam, and request/reply endpoint
- `continuum/storage/` — WAL, snapshots, state machine, and compaction
- `continuum/raft/` — Raft node, replicated log, and persistent consensus state
- `continuum/cluster/` — shard lifecycle, partitioning, bootstrap, and metadata
- `continuum/client/` — event-driven, leader-aware client routing
- `tests/` — behavior and failure-mode specifications
