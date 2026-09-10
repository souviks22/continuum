# Continuum

Continuum is a simulation-first distributed key-value store built to make
distributed-systems mechanics explicit. It starts with durable local storage,
then composes deterministic fault injection, RPC, Raft, multi-Raft sharding,
replicated metadata, MVCC, a replicated timestamp oracle, and
Percolator-inspired transactional storage.

This is an educational and experimental codebase, not a production database.
The emphasis is on stating invariants clearly and proving behaviour with
deterministic tests, especially under partitions, message loss, reordering,
crashes, restart, and recovery.

## Current capabilities

- A deterministic discrete-event simulator with a virtual clock, seeded event
  ordering, configurable per-link delay/loss/duplication, reordering, and
  symmetric or asymmetric partitions.
- Simulated node crash and restart behaviour, plus a real-cluster chaos-harness
  seam with a Linux network-namespace/`tc netem` implementation for future use.
- JSON request/reply RPC over a transport abstraction, including correlation
  IDs, scheduler-driven timeouts, and harmless handling of late replies.
- A CRC32-protected write-ahead log (WAL) with torn-tail recovery, corruption
  detection, explicit `fsync` control, and safe suffix truncation.
- Atomic snapshots and crash-safe log compaction for local storage and Raft
  logs.
- Raft leader election, durable term/vote state, log matching, conflict
  backtracking, quorum commit, committed-entry application, follower catch-up,
  and `InstallSnapshot` for replicas that have fallen behind compaction.
- Multiple independent Raft groups on each physical node, identified as
  `{physical-node-id}:{shard-id}`.
- A dedicated replicated metadata group that tracks key ranges and replica
  placement; clients use it to route requests to a shard leader and follow
  leader hints.
- MVCC state with timestamped versions, snapshot reads, tombstones, and
  safe-point-based version garbage collection.
- A Raft-backed timestamp oracle that reserves batches durably before issuing
  timestamps, so leadership changes cannot reuse timestamps.
- Percolator-style transactional storage primitives: prewrite, primary/secondary
  locks, commit pointers, rollback, conflict detection, and replicated command
  results.

## Architecture

```text
                         ┌───────────────────────────────────────────┐
                         │ deterministic simulator                    │
                         │ clock · scheduler · fault-injected network │
                         └──────────────────┬────────────────────────┘
                                            │
Client ── route(key) ──► metadata Raft group ──► range + replica placement
  │                                         │
  │ leader redirects / retries              │
  ▼                                         ▼
KV, MVCC, or transaction shard ───────► independent Raft group
  │                                         │
  │                              election · replication · snapshots
  ▼                                         ▼
state machine                         WAL + durable Raft state

Timestamp requests ──────────────────► timestamp-oracle Raft group
```

Raft is deliberately generic over its replicated state machine. The same
consensus, recovery, and snapshot plumbing runs ordinary KV shards, metadata,
MVCC, transactional state, and the timestamp oracle; only the command and
query adapters differ.

## Storage and consistency model

### Durable local storage

The WAL stores length-prefixed records with CRC32 checksums. On recovery, an
incomplete final header or payload is treated as a torn, unacknowledged write
and truncated; a complete record with an invalid checksum raises an error
instead of silently discarding durable corruption. `append()` flushes Python's
buffer to the OS page cache, while `fsync()` is an explicit stronger durability
boundary for power-loss resilience and batching.

Snapshots use write-to-temporary-file followed by atomic rename. Logical entry
indices are retained in persisted data, allowing recovery to converge correctly
even if a crash lands between publishing a snapshot and replacing an old WAL.

### Raft replication

Each Raft replica persists term and vote before responding to a relevant vote
request. Leaders replicate entries to a majority before advancing commit;
direct majority counting is restricted to entries from the current term. A
lagging follower catches up through normal log replication when possible and
through `InstallSnapshot` once the required prefix has been compacted away.

### MVCC, timestamps, and transaction primitives

`MVCCStore` keeps ordered versions per key and reads the newest version at or
before a caller-provided timestamp. A replicated timestamp oracle advances a
durable high-water mark in batches, then serves individual timestamps from that
reserved range. Unused timestamps may be lost after a leader change, but they
are never reused.

`PercolatorStore` is layered on MVCC with three logical column families:

| Family | Purpose |
| --- | --- |
| `data` | Value staged at `start_ts` during prewrite |
| `writes` | `commit_ts → start_ts` pointer that makes a staged value visible |
| `locks` | One outstanding primary-key/start-timestamp lock per key |

Prewrite detects locks and write-write conflicts. Commit requires the matching
lock and creates the visible write record; rollback releases a matching lock
idempotently. `PercolatorStateMachine` replicates those operations and retains
their per-log-index outcomes for a coordinator to inspect after commit.

## What is intentionally not done yet

- No real RPC server, node binary, authentication, observability, or operational
  tooling. The included Linux chaos harness does not yet manage real process
  lifecycles.
- No stable, packaged public API. `ContinuumClient` is callback-based and
  currently exposes basic routed `get`, `set`, and `delete` operations for the
  simulated KV path.
- Client query reads are not linearizable: a replica that has not yet learned
  it lost leadership can briefly return stale data. Read-index or lease-based
  reads are future work.
- Metadata can represent range splits, but there is no coordinated live data
  migration/split protocol across shard replicas.
- Transactional storage has no user-facing transaction coordinator or automatic
  cleanup/resolution of abandoned locks yet. The state-machine result cache is
  also not pruned.
- Garbage collection accepts a supplied safe point; selecting and coordinating
  that safe point across active transactions is not implemented.

## Running the tests

Requires Python 3.11+ and `pytest`.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m pytest
```

The tests are the primary executable specification. They cover storage
recovery, election safety, replication and leader changes, snapshot transfer,
independent shard operation, metadata routing, MVCC history, timestamp safety
across failover, and transactional conflict/visibility behaviour.

## Repository map

| Path | Responsibility |
| --- | --- |
| `continuum/sim/` | Virtual clock, deterministic scheduler, simulated unreliable network, crash/restart model |
| `continuum/chaos/` | Chaos-harness protocol, recording harness, and Linux `tc netem` command adapter |
| `continuum/rpc/` | Message envelope, transport protocol, simulated transport, request/reply endpoint |
| `continuum/storage/` | WAL, atomic JSON persistence, snapshots, single-version storage engine, and MVCC |
| `continuum/raft/` | Raft node, replicated log/compaction, durable term-vote state |
| `continuum/cluster/` | Shard addressing and lifecycle, range partitioning, replicated metadata, bootstrap helper |
| `continuum/client/` | Callback-based metadata routing and leader-aware KV client |
| `continuum/tso/` | Timestamp-oracle state machine and batched leader-side allocator |
| `continuum/txn/` | Percolator-style store and replicated transaction state-machine adapter |
| `tests/` | Deterministic unit and integration specifications |
| `docs/PROGRESS.md` | Detailed build history and design decisions by phase |

## Project status

The project has completed the simulator, durable storage, single-group Raft,
multi-Raft sharding and metadata, client routing, MVCC, and timestamp-oracle
milestones. It now contains the replicated Percolator storage substrate; the
next major transaction work is coordination and recovery of abandoned locks.
For the implementation history, see [docs/PROGRESS.md](docs/PROGRESS.md).
