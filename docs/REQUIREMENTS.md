# Continuum requirements

Purpose: every design decision is checked against this file, so Continuum stays
close to what real users of a distributed KV store need, not just to what the
Python prototype happened to implement. Numbers marked (hypothesis) are starting
points to be validated by benchmarks, not facts.

## Reference workloads (what real systems use this class of store for)

| # | Workload | Real-world analogue | What it demands |
|---|----------|---------------------|-----------------|
| W1 | Cluster metadata / coordination: config, leader election, service discovery | etcd, Consul | Linearizable reads and writes, small values, watch/change feed, leases/TTL, rock-solid correctness over throughput |
| W2 | Transactional storage behind an application or SQL layer | TiKV, CockroachDB, FoundationDB | Multi-key ACID across shards, range scans, snapshot reads, high write throughput, online range split/rebalance |
| W3 | Embedded in a Go service (single binary, 3-5 replicas of the service itself) | etcd embed, Raft libraries | `Open(Config)` API, no external ops burden, in-process transport |

Primary workload: **W1** (decided). Raft: **etcd-io/raft** (decided), driven
through `RawNode` so the node stays single-threaded and simulator-testable.

What W1 means for scope:
- One Raft group holding a small, in-memory, MVCC dataset (etcd/Consul model).
  No sharding, no Percolator two-phase commit, no timestamp oracle, no Pebble.
  Those are W2 follow-ons, not prerequisites.
- Multi-key atomicity is an etcd-style `Txn` (compare / then / else) applied
  inside the single Raft group, not a cross-shard transaction protocol.
- Watch and leases/TTL move from "users will ask" to required.
- Correctness and failure behavior outrank throughput.

## Must-have behaviors (a user would reject the product without these)

1. Linearizable reads by default (ReadIndex or leases). Stale reads are opt-in.
2. Survive f failures in a 2f+1 cluster with no acknowledged-write loss.
3. Membership change (add/replace/remove nodes) without downtime.
4. Rolling restart and rolling upgrade: versioned on-disk format and wire protocol,
   N and N+1 interoperate.
5. Durable by default: fsync before acknowledging; group commit for throughput.
6. Backpressure and limits: max key/value/txn size, bounded queues, clear errors
   instead of unbounded memory growth.
7. Disk-full, slow-disk and corrupt-record handling that fails loudly and safely.
8. Observability: metrics, structured logs, tracing hooks, health/readiness.
9. TLS/mTLS between nodes and from clients.
10. Backup and restore (consistent snapshot to external storage).
11. Honest client errors: ambiguous outcomes (`ErrResultUnknown`) are never
    reported as plain failures or plain successes.

## Features the Python prototype lacks

Required for W1:
- Watch / change feed with revisions, resumable from a revision
- Leases / TTL keys (ephemeral keys, locks, leader election)
- Multi-key atomic `Txn` with compare-and-set conditions
- Request deduplication, so a client retry after an ambiguous timeout cannot
  apply a write twice

Deferred until W2 is taken on:
- Online range split and rebalance, cross-shard transactions

## Explicit non-goals (until decided otherwise)

- AP / multi-master / geo-distributed active-active
- SQL, secondary indexes, query planning
- Multi-tenancy and quota isolation
- Client libraries in other languages (gRPC API keeps this possible)

## Proposed targets (hypothesis; same-datacenter 3-node cluster, NVMe)

- Write p99 under about 20 ms (one fsync + one quorum round trip)
- Linearizable single-key read p99 under about 5 ms
- Leader failover under about 2 s with default timeouts
- Sustained dataset tens to hundreds of GB per node
- Key up to 4 KiB, value up to 1 MiB (hypothesis; etcd and TiKV both cap far below "unbounded")

## Gate checklist (applied before closing each build step)

- [ ] Which workload (W1/W2/W3) does this step serve, and is a real user blocked without it?
- [ ] Does it respect every must-have above, or record an explicit known gap here?
- [ ] Is there a test that fails if the behavior regresses (deterministic sim, race, or fault injection)?
- [ ] Does the public API change? If so, is it the smallest surface that serves a real caller?
- [ ] Benchmarked against the targets, or logged as unmeasured?
