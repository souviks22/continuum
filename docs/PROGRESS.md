# continuum build progress

Reference for behavior: the Python prototype (github.com/souviks22/continuum).
Every step is checked against docs/REQUIREMENTS.md.

## Step 1: deterministic simulator (`internal/sim`)
Virtual clock, scheduler (stable tie-break, O(1) `Pending`, cancellable events,
step limit), fault network (asymmetric partitions, per-link delay/loss/dup,
reordering from delay, crash/restart with `onRestart` before up). 42 tests.

## Step 2: time/transport seams and RPC (`internal/clock`, `transport`, `rpc`)
- `clock.Clock` / `Timer`; `sim.Scheduler` implements it. The serialization
  contract (callbacks never run concurrently on one node) is documented on the
  interface. No real Clock yet: it must come with the event-loop runtime, since a
  bare `time.AfterFunc` would violate the contract.
- `transport.Transport` (`Send`, `SetReceiveHandler`); `sim.Transport` and a
  bounded TCP implementation with per-peer writers, frame limits, backpressure,
  connection/pending-dispatch limits and optional mutual TLS identity checks.
- `rpc.Message`: versioned binary envelope (no dependencies), opaque payload.
  Decode validates every length against remaining bytes and applies the same
  rules as Encode; fuzzed (about 825k executions, no findings).
- `rpc.Endpoint`: correlated request/reply, clock-driven timeouts, exactly-once
  callback, late and duplicate replies ignored; synchronous and one-shot async
  handlers are supported.

Deliberate deviations from the Python prototype, each driven by REQUIREMENTS.md:

| Python | Go | Why |
|---|---|---|
| Unknown method: silence, caller times out | `StatusUnknownMethod` reply, `ErrUnknownMethod` | Rolling upgrades (must-have 4) need "not implemented" distinguishable from "network down"; the request definitely did not run |
| Handler failure not representable | `StatusError` reply, `*RemoteError` | Honest errors (must-have 11): the request reached the handler and may have had effects |
| Process-global message ID counter | Per-endpoint counter with `Options.IDBase` | Global state leaks determinism across runs and tests; per-sender uniqueness is all correlation needs |
| Any peer's reply matched by ID | Reply accepted only from the peer the request went to | A stray or forged reply could complete someone else's call |
| JSON body | Version byte + varint framing, opaque payload | Versioned formats from day one (must-have 4); no dependency; each method owns its payload codec |
| Reply send / decode failures invisible | `Options.OnError` hook | Version-skew and corruption need to be countable in production (must-have 8) |

Known gaps, not papered over:
- `RegisterAsync` supports delayed replies for proposal-commit and ReadIndex
  results. Its callback must run at most once on the serial event loop. The RPC
  layer still does not deduplicate duplicate requests; handlers need idempotency.
- The RPC layer does not deduplicate requests; handlers must be idempotent.
- No payload size limit at the RPC layer; `TCPTransport` enforces a bounded
  frame size and bounded queues. Other transports must enforce their own limits
  (must-have 6).
- No `context.Context` or blocking API yet; that belongs to the client step.

Async-handler tests verify delayed replies, one-shot completion, remote errors,
and method-name uniqueness across sync, async and notify registrations.

Requirements gate: serves all of W1, W2 and W3 (foundation); touches must-haves
4, 6, 8, 11; public API unchanged (everything is `internal/`); no benchmarks yet
(logged as unmeasured).


## Step 3: durable storage primitives (`internal/vfs`, `internal/wal`)

### `vfs`: the filesystem seam
`FS`/`File` interfaces with two implementations. `OS()` is the real filesystem.
`Mem` is a simulated one that models durability instead of pretending writes are
instant: file contents are durable only after `Sync`, directory entries (create,
rename, remove) only after `SyncDir`, and `Crash` reverts to exactly that state,
optionally leaving a torn tail (a file extended by some length, with only a
prefix of the new bytes real and the rest zero). It also injects per-operation
errors and ENOSPC (partial write, then error). This is what lets crash
behavior be tested deterministically instead of by hand-truncating files.

### `wal`: segmented, checksummed, append-only log
- Each record carries its own sequence number (for Raft, the log index), so
  recovery never depends on physical layout. Segments are named by first seq;
  prefix compaction deletes whole files, suffix truncation cuts one file.
- Versioned on-disk format from byte zero (segment header has magic + version +
  checksum); separate checksums for record header and payload, so a flipped
  length cannot be mistaken for a torn tail.
- Nothing is durable until `Sync`; `Append` only buffers, so a caller can batch
  many records per fsync (group commit).
- Any I/O error poisons the log (`ErrFailed`): after a failed fsync the kernel
  may have dropped the dirty pages, so a retry could ack lost data.
- Recovery: non-final segments must be perfect; in the final segment a damaged
  tail is truncated and reported only if no intact record follows it.

| Python | Go | Why |
|---|---|---|
| One file; compaction rewrites it | Segments; compaction deletes files | O(1) compaction, no rewrite window (must-have 5/7) |
| Torn vs corrupt by "incomplete vs bad CRC" | Torn iff no intact record follows; header has its own CRC | Torn writes can leave real+zero bytes past a record's end; a flipped length must not silently drop acked records |
| Tests truncate/corrupt real temp files | Simulated power cuts, crash injected at every mutating call of truncate/discard/reset/rotate/recovery | Tests the durability contract (fsync, dir fsync, ordering), not just parsing |
| fsync errors surfaced per call | Sticky failure | Linux can report an fsync error once and drop the data |
| zlib CRC32 | CRC32-C | Hardware-accelerated; standard for logs |

Bug found by the property test during this step: the first recovery rule only
forgave a bad payload checksum when the record ended exactly at EOF. A torn
write can leave a half-real payload followed by more zero-fill, which that rule
rejected as corruption. Fixed (same "no intact record follows" rule as for bad
headers) with a regression case.

Verification: 100+ tests under `-race`; 300-seed randomized crash property
(everything before the last successful Sync survives, no gaps, no damaged
payloads); crash at every mutating filesystem call of TruncateTo (3 cut points),
DiscardBefore, Reset, rotating Append, and of recovery's own repair; 9 targeted
mutations of the durability/recovery logic, all caught (including dropped
fsyncs, dropped dir-syncs, wrong delete order, no poisoning, no forward scan,
no sequence check).

Known limitations, stated plainly:
- Damage to the very last record is indistinguishable from a torn write; it is
  truncated and reported via `TruncatedOnOpen`, not rejected. A replicated
  caller should alert on it and rely on its quorum.
- A crash that persists a LATER block of unsynced data while dropping an
  EARLIER one is reported as `ErrCorrupt` (loud, needs operator action), not
  auto-repaired. `Mem` does not model block reordering, bit rot or misdirected
  writes.
- `Reset` is not atomic (documented; callers persist their intent first).
- The public `continuum.Open` API holds a non-blocking OS lock on the data
  directory until `Node.Close`; direct internal Raft-node use is outside that
  public ownership guard. The lock file is persistent and must not be removed
  while its node is running.
- `Read` rescans a segment from its start, and recovery is O(log size)
  (about 16 ms for 200k x 128 B records on the sandbox). Fine for now; add a
  sparse index when Raft follower catch-up needs it.
- Paths are forward-slash; Linux is the supported platform.

Indicative numbers (sandbox disk, NOT validation of the targets): append+fsync
per record 284 us; 16 per fsync 17.8 us/record; 128 per fsync 2.9 us/record.
The 100x batching win is why the API separates `Append` from `Sync`.

Requirements gate: serves W1/W2/W3 (Raft log and durable state); must-haves 4
(versioned format), 5 (durable by default, group commit), 7 (disk-full/failed
fsync/corruption fail loudly and safely); public API unchanged (all `internal/`).


## Step 4: atomic files and the snapshot store (`internal/atomicfile`, `internal/snapshot`, `internal/vfs/vfstest`)

### Decisions made before this step
- Raft library: **etcd-io/raft**, latest stable v3.7.0. It requires **Go 1.26**,
  so the module's minimum Go version becomes 1.26 when the dependency is added
  (next step). Its `raftpb` types moved to the standard protobuf runtime: fields
  are pointers, so code must use the `GetX()` accessors.
- Primary workload: **W1** (see REQUIREMENTS.md). Consequences, replacing the
  earlier W2-shaped plan:
  - one Raft group, in-memory MVCC state machine rebuilt from snapshot + log;
    Pebble, multi-Raft, Percolator and the TSO are no longer on the critical path;
  - the client API sketched earlier (closure-based `Update` over Percolator) is
    replaced by an etcd-style `Txn(If..Then..Else)`, plus `Range`, `Watch` and
    `Lease`. The rule that ambiguous outcomes surface as `ErrResultUnknown`, never
    as plain failure, still applies, and is why request dedup is on the list.

### `atomicfile`
Durable all-or-nothing replace of a small file: temp file, fsync, rename, fsync
the directory. Versioned and checksummed. For Raft's term/vote and metadata.
After a crash at any point the file reads as exactly the old or exactly the new
contents; a `Write` that returned nil is the new one.

### `snapshot`
One file per snapshot, named by log index; streamed writes (unknown size,
buffered), trailer holds length and CRC32-C of the body so truncation and bit
flips are detected; `Commit` is atomic and durable (the caller may discard log
entries at or below the snapshot only after it returns). `Scan` verifies every
file; corrupt ones are reported, never hidden or auto-deleted, because whether an
older snapshot is usable depends on how much log remains, which only Raft knows.
`Prune` keeps the newest N valid snapshots and never touches newer corrupt ones.
`Meta.Extra` is opaque; the Raft layer will store the serialized ConfState there.

### `vfstest.CrashAtEveryOp`
The crash-at-every-mutating-call harness is now shared (the WAL tests still carry
their own copy; migrating them is deferred cleanup).

Verification: crash injected at every mutating filesystem call of a replace and
of a snapshot commit (clean and torn power loss), exhaustive byte-flip and
truncation tests, disk-full and I/O-error injection, OS-filesystem round trips,
and 16 targeted mutations (dropped fsync, dropped dir-sync, unchecked checksum /
length / trailer / header / name-index, leaked temp files, wrong prune bound), all
caught. Two checks were initially uncaught; tests crafting those exact files
(valid checksum with wrong length; a snapshot under the wrong name) were added.

Known limitations:
- A failed `atomicfile.Write` leaves the durable state unknown (old or new); the
  Raft layer must treat it as fatal, like a failed fsync.
- No locking: one writer per file name or snapshot directory is assumed.
- `snapshot.Load` holds a whole snapshot in memory (`Open` streams); `Scan` reads
  every snapshot fully. Fine for W1's size class; revisit for large datasets.
- No compression, and no chunked transfer protocol for sending snapshots to a
  lagging follower (that belongs to the Raft/transport step).

## Step 5: the Raft node (`internal/raftnode`, plus `rpc` one-way messages and `sim.ProcessClock`)

One Raft member on etcd-io/raft v3.7.0's `RawNode`, with durable state in the WAL,
atomic-file and snapshot packages, messaging over `rpc`, and a `StateMachine`
interface for the replicated application. Single-threaded and goroutine-free, so it
runs unchanged under the simulator.

### Per-Ready order (the safety argument)
snapshot committed + log reset, then log entries appended and fsynced, then
term/vote written atomically if changed, and only then messages sent and entries
applied. A node never acknowledges, votes on, or applies anything it could forget
after a power cut.

### Decisions and why
| Decision | Why |
|---|---|
| `RawNode`, not `Node` | No goroutines or channels: the node is a pure function of its inputs, so the simulator can drive it and replay a failing seed exactly |
| Bootstrap = install a synthetic snapshot at (index 1, term 1) holding the voter set | One atomic write, idempotent if the node crashes midway. Raft's own `Bootstrap` appends membership entries; a crash after they are written but before they commit could leave every node with uncommitted membership and nobody able to campaign |
| Persist term/vote only when they change; commit index is not persisted | Commit is re-learned from the leader and the state machine is rebuilt from snapshot + log, so persisting it on every advance would cost a second fsync per write for nothing. On recovery it is clamped into what the log backs |
| Snapshots carry a kind: local or installed | After a crash between committing a snapshot received from a leader and resetting the log, entries above it belong to a different history and must not be replayed. A local snapshot, by contrast, is a prefix of the node's own log and the entries above it are valid. Recovery tells them apart by kind and by where the log starts |
| Recovery refuses to guess: unreadable hard state is fatal; a log that does not reach the newest usable snapshot is `ErrLogGap` | Guessing term/vote can double-vote; silently skipping log entries loses data |
| **Raft's proposal forwarding is disabled; a follower returns `NotLeaderError{Leader}`** | See "Findings". Forwarded proposals are the one raft message that is not idempotent |
| Sender and destination are checked on every inbound raft message | Without it any peer can speak as another and a misdirected message is acted on |
| PreVote and CheckQuorum on | A rejoining partitioned node cannot force a needless election, and a leader cut off from its quorum steps down |
| Persist, then send, sequentially (even on the leader) | Simpler than overlapping the leader's disk write with its sends, at the cost of some latency |
| `ErrResultUnknown`, never plain failure, for a proposal that has not applied by its timeout | It may still commit; the caller must know it cannot assume either outcome |

### Findings during this step
1. **Duplicate application.** The first chaos run applied a command twice in about a
   third of seeds, identically on every node. Two controlled experiments isolated it
   (no duplication: clean; duplication on but proposals only to the leader: clean):
   raft's follower-to-leader `MsgProp` is not idempotent, so a duplicated forward
   appends the command twice. Fixed by disabling forwarding; the same chaos run
   now passes with duplication on, which is the evidence the diagnosis was right.
2. **etcd-io/raft's election randomness cannot be seeded.** It reads
   `crypto/rand.Reader` at call time, so simulated clusters were not reproducible.
   Tests swap in a seeded reader (a global, test-only, so those tests are
   serial); same seed now gives an identical election, different seeds differ.
   Production is untouched.
3. **A simulator blind spot.** Mutating the node to send messages *before* making
   state durable survived: in the simulator a power cut can only land between
   events, so the persist always finished first. The chaos test now also cuts power
   at a random filesystem call in the middle of a handler, after some messages are
   out and before the rest of the state is written. That mutation is now caught.

### Verification
- Scenario tests: election, replication, leader power loss and catch-up, whole
  cluster power loss (clean and torn), isolated leader whose stale entries are
  overwritten after healing, follower brought up to date by snapshot, no-quorum
  and stop behavior, forged and misdirected messages.
- Recovery tests on hand-crafted disks: installed vs local snapshot over a stale
  log, log shorter than snapshot, log gap, unreadable hard state, missing hard
  state.
- Crash injected at every mutating filesystem call of bootstrap (24 calls) and of
  snapshotting + log compaction + restart (78 calls), clean and torn.
- Chaos, per seed: random proposals, scheduled power cuts (clean/torn), restarts,
  partitions, 3% message loss and 3% duplication with reordering, plus power cuts at
  random filesystem calls mid-handler. Invariants: one leader per term at every
  instant; strictly increasing apply order; identical sequence on every node after
  healing; every acknowledged command present exactly once; no node failure.
  500 seeds: 12,157 acknowledged commands, 5,530 scheduled power cuts (2,715
  torn), 758 mid-handler power cuts (405 of 500 seeds), elections up to term 12,
  snapshot installs on lagging followers in a third of seeds. All pass.
- 17 targeted mutations (entries not fsynced, term/vote not durable, send before
  persist, no truncation of a conflicting suffix, no log reset on installed
  snapshot, each recovery rule removed, log discarded before the snapshot is
  durable, forwarding re-enabled, sender/destination unchecked, results delivered
  to the wrong waiter, snapshot status not reported). 16 caught; the 17th is the
  assertion that raft delivers committed entries exactly once and in order, which
  is unreachable by construction and kept as a fail-stop check.

How to run: `go mod tidy && go test -race ./...`. For a long soak:
`CHAOS_SEEDS=2000 go test -run Chaos ./internal/raftnode`. A failing seed replays
exactly with `-run 'Chaos/seed=N'`.

### Known limitations
- No real runtime yet. The node is single-threaded by contract; the event loop that
  provides the serial context for real timers and sockets comes with the server.
- Membership is fixed (`Peers`). The apply path handles conf-change entries but there
  is no API or test for adding or removing members; that is step 9.
- No reads yet. Linearizable reads (ReadIndex) are step 7.
- A snapshot travels as one rpc message; no chunking, and the rpc layer has no
  payload size limit yet. Fine for W1's size class.
- The log lives in memory between snapshots (raft's `MemoryStorage`), bounded by
  `SnapshotEvery` and `SnapshotTrailing`. After a restart the trailing entries below
  the newest snapshot are not reloaded, so a follower that was briefly behind may
  receive a snapshot instead of log entries.
- Exactly-once across client retries is not provided here: a retry is a new
  proposal. Client sessions (step 7) own that.
- The module now requires Go 1.26, and the shipped `go.mod` has no `go.sum` (the
  development sandbox could not reach the module proxy); run `go mod tidy` once.

## Step 6: the MVCC key-value state machine (`internal/kv`)

The first piece a user of the finished store would recognize: a multi-version
store with etcd-style transactions, implementing `raftnode.StateMachine`. History
is kept per key until compacted, so reads can be served as of an older revision
and (next steps) watchers can resume. Mutations are all `Txn`s: compares, then
Success or Failure ops. Put, Delete and compare-and-set are just Txns.

### Decisions and why
| Decision | Why |
|---|---|
| Revision = the Raft log index at which a write applied | No second counter to keep consistent across replicas; deterministic by construction; matches Consul and ZooKeeper. Cost: revisions have gaps (leader no-ops, membership entries), which is harmless |
| Size limits are compile-time consensus constants; configurable `Limits` may only be stricter and are checked before proposing | If replicas disagreed about a limit, a command valid on one could be rejected on another during a rolling upgrade and state would diverge. `Limits` cannot loosen the constants (tested) |
| Compares see the state before the txn; ops run in order and see earlier ops' writes | Natural read-your-writes semantics without etcd's restriction on overlapping keys |
| Everything that can fail (read revisions) is validated before the first op runs | A txn applies completely or not at all; tested by a mutation that removes the up-front check |
| `Apply` never panics and never returns an error: bad commands become identical error results on every replica | A poison command cannot crash or fork the cluster; callers still get an honest, typed error (`ErrInvalidCommand`, `ErrCompacted`, `ErrFutureRevision`) |
| Compaction keeps each key's state as of the revision; a key deleted by then disappears entirely | Bounded memory, reads at the compaction revision stay valid, and it is a replicated command so every replica compacts identically |
| Missing keys compare as version/create/mod 0; a value compare on a missing key is false | etcd-compatible, so "create if absent" is `mod == 0` |
| Canonical snapshot bytes; `Hash` is SHA-256 of the snapshot | Replicas can be compared byte for byte, which the audit tests use and a future periodic consistency check can too |
| `Restore` validates the whole snapshot (ordering, revisions, tombstones) before touching the store | A corrupt snapshot must never leave a half-restored state machine |
| Hand-written versioned binary codec, no protobuf | No generated code or dependency; every length is checked against the bytes remaining so no allocation is attacker-sized |
| Sorted-slice key index (not a B-tree) | Simple and, with the model test, easy to replace later. See the numbers below |

### Verification
- Semantics tests: create/mod/version tracking, delete-then-recreate starting a
  new life, range selection (point, half-open, prefix, from-key, all), limit/count/
  keys-only, historical reads and both revision errors, compaction, every compare
  target and relation, ordering inside a txn, only-writes-advance-the-revision,
  atomicity, PrevKV, empty values, copy-in and copy-out (no aliasing).
- **Model-based test**: a deliberately naive reference store (maps and linear
  scans) and the real store are driven by 300 seeds x 150 random transactions,
  compactions and historical reads. After every step results and error kinds must
  match exactly; every 10 steps whole-state dumps at several revisions must match,
  and a snapshot restored into a fresh store must stay identical to the original
  as both keep evolving.
- Codec: every truncation, trailing byte, oversize list, key and value, and
  unknown tag is rejected; three fuzz targets (commands, results, snapshots),
  15 seconds each with no findings. Crafted snapshots break each structural
  invariant one at a time (unsorted keys, impossible revisions, tombstone with a
  value, ...) and are all rejected without touching the store.
- **Inside the real Raft cluster**: the same chaos as step 5 (power cuts clean and
  torn, mid-handler power cuts, partitions, loss, duplication, snapshot installs)
  with every `(index, command, result)` any replica ever applied recorded in an
  audit log. It checks that no index is ever applied differently, no command is
  applied at two indexes, every result a client received equals the committed one,
  and a fresh replay of the whole log produces exactly the state every replica
  ended in. 500 seeds: 33,872 proposals, 13,955 acknowledged, 14,503 distinct log
  entries replayed, 5,456 scheduled and 861 mid-handler power cuts. All pass.
- Mutations: 25 breaks of store, codec and limits. The model test catches all of the
  core-semantics ones (version/create reset, compaction off-by-one, history read
  off-by-one, range end, missing-key compares, atomicity, revision advance, PrevKV,
  limit counting, keys-only, aliasing). Four survived at first because they
  concern structural validation of corrupt snapshots, which bit-flip testing cannot
  target; crafted-snapshot tests now catch them. One mutant remains alive and is
  equivalent: the "list length bounded by remaining bytes" check is an early exit,
  since each element consumed fails on truncation anyway. Two chaos-level breaks
  (a result that depends on a hidden global; `Restore` forgetting the compaction
  revision) are caught by the audit.

### Numbers (sandbox, indicative)
Put to an existing key: 0.5 us at 1k keys, 1.6 us at 100k, 6.3 us at 500k. Put of a
new key in the middle: 0.4 us, 48 us, 570 us (the sorted slice's O(n) insert). A
100-key range read at 100k keys: 31 us. Snapshot of 100k keys: 10.6 ms; restore 31
ms. Takeaway: adequate to about 10^5 keys; before 10^6 the index should become a
B-tree (the model test is the safety net for that swap), and the snapshot pause
(taken synchronously on the apply path) grows linearly with data.

### Known limitations
- Reads exist (`Store.Range`) but are not yet linearizable: serving them safely
  needs ReadIndex, step 7. Callers must be on the node's serial context.
- Store-level resumable watch events and in-process client watch delivery are
  implemented in Step 8a. Cursor-based watch RPC and lease commands over the
  generic proposal RPC are implemented internally; the public TCP RemoteClient
  is documented in Step 19. Lease lifecycle and leader tick scheduling are
  available for embedded nodes.
- Automatic compaction is opt-in (Step 16), not a default. Without it, MVCC
  history grows until an operator proposes `Compact`; retention still needs
  workload-based sizing and byte-oriented limits.
- Range compares and nested transactions are not supported.
- Limits are consensus constants; changing one is a migration, not a setting.
- `Hash` costs a full snapshot; a periodic cross-replica consistency check should
  use something incremental.
- The whole dataset and its history live in memory, and a snapshot briefly holds a
  second copy.

## Step 7a: client sessions and request dedup (`internal/kv`)

Why: must-have 11 and the "request deduplication" feature. A client whose write
times out cannot tell if it applied; resending a plain `Txn` could apply it twice.

### Design
- `EncodeRegister` opens a session; its ID is the **Raft log index** of the
  registration (deterministic, unique, no client-chosen IDs, no clock).
- `Txn.EncodeSession(session, seq)` sends request `seq`. Per session the state
  machine keeps the highest `seq` applied and that request's encoded result.
  Same `seq` again returns the **identical bytes** and applies nothing; a lower
  `seq` is `ErrStaleRequest`; gaps are allowed (strictly increasing is the rule).
  One request in flight per session (Raft thesis ch. 6); pipelining would need a
  window and is not provided.
- Unknown or evicted session: `ErrSessionExpired`, nothing applied. Applying it
  anyway could duplicate a write that already ran, so the client must report
  `ErrResultUnknown`.
- Bounded memory: `MaxSessions` = 10,000 and `MaxCachedResult` = 4 KiB (worst case
  40 MiB). Eviction is least-recently-used by **log index**, and a retry counts as
  use. A result larger than 4 KiB is not kept: a retry gets `ErrResultNotCached`
  ("it applied, but I no longer have the answer"), never a second application.
- Error results are remembered too, so a retry gets the original answer even when
  re-executing now would differ (a lock compare that failed stays failed; a read
  of a then-future revision stays an error).
- Sessions live in `StateMachine`, not `Store`, so the store, its model test and
  its snapshot are untouched. `StateMachine.Snapshot` is format v2 (store snapshot
  plus sorted session table); a bare v1 store snapshot still restores (no
  sessions). `Restore` validates everything before changing anything, including
  that a retained result parses as exactly one result. `StateMachine.Hash()` covers
  store and sessions.
- `Encode` (no session) still exists and is explicitly not deduplicated.

### Verification
- Behavior tests: ID = log index, retry returns first bytes and changes nothing,
  retry after another client wrote the key, stale, gaps, unknown session, failed
  compare and error results stay as first answered, oversized result, LRU eviction
  (retry refreshes), snapshot/restore keeps dedup, legacy snapshot, no aliasing of
  cached bytes.
- **Property test** (200 seeds x 150 steps): replica A gets random duplicates,
  stale resends and snapshot/restore cycles; reference B gets each distinct
  request once. Stores must stay identical at every step; first attempts must
  return identical bytes; duplicates must return the first attempt's bytes.
- Every truncation and trailing byte of a session command and of a v2 snapshot is
  rejected without changing state; 14 crafted bad session tables rejected; fuzz
  target `FuzzStateMachineRestore` (accepted input must re-serialize stably):
  about 1.3M executions in total, no findings.
- 19 targeted mutations (dedup off, stale check off, seq not recorded, errors not
  remembered, LRU not refreshed / reversed / unbounded, unknown session
  auto-created, snapshot drops or unsorts sessions, dropped result served,
  oversized kept, legacy restore keeps sessions, each snapshot validation removed,
  non-atomic restore, two aliasing breaks). 17 of the first 18 were caught; the
  aliasing one survived until a test was added, then both aliasing variants and
  all others were caught.
- All packages except `raftnode` pass under `-race`. `raftnode` could not be built
  in this sandbox (no access to etcd-io/raft). The change is additive to the
  `kv.StateMachine` API it uses (`NewStateMachine`, `Apply`, `Snapshot`, `Restore`,
  `Store`), so it should compile unchanged, but **run `go test -race ./...` on a
  networked machine before relying on it**, including the KV chaos test.

### Numbers (sandbox, indicative)
Put through `Apply`: 1.48 us plain, 1.43 us with a session (no measurable cost).
`Register` with a full table: about 200 us (linear scan for the LRU session).

### Known limitations and gaps
- **Rolling upgrade (must-have 4):** a node that does not know the new command
  kinds (register, session txn) rejects them deterministically while an upgraded
  node applies them, so mixed-version clusters would diverge if upgraded nodes
  proposed them. Needs a cluster-version gate before first release; assigned to
  step 9. The v2 snapshot is likewise unreadable by an older node.
- Eviction can be forced by any client that registers in a loop. Quotas and
  multi-tenancy are non-goals for now.
- A session is not closed explicitly; it lives until evicted. A client that wants
  to stop just stops. Registration cost at a full table is O(MaxSessions).
- A `seq` identifies one request; reusing it with different content returns the
  first request's result (the state machine does not compare contents).
- Reads inside a session txn are returned from the cache on retry, so they are
  the result as of the first attempt, which is the intended semantics.

Requirements gate: serves W1 (and W2/W3); must-haves 11 (honest ambiguity), 6
(bounded memory); 4 recorded as a gap above; public API: additive in an internal
package; tests fail on regression; benchmarked as above.

## Step 7b: linearizable reads (`internal/raftnode`): VERIFIED

The full `go test -race ./...` suite passes on Go 1.26 with etcd-io/raft v3.7.0
and protobuf v1.36.11 resolved. This includes `reads_test.go` and the
linearizability chaos coverage. The earlier sandbox note below describes the
environment in which the code was first drafted; dependency resolution was later
completed and the listed verification now passes.

### Design
- `Node.LinearizableRead(timeout, cb)`: raft's **ReadIndex, safe mode**
  (`ReadOnlyOption: raft.ReadOnlySafe`, set explicitly). The leader records its
  commit index and confirms with a heartbeat quorum that it is still leader; the
  requesting node then waits until it has **applied** that index and calls `cb(nil)`
  on its serial context, so the callback may read the state machine directly.
- **No leader leases**: they make correctness depend on bounded clock drift
  (REQUIREMENTS must-have 1 allows either; this is the safer one).
- Any member can serve a read; a follower's request is forwarded to the leader by
  raft itself. `DisableProposalForwarding` affects only `MsgProp`, not
  `MsgReadIndex`, and a duplicated forwarded read is harmless: answers are matched
  to a per-read ID and only the first is used.
- A deposed or partitioned leader cannot gather the quorum, so it never answers:
  the read ends in `ErrReadTimeout`, never in stale data.
- Raft silently **drops** a leader's read requests until it has committed an entry
  in its own term. The node checks this itself and returns `ErrLeaderNotReady`
  immediately, instead of leaving the caller to time out. A node with no known
  leader returns `*NotLeaderError`.
- Errors: reads have no side effects, so every read error is safe to retry
  (`ErrReadTimeout`, `ErrStopped`, `ErrLeaderNotReady`, `ErrNotLeader`). This is
  the counterpart of `ErrResultUnknown` for writes, which is not safe to retry.
- Determinism: reads are released in request-ID order (map order would make
  simulations irreproducible); timeouts use the node's `clock.Clock`; pending
  reads complete with `ErrStopped` on stop or fatal error, in ID order.

### Tests (`reads_test.go`)
Every node sees every acknowledged write; a lagging follower waits to apply; no
leader known is refused; non-positive timeout rejected; leader without quorum
times out exactly once; **deposed leader never serves stale data** (cut off, the
rest elect a new leader and commit, reads through the old one must fail, and
succeed with the new write after healing); pending reads fail on stop; fresh
leader refuses with `ErrLeaderNotReady` and then succeeds; and
`TestChaos_ReadsAreLinearizable` (40 seeds): reads and writes under power cuts,
mid-handler power cuts, partitions, loss and duplication; any read that succeeds
must contain every write acknowledged before the read began. It fails if no read
ever succeeds, so it cannot pass vacuously.

### Verification
- `go test -race ./...` passes, including all `raftnode` tests.
- The earlier API compile risks were resolved by compiling against etcd-io/raft
  v3.7.0 and protobuf v1.36.11.
- Extended soak and manual mutation checks remain useful follow-up work:
  - release a read without `r.index <= n.applied` -> `WaitsForALaggingFollower`
    and the chaos test;
  - use `raft.ReadOnlyLeaseBased` -> `DeposedLeaderNeverServesStaleData` and chaos;
  - delete `committedInOwnTerm` -> `NewLeaderRefusesUntilItCommitsInItsTerm`
    would see a timeout instead of the specific error;
  - accept a `ReadState` for any context (drop `!r.confirmed`) -> double-release
    or wrong index under duplication;
  - skip `releaseReads` after a snapshot restore.
- `TestRead_NewLeaderRefusesUntilItCommitsInItsTerm` accepts either outcome if
  the seed does not observe the short not-ready window; it still guards against
  hangs and wrong errors.

### Known limitations
- No client-visible read API yet beyond the node method: the client layer (7c)
  decides retries and which node to ask.
- Reads wait on the **applied** index; a very slow apply path delays reads, by
  design. A follower read costs one extra round trip to the leader.
- ReadIndex requests are not batched by this layer (raft batches heartbeats
  internally). Unmeasured: no benchmark could be run.

Requirements gate: W1 must-have 1 (linearizable by default; stale reads remain
opt-in and not yet offered); 11 (reads fail with retry-safe errors); public API:
one method on an internal type; regression tests written; benchmarks: unmeasured.

## Step 7c, first slice: session-backed client transactions (`internal/client`)

### Implemented
- A deterministic single-threaded client over a set of reachable Raft peers.
  It registers one session, permits one unresolved transaction at a time, and
  assigns monotonically increasing request sequence numbers.
- An ambiguous proposal result is exposed as `client.ErrUnknownResult`. The
  encoded command and sequence remain pending; `Retry` submits the identical
  bytes, allowing the state machine to return the cached result without applying
  the transaction twice.
- `NewWithOptions` can retry ambiguous transaction attempts with an injected
  serial `clock.Clock`, a positive backoff, and a bounded `MaxAttempts` (at most
  16). It separately configures `ReadMaxAttempts` and `ReadRetryBackoff` for
  failed ReadIndex barriers. Both backoffs double by attempt and cap at one
  second. `New` defaults both policies to one attempt.
- Retries are enabled only after the prior attempt reports an ambiguous outcome,
  and proposals never overlap. Exhaustion calls back once with
  `ErrUnknownResult`, keeps the request pending, and permits an explicit `Retry`
  with a new callback. A successful retry resolves to the original cached
  result.
- `Close` cancels a scheduled retry and reports an unresolved transaction as
  unknown; unresolved session registration is likewise reported as ambiguous,
  pending ranges complete with `ErrClosed`, and late completions are ignored.
- Session expiry or an uncached result is reported as unknown and invalidates
  the local session, requiring a new registration before another transaction.
- `Client.Range` binds each `raftnode.Node` to its own local `kv.Store` with
  `WithStore`, runs `LinearizableRead`, and reads only after the ReadIndex barrier
  succeeds. Explicit historical revisions use the store's existing revision and
  compaction semantics. Synchronous routing/readiness failures try another peer.
- A deterministic test simulates a command applying before the reply times out,
  then retries and verifies the original result is returned and the write exists
  only at its first revision.
- Range tests verify the callback does not run before the read barrier and that a
  peer that cannot start the read is skipped in favor of another peer.
- A 3-node simulated-cluster integration test exercises the actual `raftnode`
  implementation: elects a leader, registers a client session, commits a
  transaction, and reads it through the client's ReadIndex/Range path. The
  returned range revision must include the committed transaction.
- Clock driven tests verify exact write and read backoff boundaries, deduplicated
  write retry, write exhaustion and explicit recovery, synchronous retry-start
  failure, read retry completion and exhaustion, duplicate callback suppression,
  and timer cancellation on close.

### Verification and remaining scope
- `go test -race ./internal/client` passes.
- `go test -race ./...` passes, including the client and Raft packages.
- `go vet ./...` passes.
- The integration test uses the deterministic simulated network, process clocks,
  in-memory durable filesystems and three real `raftnode.Node` instances.
- `RegisterNodeRPC` and `RemotePeer` carry proposals across an RPC endpoint.
  Leader redirects are followed across configured peers; async replies return
  committed results or explicit ambiguous outcomes. This covers network
  session, transaction and lease proposals over TCP-capable RPC.
- The original `Client.Range` and `Client.Watch` still require in-process
  access; the proposal RPC section below documents their network equivalents.
  A public embedding API remains open.
  Registration itself is not automatically
  retried because an ambiguous registration can create an orphan session;
  callers can explicitly reopen and
  rely on bounded session eviction.

## Step 8a: resumable watch event pages (`internal/kv`)

### Implemented
- `Store.Watch` returns put/delete events in `(revision, sub-revision)` order,
  including previous live values when retained. The cursor can resume within a
  transaction when a page limit splits its events.
- Event history is derived from the MVCC versions already kept by the store, so
  snapshots preserve watch history without a second log. Compaction prunes the
  same old versions and a cursor at or below the compact revision returns
  `ErrCompacted`.
- Pages default to 256 events and are capped at 4,096. A heap merges the per-key
  version streams in revision order and keeps response memory bounded by the
  page size plus one heap item per key.
- `Store.Subscribe` delivers retained pages immediately and then notifies on
  later writes. Subscribers are serial, cancellable, omitted from snapshots,
  and capped at 10,000 per store; no per-subscriber event queue is retained.
- Tests cover key-range filtering, put/update/delete ordering, PrevKV, cursor
  continuation inside one revision, compaction, snapshot/restore and invalid or
  future cursors. Subscription tests cover backlog delivery across pages, apply
  notifications, cancellation, stable Store identity across restore, and the
  active watcher bound.

### Scope and verification
- `Client.Watch` now establishes a ReadIndex barrier and subscribes on the same
  replica, delivers retained pages and future events, supports cancellation,
  and retries failed barriers with the configured bounded read retry policy.
- Watch handles are capped by the store's watcher limit. Closing the client
  completes active and pending watches with `ErrClosed`; late ReadIndex
  completions are ignored.
- `Client.Watch` remains an in-process callback API; its RPC equivalent is
  documented below. Lease lifecycle support is documented below.

### Lease state-machine groundwork
- Added `PutWithLease`/`OpPutLease`; each resulting MVCC version records its
  lease ID, and a regular put clears any prior attachment. The state machine
  rejects a put whose lease is not active.
- Store snapshots now use format version 3 and retain lease IDs. Version 1
  snapshots still restore with no lease attachment. State-machine snapshot
  version 4 includes a sorted active lease table; version 2 and bare store
  snapshots remain readable, and invalid or dangling lease references reject
  restore without mutating live state.
- Lease IDs are the grant command's Raft log index. Grant and keepalive return
  the lease state; revoke deletes attached keys in key order. Each committed
  `LeaseTick` decrements all TTL counters and expires keys at zero, making the
  state transition replica deterministic without local wall-clock reads.
- `Client` now exposes `GrantLease`, `KeepAlive`, `RevokeLease` and
  `TickLeases`; unresolved operations and close surface ambiguous outcomes as
  `ErrUnknownResult`. A three-node `raftnode.Node` integration test verifies a
  grant and attached write, then verifies leader-scheduled expiry on every
  replica.
- `raftnode.Config.LeaseTickInterval` enables a node-owned timer. It proposes
  ticks only while this node is leader and its state machine reports active
  leases; one tick proposal may be pending at a time. The timer stops on node
  shutdown. TTL remains expressed in committed ticks, so interval configuration
  determines the wall-clock granularity.
- Lease lifecycle commands can be sent by a remote `Client` through the generic
  proposal RPC; no dedicated lease RPC method exists. Configure a positive
  `LeaseTickInterval` on every lease-aware Raft node to enable TTL. A public
  external-client constructor and listener are still open.
- Tests cover grant, keepalive, TTL bounds, expiration, revoke, unknown leases,
  snapshot recovery of remaining TTL, legacy snapshots, and command fuzz seeds.
- `go test -race ./...` and `go vet ./...` pass with lease lifecycle commands.

### Network proposal RPC (`internal/client`)
- `RegisterNodeRPC` registers an async proposal handler over `rpc.Endpoint`.
  Responses distinguish committed results, leader redirects, definite
  rejections and ambiguous results. `RemotePeer` adapts the endpoint to Client
  and maps RPC timeouts to `ErrUnknownResult`.
- `RegisterRangeRPC` serves a range only after the node's ReadIndex barrier
  completes, then reads the matching local store on the same serial context.
  Remote peers use this RPC path through `Client.Range`; compacted and future
  revision errors remain identifiable, and read timeouts remain retryable.
- `RegisterWatchRPC` serves cursor-based long polls. Each poll establishes a
  ReadIndex barrier before subscribing on that node, returns one bounded page
  or heartbeat, and is resumed by the client from the returned cursor. Cancel
  notifications remove the active store subscription; per-node active polls are
  bounded by `kv.MaxWatchers` and heartbeat timers clean up idle polls.
- Client proposal routing handles synchronous and asynchronous leader redirects
  and tries unattempted peers in stable order. Session and transaction
  deduplication stays in the KV state machine.
- Simulated RPC tests verify follower redirect, session registration, committed
  transactions and that remote Range waits for its ReadIndex barrier before
  returning replica data. A remote watch test verifies barrier ordering, change
  delivery and cancellation. A remote-client test also exercises lease grant,
  keepalive, an attached transaction and revoke through the generic proposal
  method. Separate-process deployment was verified later in Step 19, second
  slice.

### TCP transport (`internal/transport`)
- Added length-prefixed TCP framing, one bounded outbound queue/writer per peer,
  capped connections and inbound dispatch work, connection reuse and reconnect
  after write failure or observed peer disconnect. Asynchronous socket errors
  go to the concurrency-safe `OnError` hook. `Send` reports enqueue only; Raft
  retransmits messages.
- A required `Dispatch` callback moves inbound work onto the receiver's serial
  event loop. TLS mode requires verified client certificates and maps each
  certificate to a configured peer; plaintext requires explicit `InsecureLocal`.
- Loopback tests exercise RPC request/reply over TCP and mutual TLS, plus
  unknown peers, frame limits and closed-transport behavior.
- A three-node `raftnode.Node` test over loopback TCP elects a leader, commits a
  session transaction and performs a linearizable read. The nodes share one
  process and serial test scheduler. A separate-process client deployment is
  covered later in Step 19, second slice.
- `go test -race ./...` and `go vet ./...` pass.
- `go test -race ./internal/kv` and the full `go test -race ./...` pass.
- This step serves W1 watch/change-feed resume semantics; response limits bound
  per-call allocation. Benchmarks and a production subscriber queue remain
  unmeasured/unimplemented.

## Revised roadmap (W1-first)

5. DONE: Raft node runtime on `etcd-io/raft` `RawNode` (see Step 5).
6. DONE: MVCC KV state machine (see Step 6).
7a. DONE: client sessions and request dedup in `kv` (see Step 7a).
7b. VERIFIED: linearizable reads (ReadIndex) in `raftnode` (see Step 7b).
7c. DONE for in-process use: session registration, retry-safe transactions and
    linearizable Range with bounded retries (`internal/client` and the embedded
    API). Proposal, Range and Watch RPC protocol implementations are present in
    `internal/client`; the public TCP RemoteClient is in Step 19.
8. DONE for embedded use: store-level resumable pages, client Watch, lease
   lifecycle commands/API and configurable Raft tick scheduling (8a). Remote
   watch and lease command paths are exposed through the public RemoteClient.
### Step 9, first slice: replicated membership changes (`internal/raftnode`)

- `Config.InitialVoters` separates the initial voter set from the configured
  transport identities. A joining process can start from a synthetic snapshot
  with the current voters and itself as a learner (`BootstrapLearner`), so it
  cannot campaign before admission.
- `AddLearner`, `PromoteLearner` and `RemoveMember` propose one Raft
  `ConfChangeV2` at a time. The leader callback completes on application;
  timeout reports `ErrResultUnknown`, and another change is refused until the
  unresolved entry is applied or overwritten.
- `Membership` returns a defensive copy of the applied `ConfState`. Membership
  state is included in the existing durable snapshot metadata and recovered
  after restart.
- The replacement test starts a fourth process as a learner, waits for catch-up,
  promotes it, removes an old voter, and restarts a replica from a snapshot.
  A separate test verifies replicated removal, single-change serialization,
  protection against removing the last voter, and an isolated leader's
  ambiguous timeout with exactly-once callback behavior after healing.
- Provision the transport identity map on all existing nodes before admission;
  bootstrap the joining node with the current voter IDs. Replace a voter by
  adding/catching up/promoting a learner before removing the old voter. Stop a
  removed process operationally. The membership RPC follows leader redirects
  and returns only after apply; an RPC timeout remains ambiguous. It does not
  provision transport identities or act as a membership controller. Joint
  multi-change reconfiguration and rolling upgrades remain open.

- `RegisterMembershipRPC` and `MembershipClient` expose learner addition,
  promotion and member removal over `rpc.Endpoint`. Membership operations on
  one client are serialized, and the simulated test covers redirect, completion
  after the leader applies the change, busy rejection and an ambiguous result.
- The public embedded `Node` now exposes context-aware `Membership`,
  `AddLearner`, `PromoteLearner` and `RemoveMember`. Membership slices are
  defensive copies; state is the node's locally applied view. A follower returns
  `NotLeaderError`, and a context expiry after acceptance returns
  `ErrResultUnknown`. Transport identities still must be provisioned on every
  current member before admission.
- The public cluster `Client` now routes membership changes to its known ready
  leader, retries synchronous follower redirects against the supplied nodes,
  serializes admin changes per client, and reads membership after a leader
  ReadIndex barrier. The client topology is still a caller-supplied static set;
  after adding a node, recreate the client with the expanded node list.
- A public four-member integration test adds a preconfigured learner, waits
  until its applied index catches the leader's commit, promotes it, removes the
  replaced voter, and checks the resulting voter set.

Verification: `go test -race ./internal/raftnode` and `go vet ./...` pass.

9. IN PROGRESS: automated membership orchestration, joint multi-change
   reconfiguration and rolling upgrade. Metrics, embed API and public TCP
   RemoteClient are implemented; separate-process client deployment was
   verified later in Step 19, second slice.
   Network Range/Watch and proposal RPC are implemented; gRPC remains open.
   TCP node transport has mutual TLS support.

### Step 10, first slice: consistent portable backups (`snapshot`, `raftnode`)

- `Node.Backup` waits for a ReadIndex barrier, snapshots the state machine at
  the applied index, and includes the matching Raft term and ConfState.
- `snapshot.Marshal`/`Unmarshal` encode a versioned, CRC32C-checked portable
  archive. Decode rejects malformed headers, lengths, checksums, truncation and
  trailing bytes before exposing snapshot data.
- `raftnode.RestoreBackup` accepts only a verified archive and an empty node
  data directory, then atomically installs an internal snapshot marked as
  installed. Open recovers the state and membership from it; the restored member
  can catch up through normal Raft replication.
- A simulated test backs up a live leader, restores a follower onto a fresh
  filesystem, restarts it, and verifies the replica rejoins with the same
  applied state. Corrupt archives and repeated restore into a non-empty target
  are rejected.
- `Backup` returns archive bytes for the caller to persist to external storage;
  this layer does not select or upload to a cloud provider. Large backups are
  currently materialized in memory. Benchmarking and streaming export remain.

Verification: archive integrity and recovery tests pass under `-race`; the full
repository race suite and `go vet ./...` pass.

### Step 11, first slice: node observability (`raftnode`)

- `Config.Observer` receives low-cardinality lifecycle events for accepted,
  applied and ambiguous proposals, ReadIndex completion, snapshots, backups,
  membership changes, transport send errors and fatal node failures. Events
  include node/term/index, duration, error and request ID where applicable, so
  adapters can build metrics, structured logs and correlated trace spans.
- `Node.Health` reports leader, term, commit/applied indexes, last snapshot,
  process health and readiness. A follower is ready only when it knows a leader
  and has applied its commit; a leader also needs a committed entry in its term.
- Graceful stop now reports `ErrResultUnknown` for accepted but unresolved
  writes and membership changes. Reads still return `ErrStopped`, since they
  have no side effects. This preserves the honest-outcome contract.
- Deterministic tests check pre-election readiness, post-election health,
  proposal event correlation, read/snapshot observations and ambiguous stop.
- `internal/metrics.Exporter` implements the Prometheus text exposition format
  for lifecycle counters, duration totals and polled node health/readiness
  gauges. It is concurrency-safe, exposes no request IDs or raw error strings
  as labels, and maps unknown event names to a bounded `other` label. Register
  it on the host's HTTP mux and call `SetHealth` from each node's serial context.

Verification: `go test -race ./internal/raftnode`, `go vet ./...` and the full
repository race suite pass. Runtime alert policy and external metrics backend
configuration remain integration work for the embed/runtime layer.

## Step 12, first slice: public embedded node runtime (`continuum`)

- Added the module-root `continuum.Open(Config)` API. It owns a bounded serial
  event loop, production clock, RPC endpoint and `raftnode.Node`, and exposes
  context-aware `Propose`, `ReadIndex`, `Health`, `Addr` and idempotent `Close`
  methods. State-machine calls and Raft callbacks stay on the same serialized
  loop. TCP is available across processes; a shared `InProcessNetwork` routes
  messages between nodes in one Go process.
- `Config` wires bootstrap/learner membership, snapshot policy, timeouts, TLS,
  transport limits, observers and fatal/transport error hooks. Caller-owned
  command buffers are copied before asynchronous processing. Proposal timeouts
  after acceptance return `ErrResultUnknown`.
- The clock dispatches real timer callbacks onto the event loop. The loop has a
  bounded queue and applies backpressure; shutdown drains already accepted
  tasks before it returns.
- Integration tests exercise single-node lifecycle and observer events, plus a
  three-node in-process election, proposal replication and linearizable read
  barriers. A focused transport loop test checks bounded backpressure and
  shutdown draining.
- `NewKVStateMachine` enables the built-in MVCC state machine through the public
  API. Nodes expose context-aware compare-and-set `Txn`, linearizable `Range`,
  resumable `Watch`, retry of an unknown transaction result, and lease grant,
  keep-alive and revoke methods. Caller input is copied before queued work;
  watch callbacks are serialized on the owning node loop.
- KV transactions use a session opened lazily on first write. Ambiguous
  transaction outcomes preserve the session sequence and can be retried with
  `RetryTxn`. If only session registration is ambiguous, the wrapper retries a
  bounded number of registrations and reports that the user transaction was
  never submitted.
- `NewClient(nodes, ClientOptions)` routes proposals, linearizable ranges,
  watches and leases across multiple embedded nodes. The client owns a separate
  serial loop; node adapters marshal proposals and reads onto the owning Raft
  loop, including one-shot watch pages and cancellation. Retry policy is
  bounded, and stopped replicas are skipped during proposal routing.
- `WatchHandle.Ready` reports the first completed linearizable watch page. The
  heartbeat timer starts only after ReadIndex succeeds and the subscription is
  installed, so readiness cannot be inferred from an early timer alone.

Public KV integration tests perform compare-and-put, linearizable range, watch
delivery, lease attachment and key removal after revoke. A three-node cluster
test routes across the replicas and continues after one follower is stopped.

Verification: focused runtime tests and `go test -race ./...` pass; `go vet
./...` passes. The sandbox cannot bind loopback sockets, so the focused and full
race runs used approved socket access.

Scope still open: application persistence coordination, compaction retention
sizing, automated rolling restart orchestration, further cancellation and
configuration coverage, and performance measurements. The in-process transport
is same-process only; use TCP when replicas run in separate service processes.

### Step 13, first slice: client session checkpoints

- Added a versioned, CRC32C-checked opaque checkpoint containing the session ID,
  last completed sequence and exact unresolved session command. Restore validates
  framing, checksum and the command's session/sequence before changing client
  state. A pending command is restored as unknown and is sent only by an explicit
  retry.
- `Node` and cluster `Client` expose context-aware `ExportSessionState` and
  `RestoreSessionState`. This lets a caller persist an unresolved request and,
  after recreating the client, resolve it with `RetryTxn` without changing the
  command bytes. An empty pending field checkpoints an idle session.
- Tests cover recovery after the command committed but its reply was lost,
  byte-for-byte retry, and corruption rejection. The full race suite passes.
- This is a caller-managed checkpoint, not an atomic transaction between the
  application database and Raft. Applications must save the checkpoint before
  acknowledging their own operation; a crash before saving a newly pending
  command still requires application-level reconciliation. Sessions can also
  expire through bounded state-machine eviction, in which case restore cannot
  recover an uncached result.

Verification: `go test -race ./...` passes with loopback socket access.

Scope still open: compaction retention sizing and automated rolling restart
orchestration, further cancellation/configuration coverage, benchmarks, an application
persistence hook for atomically coordinating request intent with session
checkpoints, machine power-loss and storage-fault failover, multi-process rolling
upgrade verification, and joint multi-change consensus.
Separate-process client deployment and remote membership administration were
verified in Step 19.

### Step 14, first slice: public MVCC compaction control

- Exposed `Compact(ctx, revision)` through both the embedded `Node` and cluster
  `Client` APIs. Callers should select a revision returned by a linearizable
  `Range`; reads and watches before the compacted point retain the existing
  `ErrCompacted` behavior.
- Repeating a positive compaction target is now an idempotent success in the
  replicated state machine. This lets a caller safely resubmit after an unknown
  proposal result without mistaking a previously committed compaction for a
  failure. The lower-level `Store.Compact` still reports stale targets directly.
- Public integration coverage compacts through the latest observed data
  revision, repeats the target, and verifies an older historical Range is
  rejected. The randomized store model now includes idempotent command retries.

Verification: `go test -race ./...` and `go vet ./...` pass. Loopback access is
needed for the repository's TCP integration tests.

### Step 15, first slice: durable single-node restart example

- Added `examples/restart`, a runnable write/read pair that keeps the same node
  ID and data directory across invocations. The write process waits for leader
  readiness and acknowledges a KV transaction; a later process opens without
  bootstrapping, recovers the log/state, and verifies the value.
- The README states that the example covers clean single-node restart only and
  does not demonstrate multi-node failover, membership changes, backup restore,
  or disk-corruption recovery.
- The example test closes and reopens against the same on-disk directory. The
  exact two-command workflow was also run as separate `go run` processes and
  recovered the acknowledged value at revision 4.

Verification: `go test -race ./examples/restart` passes; separate-process write
and read commands both succeeded.

### Step 16, first slice: opt-in automatic MVCC compaction

- Added paired `KVCompactionInterval` and `KVCompactionRetain` settings to the
  public and Raft configs. Zero/zero leaves behavior unchanged; otherwise both
  must be set, the interval must be at least the Raft tick interval, and the
  state machine must provide deterministic compaction commands.
- At each interval, only the current leader asks the built-in KV state machine
  whether the data-revision window was exceeded. If so, it proposes a normal
  replicated compact command; every replica drops history only when applying
  that committed command. At most one scheduled compaction proposal is pending
  per node. Configure the same interval and retained revision count on every
  member so leadership changes do not silently change policy.
- Historical Range and Watch resume guarantees follow the compact point: Range
  at that revision remains readable, older Range revisions fail, and watch
  cursors at or below it fail with `ErrCompacted`. This bounds revision history
  by write activity only when operators enable and size the policy; the default
  remains opt-in to avoid surprising data-retention changes.
- A public integration test writes several revisions and verifies that the
  configured leader policy eventually makes the oldest revision unavailable.

Verification: the automatic-compaction integration test passes under `-race`.
The policy's interval/retention tradeoff has not been benchmarked; choosing
values remains an operator decision.

### Step 17, first slice: three-member full-restart example

- Added `examples/cluster-restart` with `write` and `read` commands. Each runs a
  three-voter cluster with stable IDs and separate data directories, using the
  public cluster client for an acknowledged write and a linearizable recovery
  read. The read phase opens without bootstrapping, so it exercises disk
  recovery and a fresh leader election across all members.
- The README identifies this as a full shutdown/restart demonstration and calls
  out its limits: no rolling restart, membership change, TCP process transport,
  or partial-quorum recovery.
- The race-enabled example test and the exact separate-process command sequence
  both recovered the acknowledged value at revision 4.

Verification: `go test -race ./examples/cluster-restart` passes; separate
`go run` write and read processes both succeeded.

### Step 18, first slice: public membership control

- Exposed locally applied membership state and leader-only add-learner,
  promote-learner and remove-member operations on the embedded `Node`. Returned
  membership slices are copied, follower redirects retain their leader hint,
  and context cancellation after acceptance is reported as an unknown result.
- Added cluster-client membership routing, including serialized operations,
  leader discovery and a ReadIndex-backed membership read.
- The replacement flow is tested through the public API, including catch-up
  before promotion and removal only after the replacement voter is admitted.
- This gives applications the control primitives, but does not automate
  transport provisioning, dynamic client topology rollout, restart sequencing,
  or multi-change joint consensus.

Verification: `go test -race . -run '^TestPublicMembershipReplacementFlow$'`
passes. Full repository verification is run below.

### Step 19, first slice: public TCP RemoteClient

- Added `NewRemoteClient` with a stable transport identity, bounded TCP
  transport, mTLS or explicit local plaintext configuration, and the existing
  bounded retry/session client. It exposes Txn/retry, linearizable Range,
  resumable Watch, leases, compaction and session checkpoint operations.
- KV-enabled `Node` instances automatically register proposal, Range, Watch and
  membership handlers on their existing endpoint. Remote RPC replies use the
  configured client listener address; server configs must include that client
  identity in `Peers`, and bootstrapped clusters must set `InitialVoters`
  explicitly so transport clients never become voters by default.
- Added `rpc.Endpoint.Close` to cancel pending requests in deterministic order
  and integrated endpoint/transport cleanup into client shutdown. Fixed
  `WatchHandle.Ready` to report readiness after the first remote barrier/page.
- A public TCP integration test exercises remote Txn, linearizable Range, Watch,
  lease grant/attachment/revoke and shutdown. Existing transport tests cover
  mTLS authentication. Multi-process client deployment and dynamic client
  topology updates remain operational follow-up.

Verification: the public TCP integration test and endpoint-close race test
pass. Full `go test -race ./...` and `go vet ./...` pass; TCP tests used
approved loopback access.

### Step 19, second slice: separate-process client example

- Added `examples/remote-client`, with independent `serve`, `put` and `read`
  commands. This exercises the public RemoteClient and node TCP transports
  across OS process boundaries using a stable client identity and explicit
  voter configuration.
- Documented its local-only plaintext setup and the operational gaps it does
  not cover. The example is a single-server deployment and does not establish
  multi-process Raft failover or production TLS provisioning.
- The first process-level run exposed a stale outbound TCP connection after a
  client process exited: the server completed ReadIndex but the next process
  did not receive its Range response. The TCP reader now signals its per-peer
  writer to discard that connection when the inbound peer closes.
- Added a transport regression test that closes and recreates a client under
  the same stable identity, then verifies the next RPC reply. The test passed
  five consecutive runs. A full separate server, write-client and read-client
  sequence passed; the read recovered the acknowledged value at revision 4.
- `go vet ./...` and `go test -race ./...` pass after the transport change.

### Step 19, third slice: public remote membership administration

- Exposed `Membership`, `AddLearner`, `PromoteLearner` and `RemoveMember` on
  `RemoteClient`. Membership reads wait for a remote ReadIndex barrier and
  return copied configuration slices. Changes follow leader redirects and are
  serialized by the remote membership client.
- Context expiry after a membership RPC may have been sent is surfaced as
  `ErrResultUnknown`. The client topology remains static and transport identity
  provisioning remains an operator responsibility.
- Extended the public TCP integration scenario to read membership, add a
  preconfigured learner, verify its learner state, remove it, and verify the
  result. A simulated RPC test covers state-read leader redirects. Full
  `go test -race ./...` and `go vet ./...` pass.

### Step 20, first slice: independent Raft processes with quorum restart

- Added `examples/cluster-process`: three independently running TCP Raft
  members, a separate RemoteClient process, stable transport identities and
  explicit initial voters. The README documents bootstrapping, a one-member
  clean stop while two voters retain quorum, and restarting that member from
  its original data directory.
- Added a subprocess integration test with dynamically allocated loopback
  addresses. It writes through a separate client process, stops one member,
  reads from the remaining quorum, acknowledges another write with only two
  voters available, restarts the member without bootstrapping, then reads the
  newer value through that member alone.
- The initial clean-shutdown subprocess scenario passed twice consecutively.
  It verifies clean member
  shutdown, write availability and member recovery while quorum remains;
  machine power-loss behavior, simultaneous quorum loss, rolling upgrades and
  member replacement remain
  open. The full `go test -race ./...` and `go vet ./...` pass.

### Step 20, second slice: abrupt member process recovery

- Extended the same subprocess scenario to force-terminate a member process
  after committed writes. The remaining two voters read existing data and
  acknowledge a new write; after the killed member restarts from its original
  directory, a client contacting only that member reads the new value.
- This verifies process-crash recovery with the host OS still running. It does
  not establish power-loss durability, storage-device behavior, or recovery
  after losing quorum simultaneously.

Verification: the extended subprocess scenario and `go test -race ./...` pass;
`go vet ./...` passes.

## Building against etcd-io/raft in an offline sandbox
The development sandbox cannot reach go.etcd.io, golang.org or proxy.golang.org.
Verification there uses `-modfile` with `replace` directives pointing at local
clones of etcd-io/raft v3.7.0 and protocolbuffers/protobuf-go v1.36.11, plus stub
modules for test-only dependencies. None of that is in the shipped `go.mod`; on a
normal network `go mod tidy` is all that is needed. A 3-node `RawNode` election
was run end to end with this setup, and the existing suite passes on Go 1.26.
