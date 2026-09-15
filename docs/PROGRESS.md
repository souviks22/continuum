# continuum — build progress

## Phase 0 — Foundations & test harness

- [x] **Step 0.1 — Deterministic simulator core.** `VirtualClock`,
      `EventScheduler` (seeded, deterministic tie-breaking, infinite-loop
      guard), `SimulatedNetwork` (partitions incl. asymmetric, per-link
      delay/loss/duplication, delay-driven reordering). 25 tests passing.
- [x] **Step 0.2 — Node crash/restart primitives in the simulator.**
      Crash/restart modelled as a network-delivery fact: sender liveness
      checked at send time, receiver liveness checked at delivery time
      (so in-flight messages to a soon-to-crash node are correctly lost,
      and messages sent to a down node are correctly delivered once it
      restarts). `on_restart` hook fires before the node is marked up,
      for future recovery logic (WAL replay etc.) to run first. 9 new
      tests, 34 total passing.
- [x] **Step 0.3 — RPC scaffolding.** `Message` envelope (JSON,
      request/reply correlation by id). `Transport` protocol +
      `SimulatedTransport` (one instance per node, wraps
      `SimulatedNetwork`) as the seam that lets node logic run unchanged
      against the simulator now and a real gRPC transport later.
      `RpcEndpoint` layers request/reply semantics on top: registered
      method handlers, per-call timeouts as scheduled events, late
      replies after a timeout silently ignored (never double-invoke the
      caller). Verified against crash/partition scenarios from Step 0.2.
      9 new tests, 43 total passing.
- [x] **Step 0.4 — Real-cluster chaos harness (skeleton).** `ChaosHarness`
      protocol mirroring `SimulatedNetwork`'s API (partition/heal/
      set_link, node crash/restart) so a Phase 7 scenario can eventually
      run against either backend. `NullChaosHarness` (records intended
      calls, for wiring/testing before real infra exists) and
      `TcNetnsChaosHarness` (Linux network-namespace + `tc netem`,
      command-construction logic unit-tested via an injected fake
      runner). Explicit known gap, not papered over: real process
      lifecycle (spawn/pid capture/signal) is deferred until Phase 2
      produces an actual node binary to manage. 12 new tests, 55 total
      passing.

**Phase 0 complete.**

## Phase 1 — Single-node durable storage engine
- [x] **Step 1.1 — WAL.** Length-prefixed, CRC32-checksummed record
      format. Explicit durability boundary: `append()` flushes to the OS
      page cache (survives process crash, not power loss); `fsync()` is
      a separate call (survives power loss too), left un-bundled so
      callers can batch several appends behind one fsync (group commit)
      instead of paying the fsync cost per write. Recovery scan on open
      distinguishes two failure modes: a torn write (incomplete
      header/payload from a crash mid-append) is silently truncated,
      since that data was never durably acknowledged; a full-length
      record with a bad checksum (corruption of already-durable data)
      raises `CorruptRecordError` instead of being discarded. 13 new
      tests, 68 total passing.
- [x] **Step 1.2 — State machine apply layer + snapshotting.** `Operation`
      (set/delete) carries its own logical index in the payload itself,
      rather than index being implied by physical position in whatever
      WAL file exists -- this is what keeps recovery correct even if a
      prior compaction was interrupted partway. `SnapshotStore` uses
      write-temp-then-rename for atomicity. `StorageEngine.compact()`
      snapshots state and replaces the WAL; three crash points were
      walked through and tested explicitly, including a direct
      reproduction of "snapshot lands, WAL replacement doesn't" (replay's
      index comparison converges to the correct state regardless). 26
      new tests, 94 total passing.

**Phase 1 complete.**

## Phase 2 — Single Raft group
- [x] **Step 2.1 — Leader election.** `NodeState` (follower/candidate/
      leader), randomized election timeouts driven off the deterministic
      scheduler, `RequestVote` with the log-up-to-date check (log fields
      wired for real in Step 2.2), and `RaftPersistentState`
      (write-temp-then-rename term/vote persistence — a hard synchronous
      dependency of the vote-granting path, since replying before
      persisting could let a node grant two conflicting votes in one
      term after a crash+restart). Minimal empty-`AppendEntries`
      heartbeat included so a leader can stay stable, deliberately with
      zero log-matching logic behind it yet.

      Real bug the test suite caught: single-node clusters never became
      leader, since the majority check only ran inside the per-peer vote
      *reply* handler -- with zero peers, that handler never fires, so
      the self-vote's already-sufficient majority was never checked.
      Fixed by checking majority immediately after casting the self-vote
      as well as on each reply.

- [x] **Step 2.2 — Log replication.** `WriteAheadLog.truncate_to()`
      (Phase 1 extended, well-justified: Raft's log-matching property
      genuinely needs suffix truncation, which a purely append-only WAL
      can't do). `RaftLog` layers entries/matching/conflict-resolution
      on top, idempotent against retried AppendEntries. `RaftNode` now
      does real per-peer replication (nextIndex/matchIndex), commit
      index advancement restricted to current-term entries only (guards
      the classic Raft "Figure 8" safety hazard — a previous-term entry
      is only ever committed indirectly, never by direct majority count),
      the nextIndex backtracking optimization (conflict_index) so a
      lagging follower catches up in one round trip instead of one entry
      at a time, and committed entries are applied via an injected
      `apply_fn` callback. `propose()` is the new client-facing entry
      point. Explicit known gap, not papered over: no InstallSnapshot
      yet, so a follower that's fallen behind whatever the leader has
      already compacted away can't catch up — that's the next
      natural step. 41 new tests (log matching/truncation/persistence,
      commit-and-apply, lagging-follower catch-up after partition heal,
      genuine same-index-different-term conflict resolution, committed
      entries surviving a forced leader change), 141 total passing.

- [x] **Step 2.3 — Snapshotting / InstallSnapshot / StorageEngine wiring.**
      `RaftLog` gained a compaction boundary (`last_included_index/term`,
      persisted via a new shared `AtomicJsonFile` helper — the third
      caller of the write-temp-then-rename pattern, finally factored out
      as flagged back in Step 2.1). `compact()` (prefix discard) is the
      mirror of Step 2.2's `truncate_to()` (suffix discard); both are
      crash-safe the same way — persist the boundary/metadata first,
      then swap the WAL, with recovery filtering by index so a crash
      mid-swap is harmless regardless of whether the file replace
      finished. `RaftNode` now takes a real `StateMachine` +
      `SnapshotStore` (Phase 1), applies committed entries into it
      directly, and auto-snapshots past a configurable
      `snapshot_threshold`. `_replicate_to_peer` falls back to
      `InstallSnapshot` whenever a peer's nextIndex has fallen at or
      before the leader's compaction boundary — closing the gap flagged
      at the end of Step 2.2. This is now an actual replicated KV store,
      not just a replicated log. 22 new tests (log compaction/
      crash-safety, install_snapshot semantics, end-to-end state-machine
      wiring, automatic snapshot triggering, a far-behind follower
      genuinely forced through InstallSnapshot rather than AppendEntries,
      and recovery from an on-disk snapshot after a simulated restart),
      163 total passing.

**Phase 2 complete.**

## Phase 3 — Sharding + placement/metadata layer
- [x] **Step 3.1 — Range partitioning + multi-Raft group lifecycle.**
      `RangePartitioner`: static, always-exhaustive key range routing
      table, `split()` as routing-table bookkeeping (explicitly NOT yet
      a real replicated split across a shard's Raft group — that needs
      to be a committed log entry so every replica splits at the same
      point, deferred honestly rather than faked). `ShardManager`: true
      multi-Raft on a single physical node via address composition
      (`{physical_node_id}:{shard_id}`) — each shard replica gets its
      own network identity, so RequestVote/AppendEntries for different
      shards never collide, with zero changes needed to the Phase 2
      RPC/Raft layers. `start_shard()`/`stop_shard()` are the lifecycle
      primitives. Tests confirm shards elect fully independent leaders,
      keep independent state machines, and that partitioning one shard's
      addresses leaves an unrelated shard's leadership untouched — real
      isolation, not just separate bookkeeping. 18 new tests, 181 total
      passing.
- [x] **Step 3.2 — Placement/metadata layer.** Generalized `RaftNode` to
      depend only on a structural `ReplicatedStateMachine` protocol
      (`apply_command`/`snapshot_state`/`load_state`) instead of being
      hardcoded to the KV `StateMachine` — the key architectural move
      this step turned on. `MetadataStateMachine` (shard ranges +
      replica placement, `create_shard`/`split_shard` commands,
      idempotent against retried splits) satisfies that protocol with a
      completely different command vocabulary than the KV store, and
      `ShardManager.start_shard()` now takes a pluggable
      `state_machine_factory` so the metadata group is started exactly
      like any KV shard. Net result: the metadata Raft group gets every
      bit of Phase 2's election, replication, crash-recovery, and
      snapshotting for free — zero new Raft code, just a new state
      machine sitting on identical plumbing, proven by tests that also
      run a real KV shard and the metadata group side by side on the
      same physical nodes. `bootstrap.py` names the chicken-and-egg
      bootstrap step honestly (find the elected leader, then propose)
      rather than hiding it. 14 new tests, 195 total passing.
- [x] **Step 3.3 — Client-side routing.** New generic `ClientPropose`/
      `ClientQuery` RPCs on `RaftNode` (via a `query_fn` adapter,
      mirroring `apply_command`'s genericity — `kv_query_fn` for KV
      shards, `metadata_query_fn` for the metadata group's `route`/
      `get_shard` queries), plus leader-hint tracking so a non-leader
      can redirect a misdirected client instead of just failing.
      `ContinuumClient` is fully callback-based (matching the codebase's
      event-driven style throughout — nothing here blocks a thread):
      queries the metadata group to route a key to its shard, composes
      real network addresses from metadata's physical-placement replica
      list, caches shard-to-believed-leader (not key-to-range — routing
      is re-queried every call, a deliberate scope limit), and follows
      `leader_hint` redirects or falls through the replica list on
      failure/timeout. Bug the tests caught immediately: the client was
      dialing metadata's raw physical node ids instead of composing
      `{physical_id}:{shard_id}` addresses — fixed by composing
      addresses client-side from metadata's (correctly physical-only)
      replica list. Explicitly NOT linearizable reads yet (ClientQuery
      trusts whichever replica currently believes it's leader, no
      lease/read-index check) — a deliberately separate concern, noted
      for a later step. 6 new tests, 201 total passing.

**Phase 3 complete.**

## Phase 4 — MVCC storage upgrade
- [x] `MVCCStore`: multi-version KV keyed by
      (key, timestamp) — parallel sorted timestamp/value arrays per key,
      bisect-based snapshot reads (newest version <= read_ts), strictly-
      increasing-timestamp enforcement per key, tombstone deletes,
      mechanical `gc(safe_point)` (policy for choosing a safe safe_point
      deferred to Phase 6, once transaction tracking exists). Timestamps
      are always caller-supplied, never invented by the store — a
      replicated state machine can't safely pick its own wall-clock
      timestamp during apply, since replicas would disagree.
      `MVCCStateMachine` + `mvcc_query_fn` adapt this to RaftNode's
      generic `ReplicatedStateMachine` protocol exactly like the KV and
      metadata state machines do — proven end-to-end by wiring an MVCC
      shard through `ShardManager` with zero new replication code:
      writes replicate with correct version history to every replica,
      snapshot reads at different timestamps return correct point-in-
      time values, deletes replicate as real tombstones, and
      take_snapshot()/compaction work unchanged. This is the substrate
      Phase 6's Percolator-style transactions will sit on. 30 new tests,
      231 total passing.

## Phase 5 — Timestamp Oracle
- [x] Added general-purpose
      `RaftNode.wait_for_commit(index, callback)` (fired from all three
      commit-advancement sites: leader majority-match, follower
      AppendEntries, follower InstallSnapshot) — needed because the TSO
      can't safely hand out a timestamp from a newly-allocated batch
      until that batch write is durable across a majority, not merely
      proposed locally. Bug caught while wiring this in and fixed:
      single-node (zero-peer) clusters never advanced commit_index on
      `propose()` at all — majority was only ever checked inside the
      AppendEntries-reply handler, which a zero-peer cluster never
      triggers; same bug shape as Step 2.1's election fix, now fixed the
      same way (check immediately after the self-append too).
      `TSOStateMachine`: durable high-water-mark, `ReplicatedStateMachine`
      protocol again, zero new Raft code, same "just another Raft group"
      pattern as the metadata layer. `TimestampOracle`: leader-side
      batch allocation (reserve a batch with one Raft write, serve
      individual requests from memory until exhausted) — batching is
      the point: one Raft round-trip per *batch*, not per timestamp,
      or the TSO would cap the whole cluster's throughput. Tracks which
      term it last resynced batch state for, so regaining leadership
      later re-derives its local counter from the durable high-water
      mark instead of trusting stale local state. Centerpiece test
      proves the actual safety property: after a leader crash with most
      of its 1000-timestamp batch unused, the new leader's first
      timestamp is not just higher than anything handed out, but higher
      than the *entire* unused portion of the old batch — nothing ever
      gets reused. 15 new tests (5 regression/commit-waiter tests on
      RaftNode, 5 TSOStateMachine unit tests, 5 oracle integration
      tests), 246 total passing.

## Phase 6 — Percolator-style 2PC transaction layer
- [x] **Step 6.1 — Prewrite/commit protocol + primary/secondary lock
      records.** `PercolatorStore`: three column families on top of
      Phase 4's MVCC store — `data` (key@start_ts, staged at prewrite,
      before anything is visible), `writes` (key@commit_ts -> pointer
      to start_ts, what makes a value visible to readers), `locks`
      (one outstanding lock per key). `prewrite` detects both lock
      conflicts and write-write conflicts (a newer commit than this
      txn's start_ts); `commit` requires the caller's exact matching
      lock; `rollback` is idempotent. `PercolatorStateMachine` adapts
      this to the generic `ReplicatedStateMachine` protocol yet again —
      zero new replication code — plus per-index result tracking
      (`get_result`) since prewrite/commit can fail, unlike plain KV
      writes, and the proposing coordinator needs to learn the outcome
      after `wait_for_commit` fires (a new public `RaftNode.state_machine`
      accessor was added for this). `Transaction` coordinator: full
      prewrite-all → commit-primary → commit-secondaries-best-effort
      flow, working across genuinely different shards (cross-shard 2PC,
      the actual point of this step), built directly on
      propose()/wait_for_commit() like the TSO rather than the
      still-gapped RPC ClientPropose path. Real bug the tests caught:
      the coordinator was waiting for *all* secondary commits before
      reporting success, contradicting Percolator's actual atomicity
      point (the primary's commit alone) — a partitioned secondary
      would have hung the whole result indefinitely; fixed to report
      success immediately after the primary commits, with secondaries
      firing off as genuinely fire-and-forget. Explicitly deferred, not
      papered over: lock-cleanup for a crashed coordinator's abandoned
      lock (a blocked read just fails with "locked, retry" for now) and
      GC tied to a transaction-aware safe point — both real complexity
      scoped to Steps 6.2/6.3. 30 new tests (store mechanics, state
      machine result-tracking, and end-to-end coordinator tests
      including single-key, multi-key same-shard atomicity, cross-shard
      commit and pre-commit invisibility, write-write conflict abort+
      rollback, and primary-commits-while-secondary-partitioned), 276
      total passing.
- [x] **Step 6.2 — Lock cleanup / stuck-transaction resolution.** New
      store-level primitives (`get_with_lock`, `get_lock`,
      `find_commit_ts`) added alongside the existing `get()` rather than
      changing it, to avoid touching already-tested Step 6.1 code.
      `LockResolver`: on a blocking read, checks the primary key's
      status (necessarily cross-shard — the primary can live on a
      different shard than the key being read) and rolls the lock
      forward (primary committed — commit the secondary with the same
      commit_ts) or back (primary shows no trace, or is itself stale —
      release the lock, resolves to legitimately absent). TTL is judged
      by elapsed TSO timestamps rather than wall-clock time — explicitly
      named as a proxy, not the same thing, since a replicated state
      machine can't safely read its own clock during apply (same
      determinism principle as Phase 4's MVCC timestamps). Wired into
      `Transaction` as an optional constructor param — without one,
      behavior is unchanged from Step 6.1 (plain "locked, retry"
      failure). 11 new tests (7 store-primitive unit tests, 4 resolver
      integration tests: roll-forward from a committed primary,
      roll-back from an abandoned transaction, TTL refusing to touch a
      fresh lock, and the no-resolver-configured baseline), 287 total
      passing.

- [x] **Step 6.3 — GC tied to a safe point.** `SafePointCalculator`:
      combines two independent bounds via `min()` (the more conservative
      one wins) — precise active-transaction tracking (in-memory only,
      NOT Raft-replicated; losing it on a crash is the *safe* failure
      direction, just more conservative GC) and a TTL floor as the real
      safety net if that tracking is ever wrong or incomplete (same
      "assume a straggler beyond the TTL is abandoned" principle as Step
      6.2's lock TTL — named as a deliberate production-style tradeoff,
      not a compromise). `PercolatorStore.gc()` applies the same
      safe_point to both `data` and `writes` independently — correct
      (never discards anything still reachable) but not maximally tight,
      an explicitly named simplification vs. a fully precise
      write-record-reachability GC. GC itself goes through the
      replicated log as a `"gc"` command rather than running locally per
      replica, even though it's deterministic given identical state —
      keeps it correctly ordered after every write below the safe point
      on every replica, rather than risking a lagging follower GC-ing
      before it's actually replicated everything the safe point assumes
      it has. `Transaction` gained an optional `safe_point_tracker`
      (register on start_ts, deregister via `close()` — auto-called on
      every commit() path, but a read-only transaction must call it
      explicitly; named honestly as exactly the kind of gap a
      `with transaction() as txn:` pattern exists to prevent, which
      this doesn't yet have). Test data bug caught while writing this:
      my own test scenarios initially encoded backwards assumptions
      about which of the two bounds should win — fixed by working
      through the actual min() semantics rather than adjusting the code
      to match a wrong intuition. 11 new tests (6 safe-point calculator
      unit tests, 3 store-level GC tests, 2 end-to-end integration
      tests: GC replicating identically across all replicas, and a
      long-running transaction's snapshot surviving GC while open and
      becoming collectible only after it closes), 298 total passing.

**Phase 6 complete.**
