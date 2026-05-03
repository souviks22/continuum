# Continuum: Distributed Key-Value Store

Continuum is a durable storage engine especially crafted for distributed system workloads.

The simplest key-value store is an in-memory hashmap, but it struggles in a distributed system. Continuum offers a seamless storage experience across machines.

## Context

Main pain points of a regular key-value store are exactly why Continuum is so much relevant:

1. Huge number of unique keys
2. Durable writes and consistent reads
3. No data loss on adding/removing machines

## Storage API

The API offers three fundamental operations:

1. PUT(key, value)
2. GET(key)
3. DELETE(key)

The challenge here is not creating and retrieving keys, that is the simple part. The real challenge is preparing the distributed operations in a way that feels like done on a single machine.

## Durability: Write-Ahead Log (WAL)

Once an API request is acknowledged, that data should never be lost. But, there are ample number of situations where it may not fulfill if not handled right. 

Memory is fast but volatile, we need all data on disk for durability. If the system acknowledges a request after putting data in memory alone and saves everything on disk asynchronously, an error can occur in the middle of those two operations and data is lost.

Write-Ahead Log (WAL) is filling the gap. Here, every operation is stored as a record in an append-only log file and only after successful logging on disk, the memory gets the data and sends acknowledgement. On every restart, each WAL record is replayed sequentially in memory to get the latest data.

Disk write on every request could sound like high latency but two things make it fast:

1. Sequential Write: Random disk writes are actually expensive but sequential writes in an append-only log file is much more efficient for its immutable nature. On every request, a unique record is added in the end of the file to reflect the change, which is fast.

2. Batch Processing: Instead of writing on every request, we can batch them and use write them all at once one after another followed by a single file sync. It eliminates invidual sync for requests resulting in reduced latency.

WAL works ahead of memory to store permanent data. Even if writes fail midway, there is no data loss:

- If WAL writes do not make it to the memory due to unexepted error, the fix is just one restart away.

- If WAL contains partial write or corrupted bytes, those are simply ignored at the time of replay. Every byte before and after the corruption, stays.

### WAL Record Format

| Magic | Length | Checksum | LSN | Payload |
| -------- | ------- | ------- | ------- | -------- |
| 4 bytes | 4 bytes | 4 bytes | 8 bytes | Length bytes |
| Starting delimeter of record | Bytelength of payload | Corruption detector | Log Sequence Number | Data in bytes |

> WAL does not care if a corruption occurs at write time, it simply keeps adding bytes sequentially. At recovery time, once a corruption is detected via checksum mismatch, it keeps skipping bytes until a valid record arrives.

## Local Storage: In-Memory Hashmap (MemStore)

WAL guarantees durability, but it is not designed for serving reads efficiently. Replaying the log for every GET would be prohibitively slow. This is where the in-memory storage layer comes in.

MemStore acts as the working state of the KV store, built on top of the WAL. It keeps the latest version of all keys in memory, allowing fast reads while relying on WAL for durability.

WAL solves persistence, but introduces a new problem:

* Data is stored sequentially, not by key
* Reads require scanning the log
* No direct lookup capability

MemStore bridges this gap.


### Write Path

Every write follows a strict order to guarantee correctness:

```text
Client → WAL → MemStore → Acknowledge
```

* No acknowledged write is ever lost
* Memory and disk stay consistent
* Recovery is deterministic


### Read Path

Reads are served directly from memory:

```text
Client → MemStore → Response
```

* Lookup is O(1) using hashmap
* Tombstoned (deleted) keys are treated as non-existent
* No disk access required in steady state

> MemStore must be combined with LSM tree in future to support data that does not entirely fit in memory. 
----

Documentation is in progress