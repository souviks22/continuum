# Restarting a node

This example demonstrates a single voter with a durable data directory. Run
the write and read commands as separate processes:

```sh
go run ./examples/restart write ./continuum-data
# Stop the first process, then start a fresh one:
go run ./examples/restart read ./continuum-data
```

The first process waits for the node to become leader and acknowledges a KV
transaction. The second opens the same directory without bootstrapping a new
cluster, waits for the recovered node to become leader, and verifies the key.
Keep the directory between runs. Removing it creates a new cluster and loses
the data.

This covers a clean single-node process restart. It does not demonstrate
multi-node failover, membership changes, backups, or recovery from a damaged
disk. For a replicated deployment, keep the node ID, peer identity, data
directory, and membership configuration stable across restarts.
