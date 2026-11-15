# Restarting a three-member cluster

The example runs a static three-voter cluster in one process and stores each
member under its own directory. Use separate processes for the write and read
phases:

```sh
go run ./examples/cluster-restart write ./cluster-data
# Stop the first process after it reports the acknowledged write.
go run ./examples/cluster-restart read ./cluster-data
```

The second process creates a fresh in-process network, reopens all three
members without bootstrapping, elects a leader, and verifies the value through
the cluster client. Keep the root directory and all three member directories.
Deleting or mixing member directories creates a different recovery scenario.

This demonstrates full cluster shutdown and restart. It does not demonstrate a
rolling restart, node replacement, membership change, separate-process Raft
transport, or behavior when a quorum remains down. Those need operational
coordination beyond this static-cluster example.
