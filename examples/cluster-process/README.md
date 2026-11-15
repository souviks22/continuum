# Three-process TCP cluster

This example runs each Raft voter in its own OS process and accesses the
cluster from separate `RemoteClient` processes. It uses fixed loopback
addresses, plaintext transport and fresh directories for local demonstration
only.

Start one command in each terminal from the repository root:

```sh
go run ./examples/cluster-process serve 1 ./cluster-process-data/node-1 bootstrap
go run ./examples/cluster-process serve 2 ./cluster-process-data/node-2 bootstrap
go run ./examples/cluster-process serve 3 ./cluster-process-data/node-3 bootstrap
```

Write a value, stop one member with Ctrl-C, and read while the other two
members retain quorum:

```sh
go run ./examples/cluster-process put hello
# Stop one server process.
go run ./examples/cluster-process read
go run ./examples/cluster-process put write-while-one-member-is-down
go run ./examples/cluster-process read
```

Restart the stopped member with its original data directory and `recover`
mode, then read again:

```sh
go run ./examples/cluster-process serve 2 ./cluster-process-data/node-2 recover
go run ./examples/cluster-process read-node 2
```

Every server has a stable transport identity for all three voters and the
client. The client identity is included in `Peers` but excluded from the Raft
voter set by explicit `InitialVoters`. The restarted member must use
`recover`; bootstrapping an existing data directory is not a recovery
procedure. `read-node 2` contacts only the recovered member, verifying that it
replayed the write acknowledged while it was down. The example covers a clean
single-member stop with a two-member quorum and subsequent member recovery. It
does not simulate multiple simultaneous failures, member replacement, rolling
upgrades, TLS provisioning or topology changes. The subprocess test also force
kills a member and checks recovery from its retained files; this is a process
crash test, not a machine power-loss or storage fault test.
