# Separate-process TCP client

This example runs a KV node and its application client in separate processes.
The addresses and IDs are fixed for a local demonstration; use stable,
reachable addresses, unique IDs and mTLS for a real deployment.

From the repository root, start the server:

```sh
go run ./examples/remote-client serve ./remote-client-data
```

In another terminal, send a write and read it through separate client
processes:

```sh
go run ./examples/remote-client put hello
go run ./examples/remote-client read
```

Stop the server with Ctrl-C. The example opts into `InsecureLocal` on both
ends, uses loopback TCP, and explicitly sets `InitialVoters` so the registered
client transport identity does not become a Raft voter. It demonstrates a
single server; it does not demonstrate failover, multi-process Raft, TLS
provisioning, or dynamic topology changes. Remove `./remote-client-data` to
start over.
