# External Go clients

`continuum.NewRemoteClient` connects a Go application to KV-enabled nodes over
the bounded TCP transport. KV RPC handlers are registered automatically when a
node is opened with `NewKVStateMachine`. The remote client supports transactions,
retry of unknown transaction results, linearizable Range, resumable Watch,
leases, compaction and context-aware membership administration.

## Configure the client identity

The transport is symmetric: servers send RPC replies back to the client's
listener. Give the client a stable, reachable `Name`, nonzero `ID`, and
`ListenAddr`. Reserve an ID that no Raft member uses. Every server's `Config.Peers`
must include the same client identity and address, along with the cluster's
server identities. Each server must also retain a `Peers` entry for every
member that may join later.

When bootstrapping the Raft cluster, set `InitialVoters` explicitly. If it is
nil, all IDs in `Peers` become voters, which would incorrectly include client
identities. Raft membership and transport identity are separate: registering a
client as a transport peer does not add it to the voter set when `InitialVoters`
is explicit.

The client config lists the server IDs, names and addresses it can reach:

```go
remote, err := continuum.NewRemoteClient(continuum.RemoteClientConfig{
    ID: 100, Name: "orders-client", ListenAddr: "10.0.0.20:7400",
    Peers: map[uint64]continuum.Peer{
        1: {Name: "store-1", Address: "10.0.0.11:7000"},
        2: {Name: "store-2", Address: "10.0.0.12:7000"},
        3: {Name: "store-3", Address: "10.0.0.13:7000"},
    },
    TLSClient: clientTLSForPeer,
    TLSServer: serverTLS,
    PeerName:  verifiedPeerName,
}, continuum.ClientOptions{})
if err != nil {
    return err
}
defer remote.Close()
```

For mTLS, the client verifies server certificates through `TLSClient`; servers
must verify the client certificate and map it to `orders-client` with
`PeerName`. The server's `Peers` entry for ID 100 must use that name and the
client listener address. Plaintext requires `InsecureLocal: true` on both sides
and is intended only for trusted local development or tests.

## Result and deployment limits

Writes that time out after acceptance return `ErrResultUnknown`. Keep the
client's session checkpoint if the application needs to restore that exact
pending transaction after its own restart. A context expiry after proposal
acceptance is still an ambiguous result.

The client topology is static for its lifetime. After changing the reachable
server set, create a new remote client. `RemoteClient.Membership` uses a
ReadIndex barrier. `AddLearner`, `PromoteLearner` and `RemoveMember` route to a
known leader and serialize changes per remote client. As with writes, a context
expiry after a membership request may have been sent returns
`ErrResultUnknown`; inspect membership before retrying. Provision each new
member's transport identity on every current node before adding it, and wait
for a learner to catch up before promoting it. The
[`separate-process example`](../examples/remote-client/README.md) demonstrates
a server and application client in separate processes over loopback TCP. It is
a single-server example; multi-process Raft, failover, TLS provisioning and
dynamic topology changes remain unverified. Separate server, writer-client and
reader-client processes have been run successfully; the reader recovered the
acknowledged value through a linearizable Range.
