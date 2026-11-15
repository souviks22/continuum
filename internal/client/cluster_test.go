package client

import (
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/souviks22/continuum-go/internal/kv"
	"github.com/souviks22/continuum-go/internal/raftnode"
	"github.com/souviks22/continuum-go/internal/rpc"
	"github.com/souviks22/continuum-go/internal/sim"
	"github.com/souviks22/continuum-go/internal/transport"
	"github.com/souviks22/continuum-go/internal/vfs"
)

type clusterMember struct {
	node  *raftnode.Node
	store *kv.StateMachine
}

func TestClientTxnAndRangeAcrossSimulatedRaftCluster(t *testing.T) {
	sched := sim.NewScheduler()
	network := sim.NewNetwork(sched, 0x7c)
	if err := network.SetDefaultLink(sim.LinkConfig{MinDelay: time.Millisecond, MaxDelay: 3 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	peers := map[uint64]string{1: "n1", 2: "n2", 3: "n3"}
	members := make(map[uint64]clusterMember, len(peers))
	var clientPeers []Peer
	for id, name := range peers {
		tr, err := sim.NewTransport(name, network)
		if err != nil {
			t.Fatal(err)
		}
		processClock := sched.NewProcessClock()
		ep := rpc.NewEndpoint(tr, processClock, rpc.Options{IDBase: id << 32})
		sm := kv.NewStateMachine()
		node, err := raftnode.Open(raftnode.Config{
			ID: id, Peers: peers, Bootstrap: true,
			Endpoint: ep, Clock: processClock, FS: vfs.NewMem(), Dir: "node",
			StateMachine: sm, TickInterval: 10 * time.Millisecond,
			LeaseTickInterval: 50 * time.Millisecond,
			ElectionTick:      8, HeartbeatTick: 1, RequestIDBase: id << 32,
		})
		if err != nil {
			t.Fatalf("open node %d: %v", id, err)
		}
		members[id] = clusterMember{node: node, store: sm}
		bound, err := WithStore(node, sm.Store())
		if err != nil {
			t.Fatal(err)
		}
		clientPeers = append(clientPeers, bound)
	}
	t.Cleanup(func() {
		for _, member := range members {
			_ = member.node.Stop()
		}
	})

	advanceUntil(t, sched, 5*time.Second, func() bool {
		for _, member := range members {
			if member.node.IsLeader() {
				return true
			}
		}
		return false
	}, "Raft leader election")

	cl, err := New(clientPeers, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var sessionErr error
	if err := cl.OpenSession(func(err error) { sessionErr = err }); err != nil {
		t.Fatalf("open session: %v", err)
	}
	advanceUntil(t, sched, 3*time.Second, func() bool { return cl.Session() != 0 || sessionErr != nil }, "session registration")
	if sessionErr != nil || cl.Session() == 0 {
		t.Fatalf("session=%d err=%v", cl.Session(), sessionErr)
	}

	var txnResult kv.TxnResult
	var txnErr error
	if err := cl.Txn(kv.Put([]byte("service/api"), []byte("10.0.0.8")), func(r kv.TxnResult, err error) {
		txnResult, txnErr = r, err
	}); err != nil {
		t.Fatalf("txn start: %v", err)
	}
	advanceUntil(t, sched, 3*time.Second, func() bool { return txnErr != nil || txnResult.Revision != 0 }, "transaction commit")
	if txnErr != nil || txnResult.Revision == 0 {
		t.Fatalf("txn result=%+v err=%v", txnResult, txnErr)
	}

	var rangeResult kv.RangeResult
	var rangeErr error
	if err := cl.Range([]byte("service/"), kv.PrefixEnd([]byte("service/")), kv.RangeOptions{}, func(r kv.RangeResult, err error) {
		rangeResult, rangeErr = r, err
	}); err != nil {
		t.Fatalf("range start: %v", err)
	}
	advanceUntil(t, sched, 3*time.Second, func() bool { return rangeErr != nil || rangeResult.Revision != 0 }, "linearizable range")
	if rangeErr != nil || len(rangeResult.KVs) != 1 || string(rangeResult.KVs[0].Key) != "service/api" || string(rangeResult.KVs[0].Value) != "10.0.0.8" {
		t.Fatalf("range result=%+v err=%v", rangeResult, rangeErr)
	}
	if rangeResult.Revision < txnResult.Revision {
		t.Fatalf("linearizable range revision %d is behind committed txn %d", rangeResult.Revision, txnResult.Revision)
	}

	var lease kv.Lease
	var leaseErr error
	if err := cl.GrantLease(2, func(l kv.Lease, err error) { lease, leaseErr = l, err }); err != nil {
		t.Fatalf("lease grant start: %v", err)
	}
	advanceUntil(t, sched, 3*time.Second, func() bool { return leaseErr != nil || lease.ID != 0 }, "lease grant")
	if leaseErr != nil || lease.TTL != 2 {
		t.Fatalf("lease=%+v err=%v", lease, leaseErr)
	}

	var leasedTxn kv.TxnResult
	var leasedTxnErr error
	if err := cl.Txn(kv.PutWithLease([]byte("ephemeral/leader"), []byte("n1"), lease.ID), func(r kv.TxnResult, err error) {
		leasedTxn, leasedTxnErr = r, err
	}); err != nil {
		t.Fatalf("leased put start: %v", err)
	}
	advanceUntil(t, sched, 3*time.Second, func() bool { return leasedTxnErr != nil || leasedTxn.Revision != 0 }, "leased put")
	if leasedTxnErr != nil {
		t.Fatalf("leased put: %v", leasedTxnErr)
	}

	advanceUntil(t, sched, 3*time.Second, func() bool {
		for _, member := range members {
			if member.store.LeaseCount() != 0 {
				return false
			}
		}
		return true
	}, "lease expiry apply on every replica")
	for id, member := range members {
		if member.store.LeaseCount() != 0 {
			t.Errorf("node %d retains expired lease", id)
		}
		rr, err := member.store.Store().Range([]byte("ephemeral/leader"), nil, kv.RangeOptions{})
		if err != nil || len(rr.KVs) != 0 {
			t.Errorf("node %d retains expired key: %+v err=%v", id, rr, err)
		}
	}
}

func TestRaftClusterOverTCPTransport(t *testing.T) {
	sched := sim.NewScheduler()
	serialQueue := make(chan func(), 4096)
	dispatch := func(f func()) { serialQueue <- f }
	ids := []uint64{1, 2, 3}
	addresses := map[string]string{"n1": freeTCPAddress(t), "n2": freeTCPAddress(t), "n3": freeTCPAddress(t)}
	peers := map[uint64]string{1: "n1", 2: "n2", 3: "n3"}
	transports := make(map[uint64]*transport.TCPTransport, len(ids))
	members := make(map[uint64]clusterMember, len(ids))
	clientPeers := make([]Peer, 0, len(ids))
	t.Cleanup(func() {
		for _, id := range ids {
			if member, ok := members[id]; ok {
				_ = member.node.Stop()
			}
		}
		for _, id := range ids {
			if tr := transports[id]; tr != nil {
				_ = tr.Close()
			}
		}
	})
	for _, id := range ids {
		name := peers[id]
		tr, err := transport.NewTCPTransport(transport.TCPOptions{
			Name: name, ListenAddr: addresses[name], Peers: addresses,
			Dispatch: dispatch, InsecureLocal: true,
		})
		if err != nil {
			t.Fatalf("create TCP transport %s: %v", name, err)
		}
		transports[id] = tr
		processClock := sched.NewProcessClock()
		endpoint := rpc.NewEndpoint(tr, processClock, rpc.Options{IDBase: id << 32})
		sm := kv.NewStateMachine()
		node, err := raftnode.Open(raftnode.Config{
			ID: id, Peers: peers, Bootstrap: true, Endpoint: endpoint,
			Clock: processClock, FS: vfs.NewMem(), Dir: "node", StateMachine: sm,
			TickInterval: 10 * time.Millisecond, ElectionTick: 8, HeartbeatTick: 1,
			RequestIDBase: id << 32,
		})
		if err != nil {
			t.Fatalf("open node %d: %v", id, err)
		}
		members[id] = clusterMember{node: node, store: sm}
		bound, err := WithStore(node, sm.Store())
		if err != nil {
			t.Fatal(err)
		}
		clientPeers = append(clientPeers, bound)
	}
	advanceTCPUntil(t, sched, serialQueue, 5*time.Second, func() bool {
		for _, member := range members {
			if member.node.IsLeader() {
				return true
			}
		}
		return false
	}, "TCP Raft leader election")
	cl, err := New(clientPeers, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var openErr error
	if err := cl.OpenSession(func(err error) { openErr = err }); err != nil {
		t.Fatal(err)
	}
	advanceTCPUntil(t, sched, serialQueue, 3*time.Second, func() bool { return cl.Session() != 0 || openErr != nil }, "TCP session registration")
	if openErr != nil || cl.Session() == 0 {
		t.Fatalf("TCP session=%d err=%v", cl.Session(), openErr)
	}
	var result kv.TxnResult
	var txnErr error
	if err := cl.Txn(kv.Put([]byte("tcp/key"), []byte("committed")), func(r kv.TxnResult, err error) { result, txnErr = r, err }); err != nil {
		t.Fatal(err)
	}
	advanceTCPUntil(t, sched, serialQueue, 3*time.Second, func() bool { return result.Revision != 0 || txnErr != nil }, "TCP transaction commit")
	if txnErr != nil || result.Revision == 0 {
		t.Fatalf("TCP transaction result=%+v err=%v", result, txnErr)
	}
	var ranged kv.RangeResult
	var rangeErr error
	if err := cl.Range([]byte("tcp/key"), nil, kv.RangeOptions{}, func(r kv.RangeResult, err error) { ranged, rangeErr = r, err }); err != nil {
		t.Fatal(err)
	}
	advanceTCPUntil(t, sched, serialQueue, 3*time.Second, func() bool { return rangeErr != nil || ranged.Revision != 0 }, "TCP linearizable read")
	if rangeErr != nil || len(ranged.KVs) != 1 || string(ranged.KVs[0].Value) != "committed" {
		t.Fatalf("TCP range=%+v err=%v", ranged, rangeErr)
	}
}

func advanceTCPUntil(t *testing.T, sched *sim.Scheduler, queue <-chan func(), within time.Duration, ready func() bool, what string) {
	t.Helper()
	deadline := sched.Now() + within
	wallDeadline := time.Now().Add(10 * time.Second)
	for !ready() && sched.Now() < deadline && time.Now().Before(wallDeadline) {
		select {
		case f := <-queue:
			f()
		default:
			sched.RunUntil(sched.Now() + 2*time.Millisecond)
			runtime.Gosched()
			time.Sleep(time.Millisecond)
		}
	}
	if !ready() {
		t.Fatalf("timed out waiting for %s at virtual time %v", what, sched.Now())
	}
}

func freeTCPAddress(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

func advanceUntil(t *testing.T, sched *sim.Scheduler, within time.Duration, ready func() bool, what string) {
	t.Helper()
	deadline := sched.Now() + within
	for !ready() && sched.Now() < deadline {
		next := sched.Now() + 5*time.Millisecond
		if next > deadline {
			next = deadline
		}
		sched.RunUntil(next)
	}
	if !ready() {
		t.Fatalf("timed out waiting for %s at %v", what, sched.Now())
	}
}
