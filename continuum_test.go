package continuum_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/souviks22/continuum-go"
)

type testMachine struct{ value string }

func (m *testMachine) Apply(_ uint64, command []byte) []byte {
	m.value = string(command)
	return []byte("ok:" + m.value)
}

func TestPublicMembershipReplacementFlow(t *testing.T) {
	network := continuum.NewInProcessNetwork()
	peers := map[uint64]continuum.Peer{
		1: {Name: "member-1"}, 2: {Name: "member-2"}, 3: {Name: "member-3"}, 4: {Name: "member-4"},
	}
	voters := []uint64{1, 2, 3}
	nodes := make(map[uint64]*continuum.Node)
	closeAll := func() {
		for id := uint64(4); id > 0; id-- {
			if node := nodes[id]; node != nil {
				_ = node.Close()
			}
		}
	}
	open := func(id uint64, bootstrap, learner bool) (*continuum.Node, error) {
		return continuum.Open(continuum.Config{
			ID: id, Peers: peers, InProcess: network, DataDir: t.TempDir(),
			StateMachine: continuum.NewKVStateMachine(), Bootstrap: bootstrap,
			BootstrapLearner: learner, InitialVoters: voters,
			TickInterval: 10 * time.Millisecond, ElectionTick: 3, HeartbeatTick: 1,
		})
	}
	for id := uint64(1); id <= 3; id++ {
		node, err := open(id, true, false)
		if err != nil {
			closeAll()
			t.Fatalf("open voter %d: %v", id, err)
		}
		nodes[id] = node
	}
	defer closeAll()
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	controller := waitPublicLeader(t, ctx, nodes, 1, 2, 3)
	admin, err := continuum.NewClient([]*continuum.Node{nodes[1], nodes[2], nodes[3]}, continuum.ClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	initial, err := admin.Membership(ctx)
	if err != nil || !sameMembers(initial.Voters, voters) {
		t.Fatalf("initial membership=%+v err=%v", initial, err)
	}

	learner, err := open(4, false, true)
	if err != nil {
		t.Fatal(err)
	}
	nodes[4] = learner
	if err := admin.AddLearner(ctx, 4); err != nil {
		t.Fatalf("add learner: %v", err)
	}
	for {
		leaderHealth, err := controller.Health(ctx)
		if err != nil {
			t.Fatal(err)
		}
		learnerHealth, err := learner.Health(ctx)
		if err != nil {
			t.Fatal(err)
		}
		state, err := learner.Membership(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if hasMember(state.Learners, 4) && learnerHealth.Applied >= leaderHealth.Commit {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("learner did not catch up: health=%+v membership=%+v", learnerHealth, state)
		case <-time.After(10 * time.Millisecond):
		}
	}
	if err := admin.PromoteLearner(ctx, 4); err != nil {
		t.Fatalf("promote learner: %v", err)
	}
	if err := admin.RemoveMember(ctx, 3); err != nil {
		t.Fatalf("remove replaced voter: %v", err)
	}
	for {
		state, err := admin.Membership(ctx)
		if err == nil && sameMembers(state.Voters, []uint64{1, 2, 4}) {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("replacement membership did not converge: state=%+v err=%v", state, err)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func waitPublicLeader(t *testing.T, ctx context.Context, nodes map[uint64]*continuum.Node, ids ...uint64) *continuum.Node {
	t.Helper()
	for {
		for _, id := range ids {
			node := nodes[id]
			if node == nil {
				continue
			}
			health, err := node.Health(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if health.Ready && health.IsLeader {
				return node
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("no ready leader: %v", ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func hasMember(members []uint64, id uint64) bool {
	for _, member := range members {
		if member == id {
			return true
		}
	}
	return false
}

func sameMembers(got, want []uint64) bool {
	if len(got) != len(want) {
		return false
	}
	for _, id := range want {
		if !hasMember(got, id) {
			return false
		}
	}
	return true
}

func (m *testMachine) Snapshot() ([]byte, error) { return []byte(m.value), nil }
func (m *testMachine) Restore(data []byte) error { m.value = string(data); return nil }

func TestOpenOwnsRuntimeAndSerializesNodeCalls(t *testing.T) {
	node, err := continuum.Open(continuum.Config{ID: 1, Peers: map[uint64]continuum.Peer{1: {Name: "single", Address: "127.0.0.1:0"}}, ListenAddr: "127.0.0.1:0", DataDir: t.TempDir(), StateMachine: &testMachine{}, Bootstrap: true, InsecureLocal: true, TickInterval: 10 * time.Millisecond, ElectionTick: 3, HeartbeatTick: 1})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	deadline := time.Now().Add(4 * time.Second)
	for {
		health, err := node.Health(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if health.Ready && health.IsLeader {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("node did not become ready: %+v", health)
		}
		time.Sleep(10 * time.Millisecond)
	}
	result, err := node.Propose(ctx, []byte("embedded"))
	if err != nil {
		t.Fatal(err)
	}
	if string(result) != "ok:embedded" {
		t.Fatalf("proposal result=%q", result)
	}
	if err := node.ReadIndex(ctx); err != nil {
		t.Fatal(err)
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := node.Propose(ctx, []byte("after-close")); !errors.Is(err, continuum.ErrStopped) {
		t.Fatalf("proposal after close: %v", err)
	}
}

func TestObserverReceivesPublicEvents(t *testing.T) {
	var events []continuum.Event
	node, err := continuum.Open(continuum.Config{ID: 1, Peers: map[uint64]continuum.Peer{1: {Name: "observed", Address: "127.0.0.1:0"}}, ListenAddr: "127.0.0.1:0", DataDir: t.TempDir(), StateMachine: &testMachine{}, Bootstrap: true, InsecureLocal: true, TickInterval: 10 * time.Millisecond, ElectionTick: 3, HeartbeatTick: 1, Observer: func(event continuum.Event) { events = append(events, event) }})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	deadline := time.Now().Add(4 * time.Second)
	for {
		h, _ := node.Health(ctx)
		if h.Ready {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("node did not become ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := node.Propose(ctx, []byte("trace-me")); err != nil {
		t.Fatal(err)
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, event := range events {
		if event.Name == "proposal.applied" && event.RequestID != 0 {
			found = true
		}
	}
	if !found {
		t.Fatal("public observer did not receive a correlated proposal event")
	}
}

func TestInProcessNetworkReplicatesAcrossEmbeddedNodes(t *testing.T) {
	network := continuum.NewInProcessNetwork()
	peers := map[uint64]continuum.Peer{
		1: {Name: "local-1"},
		2: {Name: "local-2"},
		3: {Name: "local-3"},
	}
	nodes := make([]*continuum.Node, 0, len(peers))
	for id := uint64(1); id <= 3; id++ {
		node, err := continuum.Open(continuum.Config{
			ID: id, Peers: peers, InProcess: network, DataDir: t.TempDir(),
			StateMachine: &testMachine{}, Bootstrap: true,
			TickInterval: 10 * time.Millisecond, ElectionTick: 3, HeartbeatTick: 1,
		})
		if err != nil {
			for i := len(nodes) - 1; i >= 0; i-- {
				_ = nodes[i].Close()
			}
			t.Fatal(err)
		}
		nodes = append(nodes, node)
	}
	defer func() {
		for i := len(nodes) - 1; i >= 0; i-- {
			if err := nodes[i].Close(); err != nil {
				t.Errorf("close node %d: %v", i+1, err)
			}
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	var leader *continuum.Node
	for leader == nil {
		for _, node := range nodes {
			h, err := node.Health(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if h.IsLeader && h.Ready {
				leader = node
				break
			}
		}
		if leader == nil {
			select {
			case <-ctx.Done():
				t.Fatal("in-process cluster did not elect a ready leader")
			case <-time.After(10 * time.Millisecond):
			}
		}
	}
	if _, err := leader.Propose(ctx, []byte("replicated")); err != nil {
		t.Fatal(err)
	}
	for _, node := range nodes {
		if err := node.ReadIndex(ctx); err != nil {
			t.Fatalf("linearizable read barrier: %v", err)
		}
	}
	for i, node := range nodes {
		h, err := node.Health(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if h.Applied < 2 {
			t.Fatalf("node %d applied index %d, want replicated command", i+1, h.Applied)
		}
	}
}

func TestPublicKVTxnRangeAndWatch(t *testing.T) {
	network := continuum.NewInProcessNetwork()
	node, err := continuum.Open(continuum.Config{
		ID: 1, Peers: map[uint64]continuum.Peer{1: {Name: "kv-local"}},
		InProcess: network, DataDir: t.TempDir(), StateMachine: continuum.NewKVStateMachine(),
		Bootstrap: true, TickInterval: 10 * time.Millisecond, ElectionTick: 3, HeartbeatTick: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	deadline := time.Now().Add(4 * time.Second)
	for {
		h, err := node.Health(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if h.Ready && h.IsLeader {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("KV node did not become ready: %+v", h)
		}
		time.Sleep(10 * time.Millisecond)
	}

	write, err := node.Txn(ctx, continuum.Txn{
		Compares: []continuum.Compare{{Target: continuum.CompareVersion, Result: continuum.Equal, Key: []byte("service/a"), Int: 0}},
		Success:  []continuum.Op{{Type: continuum.OpPut, Key: []byte("service/a"), Value: []byte("one")}},
	})
	if err != nil || !write.Succeeded {
		t.Fatalf("Txn result=%+v err=%v", write, err)
	}
	read, err := node.Range(ctx, []byte("service/"), continuum.PrefixEnd([]byte("service/")), continuum.RangeOptions{})
	if err != nil || len(read.KVs) != 1 || string(read.KVs[0].Value) != "one" {
		t.Fatalf("Range result=%+v err=%v", read, err)
	}
	lease, err := node.GrantLease(ctx, 10)
	if err != nil || lease.ID == 0 {
		t.Fatalf("GrantLease result=%+v err=%v", lease, err)
	}
	if _, err := node.Txn(ctx, continuum.PutWithLease([]byte("service/ephemeral"), []byte("temporary"), lease.ID)); err != nil {
		t.Fatal(err)
	}

	events := make(chan continuum.WatchResult, 1)
	watch, err := node.Watch(ctx, []byte("service/"), continuum.PrefixEnd([]byte("service/")), continuum.WatchOptions{}, func(result continuum.WatchResult, err error) {
		if err == nil && len(result.Events) > 0 {
			events <- result
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer watch.Cancel()
	if _, err := node.Txn(ctx, continuum.Put([]byte("service/b"), []byte("two"))); err != nil {
		t.Fatal(err)
	}
	select {
	case page := <-events:
		if len(page.Events) != 1 || string(page.Events[0].KV.Key) != "service/b" || string(page.Events[0].KV.Value) != "two" {
			t.Fatalf("watch page=%+v", page)
		}
	case <-ctx.Done():
		t.Fatal("watch did not receive committed change")
	}
	if err := node.RevokeLease(ctx, lease.ID); err != nil {
		t.Fatal(err)
	}
	if result, err := node.Range(ctx, []byte("service/ephemeral"), nil, continuum.RangeOptions{}); err != nil || len(result.KVs) != 0 {
		t.Fatalf("revoked lease key remained: result=%+v err=%v", result, err)
	}
	latest, err := node.Range(ctx, []byte("service/"), continuum.PrefixEnd([]byte("service/")), continuum.RangeOptions{})
	if err != nil || latest.Revision <= read.Revision {
		t.Fatalf("latest revision=%d earlier=%d err=%v", latest.Revision, read.Revision, err)
	}
	if err := node.Compact(ctx, latest.Revision); err != nil {
		t.Fatalf("Compact(%d): %v", latest.Revision, err)
	}
	if err := node.Compact(ctx, latest.Revision); err != nil {
		t.Fatalf("repeated Compact(%d): %v", latest.Revision, err)
	}
	if _, err := node.Range(ctx, []byte("service/"), continuum.PrefixEnd([]byte("service/")), continuum.RangeOptions{Revision: read.Revision}); !errors.Is(err, continuum.ErrCompacted) {
		t.Fatalf("historical Range at compacted revision: %v", err)
	}
}

func TestPublicAutomaticMVCCCompaction(t *testing.T) {
	network := continuum.NewInProcessNetwork()
	node, err := continuum.Open(continuum.Config{
		ID: 1, Peers: map[uint64]continuum.Peer{1: {Name: "compact-local"}},
		InProcess: network, DataDir: t.TempDir(), StateMachine: continuum.NewKVStateMachine(),
		Bootstrap: true, TickInterval: 10 * time.Millisecond, ElectionTick: 3, HeartbeatTick: 1,
		KVCompactionInterval: 30 * time.Millisecond, KVCompactionRetain: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	deadline := time.Now().Add(4 * time.Second)
	for {
		health, err := node.Health(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if health.Ready && health.IsLeader {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("node did not become ready: %+v", health)
		}
		time.Sleep(10 * time.Millisecond)
	}

	var firstRevision uint64
	for i := 0; i < 5; i++ {
		result, err := node.Txn(ctx, continuum.Put([]byte("revision"), []byte{byte('0' + i)}))
		if err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		if i == 0 {
			firstRevision = result.Revision
		}
	}
	for time.Now().Before(deadline) {
		_, err := node.Range(ctx, []byte("revision"), nil, continuum.RangeOptions{Revision: firstRevision})
		if errors.Is(err, continuum.ErrCompacted) {
			return
		}
		if err != nil {
			t.Fatalf("historical Range: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("automatic policy did not compact the first data revision")
}

func TestPublicRemoteClientUsesTCPProposalRangeWatchAndLeases(t *testing.T) {
	serverAddr := availableTCPAddress(t)
	clientAddr := availableTCPAddress(t)
	networkPeers := map[uint64]continuum.Peer{
		1:   {Name: "remote-server", Address: serverAddr},
		2:   {Name: "remote-learner", Address: availableTCPAddress(t)},
		100: {Name: "remote-client", Address: clientAddr},
	}
	node, err := continuum.Open(continuum.Config{
		ID: 1, Peers: networkPeers, ListenAddr: serverAddr, DataDir: t.TempDir(),
		StateMachine: continuum.NewKVStateMachine(), Bootstrap: true, InitialVoters: []uint64{1},
		InsecureLocal: true, TickInterval: 10 * time.Millisecond, ElectionTick: 3, HeartbeatTick: 1,
		ProposalTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	waitPublicLeader(t, ctx, map[uint64]*continuum.Node{1: node}, 1)
	remote, err := continuum.NewRemoteClient(continuum.RemoteClientConfig{
		ID: 100, Name: "remote-client", ListenAddr: clientAddr,
		Peers:         map[uint64]continuum.Peer{1: {Name: "remote-server", Address: node.Addr().String()}},
		InsecureLocal: true,
	}, continuum.ClientOptions{Timeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer remote.Close()

	if _, err := remote.Txn(ctx, continuum.Put([]byte("remote/value"), []byte("one"))); err != nil {
		t.Fatalf("remote Txn: %v", err)
	}
	got, err := remote.Range(ctx, []byte("remote/value"), nil, continuum.RangeOptions{})
	if err != nil || len(got.KVs) != 1 || string(got.KVs[0].Value) != "one" {
		t.Fatalf("remote Range=%+v err=%v", got, err)
	}

	events := make(chan continuum.WatchResult, 2)
	watch, err := remote.Watch(ctx, []byte("remote/watch"), nil, continuum.WatchOptions{}, func(page continuum.WatchResult, err error) {
		if err == nil && len(page.Events) > 0 {
			events <- page
		}
	})
	if err != nil {
		t.Fatalf("remote Watch: %v", err)
	}
	defer watch.Cancel()
	for !watch.Ready() {
		select {
		case <-ctx.Done():
			t.Fatal("remote watch did not become ready")
		case <-time.After(10 * time.Millisecond):
		}
	}
	if _, err := remote.Txn(ctx, continuum.Put([]byte("remote/watch"), []byte("event"))); err != nil {
		t.Fatalf("remote watched Txn: %v", err)
	}
	select {
	case page := <-events:
		if len(page.Events) != 1 || string(page.Events[0].KV.Value) != "event" {
			t.Fatalf("remote watch page=%+v", page)
		}
	case <-ctx.Done():
		t.Fatal("remote watch did not deliver the committed write")
	}

	lease, err := remote.GrantLease(ctx, 10)
	if err != nil || lease.ID == 0 {
		t.Fatalf("remote GrantLease=%+v err=%v", lease, err)
	}
	if _, err := remote.Txn(ctx, continuum.PutWithLease([]byte("remote/leased"), []byte("temp"), lease.ID)); err != nil {
		t.Fatalf("remote lease-attached Txn: %v", err)
	}
	if err := remote.RevokeLease(ctx, lease.ID); err != nil {
		t.Fatalf("remote RevokeLease: %v", err)
	}

	membership, err := remote.Membership(ctx)
	if err != nil || !sameMembers(membership.Voters, []uint64{1}) {
		t.Fatalf("remote Membership=%+v err=%v", membership, err)
	}
	if err := remote.AddLearner(ctx, 2); err != nil {
		t.Fatalf("remote AddLearner: %v", err)
	}
	membership, err = remote.Membership(ctx)
	if err != nil || !sameMembers(membership.Learners, []uint64{2}) {
		t.Fatalf("remote membership after add=%+v err=%v", membership, err)
	}
	if err := remote.RemoveMember(ctx, 2); err != nil {
		t.Fatalf("remote RemoveMember: %v", err)
	}
	membership, err = remote.Membership(ctx)
	if err != nil || !sameMembers(membership.Learners, nil) {
		t.Fatalf("remote membership after removal=%+v err=%v", membership, err)
	}
}

func availableTCPAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

func TestClusterClientRoutesAcrossEmbeddedNodes(t *testing.T) {
	network := continuum.NewInProcessNetwork()
	peers := map[uint64]continuum.Peer{
		1: {Name: "route-1"},
		2: {Name: "route-2"},
		3: {Name: "route-3"},
	}
	nodes := make([]*continuum.Node, 0, 3)
	for id := uint64(1); id <= 3; id++ {
		node, err := continuum.Open(continuum.Config{
			ID: id, Peers: peers, InProcess: network, DataDir: t.TempDir(),
			StateMachine: continuum.NewKVStateMachine(), Bootstrap: true,
			TickInterval: 10 * time.Millisecond, ElectionTick: 3, HeartbeatTick: 1,
		})
		if err != nil {
			for i := len(nodes) - 1; i >= 0; i-- {
				_ = nodes[i].Close()
			}
			t.Fatal(err)
		}
		nodes = append(nodes, node)
	}
	defer func() {
		for i := len(nodes) - 1; i >= 0; i-- {
			_ = nodes[i].Close()
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	deadline := time.Now().Add(4 * time.Second)
	for {
		ready := 0
		for _, node := range nodes {
			h, err := node.Health(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if h.Ready {
				ready++
			}
		}
		if ready == len(nodes) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d/%d embedded nodes became ready", ready, len(nodes))
		}
		time.Sleep(10 * time.Millisecond)
	}
	cluster, err := continuum.NewClient([]*continuum.Node{nodes[2], nodes[0], nodes[1]}, continuum.ClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	for _, node := range nodes {
		h, err := node.Health(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !h.IsLeader {
			if err := node.Close(); err != nil {
				t.Fatal(err)
			}
			break // the client must skip this stopped replica and find the leader
		}
	}
	write, err := cluster.Txn(ctx, continuum.Txn{
		Compares: []continuum.Compare{{Target: continuum.CompareVersion, Result: continuum.Equal, Key: []byte("cluster/key"), Int: 0}},
		Success:  []continuum.Op{{Type: continuum.OpPut, Key: []byte("cluster/key"), Value: []byte("routed")}},
	})
	if err != nil || !write.Succeeded {
		t.Fatalf("cluster Txn result=%+v err=%v", write, err)
	}
	read, err := cluster.Range(ctx, []byte("cluster/"), continuum.PrefixEnd([]byte("cluster/")), continuum.RangeOptions{})
	if err != nil || len(read.KVs) != 1 || string(read.KVs[0].Value) != "routed" {
		t.Fatalf("cluster Range result=%+v err=%v", read, err)
	}

	events := make(chan continuum.WatchResult, 1)
	watch, err := cluster.Watch(ctx, []byte("cluster/"), continuum.PrefixEnd([]byte("cluster/")), continuum.WatchOptions{}, func(page continuum.WatchResult, err error) {
		if err == nil && len(page.Events) > 0 {
			events <- page
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer watch.Cancel()
	for !watch.Ready() {
		select {
		case <-ctx.Done():
			t.Fatal("cluster client watch did not become ready")
		case <-time.After(10 * time.Millisecond):
		}
	}
	if _, err := cluster.Txn(ctx, continuum.Put([]byte("cluster/next"), []byte("event"))); err != nil {
		t.Fatal(err)
	}
	select {
	case page := <-events:
		if len(page.Events) != 1 || string(page.Events[0].KV.Key) != "cluster/next" {
			t.Fatalf("cluster watch page=%+v", page)
		}
	case <-ctx.Done():
		t.Fatal("cluster client watch did not receive change")
	}
}
