// Command cluster-process demonstrates a three-member TCP Raft cluster where
// each node and application client run in independent processes.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/souviks22/continuum"
)

const (
	clientID   = 100
	clientName = "cluster-process-client"
	key        = "examples/cluster-process/value"
)

var (
	clientAddr  = "127.0.0.1:17500"
	memberNames = map[uint64]string{1: "cluster-process-1", 2: "cluster-process-2", 3: "cluster-process-3", 4: "cluster-process-4"}
	memberAddrs = map[uint64]string{1: "127.0.0.1:17501", 2: "127.0.0.1:17502", 3: "127.0.0.1:17503", 4: "127.0.0.1:17504"}
)

func init() {
	if addr := os.Getenv("CONTINUUM_CLUSTER_PROCESS_CLIENT_ADDR"); addr != "" {
		clientAddr = addr
	}
	for id := uint64(1); id <= 4; id++ {
		if addr := os.Getenv(fmt.Sprintf("CONTINUUM_CLUSTER_PROCESS_MEMBER_%d_ADDR", id)); addr != "" {
			memberAddrs[id] = addr
		}
	}
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 4 && args[0] == "serve" {
		id, err := strconv.ParseUint(args[1], 10, 64)
		if err != nil || id < 1 || id > 3 {
			return errors.New("serve requires member ID 1, 2 or 3")
		}
		if args[3] != "bootstrap" && args[3] != "recover" {
			return errors.New("serve requires bootstrap or recover mode")
		}
		return serve(id, args[2], args[3] == "bootstrap")
	}
	if (len(args) == 2 && (args[0] == "put" || args[0] == "read-node")) || (len(args) == 1 && args[0] == "read") {
		return clientCommand(args)
	}
	return errors.New("usage: cluster-process serve (1|2|3) DATA_DIR (bootstrap|recover) | put VALUE | read | read-node ID")
}

func serve(id uint64, dataDir string, bootstrap bool) error {
	peers := make(map[uint64]continuum.Peer, 4)
	for memberID, name := range memberNames {
		peers[memberID] = continuum.Peer{Name: name, Address: memberAddrs[memberID]}
	}
	peers[clientID] = continuum.Peer{Name: clientName, Address: clientAddr}
	node, err := continuum.Open(continuum.Config{
		ID: id, Peers: peers, ListenAddr: memberAddrs[id], DataDir: dataDir,
		StateMachine: continuum.NewKVStateMachine(), Bootstrap: bootstrap,
		InitialVoters: []uint64{1, 2, 3}, InsecureLocal: true,
		ProposalTimeout: 3 * time.Second,
		TickInterval:    50 * time.Millisecond, ElectionTick: 8, HeartbeatTick: 1,
	})
	if err != nil {
		return fmt.Errorf("open member %d: %w", id, err)
	}
	defer node.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := waitReady(ctx, node); err != nil {
		return err
	}
	fmt.Printf("member %d ready at %s (bootstrap=%t); press Ctrl-C to stop\n", id, node.Addr(), bootstrap)
	<-ctx.Done()
	return nil
}

func waitReady(ctx context.Context, node *continuum.Node) error {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		health, err := node.Health(ctx)
		if err != nil {
			return fmt.Errorf("check member health: %w", err)
		}
		if health.Ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("member did not become ready: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func clientCommand(args []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	peers := make(map[uint64]continuum.Peer, len(memberNames))
	for id, name := range memberNames {
		if id > 3 {
			continue
		}
		if args[0] == "read-node" {
			selected, parseErr := strconv.ParseUint(args[1], 10, 64)
			if parseErr != nil || selected < 1 || selected > 3 {
				return errors.New("read-node requires member ID 1, 2 or 3")
			}
			if id != selected {
				continue
			}
		}
		peers[id] = continuum.Peer{Name: name, Address: memberAddrs[id]}
	}
	remote, err := continuum.NewRemoteClient(continuum.RemoteClientConfig{
		ID: clientID, Name: clientName, ListenAddr: clientAddr,
		Peers: peers, InsecureLocal: true,
	}, continuum.ClientOptions{Timeout: 3 * time.Second, MaxAttempts: 5, ReadMaxAttempts: 5})
	if err != nil {
		return fmt.Errorf("open remote client: %w", err)
	}
	defer remote.Close()
	if args[0] == "put" {
		if _, err := remote.Txn(ctx, continuum.Put([]byte(key), []byte(args[1]))); err != nil {
			return fmt.Errorf("write replicated value: %w", err)
		}
		fmt.Printf("acknowledged %q = %q\n", key, args[1])
		return nil
	}
	result, err := remote.Range(ctx, []byte(key), nil, continuum.RangeOptions{})
	if err != nil {
		return fmt.Errorf("linearizable read: %w", err)
	}
	if len(result.KVs) != 1 {
		return fmt.Errorf("expected one value for %q, found %d", key, len(result.KVs))
	}
	fmt.Printf("read %q = %q at revision %d\n", key, result.KVs[0].Value, result.Revision)
	return nil
}
