// Command cluster-restart demonstrates a clean stop and restart of a static
// three-member Continuum cluster with persistent per-member data directories.
//
// Run `go run ./examples/cluster-restart write ./cluster-data`, stop the
// process, then run `go run ./examples/cluster-restart read ./cluster-data`.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/souviks22/continuum-go"
)

const (
	key   = "examples/cluster-restart/value"
	value = "replicated value survives cluster restart"
)

var peers = map[uint64]continuum.Peer{
	1: {Name: "cluster-restart-1"},
	2: {Name: "cluster-restart-2"},
	3: {Name: "cluster-restart-3"},
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) != 2 || (args[0] != "write" && args[0] != "read") {
		return errors.New("usage: cluster-restart (write|read) DATA_DIR")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	network := continuum.NewInProcessNetwork()
	nodes := make([]*continuum.Node, 0, len(peers))
	for id := uint64(1); id <= 3; id++ {
		node, err := continuum.Open(continuum.Config{
			ID: id, Peers: peers, InProcess: network,
			DataDir:      filepath.Join(args[1], fmt.Sprintf("node-%d", id)),
			StateMachine: continuum.NewKVStateMachine(), Bootstrap: args[0] == "write",
			TickInterval: 20 * time.Millisecond, ElectionTick: 5, HeartbeatTick: 1,
		})
		if err != nil {
			closeNodes(nodes)
			return fmt.Errorf("open member %d: %w", id, err)
		}
		nodes = append(nodes, node)
	}
	defer closeNodes(nodes)
	if err := waitForLeader(ctx, nodes); err != nil {
		return err
	}
	client, err := continuum.NewClient(nodes, continuum.ClientOptions{})
	if err != nil {
		return fmt.Errorf("create cluster client: %w", err)
	}
	defer client.Close()

	switch args[0] {
	case "write":
		if _, err := client.Txn(ctx, continuum.Put([]byte(key), []byte(value))); err != nil {
			return fmt.Errorf("write replicated value: %w", err)
		}
		fmt.Println("write acknowledged by the three-member cluster; stop this process before running read")
	case "read":
		result, err := client.Range(ctx, []byte(key), nil, continuum.RangeOptions{})
		if err != nil {
			return fmt.Errorf("read after restart: %w", err)
		}
		if len(result.KVs) != 1 || string(result.KVs[0].Value) != value {
			return fmt.Errorf("replicated value missing or changed: got %+v", result.KVs)
		}
		fmt.Printf("recovered %q = %q at revision %d\n", key, result.KVs[0].Value, result.Revision)
	}
	return nil
}

func waitForLeader(ctx context.Context, nodes []*continuum.Node) error {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		for _, node := range nodes {
			health, err := node.Health(ctx)
			if err != nil {
				return fmt.Errorf("check node health: %w", err)
			}
			if health.Ready && health.IsLeader {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("cluster did not elect a ready leader: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func closeNodes(nodes []*continuum.Node) {
	for i := len(nodes) - 1; i >= 0; i-- {
		_ = nodes[i].Close()
	}
}
