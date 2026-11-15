// Command restart demonstrates a clean process restart of a single-member
// Continuum node backed by a persistent data directory.
//
// Run `go run ./examples/restart write ./continuum-data`, stop the process,
// then run `go run ./examples/restart read ./continuum-data`.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/souviks22/continuum-go"
)

const (
	key   = "examples/restart/value"
	value = "durable across process restart"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) != 2 || (args[0] != "write" && args[0] != "read") {
		return errors.New("usage: restart (write|read) DATA_DIR")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	network := continuum.NewInProcessNetwork()
	node, err := continuum.Open(continuum.Config{
		ID: 1, Peers: map[uint64]continuum.Peer{1: {Name: "restart-example"}},
		InProcess: network, DataDir: args[1], StateMachine: continuum.NewKVStateMachine(),
		Bootstrap: args[0] == "write", TickInterval: 50 * time.Millisecond,
		ElectionTick: 4, HeartbeatTick: 1,
	})
	if err != nil {
		return fmt.Errorf("open node: %w", err)
	}
	defer node.Close()
	if err := waitReady(ctx, node); err != nil {
		return err
	}

	switch args[0] {
	case "write":
		if _, err := node.Txn(ctx, continuum.Put([]byte(key), []byte(value))); err != nil {
			return fmt.Errorf("write value: %w", err)
		}
		fmt.Printf("acknowledged write to %q; stop this process and run the read command\n", key)
	case "read":
		result, err := node.Range(ctx, []byte(key), nil, continuum.RangeOptions{})
		if err != nil {
			return fmt.Errorf("read value: %w", err)
		}
		if len(result.KVs) != 1 || string(result.KVs[0].Value) != value {
			return fmt.Errorf("durable value missing or changed: got %+v", result.KVs)
		}
		fmt.Printf("recovered %q = %q at revision %d\n", key, result.KVs[0].Value, result.Revision)
	}
	return nil
}

func waitReady(ctx context.Context, node *continuum.Node) error {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		health, err := node.Health(ctx)
		if err != nil {
			return fmt.Errorf("check node health: %w", err)
		}
		if health.Ready && health.IsLeader {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("node did not become ready: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}
