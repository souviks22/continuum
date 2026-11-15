// Command remote-client demonstrates a Continuum node and application client
// running as separate processes over TCP.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/souviks22/continuum"
)

const (
	serverID = 1
	clientID = 100
	server   = "127.0.0.1:17400"
	client   = "127.0.0.1:17401"
	key      = "examples/remote-client/value"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) < 1 {
		return errors.New("usage: remote-client serve DATA_DIR | (put|read) VALUE")
	}
	switch args[0] {
	case "serve":
		if len(args) != 2 {
			return errors.New("usage: remote-client serve DATA_DIR")
		}
		return serve(args[1])
	case "put", "read":
		if (args[0] == "put" && len(args) != 2) || (args[0] == "read" && len(args) != 1) {
			return errors.New("usage: remote-client put VALUE | remote-client read")
		}
		return clientCommand(args)
	default:
		return errors.New("usage: remote-client serve DATA_DIR | (put|read) VALUE")
	}
}

func serve(dataDir string) error {
	peers := map[uint64]continuum.Peer{
		serverID: {Name: "continuum-server", Address: server},
		clientID: {Name: "continuum-example-client", Address: client},
	}
	node, err := continuum.Open(continuum.Config{
		ID: serverID, Peers: peers, ListenAddr: server, DataDir: dataDir,
		StateMachine: continuum.NewKVStateMachine(), Bootstrap: true,
		InitialVoters: []uint64{serverID}, InsecureLocal: true,
		ProposalTimeout: 2 * time.Second,
		TickInterval:    10 * time.Millisecond, ElectionTick: 3, HeartbeatTick: 1,
	})
	if err != nil {
		return fmt.Errorf("open server: %w", err)
	}
	defer node.Close()
	readyCtx, readyCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer readyCancel()
	if err := waitReady(readyCtx, node); err != nil {
		return err
	}
	if err := node.ReadIndex(readyCtx); err != nil {
		return fmt.Errorf("confirm initial ReadIndex: %w", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	fmt.Printf("server listening at %s; press Ctrl-C to stop\n", node.Addr())
	<-ctx.Done()
	return nil
}

func waitReady(ctx context.Context, node *continuum.Node) error {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		health, err := node.Health(ctx)
		if err != nil {
			return fmt.Errorf("check server health: %w", err)
		}
		if health.Ready && health.IsLeader {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("server did not become ready: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func clientCommand(args []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	remote, err := continuum.NewRemoteClient(continuum.RemoteClientConfig{
		ID: clientID, Name: "continuum-example-client", ListenAddr: client,
		Peers: map[uint64]continuum.Peer{
			serverID: {Name: "continuum-server", Address: server},
		},
		InsecureLocal: true,
	}, continuum.ClientOptions{Timeout: 2 * time.Second})
	if err != nil {
		return fmt.Errorf("open remote client: %w", err)
	}
	defer remote.Close()
	switch args[0] {
	case "put":
		if _, err := remote.Txn(ctx, continuum.Put([]byte(key), []byte(args[1]))); err != nil {
			return fmt.Errorf("write: %w", err)
		}
		fmt.Printf("acknowledged %q = %q\n", key, args[1])
	case "read":
		result, err := remote.Range(ctx, []byte(key), nil, continuum.RangeOptions{})
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}
		if len(result.KVs) != 1 {
			return fmt.Errorf("expected one value for %q, found %d", key, len(result.KVs))
		}
		fmt.Printf("read %q = %q at revision %d\n", key, result.KVs[0].Value, result.Revision)
	}
	return nil
}
