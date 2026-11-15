package main

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestClusterProcessHelper(t *testing.T) {
	if os.Getenv("CONTINUUM_CLUSTER_PROCESS_HELPER") != "1" {
		t.Skip("helper subprocess only")
	}
	separator := -1
	for i, arg := range os.Args {
		if arg == "--" {
			separator = i
			break
		}
	}
	if separator < 0 || separator+1 >= len(os.Args) {
		t.Fatal("missing helper command")
	}
	if err := run(os.Args[separator+1:]); err != nil {
		t.Fatal(err)
	}
}

func TestThreeProcessClusterSurvivesMemberRestart(t *testing.T) {
	addresses := freeProcessAddresses(t, 4)
	helperEnv := append(os.Environ(), "CONTINUUM_CLUSTER_PROCESS_HELPER=1",
		"CONTINUUM_CLUSTER_PROCESS_CLIENT_ADDR="+addresses[0],
		"CONTINUUM_CLUSTER_PROCESS_MEMBER_1_ADDR="+addresses[1],
		"CONTINUUM_CLUSTER_PROCESS_MEMBER_2_ADDR="+addresses[2],
		"CONTINUUM_CLUSTER_PROCESS_MEMBER_3_ADDR="+addresses[3],
	)
	dataRoot := filepath.Join(t.TempDir(), "members")
	if err := os.MkdirAll(dataRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	logs := filepath.Join(t.TempDir(), "logs")
	if err := os.MkdirAll(logs, 0o700); err != nil {
		t.Fatal(err)
	}
	servers := make(map[uint64]*exec.Cmd, 3)
	start := func(id uint64, mode string) {
		t.Helper()
		args := []string{"serve", fmt.Sprint(id), filepath.Join(dataRoot, fmt.Sprintf("node-%d", id)), mode}
		childArgs := append([]string{"-test.run=^TestClusterProcessHelper$", "--"}, args...)
		cmd := exec.Command(os.Args[0], childArgs...)
		cmd.Env = helperEnv
		log, err := os.Create(filepath.Join(logs, fmt.Sprintf("node-%d-%s.log", id, mode)))
		if err != nil {
			t.Fatal(err)
		}
		cmd.Stdout, cmd.Stderr = log, log
		if err := cmd.Start(); err != nil {
			log.Close()
			t.Fatal(err)
		}
		servers[id] = cmd
		t.Cleanup(func() {
			if cmd.Process != nil && cmd.ProcessState == nil {
				_ = cmd.Process.Signal(os.Interrupt)
				_ = cmd.Wait()
			}
			_ = log.Close()
		})
	}
	stop := func(id uint64) {
		t.Helper()
		cmd := servers[id]
		if cmd == nil || cmd.Process == nil {
			t.Fatalf("member %d is not running", id)
		}
		if err := cmd.Process.Signal(os.Interrupt); err != nil {
			t.Fatalf("stop member %d: %v", id, err)
		}
		if err := cmd.Wait(); err != nil {
			t.Fatalf("member %d exited with error: %v", id, err)
		}
		servers[id] = nil
	}
	crash := func(id uint64) {
		t.Helper()
		cmd := servers[id]
		if cmd == nil || cmd.Process == nil {
			t.Fatalf("member %d is not running", id)
		}
		if err := cmd.Process.Kill(); err != nil {
			t.Fatalf("kill member %d: %v", id, err)
		}
		if err := cmd.Wait(); err == nil {
			t.Fatalf("member %d unexpectedly exited cleanly after forced termination", id)
		}
		servers[id] = nil
	}
	for id := uint64(1); id <= 3; id++ {
		start(id, "bootstrap")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	waitForCluster := func(want string) {
		t.Helper()
		for {
			cmd := exec.Command(os.Args[0], "-test.run=^TestClusterProcessHelper$", "--", "read")
			cmd.Env = helperEnv
			var output bytes.Buffer
			cmd.Stdout, cmd.Stderr = &output, &output
			err := cmd.Run()
			if err == nil && strings.Contains(output.String(), want) {
				return
			}
			if err != nil && strings.Contains(output.String(), "expected one value") {
				return // Range succeeded before the initial write.
			}
			select {
			case <-ctx.Done():
				t.Fatalf("cluster did not serve remote Range: err=%v output=%s: %v", err, output.String(), ctx.Err())
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
	waitForCluster("")
	runClient := func(args ...string) string {
		t.Helper()
		cmdArgs := append([]string{"-test.run=^TestClusterProcessHelper$", "--"}, args...)
		cmd := exec.Command(os.Args[0], cmdArgs...)
		cmd.Env = helperEnv
		var output bytes.Buffer
		cmd.Stdout, cmd.Stderr = &output, &output
		if err := cmd.Run(); err != nil {
			t.Fatalf("client %v failed: %v\n%s", args, err, output.String())
		}
		return output.String()
	}
	runClient("put", "survives-member-restart")
	stop(2)
	if output := runClient("read"); !strings.Contains(output, "survives-member-restart") {
		t.Fatalf("read after stopping member 2: %s", output)
	}
	runClient("put", "write-committed-with-member-down")
	if output := runClient("read"); !strings.Contains(output, "write-committed-with-member-down") {
		t.Fatalf("read quorum write while member 2 is stopped: %s", output)
	}
	start(2, "recover")
	waitForLog(t, filepath.Join(logs, "node-2-recover.log"), "member 2 ready")
	if output := runClient("read-node", "2"); !strings.Contains(output, "write-committed-with-member-down") {
		t.Fatalf("read after recovering member 2: %s", output)
	}
	crash(3)
	if output := runClient("read"); !strings.Contains(output, "write-committed-with-member-down") {
		t.Fatalf("read after abrupt member 3 termination: %s", output)
	}
	runClient("put", "write-committed-after-process-crash")
	if output := runClient("read"); !strings.Contains(output, "write-committed-after-process-crash") {
		t.Fatalf("read write committed after member 3 termination: %s", output)
	}
	start(3, "recover")
	waitForLog(t, filepath.Join(logs, "node-3-recover.log"), "member 3 ready")
	if output := runClient("read-node", "3"); !strings.Contains(output, "write-committed-after-process-crash") {
		t.Fatalf("read after recovering abruptly terminated member 3: %s", output)
	}
	for _, id := range []uint64{1, 2, 3} {
		if servers[id] != nil {
			stop(id)
		}
	}
}

func freeProcessAddresses(t *testing.T, count int) []string {
	t.Helper()
	listeners := make([]net.Listener, 0, count)
	addresses := make([]string, 0, count)
	for i := 0; i < count; i++ {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			for _, opened := range listeners {
				_ = opened.Close()
			}
			t.Fatal(err)
		}
		listeners = append(listeners, listener)
		addresses = append(addresses, listener.Addr().String())
	}
	for _, listener := range listeners {
		_ = listener.Close()
	}
	return addresses
}

func waitForLog(t *testing.T, path, want string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		contents, _ := os.ReadFile(path)
		if strings.Contains(string(contents), want) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	contents, _ := os.ReadFile(path)
	t.Fatalf("log %s did not contain %q: %s", path, want, contents)
}
