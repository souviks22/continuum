package main

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/souviks22/continuum"
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

// TestMixedRevisionRollingUpgradeProcess builds the last pre-barrier revision
// and the current revision as separate binaries, then runs them in one TCP
// Raft cluster. Set CONTINUUM_RUN_MIXED_REVISION_TEST=1 to run this expensive,
// history-dependent release check.
func TestMixedRevisionRollingUpgradeProcess(t *testing.T) {
	if os.Getenv("CONTINUUM_RUN_MIXED_REVISION_TEST") != "1" {
		t.Skip("set CONTINUUM_RUN_MIXED_REVISION_TEST=1 to build and run both revisions")
	}
	const legacyRevision = "49fd507"
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate test source")
	}
	repoRoot, err := filepath.Abs(filepath.Join(filepath.Dir(sourceFile), "../.."))
	if err != nil {
		t.Fatal(err)
	}
	buildDir := t.TempDir()
	legacyDir := filepath.Join(buildDir, "legacy-source")
	if err := os.MkdirAll(legacyDir, 0o700); err != nil {
		t.Fatal(err)
	}
	archive := exec.Command("git", "archive", legacyRevision)
	archive.Dir = repoRoot
	archiveData, err := archive.Output()
	if err != nil {
		t.Fatalf("read legacy revision %s: %v", legacyRevision, err)
	}
	untar := exec.Command("tar", "-x", "-C", legacyDir)
	untar.Stdin = bytes.NewReader(archiveData)
	if output, err := untar.CombinedOutput(); err != nil {
		t.Fatalf("extract legacy source: %v\n%s", err, output)
	}
	legacyBin := filepath.Join(buildDir, "cluster-n")
	currentBin := filepath.Join(buildDir, "cluster-n-plus-1")
	build := func(dir, output string) {
		t.Helper()
		cmd := exec.Command("go", "build", "-o", output, "./examples/cluster-process")
		cmd.Dir = dir
		if result, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("build %s: %v\n%s", dir, err, result)
		}
	}
	build(legacyDir, legacyBin)
	build(repoRoot, currentBin)

	addresses := freeProcessAddresses(t, 4)
	helpers := []string{
		"CONTINUUM_CLUSTER_PROCESS_CLIENT_ADDR=" + addresses[0],
		"CONTINUUM_CLUSTER_PROCESS_MEMBER_1_ADDR=" + addresses[1],
		"CONTINUUM_CLUSTER_PROCESS_MEMBER_2_ADDR=" + addresses[2],
		"CONTINUUM_CLUSTER_PROCESS_MEMBER_3_ADDR=" + addresses[3],
	}
	dataRoot := filepath.Join(buildDir, "members")
	if err := os.MkdirAll(dataRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	type process struct {
		cmd *exec.Cmd
		log *os.File
	}
	servers := map[uint64]*process{}
	stop := func(id uint64) {
		t.Helper()
		p := servers[id]
		if p == nil {
			return
		}
		if p.cmd.ProcessState == nil {
			if err := p.cmd.Process.Signal(os.Interrupt); err != nil {
				t.Fatalf("stop member %d: %v", id, err)
			}
			if err := p.cmd.Wait(); err != nil {
				t.Fatalf("member %d exited with error: %v", id, err)
			}
		}
		_ = p.log.Close()
		servers[id] = nil
	}
	start := func(id uint64, binary, mode, suffix string) {
		t.Helper()
		logPath := filepath.Join(buildDir, fmt.Sprintf("node-%d-%s.log", id, suffix))
		log, err := os.Create(logPath)
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(binary, "serve", fmt.Sprint(id), filepath.Join(dataRoot, fmt.Sprintf("node-%d", id)), mode)
		cmd.Env = append(os.Environ(), helpers...)
		cmd.Stdout, cmd.Stderr = log, log
		if err := cmd.Start(); err != nil {
			_ = log.Close()
			t.Fatalf("start member %d (%s): %v", id, suffix, err)
		}
		servers[id] = &process{cmd: cmd, log: log}
		t.Cleanup(func() { stop(id) })
	}
	for id := uint64(1); id <= 3; id++ {
		binary := legacyBin
		if id == 3 {
			binary = currentBin
		}
		start(id, binary, "bootstrap", "initial")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	for id := uint64(1); id <= 3; id++ {
		waitForLog(t, filepath.Join(buildDir, fmt.Sprintf("node-%d-initial.log", id)), fmt.Sprintf("member %d ready", id))
	}
	peers := map[uint64]continuum.Peer{
		1: {Name: memberNames[1], Address: addresses[1]},
		2: {Name: memberNames[2], Address: addresses[2]},
		3: {Name: memberNames[3], Address: addresses[3]},
	}
	remote, err := continuum.NewRemoteClient(continuum.RemoteClientConfig{
		ID: clientID, Name: clientName, ListenAddr: addresses[0], Peers: peers, InsecureLocal: true,
	}, continuum.ClientOptions{Timeout: 4 * time.Second, MaxAttempts: 5, ReadMaxAttempts: 5})
	if err != nil {
		t.Fatal(err)
	}
	defer remote.Close()

	reports, err := remote.Compatibility(ctx)
	if err != nil {
		t.Fatal(err)
	}
	legacyReports := 0
	for _, report := range reports {
		if report.ID < 3 && report.CompatibilityReportVersion == 2 {
			legacyReports++
		}
	}
	if legacyReports != 2 {
		t.Fatalf("expected two N report-v2 peers, got %+v", reports)
	}
	requirements, err := continuum.CompatibilityRequirementsForLevel(continuum.CompatibilityLevelLeases)
	if err != nil {
		t.Fatal(err)
	}
	check, err := remote.CheckCompatibility(ctx, requirements)
	if err != nil || check.Ready {
		t.Fatalf("mixed N/N+1 preflight ready=%v issues=%+v err=%v", check.Ready, check.Issues, err)
	}

	// More than the default snapshot interval forces N to write a real on-disk
	// state-machine snapshot while N and N+1 are members of the same cluster.
	const snapshotWrites = 10020
	for i := 0; i < snapshotWrites; i++ {
		key := []byte(fmt.Sprintf("mixed/revision/%05d", i))
		if _, err := remote.Txn(ctx, continuum.Put(key, []byte("committed-by-N+1-client"))); err != nil {
			t.Fatalf("mixed-version write %d/%d: %v", i+1, snapshotWrites, err)
		}
	}
	for id := uint64(1); id <= 3; id++ {
		// Bootstrap creates snapshot index 1; the default interval is 10,000
		// applied entries, so require a data-bearing snapshot beyond that point.
		waitForSnapshotIndex(t, filepath.Join(dataRoot, fmt.Sprintf("node-%d", id), "snap"), 10001)
	}
	readKey := []byte(fmt.Sprintf("mixed/revision/%05d", snapshotWrites-1))
	if got, err := remote.Range(ctx, readKey, nil, continuum.RangeOptions{}); err != nil || len(got.KVs) != 1 || string(got.KVs[0].Value) != "committed-by-N+1-client" {
		t.Fatalf("mixed-version read before restart=%+v err=%v", got, err)
	}

	// Upgrade each N process in place. N+1 must recover its state from an N
	// snapshot plus the durable Raft directory, while N remains in the quorum.
	for id := uint64(1); id <= 2; id++ {
		stop(id)
		start(id, currentBin, "recover", "upgraded")
		waitForLog(t, filepath.Join(buildDir, fmt.Sprintf("node-%d-upgraded.log", id)), fmt.Sprintf("member %d ready", id))
		got, err := remote.Range(ctx, readKey, nil, continuum.RangeOptions{})
		if err != nil || len(got.KVs) != 1 || string(got.KVs[0].Value) != "committed-by-N+1-client" {
			t.Fatalf("read after upgrading node %d=%+v err=%v", id, got, err)
		}
		if id == 1 {
			// Before activation, the N binary can still recover the N-format
			// snapshot plus the tail written by the N+1 process.
			stop(id)
			start(id, legacyBin, "recover", "rollback-before-activation")
			waitForLog(t, filepath.Join(buildDir, "node-1-rollback-before-activation.log"), "member 1 ready")
			got, err := remote.Range(ctx, readKey, nil, continuum.RangeOptions{})
			if err != nil || len(got.KVs) != 1 || string(got.KVs[0].Value) != "committed-by-N+1-client" {
				t.Fatalf("read after pre-activation rollback=%+v err=%v", got, err)
			}
			stop(id)
			start(id, currentBin, "recover", "upgraded-again")
			waitForLog(t, filepath.Join(buildDir, "node-1-upgraded-again.log"), "member 1 ready")
		}
	}
	check, err = remote.CheckCompatibility(ctx, requirements)
	if err != nil || !check.Ready {
		t.Fatalf("post-upgrade preflight ready=%v issues=%+v err=%v", check.Ready, check.Issues, err)
	}
	if err := remote.ActivateCompatibility(ctx, continuum.CompatibilityLevelLeases); err != nil {
		t.Fatalf("activation after every process runs N+1: %v", err)
	}
	// A rollback after activation must fail before N opens the durable state:
	// the committed activation forces snapshot format 5 on every current member.
	stop(1)
	rollbackLogPath := filepath.Join(buildDir, "node-1-rollback-after-activation.log")
	rollbackLog, err := os.Create(rollbackLogPath)
	if err != nil {
		t.Fatal(err)
	}
	rollback := exec.Command(legacyBin, "serve", "1", filepath.Join(dataRoot, "node-1"), "recover")
	rollback.Env = append(os.Environ(), helpers...)
	rollback.Stdout, rollback.Stderr = rollbackLog, rollbackLog
	if err := rollback.Start(); err != nil {
		_ = rollbackLog.Close()
		t.Fatal(err)
	}
	rollbackErr := rollback.Wait()
	_ = rollbackLog.Close()
	rollbackOutput, readErr := os.ReadFile(rollbackLogPath)
	if rollbackErr == nil || readErr != nil || !strings.Contains(string(rollbackOutput), "unsupported snapshot version 5") {
		t.Fatalf("post-activation N rollback err=%v readErr=%v output=%s", rollbackErr, readErr, rollbackOutput)
	}
	start(1, currentBin, "recover", "recovered-after-rejected-rollback")
	waitForLog(t, filepath.Join(buildDir, "node-1-recovered-after-rejected-rollback.log"), "member 1 ready")
	if got, err := remote.Range(ctx, []byte("mixed/after-activation"), nil, continuum.RangeOptions{}); err != nil || len(got.KVs) != 0 {
		t.Fatalf("state after rejected downgrade=%+v err=%v", got, err)
	}
	if _, err := remote.Txn(ctx, continuum.Put([]byte("mixed/after-activation"), []byte("ok"))); err != nil {
		t.Fatalf("write after activation: %v", err)
	}
	for id := uint64(1); id <= 3; id++ {
		stop(id)
	}
}

func waitForSnapshotIndex(t *testing.T, dir string, minimum uint64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		files, _ := filepath.Glob(filepath.Join(dir, "snap-*.snap"))
		for _, file := range files {
			name := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(file), "snap-"), ".snap")
			index, err := strconv.ParseUint(name, 10, 64)
			if err == nil && index >= minimum {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	files, _ := filepath.Glob(filepath.Join(dir, "snap-*.snap"))
	t.Fatalf("no N-created Raft snapshot at index >= %d in %s: %v", minimum, dir, files)
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
