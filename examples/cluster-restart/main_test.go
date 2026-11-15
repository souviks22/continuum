package main

import (
	"path/filepath"
	"testing"
)

func TestClusterWriteThenFullRestartAndRead(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "cluster-data")
	if err := run([]string{"write", dir}); err != nil {
		t.Fatalf("write process: %v", err)
	}
	if err := run([]string{"read", dir}); err != nil {
		t.Fatalf("read process after full cluster restart: %v", err)
	}
}
