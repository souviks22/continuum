package main

import (
	"path/filepath"
	"testing"
)

func TestWriteThenReopenAndRead(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "node-data")
	if err := run([]string{"write", dir}); err != nil {
		t.Fatalf("write process: %v", err)
	}
	if err := run([]string{"read", dir}); err != nil {
		t.Fatalf("read process after restart: %v", err)
	}
}
