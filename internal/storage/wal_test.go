package storage

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWAL_BasicWriteRecover(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	wal, err := NewWAL(path)
	require.NoError(t, err)

	inputs := []string{"A", "B", "C"}
	for _, in := range inputs {
		require.NoError(t, wal.Append([]byte(in)))
	}

	require.NoError(t, wal.Close())

	recovered := make([]string, 0)
	err = Recover(path, func(_ uint64, record []byte) error {
		recovered = append(recovered, string(record))
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, inputs, recovered)
}

func TestWAL_ConcurrentIntegrity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	wal, _ := NewWAL(path)

	var n uint64 = 1000
	var wg sync.WaitGroup

	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			wal.Append([]byte("x"))
		}()
	}

	wg.Wait()
	wal.Close()

	var lsn uint64
	Recover(path, func(nextLSN uint64, _ []byte) error {
		require.Greater(t, nextLSN, lsn, "LSN must be strictly increasing")
		lsn = nextLSN
		return nil
	})

	require.Equal(t, n, lsn)
}

func TestWAL_CorruptionOrPartialWriteRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	wal, _ := NewWAL(path)

	inputs := []string{"A", "B", "C", "D", "E"}
	for _, in := range inputs {
		wal.Append([]byte(in))
	}

	wal.Close()

	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	require.NoError(t, err)
	info, _ := f.Stat()
	size := info.Size()
	corruption := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
	_, err = f.WriteAt(corruption, size-24)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	wal, _ = NewWAL(path)

	inputs = []string{"F", "G", "H"}
	for _, in := range inputs {
		err := wal.Append([]byte(in))
		require.NoError(t, err)
	}

	wal.Close()

	f, err = os.OpenFile(path, os.O_RDWR, 0644)
	require.NoError(t, err)
	info, _ = f.Stat()
	size = info.Size()
	err = f.Truncate(size - 4)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	recovered := make([]string, 0)
	Recover(path, func(_ uint64, record []byte) error {
		recovered = append(recovered, string(record))
		return nil
	})

	require.Equal(t, recovered, []string{"A", "B", "C", "F", "G"})
}

func TestWAL_LargePayload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	wal, _ := NewWAL(path)

	large := make([]byte, 1<<20)
	rand.Read(large)

	wal.Append(large)
	wal.Close()

	recovered := make([][]byte, 0)
	Recover(path, func(_ uint64, data []byte) error {
		recovered = append(recovered, data)
		return nil
	})

	require.Equal(t, large, recovered[0])
}

func TestWAL_AppendAfterClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	wal, err := NewWAL(path)
	require.NoError(t, err)

	require.NoError(t, wal.Close())

	err = wal.Append([]byte("should-fail"))
	require.ErrorIs(t, err, ErrWALClosed)
}

func TestWAL_LSNContinuityAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	wal, err := NewWAL(path)
	require.NoError(t, err)

	initial := []string{"A", "B", "C"}
	for _, v := range initial {
		require.NoError(t, wal.Append([]byte(v)))
	}
	require.NoError(t, wal.Close())

	wal, err = NewWAL(path)
	require.NoError(t, err)

	next := []string{"D", "E"}
	for _, v := range next {
		require.NoError(t, wal.Append([]byte(v)))
	}
	require.NoError(t, wal.Close())

	var lastLSN uint64 = 0
	var recovered []string

	err = Recover(path, func(lsn uint64, record []byte) error {
		require.Greater(t, lsn, lastLSN, "LSN must strictly increase across restart")
		lastLSN = lsn
		recovered = append(recovered, string(record))
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, []string{"A", "B", "C", "D", "E"}, recovered)
}

func TestWAL_CloseDuringConcurrentAppends(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	wal, err := NewWAL(path)
	require.NoError(t, err)

	var (
		total = 1000
		wg    sync.WaitGroup
		mu    sync.Mutex
		ok    = make(map[string]struct{})
	)

	for i := range total {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			data := []byte(fmt.Sprintf("rec-%d", i))
			err := wal.Append(data)

			if err == nil {
				mu.Lock()
				ok[string(data)] = struct{}{}
				mu.Unlock()
			}
		}(i)
	}

	require.NoError(t, wal.Close())

	wg.Wait()

	recovered := make(map[string]struct{})
	err = Recover(path, func(_ uint64, record []byte) error {
		recovered[string(record)] = struct{}{}
		return nil
	})
	require.NoError(t, err)

	require.Equal(t, ok, recovered)
}

func TestWAL_FlushOnTimeout(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	wal, err := NewWAL(path)
	require.NoError(t, err)

	require.NoError(t, wal.Append([]byte("delayed-write")))

	time.Sleep(10 * time.Millisecond)

	require.NoError(t, wal.Close())

	var recovered []string
	err = Recover(path, func(_ uint64, record []byte) error {
		recovered = append(recovered, string(record))
		return nil
	})
	require.NoError(t, err)

	require.Equal(t, []string{"delayed-write"}, recovered)
}

func TestWAL_EmptyPayloadBehavior(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	wal, err := NewWAL(path)
	require.NoError(t, err)

	// Append empty payload
	err = wal.Append([]byte{})
	require.NoError(t, err)

	require.NoError(t, wal.Close())

	// Recover
	var recovered [][]byte
	err = Recover(path, func(_ uint64, record []byte) error {
		recovered = append(recovered, record)
		return nil
	})

	// Decide expected behavior (see below)
	require.NoError(t, err)

	// Option A (recommended): empty payload is valid
	require.Len(t, recovered, 1)
	require.Equal(t, []byte{}, recovered[0])
}
