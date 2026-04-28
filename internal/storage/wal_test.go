package storage

import (
	"crypto/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"

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

	n, lsn := uint64(1000), uint64(0)
	wg := &sync.WaitGroup{}

	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			wal.Append([]byte("x"))
		}()
	}

	wg.Wait()
	wal.Close()

	Recover(path, func(nextLsn uint64, _ []byte) error {
		require.Greater(t, nextLsn, lsn, "LSN must be strictly increasing")
		lsn = nextLsn
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
