package storage

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type JSONCodec struct{}

func (c *JSONCodec) Encode(record Record) ([]byte, error) {
	return json.Marshal(record)
}

func (c *JSONCodec) Decode(data []byte) (Record, error) {
	var record Record
	err := json.Unmarshal(data, &record)
	return record, err
}

func newTestStore(t *testing.T) (*MemStore, func()) {
	tmpFile, err := os.CreateTemp("", "wal-*")
	require.NoError(t, err)

	path := tmpFile.Name()
	tmpFile.Close()

	wal, err := NewWAL(path)
	require.NoError(t, err)

	codec := &JSONCodec{}

	store, err := NewMemStore(wal, codec)
	require.NoError(t, err)

	cleanup := func() {
		require.NoError(t, store.Close())
		require.NoError(t, os.Remove(path))
	}

	return store, cleanup
}

func TestMemStore_PutGet(t *testing.T) {
	store, cleanup := newTestStore(t)
	defer cleanup()

	rec := Record{
		Key:       "k1",
		Value:     []byte("v1"),
		Version:   1,
		Timestamp: time.Now().UnixNano(),
	}

	_, err := store.Put(rec, 0)
	require.NoError(t, err)

	entry, err := store.Get("k1")
	require.NoError(t, err)

	require.Equal(t, "v1", string(entry.Value))
}

func TestMemStore_VersionConflict(t *testing.T) {
	store, cleanup := newTestStore(t)
	defer cleanup()

	rec1 := Record{
		Key:       "k",
		Value:     []byte("v1"),
		Version:   1,
		Timestamp: time.Now().UnixNano(),
	}

	_, err := store.Put(rec1, 0)
	require.NoError(t, err)

	rec2 := Record{
		Key:       "k",
		Value:     []byte("v2"),
		Version:   2,
		Timestamp: time.Now().UnixNano(),
	}

	_, err = store.Put(rec2, 999)
	require.ErrorIs(t, err, ErrVersionConflict)
}

func TestMemStore_Delete(t *testing.T) {
	store, cleanup := newTestStore(t)
	defer cleanup()

	rec := Record{
		Key:       "k",
		Value:     []byte("v"),
		Version:   1,
		Timestamp: time.Now().UnixNano(),
	}

	_, err := store.Put(rec, 0)
	require.NoError(t, err)

	_, err = store.Delete("k", 2, time.Now().UnixNano())
	require.NoError(t, err)

	_, err = store.Get("k")
	require.ErrorIs(t, err, ErrNotFound)
}

func TestMemStore_VersionOrdering(t *testing.T) {
	store, cleanup := newTestStore(t)
	defer cleanup()

	now := time.Now().UnixNano()

	rec1 := Record{
		Key:       "k",
		Value:     []byte("v1"),
		Version:   2,
		Timestamp: now,
	}

	rec2 := Record{
		Key:       "k",
		Value:     []byte("v2"),
		Version:   1,
		Timestamp: now,
	}

	_, err := store.Put(rec1, 0)
	require.NoError(t, err)

	_, err = store.Put(rec2, 0)
	require.NoError(t, err)

	entry, err := store.Get("k")
	require.NoError(t, err)

	require.Equal(t, "v1", string(entry.Value))
}

func TestMemStore_TimestampTieBreak(t *testing.T) {
	store, cleanup := newTestStore(t)
	defer cleanup()

	rec1 := Record{
		Key:       "k",
		Value:     []byte("v1"),
		Version:   1,
		Timestamp: 100,
	}

	rec2 := Record{
		Key:       "k",
		Value:     []byte("v2"),
		Version:   1,
		Timestamp: 200,
	}

	_, err := store.Put(rec1, 0)
	require.NoError(t, err)

	_, err = store.Put(rec2, 0)
	require.NoError(t, err)

	entry, err := store.Get("k")
	require.NoError(t, err)

	require.Equal(t, "v2", string(entry.Value))
}

func TestMemStore_LSNTieBreak(t *testing.T) {
	store, cleanup := newTestStore(t)
	defer cleanup()

	rec := Record{
		Key:       "k",
		Value:     []byte("v1"),
		Version:   1,
		Timestamp: 100,
	}

	e1, err := store.Put(rec, 0)
	require.NoError(t, err)

	e2, err := store.Put(rec, 0)
	require.NoError(t, err)

	require.Greater(t, e2.LSN, e1.LSN)

	entry, err := store.Get("k")
	require.NoError(t, err)

	require.Equal(t, e2.LSN, entry.LSN)
}

