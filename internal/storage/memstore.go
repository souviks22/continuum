package storage

import (
	"errors"
	"sync"
)

var (
	ErrNotFound        = errors.New("not found")
	ErrVersionConflict = errors.New("version conflict")
)

type Entry struct {
	Record
	LSN uint64
}

type Codec interface {
	Encode(record Record) ([]byte, error)
	Decode(data []byte) (Record, error)
}

type MemStore struct {
	mu      sync.RWMutex
	data    map[string]Entry
	wal     *WAL
	codec   Codec
	lastLSN uint64
}

func NewMemStore(wal *WAL, codec Codec) (*MemStore, error) {
	m := &MemStore{
		data:  make(map[string]Entry),
		wal:   wal,
		codec: codec,
	}

	if err := Recover(wal.file.Name(), func(lsn uint64, payload []byte) error {
		record, err := codec.Decode(payload)
		if err != nil {
			return err
		}
		m.mu.Lock()
		defer m.mu.Unlock()
		_, err = m.applyRecordLocked(record, lsn)
		return err
	}); err != nil {
		return nil, err
	}

	return m, nil
}

func (m *MemStore) Get(key string) (Entry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entry, exists := m.data[key]
	if !exists || entry.Tombstone {
		return Entry{}, ErrNotFound
	}

	return entry, nil
}

func (m *MemStore) Put(record Record, expectedVersion uint64) (Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	current, exists := m.data[record.Key]
	if expectedVersion != 0 && exists && current.Version != expectedVersion {
		return Entry{}, ErrVersionConflict
	}

	payload, err := m.codec.Encode(record)
	if err != nil {
		return Entry{}, err
	}

	lsn, err := m.wal.Append(payload)
	if err != nil {
		return Entry{}, err
	}

	return m.applyRecordLocked(record, lsn)
}

func (m *MemStore) Delete(key string, version uint64, ts int64) (Entry, error) {
	record := Record{
		Key:       key,
		Version:   version,
		Tombstone: true,
		Timestamp: ts,
	}
	return m.Put(record, 0)
}

func (m *MemStore) applyRecordLocked(record Record, lsn uint64) (Entry, error) {
	current, exists := m.data[record.Key]

	if exists && current.LSN == lsn {
		return current, nil
	}

	if exists && (current.Version > record.Version ||
		(current.Version == record.Version &&
			(current.Timestamp > record.Timestamp ||
				(current.Timestamp == record.Timestamp &&
					current.LSN > lsn)))) {
		return current, nil
	}

	entry := Entry{
		Record: record,
		LSN:    lsn,
	}

	m.data[record.Key] = entry
	if lsn > m.lastLSN {
		m.lastLSN = lsn
	}

	return entry, nil
}

func (m *MemStore) Close() error {
	clear(m.data)
	return m.wal.Close()
}
