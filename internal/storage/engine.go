package storage

type Record struct {
	Key       string
	Value     []byte
	Version   uint64
	Tombstone bool
	Timestamp int64
}

