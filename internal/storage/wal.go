package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"time"
)

const Magic = 0xCAFEBABE

var (
	ErrCorruptRecord = errors.New("corrupt WAL record")
	ErrWALClosed     = errors.New("wal is closed")
	table            = crc32.MakeTable(crc32.IEEE)
)

type WAL struct {
	file      *os.File
	bufWriter *bufio.Writer
	writeCh   chan *WriteRequest
	closeCh   chan struct{}

	maxBatchSize int
	maxDelay     time.Duration

	timer   *time.Timer
	nextLSN uint64

	wg     sync.WaitGroup
	mu     sync.Mutex
	closed bool
}

type WriteRequest struct {
	payload    []byte
	responseCh chan error
}

func NewWAL(filePath string) (*WAL, error) {
	var lastLSN uint64

	if err := Recover(filePath, func(lsn uint64, _ []byte) error {
		lastLSN = lsn
		return nil
	}); err != nil {
		return nil, err
	}

	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)

	if err != nil {
		return nil, err
	}

	w := &WAL{
		file:         f,
		bufWriter:    bufio.NewWriterSize(f, 1<<20),
		writeCh:      make(chan *WriteRequest, 4096),
		closeCh:      make(chan struct{}),
		maxBatchSize: 1024,
		maxDelay:     5 * time.Millisecond,
		timer:        time.NewTimer(time.Hour),
		nextLSN:      lastLSN,
	}

	w.wg.Add(1)
	w.timer.Stop()
	go w.writerLoop()

	return w, nil
}

func (w *WAL) Append(payload []byte) error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return ErrWALClosed
	}
	w.mu.Unlock()

	req := &WriteRequest{
		payload:    payload,
		responseCh: make(chan error, 1),
	}

	w.writeCh <- req
	return <-req.responseCh
}

func (w *WAL) writerLoop() {
	defer w.wg.Done()

	batch := make([]*WriteRequest, 0, w.maxBatchSize)
	for {
		select {
		case req := <-w.writeCh:
			batch = append(batch, req)
		case <-w.closeCh:
			w.drainAndExit()
			return
		}

		if !w.timer.Stop() {
			select {
			case <-w.timer.C:
			default:
			}
		}
		w.timer.Reset(w.maxDelay)

	collect:
		for len(batch) < w.maxBatchSize {
			select {
			case req := <-w.writeCh:
				batch = append(batch, req)

			case <-w.timer.C:
				break collect

			case <-w.closeCh:
				err := w.writeBatch(batch)
				w.syncBatch(batch, err)
				w.drainAndExit()
				return
			}
		}

		err := w.writeBatch(batch)
		w.syncBatch(batch, err)
		batch = batch[:0]
	}
}

func (w *WAL) writeBatch(batch []*WriteRequest) error {
	var header [20]byte
	var lsnBuf [8]byte

	for _, req := range batch {
		payload := req.payload
		length := uint32(len(payload))

		w.nextLSN++
		lsn := w.nextLSN
		binary.BigEndian.PutUint64(lsnBuf[:], lsn)

		crc := crc32.Update(0, table, lsnBuf[:])
		crc = crc32.Update(crc, table, payload)

		binary.BigEndian.PutUint32(header[0:4], Magic)
		binary.BigEndian.PutUint32(header[4:8], length)
		binary.BigEndian.PutUint32(header[8:12], crc)
		binary.BigEndian.PutUint64(header[12:20], lsn)

		if _, err := w.bufWriter.Write(header[:]); err != nil {
			return err
		}
		if _, err := w.bufWriter.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

func (w *WAL) syncBatch(batch []*WriteRequest, err error) {
	if err == nil {
		if e := w.bufWriter.Flush(); e != nil {
			err = e
		}
	}
	if err == nil {
		if e := w.file.Sync(); e != nil {
			err = e
		}
	}

	for _, req := range batch {
		req.responseCh <- err
	}
}

func (w *WAL) drainAndExit() {
	batch := make([]*WriteRequest, 0, w.maxBatchSize)
	for {
		select {
		case req := <-w.writeCh:
			batch = append(batch, req)

			if len(batch) >= w.maxBatchSize {
				err := w.writeBatch(batch)
				w.syncBatch(batch, err)
				batch = batch[:0]
			}

		default:
			if len(batch) > 0 {
				err := w.writeBatch(batch)
				w.syncBatch(batch, err)
			}
			return
		}
	}
}

func (w *WAL) Close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	close(w.closeCh)
	w.mu.Unlock()

	w.wg.Wait()
	return w.file.Close()
}

func Recover(walFilePath string, apply func(uint64, []byte) error) error {
	f, err := os.OpenFile(walFilePath, os.O_CREATE|os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	reader := bufio.NewReader(f)

	for {
		lsn, record, err := readRecord(reader)
		if err == io.EOF {
			return nil
		}

		if err != nil {
			err := resync(reader)
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			continue
		}

		if err := apply(lsn, record); err != nil {
			return err
		}
	}
}

func readRecord(reader *bufio.Reader) (uint64, []byte, error) {
	var header [20]byte

	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return 0, nil, err
	}

	magic := binary.BigEndian.Uint32(header[0:4])
	if magic != Magic {
		return 0, nil, ErrCorruptRecord
	}

	length := binary.BigEndian.Uint32(header[4:8])
	if length > (1<<20) {
		return 0, nil, ErrCorruptRecord
	}

	checksum := binary.BigEndian.Uint32(header[8:12])
	lsn := binary.BigEndian.Uint64(header[12:20])
	var lsnBuf [8]byte
	binary.BigEndian.PutUint64(lsnBuf[:], lsn)

	payload := make([]byte, length)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return 0, nil, err
	}

	crc := crc32.Update(0, table, lsnBuf[:])
	crc = crc32.Update(crc, table, payload)

	if crc != checksum {
		return 0, nil, ErrCorruptRecord
	}

	return lsn, payload, nil
}

func resync(reader *bufio.Reader) error {
	for {
		peek, err := reader.Peek(4)
		if err != nil {
			return err
		}

		if binary.BigEndian.Uint32(peek) == Magic {
			return nil
		}

		reader.ReadByte()
	}
}
