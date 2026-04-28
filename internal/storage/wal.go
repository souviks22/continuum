package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const (
	Magic = 0xCAFEBABE
)

var (
	ErrCorruptRecord = errors.New("corrupt WAL record")
)

type WAL struct {
	file      *os.File
	bufWriter *bufio.Writer
	writeCh   chan *WriteRequest
	stopCh    chan struct{}
	wg        sync.WaitGroup
	nextLSN   uint64
}

type WriteRequest struct {
	payload    []byte
	responseCh chan error
}

func NewWAL(filePath string) (*WAL, error) {
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)

	if err != nil {
		return nil, err
	}

	w := &WAL{
		file:      f,
		bufWriter: bufio.NewWriter(f),
		writeCh:   make(chan *WriteRequest, 1024),
		stopCh:    make(chan struct{}),
		nextLSN:   0,
	}

	w.wg.Add(1)
	go w.writerLoop()
	return w, nil
}

func (w *WAL) Append(payload []byte) error {
	req := &WriteRequest{
		payload:    payload,
		responseCh: make(chan error, 1),
	}

	w.writeCh <- req
	return <-req.responseCh
}

func (w *WAL) writerLoop() {
	defer w.wg.Done()

	const maxBatchSize = 1024
	const maxDelay = 5 * time.Millisecond

	for {
		var batch []*WriteRequest

		select {
		case req := <-w.writeCh:
			batch = append(batch, req)
		case <-w.stopCh:
			return
		}

		timeout := time.After(maxDelay)

	collect:
		for len(batch) < maxBatchSize {
			select {
			case req := <-w.writeCh:
				batch = append(batch, req)
			case <-timeout:
				break collect
			case <-w.stopCh:
				return
			}
		}

		err := w.writeBatch(batch)

		if err == nil {
			w.bufWriter.Flush()
			err = w.file.Sync()
		}

		for _, req := range batch {
			req.responseCh <- err
		}
	}
}

func (w *WAL) writeBatch(batch []*WriteRequest) error {
	for _, req := range batch {
		payload := req.payload
		length := uint32(len(payload))

		lsn := atomic.AddUint64(&w.nextLSN, 1)
		lsnBuf := make([]byte, 8)
		binary.BigEndian.PutUint64(lsnBuf, lsn)

		crc := crc32.NewIEEE()
		crc.Write(lsnBuf)
		crc.Write(payload)
		checksum := crc.Sum32()

		header := make([]byte, 20)
		binary.BigEndian.PutUint32(header[0:4], Magic)
		binary.BigEndian.PutUint32(header[4:8], length)
		binary.BigEndian.PutUint32(header[8:12], checksum)
		binary.BigEndian.PutUint64(header[12:20], lsn)

		if _, err := w.bufWriter.Write(header); err != nil {
			return err
		}
		if _, err := w.bufWriter.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

func (w *WAL) Close() error {
	close(w.stopCh)
	w.wg.Wait()
	w.bufWriter.Flush()
	return w.file.Close()
}

func Recover(walFilePath string, apply func(uint64, []byte) error) error {
	f, err := os.Open(walFilePath)
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
	header := make([]byte, 20)

	if _, err := io.ReadFull(reader, header); err != nil {
		return 0, nil, err
	}

	magic := binary.BigEndian.Uint32(header[0:4])
	if magic != Magic {
		return 0, nil, ErrCorruptRecord
	}

	length := binary.BigEndian.Uint32(header[4:8])
	if length == 0 || length > (1<<20) {
		return 0, nil, ErrCorruptRecord
	}

	checksum := binary.BigEndian.Uint32(header[8:12])
	lsn := binary.BigEndian.Uint64(header[12:20])
	lsnBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(lsnBuf, lsn)

	payload := make([]byte, length)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return 0, nil, err
	}

	crc := crc32.NewIEEE()
	crc.Write(lsnBuf)
	crc.Write(payload)

	if crc.Sum32() != checksum {
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
