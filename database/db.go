package datastore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	dataFileName    = "data"
	bufferSize      = 8192
	defaultFileMode = 0644
	minSegments     = 3
)

const tombstoneMarker = "__deleted__"

type keyIndex map[string]int64

type IndexOperation struct {
	isWrite  bool
	key      string
	position int64
	response chan *KeyPosition
}

type WriteOperation struct {
	data     Entry
	response chan error
}

type KeyPosition struct {
	segment  *Segment
	position int64
}

type Datastore struct {
	activeFile      *os.File
	activeFilePath  string
	currentOffset   int64
	directory       string
	maxSegmentSize  int64
	segmentCounter  int
	indexOperations chan IndexOperation
	writeOperations chan WriteOperation
	segments        []*Segment
	fileLock        sync.Mutex
	segmentLock     sync.RWMutex
	closed          bool
	closeMutex      sync.Mutex
	indexWG         sync.WaitGroup
	writeWG         sync.WaitGroup
}

type Segment struct {
	startOffset int64
	keyIndex    keyIndex
	path        string
	mu          sync.RWMutex
}

func createDatabase(directory string, SegmentSize int64) (*Datastore, error) {
	if err := os.MkdirAll(directory, defaultFileMode); err != nil {
		return nil, err
	}

	database := &Datastore{
		segments:        make([]*Segment, 0),
		directory:       directory,
		maxSegmentSize:  SegmentSize,
		indexOperations: make(chan IndexOperation, 100),
		writeOperations: make(chan WriteOperation, 100),
	}

	if err := database.setNewSegment(); err != nil {
		return nil, err
	}

	if err := database.recoverAllSegments(); err != nil && err != io.EOF {
		return nil, err
	}

	database.startIndexHandler()
	database.startWriteHandler()

	return database, nil
}

func (datastore *Datastore) close() error {
	datastore.closeMutex.Lock()
	defer datastore.closeMutex.Unlock()

	if datastore.closed {
		return nil
	}

	datastore.closed = true
	close(datastore.indexOperations)
	close(datastore.writeOperations)

	datastore.indexWG.Wait()
	datastore.writeWG.Wait()

	if datastore.activeFile != nil {
		return datastore.activeFile.Close()
	}
	return nil
}

func (datastore *Datastore) startIndexHandler() {
	datastore.indexWG.Add(1)
	go func() {
		defer datastore.indexWG.Done()
		for operation := range datastore.indexOperations {
			if operation.isWrite {
				datastore.updateIndex(operation.key, operation.position)
			} else {
				segment, pos, err := datastore.findKeyLocation(operation.key)
				if err != nil {
					operation.response <- nil
				} else {
					operation.response <- &KeyPosition{segment, pos}
				}
			}
		}
	}()
}

func (datastore *Datastore) startWriteHandler() {
	datastore.writeWG.Add(1)
	go func() {
		defer datastore.writeWG.Done()
		for operation := range datastore.writeOperations {
			datastore.fileLock.Lock()

			entrySize := operation.data.getLength()
			fileInfo, err := datastore.activeFile.Stat()
			if err != nil {
				operation.response <- err
				datastore.fileLock.Unlock()
				continue
			}

			if fileInfo.Size()+entrySize > datastore.maxSegmentSize {
				if err := datastore.setNewSegment(); err != nil {
					operation.response <- err
					datastore.fileLock.Unlock()
					continue
				}
			}

			currentPos := datastore.currentOffset
			bytesWritten, err := datastore.activeFile.Write(operation.data.encode())
			if err == nil {
				datastore.currentOffset += int64(bytesWritten)
				datastore.updateIndex(operation.data.key, currentPos)
			}

			operation.response <- err
			datastore.fileLock.Unlock()
		}
	}()
}

func (datastore *Datastore) setNewSegment() error {
	newFilePath := datastore.generateFileName()
	file, err := os.OpenFile(newFilePath, os.O_APPEND|os.O_RDWR|os.O_CREATE, defaultFileMode)
	if err != nil {
		return err
	}

	segment := &Segment{
		path:     newFilePath,
		keyIndex: make(keyIndex),
	}

	if datastore.activeFile != nil {
		datastore.activeFile.Close()
	}

	datastore.activeFile = file
	datastore.currentOffset = 0
	datastore.activeFilePath = newFilePath

	datastore.segmentLock.Lock()
	datastore.segments = append(datastore.segments, segment)
	datastore.segmentLock.Unlock()

	if len(datastore.segments) >= minSegments {
		go datastore.compactOldSegments()
	}

	return nil
}

func (datastore *Datastore) generateFileName() string {
	fileName := filepath.Join(datastore.directory, fmt.Sprintf("%s%d", dataFileName, datastore.segmentCounter))
	datastore.segmentCounter++
	return fileName
}

func (datastore *Datastore) compactOldSegments() {
	datastore.segmentLock.Lock()
	defer datastore.segmentLock.Unlock()

	if len(datastore.segments) < minSegments {
		return
	}

	compactedFilePath := datastore.generateFileName()
	compactedFile, err := os.OpenFile(compactedFilePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, defaultFileMode)
	if err != nil {
		return
	}
	defer compactedFile.Close()

	compactedSegment := &Segment{
		path:     compactedFilePath,
		keyIndex: make(keyIndex),
	}

	var writeOffset int64
	keysWritten := make(map[string]bool)

	for i := len(datastore.segments) - 2; i >= 0; i-- {
		segment := datastore.segments[i]
		segment.mu.RLock()

		for key, position := range segment.keyIndex {
			if !keysWritten[key] {
				value, err := segment.readFromSegment(position)
				if err != nil || value == tombstoneMarker {
					continue
				}

				record := Entry{
					key:   key,
					value: value,
				}

				bytesWritten, err := compactedFile.Write(record.encode())
				if err == nil {
					compactedSegment.keyIndex[key] = writeOffset
					writeOffset += int64(bytesWritten)
					keysWritten[key] = true
				}
			}
		}
		segment.mu.RUnlock()
	}

	newSegments := []*Segment{compactedSegment, datastore.segments[len(datastore.segments)-1]}
	for i := 0; i < len(datastore.segments)-1; i++ {
		os.Remove(datastore.segments[i].path)
	}

	datastore.segments = newSegments
}

func (datastore *Datastore) recoverAllSegments() error {
	datastore.segmentLock.RLock()
	defer datastore.segmentLock.RUnlock()

	for _, segment := range datastore.segments {
		if err := datastore.recoverSegmentData(segment); err != nil && err != io.EOF {
			return err
		}
	}
	return nil
}

func (datastore *Datastore) recoverSegmentData(segment *Segment) error {
	file, err := os.Open(segment.path)
	if err != nil {
		return err
	}
	defer file.Close()

	return datastore.processRecovery(file, segment)
}

func (datastore *Datastore) processRecovery(file *os.File, segment *Segment) error {
	var err error
	var buffer [bufferSize]byte
	var currentOffset int64

	reader := bufio.NewReaderSize(file, bufferSize)
	for err == nil {
		var header, data []byte
		var bytesRead int

		header, err = reader.Peek(bufferSize)
		if err == io.EOF {
			if len(header) == 0 {
				return err
			}
		} else if err != nil {
			return err
		}

		if len(header) < 4 {
			return io.EOF
		}

		recordSize := binary.LittleEndian.Uint32(header)
		if recordSize == 0 || recordSize > uint32(bufferSize*10) {
			return fmt.Errorf("invalid record size: %d", recordSize)
		}

		if recordSize < bufferSize {
			data = buffer[:recordSize]
		} else {
			data = make([]byte, recordSize)
		}

		bytesRead, err = reader.Read(data)
		if err == nil {
			if bytesRead != int(recordSize) {
				return fmt.Errorf("data corruption detected: expected %d bytes, got %d", recordSize, bytesRead)
			}

			var record Entry
			record.decode(data)

			segment.mu.Lock()
			segment.keyIndex[record.key] = currentOffset
			segment.mu.Unlock()

			currentOffset += int64(bytesRead)
		}
	}

	if segment == datastore.getCurrentSegment() {
		datastore.currentOffset = currentOffset
	}

	return err
}

func (datastore *Datastore) updateIndex(key string, position int64) {
	currentSegment := datastore.getCurrentSegment()
	currentSegment.mu.Lock()
	defer currentSegment.mu.Unlock()

	if position == -1 {
		delete(currentSegment.keyIndex, key)
	} else {
		currentSegment.keyIndex[key] = position
	}
}

func (datastore *Datastore) findKeyLocation(key string) (*Segment, int64, error) {
	datastore.segmentLock.RLock()
	defer datastore.segmentLock.RUnlock()

	for i := len(datastore.segments) - 1; i >= 0; i-- {
		segment := datastore.segments[i]
		segment.mu.RLock()
		position, found := segment.keyIndex[key]
		segment.mu.RUnlock()

		if found {
			return segment, position, nil
		}
	}
	return nil, 0, fmt.Errorf("key not found in datastore")
}

func (datastore *Datastore) getKeyPosition(key string) *KeyPosition {
	datastore.closeMutex.Lock()
	defer datastore.closeMutex.Unlock()

	if datastore.closed {
		return nil
	}

	segment, pos, err := datastore.findKeyLocation(key)
	if err != nil {
		return nil
	}
	return &KeyPosition{segment, pos}
}

func (datastore *Datastore) Get(key string) (string, error) {
	location := datastore.getKeyPosition(key)
	if location == nil {
		return "", fmt.Errorf("key not found in datastore")
	}

	value, err := location.segment.readFromSegment(location.position)
	if err != nil {
		return "", err
	}

	if value == tombstoneMarker {
		return "", fmt.Errorf("key not found in datastore (tombstone)")
	}

	return value, nil
}

func (datastore *Datastore) Put(key, value string) error {
	datastore.closeMutex.Lock()
	defer datastore.closeMutex.Unlock()

	if datastore.closed {
		return fmt.Errorf("database is closed")
	}

	responseChannel := make(chan error, 1)
	operation := WriteOperation{
		data: Entry{
			key:   key,
			value: value,
		},
		response: responseChannel,
	}

	datastore.writeOperations <- operation
	return <-responseChannel
}

func (datastore *Datastore) Delete(key string) error {
	datastore.closeMutex.Lock()
	defer datastore.closeMutex.Unlock()

	if datastore.closed {
		return fmt.Errorf("database is closed")
	}

	responseChannel := make(chan error, 1)

	deleteOp := WriteOperation{
		data: Entry{
			key:   key,
			value: tombstoneMarker,
		},
		response: responseChannel,
	}

	datastore.writeOperations <- deleteOp

	datastore.indexOperations <- IndexOperation{
		isWrite:  true,
		key:      key,
		position: -1,
		response: make(chan *KeyPosition, 1),
	}

	return <-responseChannel
}

func (datastore *Datastore) getCurrentSegment() *Segment {
	datastore.segmentLock.RLock()
	defer datastore.segmentLock.RUnlock()

	if len(datastore.segments) == 0 {
		return nil
	}
	return datastore.segments[len(datastore.segments)-1]
}

func (segment *Segment) readFromSegment(position int64) (string, error) {
	file, err := os.Open(segment.path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	_, err = file.Seek(position, 0)
	if err != nil {
		return "", err
	}

	reader := bufio.NewReader(file)
	value, err := readValue(reader)
	if err != nil {
		return "", err
	}
	return value, nil
}
