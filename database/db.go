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

type index map[string]int64

type KeyPosition struct {
	segment  *Segment
	position int64
}

type WriteOperation struct {
	data     Entry
	response chan error
}

type Operation struct {
	isWriteable bool
	key         string
	position    int64
}

type Datastore struct {
	activeFile      *os.File
	activeFilePath  string
	currentOffset   int64
	directory       string
	maxSegmentSize  int64
	segmentCounter  int
	indexOperations chan Operation
	keyLocations    chan *KeyPosition
	writeOperations chan WriteOperation
	writeComplete   chan error
	keyIndex        index
	segments        []*Segment
	fileLock        sync.Mutex
	indexLock       sync.Mutex
}

type Segment struct {
	path   string
	offset int64
	index  index
}

func createDatabase(directory string, segmentSize int64) (*Datastore, error) {
	database := &Datastore{
		segments:        make([]*Segment, 0),
		directory:       directory,
		maxSegmentSize:  segmentSize,
		indexOperations: make(chan Operation),
		keyLocations:    make(chan *KeyPosition),
		writeOperations: make(chan WriteOperation),
		writeComplete:   make(chan error),
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
	if datastore.activeFile != nil {
		return datastore.activeFile.Close()
	}
	return nil
}

func (datastore *Datastore) startIndexHandler() {
	go func() {
		for operation := range datastore.indexOperations {
			datastore.indexLock.Lock()
			if operation.isWriteable {
				datastore.updateIndex(operation.key, operation.position)
			} else {
				segment, pos, err := datastore.findKeyLocation(operation.key)
				if err != nil {
					datastore.keyLocations <- nil
				} else {
					datastore.keyLocations <- &KeyPosition{segment, pos}
				}
			}
			datastore.indexLock.Unlock()
		}
	}()
}

func (datastore *Datastore) startWriteHandler() {
	go func() {
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

			bytesWritten, err := datastore.activeFile.Write(operation.data.encode())
			if err == nil {
				datastore.indexOperations <- Operation{
					isWriteable: true,
					key:         operation.data.key,
					position:    int64(bytesWritten),
				}
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
		path:  newFilePath,
		index: make(index),
	}

	datastore.activeFile = file
	datastore.currentOffset = 0
	datastore.activeFilePath = newFilePath
	datastore.segments = append(datastore.segments, segment)

	if len(datastore.segments) >= minSegments {
		datastore.compactOldSegments()
	}

	return nil
}

func (datastore *Datastore) generateFileName() string {
	fileName := filepath.Join(datastore.directory, fmt.Sprintf("%s%d", dataFileName, datastore.segmentCounter))
	datastore.segmentCounter++
	return fileName
}

func (datastore *Datastore) compactOldSegments() {
	go func() {
		compactedFilePath := datastore.generateFileName()
		compactedSegment := &Segment{
			path:  compactedFilePath,
			index: make(index),
		}

		var writeOffset int64
		compactedFile, err := os.OpenFile(compactedFilePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, defaultFileMode)
		if err != nil {
			return
		}
		defer compactedFile.Close()

		lastIndex := len(datastore.segments) - 2
		for i := 0; i <= lastIndex; i++ {
			currentSegment := datastore.segments[i]
			for key, position := range currentSegment.index {
				if i < lastIndex {
					if datastore.keyExistsInNewerSegments(datastore.segments[i+1:lastIndex+1], key) {
						continue
					}
				}

				value, _ := currentSegment.readFromSegment(position)
				record := Entry{
					key:   key,
					value: value,
				}

				bytesWritten, err := compactedFile.Write(record.encode())
				if err == nil {
					compactedSegment.index[key] = writeOffset
					writeOffset += int64(bytesWritten)
				}
			}
		}
		datastore.segments = []*Segment{compactedSegment, datastore.getCurrentSegment()}
	}()
}

func (datastore *Datastore) keyExistsInNewerSegments(segments []*Segment, key string) bool {
	for _, segment := range segments {
		if _, exists := segment.index[key]; exists {
			return true
		}
	}
	return false
}

func (datastore *Datastore) recoverAllSegments() error {
	for _, segment := range datastore.segments {
		if err := datastore.recoverSegmentData(segment); err != nil {
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

		recordSize := binary.LittleEndian.Uint32(header)

		if recordSize < bufferSize {
			data = buffer[:recordSize]
		} else {
			data = make([]byte, recordSize)
		}

		bytesRead, err = reader.Read(data)
		if err == nil {
			if bytesRead != int(recordSize) {
				return fmt.Errorf("data corruption detected")
			}

			var record Entry
			record.decode(data)
			datastore.updateIndex(record.key, int64(bytesRead))
		}
	}
	return err
}

func (datastore *Datastore) updateIndex(key string, position int64) {
	datastore.getCurrentSegment().index[key] = datastore.currentOffset
	datastore.currentOffset += position
}

func (datastore *Datastore) findKeyLocation(key string) (*Segment, int64, error) {
	for i := len(datastore.segments) - 1; i >= 0; i-- {
		segment := datastore.segments[i]
		if position, found := segment.index[key]; found {
			return segment, position, nil
		}
	}
	return nil, 0, fmt.Errorf("key not found in datastore")
}

func (datastore *Datastore) getKeyPosition(key string) *KeyPosition {
	operation := Operation{
		isWriteable: false,
		key:         key,
	}
	datastore.indexOperations <- operation
	return <-datastore.keyLocations
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
	return value, nil
}

func (datastore *Datastore) Put(key, value string) error {
	responseChannel := make(chan error)
	datastore.writeOperations <- WriteOperation{
		data: Entry{
			key:   key,
			value: value,
		},
		response: responseChannel,
	}

	err := <-responseChannel
	close(responseChannel)
	return err
}

func (datastore *Datastore) getCurrentSegment() *Segment {
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
