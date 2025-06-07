package datastore

import (
	"bufio"
	"encoding/binary"
	"fmt"
)

type Entry struct {
	key   string
	value string
}

const (
	headerSize      = 4
	keyLengthSize   = 4
	valueLengthSize = 4
	totalHeaderSize = headerSize + keyLengthSize + valueLengthSize
)

func (e *Entry) getLength() int64 {
	return calculateEntryLength(e.key, e.value)
}

func calculateEntryLength(key, value string) int64 {
	return int64(len(key) + len(value) + totalHeaderSize)
}

func (e *Entry) decode(data []byte) {
	keyLength := binary.LittleEndian.Uint32(data[headerSize:])

	keyStart := headerSize + keyLengthSize
	keyEnd := keyStart + int(keyLength)

	keyBytes := make([]byte, keyLength)
	copy(keyBytes, data[keyStart:keyEnd])
	e.key = string(keyBytes)

	valueStart := keyEnd
	valueLength := binary.LittleEndian.Uint32(data[valueStart:])

	valueDataStart := valueStart + valueLengthSize
	valueDataEnd := valueDataStart + int(valueLength)

	valueBytes := make([]byte, valueLength)
	copy(valueBytes, data[valueDataStart:valueDataEnd])
	e.value = string(valueBytes)
}

func readValue(reader *bufio.Reader) (string, error) {
	headerBytes, err := reader.Peek(headerSize + keyLengthSize)
	if err != nil {
		return "", err
	}

	keySize := int(binary.LittleEndian.Uint32(headerBytes[headerSize:]))

	bytesToSkip := headerSize + keyLengthSize + keySize
	_, err = reader.Discard(bytesToSkip)
	if err != nil {
		return "", err
	}

	valueSizeBytes, err := reader.Peek(valueLengthSize)
	if err != nil {
		return "", err
	}

	valueSize := int(binary.LittleEndian.Uint32(valueSizeBytes))

	_, err = reader.Discard(valueLengthSize)
	if err != nil {
		return "", err
	}

	valueData := make([]byte, valueSize)
	bytesRead, err := reader.Read(valueData)
	if err != nil {
		return "", err
	}

	if bytesRead != valueSize {
		return "", fmt.Errorf("incomplete value read: got %d bytes, expected %d", bytesRead, valueSize)
	}

	return string(valueData), nil
}

func (e *Entry) encode() []byte {
	keyLength := len(e.key)
	valueLength := len(e.value)
	totalSize := keyLength + valueLength + totalHeaderSize

	buffer := make([]byte, totalSize)

	binary.LittleEndian.PutUint32(buffer, uint32(totalSize))

	binary.LittleEndian.PutUint32(buffer[headerSize:], uint32(keyLength))

	copy(buffer[headerSize+keyLengthSize:], e.key)

	valueStart := headerSize + keyLengthSize + keyLength
	binary.LittleEndian.PutUint32(buffer[valueStart:], uint32(valueLength))

	copy(buffer[valueStart+valueLengthSize:], e.value)

	return buffer
}
