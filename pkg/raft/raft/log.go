// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package raft

import (
	"encoding/binary"
	"errors"
	"math"

	log "github.com/golang/glog"
	"github.com/westerndigitalcorporation/blb/pkg/wal"
)

// nilSliceMarker is the length used to encode a nil slice. Makes
// testing easier since Entries roundtrip exactly.
const nilSliceMarker = math.MaxUint32

const (
	entryFormat1 byte = 0x80
)

var errBadEntryFormat = errors.New("bad entry format")
var errCorruptedEntry = errors.New("corrupted entry")

// entryLog is a persistent log implemented on top of package wal's log.
type entryLog struct {
	wal wal.Log
}

// NewFSLog creates a durable file-based implementation of Log.
func NewFSLog(homeDir string, cacheCapacity uint) Log {
	l, err := wal.OpenFSLog(homeDir)
	if err != nil {
		log.Fatalf("Failed to open WAL: %v", err)
	}
	if cacheCapacity == 0 {
		// No cache is needed.
		return newLog(l)
	}
	return newLog(newWALCache(cacheCapacity, l))
}

func newLog(wal wal.Log) Log {
	return &entryLog{wal: wal}
}

func (l *entryLog) FirstIndex() (index uint64, empty bool) {
	return l.wal.FirstID()
}

func (l *entryLog) LastIndex() (index uint64, empty bool) {
	return l.wal.LastID()
}

func (l *entryLog) GetBound() (firstIndex, lastIndex uint64, empty bool) {
	firstIndex, empty = l.FirstIndex()
	lastIndex, empty = l.LastIndex()
	return
}

func (l *entryLog) Append(entries ...Entry) {
	records := make([]wal.Record, len(entries))
	for i, e := range entries {
		encoded, err := serializeEntry(e)
		if err != nil {
			log.Fatalf("%v", err)
		}
		records[i] = wal.Record{
			ID:   e.Index,
			Data: encoded,
		}
	}

	err := l.wal.Append(records...)
	if err != nil {
		log.Fatalf("%v", err)
	}
}

func (l *entryLog) GetIterator(first uint64) LogIterator {
	return &entryLogIter{iter: l.wal.GetIterator(first)}
}

type entryLogIter struct {
	iter wal.Iterator
}

func (i *entryLogIter) Next() bool {
	return i.iter.Next()
}

func (i *entryLogIter) Entry() Entry {
	record := i.iter.Record()
	e, err := deserializeEntry(record.Data)
	if err != nil {
		log.Fatalf("%v", err)
	}
	e.Index = record.ID
	return e
}

func (i *entryLogIter) Err() error {
	return i.iter.Err()
}

func (i *entryLogIter) Close() error {
	return i.iter.Close()
}

func serializeEntry(e Entry) ([]byte, error) {
	// Format 1 is:
	// - 1 byte: Version marker (0x80)
	// - 1 byte: Type
	// - uvarint64: Term
	// - remaining bytes: Command
	// Note that index is not stored explicitly, since it's the same as the WAL id.
	// Also the command length is implicit, since the WAL delimits records for us.
	maxLen := 1 + 1 + binary.MaxVarintLen64 + len(e.Cmd)
	buf := make([]byte, maxLen)
	n := 0
	buf[n] = entryFormat1
	n++
	buf[n] = e.Type
	n++
	n += binary.PutUvarint(buf[n:], e.Term)
	n += copy(buf[n:], e.Cmd)
	return buf[:n], nil
}

func deserializeEntry(b []byte) (e Entry, err error) {
	switch b[0] {
	case entryFormat1:
		return deserializeEntryV1(b)
	}
	return Entry{}, errBadEntryFormat
}

func deserializeEntryV1(b []byte) (e Entry, err error) {
	n := 0
	// b[0] is format marker, already checked.
	n++
	e.Type = b[n]
	n++
	var k int
	if e.Term, k = binary.Uvarint(b[n:]); k <= 0 {
		return Entry{}, errCorruptedEntry
	}
	n += k
	// Remaining bytes are command:
	e.Cmd = b[n:]
	if len(e.Cmd) == 0 {
		// Put an actual nil here to make testing easier.
		e.Cmd = nil
	}
	return
}

func (l *entryLog) Entries(beg, end uint64) []Entry {
	itr := l.wal.GetIterator(beg)
	entries := []Entry{}
	defer itr.Close()
	expectedIndex := beg
	for itr.Next() {
		record := itr.Record()
		e, err := deserializeEntry(record.Data)
		if err != nil {
			log.Fatalf("%v", err)
		}
		e.Index = record.ID

		// Be paranoid and make sure we get back the expected,
		// continuous range.
		if e.Index != expectedIndex {
			log.Fatalf("Mismatch in entry retrieved from log: expected %d, got %d", expectedIndex, e.Index)
		}

		if e.Index >= end {
			break
		}

		entries = append(entries, e)
		expectedIndex++
	}
	if err := itr.Err(); err != nil {
		log.Fatalf("%v", err)
	}

	return entries
}

func (l *entryLog) Truncate(index uint64) {
	err := l.wal.Truncate(index)
	if err != nil {
		log.Fatalf("%v", err)
	}
}

func (l *entryLog) Trim(index uint64) {
	err := l.wal.Trim(index)
	if err != nil {
		log.Fatalf("%v", err)
	}
}

func (l *entryLog) Term(index uint64) uint64 {
	entries := l.Entries(index, index+1)
	if len(entries) == 0 {
		log.Fatalf("Can't find any entry with index %d", index)
	}
	return entries[0].Term
}

func (l *entryLog) Close() {
	l.wal.Close()
}
