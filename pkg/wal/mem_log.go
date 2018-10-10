// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package wal

import (
	"fmt"
)

type memLog struct {
	records []Record
}

// NewMemLog returns a non-persistent implementation of Log for testing.
// It is not thread safe.
func NewMemLog() Log {
	return &memLog{}
}

func (ml *memLog) FirstID() (id uint64, empty bool) {
	if len(ml.records) == 0 {
		return 0, true
	}
	return ml.records[0].ID, false
}

func (ml *memLog) LastID() (id uint64, empty bool) {
	if len(ml.records) == 0 {
		return 0, true
	}
	return ml.records[len(ml.records)-1].ID, false
}

// Iterator

type memItr struct {
	ml *memLog
	i  int // Index into ml.records.
}

func (itr *memItr) Next() bool {
	itr.i++
	if itr.i >= len(itr.ml.records) {
		return false
	}

	return true
}

func (itr *memItr) Record() Record {
	return itr.ml.records[itr.i]
}

func (itr *memItr) Err() error {
	return nil
}

func (itr *memItr) Close() error {
	return nil
}

func (ml *memLog) GetIterator(startID uint64) Iterator {
	var i int
	// Records are strictly stored by increasing ID, so we can
	// do a linear scan from the start.
	// Use the i < len() instead of range so i runs past the end
	// in the not found case; with range it stays at len()-1.
	for ; i < len(ml.records); i++ {
		if ml.records[i].ID >= startID {
			break
		}
	}
	return &memItr{
		ml: ml,
		// Start 1 before the record if we found one, as
		// Next() must be called first. If not, stay at the
		// end.
		i: i - 1,
	}
}

func (ml *memLog) Append(records ...Record) error {
	for _, r := range records {
		// IDs must be sequential
		if len(ml.records) > 0 && r.ID != ml.records[len(ml.records)-1].ID+1 {
			return fmt.Errorf("Invalid index %d, should be greater than %d", r.ID, ml.records[len(ml.records)-1].ID)
		}

		// Byte slices are mutable, so make a deep copy
		myB := make([]byte, len(r.Data))
		copy(myB, r.Data)
		ml.records = append(ml.records, Record{Data: myB, ID: r.ID})
	}

	return nil
}

func (ml *memLog) Truncate(lastToKeep uint64) error {
	i := 0
	for ; i < len(ml.records); i++ {
		if ml.records[len(ml.records)-i-1].ID <= lastToKeep {
			break
		}
	}
	ml.records = ml.records[:len(ml.records)-i]
	return nil
}

func (ml *memLog) Trim(discardUpTo uint64) error {
	var i int

	// Use the i < len() instead of range so we run past the end in the
	// trim the whole log case; with range it stays at len()-1.
	for i = 0; i < len(ml.records); i++ {
		if ml.records[i].ID > discardUpTo {
			break
		}
	}

	newLen := len(ml.records) - i
	newRecords := make([]Record, newLen, 2*newLen)
	copy(newRecords, ml.records[i:])
	ml.records = newRecords

	return nil
}

func (ml *memLog) Close() {}
