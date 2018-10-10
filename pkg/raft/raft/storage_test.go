// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package raft

import (
	"io/ioutil"
	"reflect"
	"testing"
)

func initLog(ents ...Entry) Log {
	log := NewMemLog()
	log.Append(ents...)
	return log
}

func setupStorage(snapMeta SnapshotMetadata, logs []Entry, t *testing.T) *Storage {
	log := initLog(logs...)
	storage := NewStorage(&memSnapshotMgr{}, log, NewMemState(1))
	if snapMeta.LastIndex != 0 {
		// Then we should create a snapshot file.
		w, err := storage.BeginSnapshot(snapMeta)
		if err != nil {
			t.Fatalf("Can't create snapshot file: %v", err)
		}
		w.Write([]byte("snap data"))
		// Make the snapshot file visible.
		w.Commit()
	}
	return storage
}

// testEntries tests "Log.Entries" method.
func testEntries(makeLog func() Log, t *testing.T) {
	tests := []struct {
		ents   []Entry
		beg    uint64
		end    uint64
		result []Entry
	}{

		// Get the whole log.
		{
			[]Entry{Entry{Term: 1, Index: 1}, Entry{Term: 1, Index: 2}},
			1, 3,
			[]Entry{Entry{Term: 1, Index: 1}, Entry{Term: 1, Index: 2}},
		},

		// Get the prefix of a log.
		{
			[]Entry{Entry{Term: 1, Index: 1}, Entry{Term: 1, Index: 2}},
			1, 2,
			[]Entry{Entry{Term: 1, Index: 1}},
		},

		// Get the middle part of a log.
		{
			[]Entry{Entry{Term: 1, Index: 1}, Entry{Term: 1, Index: 2}, Entry{Term: 1, Index: 3}},
			2, 3,
			[]Entry{Entry{Term: 1, Index: 2}},
		},

		// end index(100) exceeds the last index of log.
		{
			[]Entry{Entry{Term: 1, Index: 1}, Entry{Term: 1, Index: 2}, Entry{Term: 1, Index: 3}},
			2, 100,
			[]Entry{Entry{Term: 1, Index: 2}, Entry{Term: 1, Index: 3}},
		},
	}

	for i, test := range tests {
		// Initialize the log state.
		log := makeLog()
		log.Append(test.ents...)

		ents := log.Entries(test.beg, test.end)

		if !reflect.DeepEqual(ents, test.result) {
			t.Fatalf("incorrect return result from 'Entries' method on test %d: %#v vs %#v", i, ents, test.result)
		}
		log.Close()
	}
}

// testFirstLastIndex tests "Log.FirstIndex()" and "Log.LastIndex()" methods.
func testFirstLastIndex(makeLog func() Log, t *testing.T) {
	tests := []struct {
		ents     []Entry
		firstIdx uint64
		lastIdx  uint64
		empty    bool
	}{
		// log contains no entry
		{
			[]Entry{},
			0, 0, true,
		},

		// log contains one entry
		{
			[]Entry{Entry{Term: 1, Index: 2}},
			2, 2, false,
		},

		// log contains two entries
		{
			[]Entry{Entry{Term: 1, Index: 2}, Entry{Term: 1, Index: 3}},
			2, 3, false,
		},
	}

	for i, test := range tests {
		// Initialize the log state.
		log := makeLog()
		log.Append(test.ents...)

		fi, _ := log.FirstIndex()
		li, empty := log.LastIndex()

		if test.empty {
			if empty != test.empty {
				t.Fatalf("expected empty to be returned as %v, but got %v on test %d", test.empty, empty, i)
			}
		} else {
			if test.empty {
				t.Errorf("expected the log is not empty")
			}

			if fi != test.firstIdx {
				t.Fatalf("expected first index to be %d, but got %d on test %d", test.firstIdx, fi, i)
			}
			if li != test.lastIdx {
				t.Fatalf("expected last index to be %d, but got %d on test %d", test.lastIdx, li, i)
			}
		}
	}
}

// testTruncate tests "Log.Truncate(uint64)" method.
func testTruncate(makeLog func() Log, t *testing.T) {
	tests := []struct {
		entsBeforeTrunc []Entry
		truncIdx        uint64
		entsAfterTrunc  []Entry
	}{
		// Truncate nothing
		{
			[]Entry{Entry{Term: 1, Index: 1}},
			1,
			[]Entry{Entry{Term: 1, Index: 1}},
		},

		// Truncate the last one entry
		{
			[]Entry{Entry{Term: 1, Index: 1}, Entry{Term: 1, Index: 2}},
			1,
			[]Entry{Entry{Term: 1, Index: 1}},
		},

		// Truncate the last two entries
		{
			[]Entry{Entry{Term: 1, Index: 1}, Entry{Term: 1, Index: 2}, Entry{Term: 1, Index: 3}},
			1,
			[]Entry{Entry{Term: 1, Index: 1}},
		},

		// Truncate all the entries
		{
			[]Entry{Entry{Term: 1, Index: 1}, Entry{Term: 1, Index: 2}, Entry{Term: 1, Index: 3}},
			0,
			[]Entry{},
		},
	}

	for i, test := range tests {
		log := makeLog()
		log.Append(test.entsBeforeTrunc...)

		// Perform truncation.
		log.Truncate(test.truncIdx)

		fi, empty := log.FirstIndex()
		li, _ := log.LastIndex()

		if empty {
			// Log is empty after truncation.
			if len(test.entsAfterTrunc) > 0 {
				t.Fatal("expected the log to become empty after truncation")
			}
		} else {
			ents := log.Entries(fi, li+1)
			if !reflect.DeepEqual(ents, test.entsAfterTrunc) {
				t.Fatalf("incorrect log state after truncation on test %d: %#v != %#v", i, ents, test.entsAfterTrunc)
			}
		}
		log.Close()
	}
}

func testIterator(makeLog func() Log, t *testing.T) {
	tests := []struct {
		ents     []Entry
		first    uint64
		expected []Entry
	}{
		// Iterating all entries, starting from the first one.
		{
			ents:     []Entry{Entry{Term: 1, Index: 1}, Entry{Term: 1, Index: 2}},
			first:    1,
			expected: []Entry{Entry{Term: 1, Index: 1}, Entry{Term: 1, Index: 2}},
		},

		// Iterating all entries, starting from the one before the first one.
		{
			ents:     []Entry{Entry{Term: 1, Index: 1}, Entry{Term: 1, Index: 2}},
			first:    0,
			expected: []Entry{Entry{Term: 1, Index: 1}, Entry{Term: 1, Index: 2}},
		},

		// Iterating the last entry.
		{
			ents:     []Entry{Entry{Term: 1, Index: 1}, Entry{Term: 1, Index: 2}},
			first:    2,
			expected: []Entry{Entry{Term: 1, Index: 2}},
		},

		// Iterating none.
		{
			ents:     []Entry{Entry{Term: 1, Index: 1}, Entry{Term: 1, Index: 2}},
			first:    3,
			expected: nil,
		},
	}

	for _, test := range tests {
		log := makeLog()
		log.Append(test.ents...)

		iter := log.GetIterator(test.first)
		var got []Entry
		for iter.Next() {
			got = append(got, iter.Entry())
		}

		if !reflect.DeepEqual(got, test.expected) {
			t.Fatalf("failed to get expected entries using iterator")
		}
	}
}

// testLogImpl tests one implementation of Log interface.
func testLogImpl(makeLog func() Log, t *testing.T) {
	testEntries(makeLog, t)
	testFirstLastIndex(makeLog, t)
	testTruncate(makeLog, t)
	testIterator(makeLog, t)
}

// Test memlog implementation.
func TestMemLog(t *testing.T) {
	makeLog := func() Log { return NewMemLog() }
	testLogImpl(makeLog, t)
}

// Test "lastIndex" implementation.
func TestStorageLastIndex(t *testing.T) {
	tests := []struct {
		lastSnapIdx  uint64
		lastSnapTerm uint64
		logs         []Entry
		expectedRes  uint64
	}{

		// No snapshot file. It should return the last index of the log file.
		{
			lastSnapIdx:  0,
			lastSnapTerm: 0,
			logs:         []Entry{Entry{Index: 1, Term: 1}, Entry{Index: 2, Term: 1}},
			expectedRes:  2,
		},

		// Have a snapshot file, but log is empty. It should return the last index
		// of snapshot file.
		{
			lastSnapIdx:  5,
			lastSnapTerm: 1,
			logs:         []Entry{},
			expectedRes:  5,
		},

		// Have both snapshot file and log, and log.lastIndex > snap.lastIndex, should
		// return the last index of the log.
		{
			lastSnapIdx:  2,
			lastSnapTerm: 1,
			logs:         []Entry{Entry{Index: 2, Term: 1}, Entry{Index: 3, Term: 1}},
			expectedRes:  3,
		},

		// Both snapshot and log are empty, should return 0.
		{
			lastSnapIdx:  0,
			lastSnapTerm: 0,
			logs:         []Entry{},
			expectedRes:  0,
		},
	}

	for _, test := range tests {
		t.Logf("running test: %v", test)
		storage := setupStorage(SnapshotMetadata{LastIndex: test.lastSnapIdx, LastTerm: test.lastSnapTerm}, test.logs, t)
		li := storage.lastIndex()

		if li != test.expectedRes {
			t.Fatalf("expected to get last index %d, but got %d", test.expectedRes, li)
		}
	}
}

// Test "hasEntry" implementation.
func TestStorageHasEntry(t *testing.T) {
	tests := []struct {
		lastSnapIdx  uint64
		lastSnapTerm uint64
		logs         []Entry
		checkIdx     uint64
		checkTerm    uint64
		expectedRes  bool
	}{

		// snap	:	{ last appllied: (1, 10) }
		// log	:	empty
		// check: (1, 10)
		//
		// expected: true
		{
			lastSnapIdx:  10,
			lastSnapTerm: 1,
			logs:         []Entry{},
			checkIdx:     10,
			checkTerm:    1,
			expectedRes:  true,
		},

		// snap	:	{ last appllied: (1, 1) }
		// log	: (1, 1), (1, 2)
		// check: (1, 2)
		//
		// expected: true
		{
			lastSnapIdx:  1,
			lastSnapTerm: 1,
			logs:         []Entry{Entry{Index: 1, Term: 2}, Entry{Index: 2, Term: 1}},
			checkIdx:     2,
			checkTerm:    1,
			expectedRes:  true,
		},

		// snap	:	{ last appllied: (1, 1) }
		// log	: (1, 1), (1, 2)
		// check: (2, 2)
		//
		// expected: false
		{
			lastSnapIdx:  1,
			lastSnapTerm: 1,
			logs:         []Entry{Entry{Index: 1, Term: 2}, Entry{Index: 2, Term: 1}},
			checkIdx:     2,
			checkTerm:    2,
			expectedRes:  false,
		},

		// snap	:	{ last appllied: (1, 10) }
		// log	: (1, 11), (1, 12)
		// check: (1, 2)
		//
		// expected: true
		{
			lastSnapIdx:  10,
			lastSnapTerm: 1,
			logs:         []Entry{Entry{Index: 11, Term: 2}, Entry{Index: 12, Term: 1}},
			checkIdx:     2,
			checkTerm:    1,
			expectedRes:  true,
		},

		// snap	:	{ last appllied: (1, 10) }
		// log	: (1, 11), (1, 12)
		// check: (1, 13)
		//
		// expected: false
		{
			lastSnapIdx:  10,
			lastSnapTerm: 1,
			logs:         []Entry{Entry{Index: 11, Term: 2}, Entry{Index: 12, Term: 1}},
			checkIdx:     13,
			checkTerm:    1,
			expectedRes:  false,
		},

		// snap	:	empty
		// log	: empty
		// check: (0, 0)
		//
		// expected: true
		{
			lastSnapIdx:  0,
			lastSnapTerm: 0,
			logs:         []Entry{},
			checkIdx:     0,
			checkTerm:    0,
			expectedRes:  true,
		},
	}

	for _, test := range tests {
		t.Logf("running test: %v", test)
		storage := setupStorage(SnapshotMetadata{LastIndex: test.lastSnapIdx, LastTerm: test.lastSnapTerm}, test.logs, t)
		res := storage.hasEntry(test.checkIdx, test.checkTerm)
		if res != test.expectedRes {
			t.Fatalf("expected to get result %v, but got %v", test.expectedRes, res)
		}
	}
}

// Test "term" implementation.
func TestStorageTerm(t *testing.T) {
	tests := []struct {
		lastSnapIdx  uint64
		lastSnapTerm uint64
		logs         []Entry
		index        uint64
		// expected result.
		expectedTerm uint64
		ok           bool
	}{

		// Both snapshot and log are empty, for index 0 it should return term 0.
		{
			lastSnapIdx:  0,
			lastSnapTerm: 0,
			logs:         []Entry{},
			index:        0,
			expectedTerm: 0,
			ok:           true,
		},

		// The command of the given index has been compacted.
		{
			lastSnapIdx:  10,
			lastSnapTerm: 1,
			logs:         []Entry{},
			index:        9,
			expectedTerm: 0,
			ok:           false,
		},

		// The command is in both snapshot and log, we should find its term number anyway.
		{
			lastSnapIdx:  10,
			lastSnapTerm: 1,
			logs:         []Entry{Entry{Index: 9, Term: 1}, Entry{Index: 10, Term: 1}},
			index:        9,
			expectedTerm: 1,
			ok:           true,
		},

		// The command is in log only.
		{
			lastSnapIdx:  8,
			lastSnapTerm: 1,
			logs:         []Entry{Entry{Index: 9, Term: 1}, Entry{Index: 10, Term: 1}},
			index:        9,
			expectedTerm: 1,
			ok:           true,
		},

		// The command is in snapshot only, and it's not the last applied command so
		// we don't know its term.
		{
			lastSnapIdx:  8,
			lastSnapTerm: 1,
			logs:         []Entry{Entry{Index: 9, Term: 1}, Entry{Index: 10, Term: 1}},
			index:        7,
			expectedTerm: 0,
			ok:           false,
		},

		// The command is in snapshot only, and it's the last applied command so we
		// can find out its term.
		{
			lastSnapIdx:  8,
			lastSnapTerm: 1,
			logs:         []Entry{Entry{Index: 9, Term: 1}, Entry{Index: 10, Term: 1}},
			index:        8,
			expectedTerm: 1,
			ok:           true,
		},
	}

	for _, test := range tests {
		t.Logf("running test: %v", test)
		storage := setupStorage(SnapshotMetadata{LastIndex: test.lastSnapIdx, LastTerm: test.lastSnapTerm}, test.logs, t)

		term, ok := storage.term(test.index)
		if test.ok != ok {
			t.Fatalf("expected the ok to be returned as %v, but got %v", test.ok, ok)
		}

		if test.ok && test.expectedTerm != term {
			t.Fatalf("expected term %d, but got %d", test.expectedTerm, term)
		}
	}
}

// Test "getLogEntries" implementation.
func TestStorageGetLogEntries(t *testing.T) {
	tests := []struct {
		// Storage state.
		lastSnapIdx  uint64
		lastSnapTerm uint64
		logs         []Entry
		// Parameters
		beg uint64
		end uint64
		// Expected result
		prevTerm uint64
		ents     []Entry
		ok       bool
	}{

		// storage state:
		//
		//  snapshot: { last (1, 4) }
		//  log     :                 (2, 5), (2, 6)
		//
		// test: getLogEntries(4, 5, 7)
		{
			// state
			lastSnapIdx:  4,
			lastSnapTerm: 1,
			logs:         []Entry{Entry{Index: 5, Term: 2}, Entry{Index: 6, Term: 2}},
			// parameters
			beg: 5,
			end: 7,
			// expected result
			prevTerm: 1,
			ents:     []Entry{Entry{Index: 5, Term: 2}, Entry{Index: 6, Term: 2}},
			ok:       true,
		},

		// storage state:
		//
		//  snapshot: { last (1, 4) }
		//  log     :                 (2, 5), (2, 6)
		//
		// test: getLogEntries(3, 4, 7)
		{
			// state
			lastSnapIdx:  4,
			lastSnapTerm: 1,
			logs:         []Entry{Entry{Index: 5, Term: 2}, Entry{Index: 6, Term: 2}},
			// parameters
			beg: 4,
			end: 7,
			// expected result
			prevTerm: 0,
			ents:     nil,
			ok:       false,
		},

		// storage state:
		//
		//  snapshot: { last (1, 4) }
		//  log     :         (1, 4), (2, 5), (2, 6)
		//
		// test: getLogEntries(3, 4, 7)
		{
			// state
			lastSnapIdx:  4,
			lastSnapTerm: 1,
			logs:         []Entry{Entry{Index: 4, Term: 1}, Entry{Index: 5, Term: 2}, Entry{Index: 6, Term: 2}},
			// parameters
			beg: 4,
			end: 7,
			// expected result
			prevTerm: 0,
			ents:     nil,
			ok:       false,
		},

		// storage state:
		//
		//  snapshot: { last (1, 4) }
		//  log     : (1, 3), (1, 4), (2, 5), (2, 6)
		//
		// test: getLogEntries(3, 4, 7)
		{
			// state
			lastSnapIdx:  4,
			lastSnapTerm: 1,
			logs:         []Entry{Entry{Index: 3, Term: 1}, Entry{Index: 4, Term: 1}, Entry{Index: 5, Term: 2}, Entry{Index: 6, Term: 2}},
			// parameters
			beg: 4,
			end: 7,
			// expected result
			prevTerm: 1,
			ents:     []Entry{Entry{Index: 4, Term: 1}, Entry{Index: 5, Term: 2}, Entry{Index: 6, Term: 2}},
			ok:       true,
		},

		// storage state:
		//
		//  snapshot: { last (1, 4) }
		//  log     :                 (2, 5), (2, 6)
		//
		// test: getLogEntries(0, 1, 5)
		{
			// state
			lastSnapIdx:  4,
			lastSnapTerm: 1,
			logs:         []Entry{Entry{Index: 5, Term: 2}, Entry{Index: 6, Term: 2}},
			// parameters
			beg: 1,
			end: 5,
			// expected result
			prevTerm: 0,
			ents:     nil,
			ok:       false,
		},
	}

	for _, test := range tests {
		t.Logf("running test: %v", test)
		storage := setupStorage(SnapshotMetadata{LastIndex: test.lastSnapIdx, LastTerm: test.lastSnapTerm}, test.logs, t)

		term, ents, ok := storage.getLogEntries(test.beg, test.end)
		if ok != test.ok {
			t.Fatalf("expected returned ok to be %v, but got %v", test.ok, ok)
		}

		if ok {
			if term != test.prevTerm {
				t.Fatalf("expected prevTerm to be %d, but got %d", test.prevTerm, term)
			}
			if !reflect.DeepEqual(test.ents, ents) {
				t.Fatalf("wrong entries returned by 'getLogEntries'")
			}
		}
	}
}

// testSnapshot tests writing/reading the snapshot file of storage.
func testSnapshot(s SnapshotManager, t *testing.T) {
	// Expected no snapshot at beginning.
	meta, _ := s.GetSnapshotMetadata()
	if meta != NilSnapshotMetadata {
		t.Fatal("expected no snapshot at beginning")
	}
	reader := s.GetSnapshot()
	if reader != nil {
		t.Fatal("expected no snapshot at beginning")
	}

	// Create a snapshot file and write data to it.
	w, err := s.BeginSnapshot(SnapshotMetadata{LastIndex: 10, LastTerm: 1})
	if err != nil {
		t.Fatalf("expected no error to create a snapshot file but got: %v", err)
	}
	data := []byte("Hello World at index 10!")
	n, err := w.Write(data)
	if err != nil || n != len(data) {
		t.Fatal("Failed to write to snapshot file.")
	}

	// Before calling "Commit", the snapshot file shouldn't be visible.
	reader = s.GetSnapshot()
	if reader != nil {
		t.Fatal("expected no snapshot file to be found.")
	}

	// After calling "Commit", the snapshot file should be visible and we should
	// be able to read the snapshot data back.
	w.Commit()
	r := s.GetSnapshot()
	if r == nil {
		t.Fatal("expected the snapshot file to be visible.")
	}
	d, err := ioutil.ReadAll(r)
	if err != nil || len(d) != len(data) || string(d) != string(data) {
		t.Fatal("Failed to read back the written data")
	}
	r.Close()

	// Create a new snapshot file and overwrite the previous snapshot file.
	w, err = s.BeginSnapshot(SnapshotMetadata{LastIndex: 100, LastTerm: 1})
	if err != nil {
		t.Fatalf("expected no error but got: %v", err)
	}
	data = []byte("Hello World at index 100!")
	n, err = w.Write(data)
	if err != nil || n != len(data) {
		t.Fatal("Failed to write to snapshot file.")
	}
	// After calling "Commit", the snapshot file should be visible and we should
	// be able to read the snapshot data back.
	w.Commit()
	r = s.GetSnapshot()
	if r == nil {
		t.Fatal("expected the snapshot file to be visible.")
	}
	d, err = ioutil.ReadAll(r)
	if err != nil || len(d) != len(data) || string(d) != string(data) {
		t.Fatalf("Failed to read back the written data")
	}
	r.Close()
}

// Tests snapshot of memory storage implementation.
func TestMemStorageSnapshot(t *testing.T) {
	testSnapshot(NewMemSnapshotMgr(), t)
}
