// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package wal

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"testing"

	test "github.com/westerndigitalcorporation/blb/pkg/testutil"
)

// Generic tests for the interface contract.

func TestMemLogEmpty(t *testing.T) {
	l := NewMemLog()
	testLogEmpty(l, t)
}

func TestFSLogEmpty(t *testing.T) {
	l, path := openTempFSLog(t)
	defer func() {
		l.Close()
		os.RemoveAll(path)
	}()
	testLogEmpty(l, t)
}

func testLogEmpty(l Log, t *testing.T) {
	// Make sure that the FirstID and LastID are correct for an empty log.
	checkFirstID(l, 0, true, t)
	checkLastID(l, 0, true, t)
}

func TestMemLogAppend(t *testing.T) {
	l := NewMemLog()
	testLogAppend(l, t)
}

func TestFSLogAppend(t *testing.T) {
	l, path := openTempFSLog(t)
	defer func() {
		l.Close()
		os.RemoveAll(path)
	}()
	testLogAppend(l, t)
}

func testLogAppend(l Log, t *testing.T) {
	// Make sure that FirstID and LastID are updated correctly after appending.
	err := l.Append(Record{ID: 1, Data: []byte("First record")})
	if err != nil {
		t.Fatalf("Failed 1st append: %v", err)
	}
	checkFirstID(l, 1, false, t)
	checkLastID(l, 1, false, t)

	err = l.Append(Record{ID: 2, Data: []byte("Second record")})
	if err != nil {
		t.Fatalf("Failed 2nd append: %v", err)
	}

	checkFirstID(l, 1, false, t)
	checkLastID(l, 2, false, t)
}

func checkFirstID(l Log, expected uint64, expectEmpty bool, t *testing.T) {
	id, empty := l.FirstID()
	if empty != expectEmpty {
		t.Fatalf("FirstID incorrectly said empty=%t", empty)
	}
	if id != expected {
		t.Errorf("FirstID returned %d, expected %d", id, expected)
	}
}

func checkLastID(l Log, expected uint64, expectEmpty bool, t *testing.T) {
	id, empty := l.LastID()
	if empty != expectEmpty {
		t.Fatalf("LastID incorrectly said empty=%t", empty)
	}
	if id != expected {
		t.Errorf("LastID returned %d, expected %d", id, expected)
	}
}

func TestMemLogAppendCheckIDs(t *testing.T) {
	l := NewMemLog()
	testLogAppendCheckIDs(l, t)
}

func TestFSLogAppendCheckIDs(t *testing.T) {
	l, path := openTempFSLog(t)
	defer func() {
		l.Close()
		os.RemoveAll(path)
	}()
	testLogAppendCheckIDs(l, t)
}

func testLogAppendCheckIDs(l Log, t *testing.T) {
	// Check that the log enforces that appends must be of
	// Records with sequential IDs.
	err := l.Append(Record{ID: 5, Data: []byte("a record")})
	if err != nil {
		t.Fatalf("Failed to append first record: %v", err)
	}

	err = l.Append(Record{ID: 7, Data: []byte("my ID's too big")})
	if err == nil {
		t.Fatalf("Append should have failed")
	}

	// Make sure it works for batches too
	err = l.Append(Record{ID: 6, Data: []byte("this one is cool")},
		Record{ID: 10, Data: []byte("this one goes too far")})
	if err == nil {
		t.Fatalf("Batch append should have failed")
	}
}

func TestMemLogItr(t *testing.T) {
	l := NewMemLog()
	testLogItr(l, t)
}

func TestFSLogItr(t *testing.T) {
	l, path := openTempFSLog(t)
	defer func() {
		l.Close()
		os.RemoveAll(path)
	}()
	testLogItr(l, t)
}

func testLogItr(l Log, t *testing.T) {
	// Check basic iterator correctness (it returns the records appended).
	recs := fillLog(l, 2, t)
	itr := l.GetIterator(0)
	checkItrMatchesSlice(itr, recs, t)
}

func checkItrMatchesSlice(itr Iterator, recs []Record, t *testing.T) {
	i := 0
	for itr.Next() {
		r := itr.Record()
		if !bytes.Equal(r.Data, recs[i].Data) {
			t.Errorf("Data mismatach at %d: %s vs %s", i, string(r.Data), string(recs[i].Data))
		}
		if r.ID != recs[i].ID {
			t.Errorf("ID mismatch %d: %d vs %d", i, r.ID, recs[i].ID)
		}
		i++
	}
	if len(recs) != i {
		t.Errorf("Itr return different number of records: got %d, expected %d", i, len(recs))
	}
	err := itr.Err()
	if err != nil {
		t.Errorf("Unexpected error from itr: %v", err)
	}

	err = itr.Close()
	if err != nil {
		t.Errorf("Unexpected error closing itr: %v", err)
	}
}

func TestMemLogItrPastEnd(t *testing.T) {
	l := NewMemLog()
	testLogItrPastEnd(l, t)
}

func TestFSLogItrPastEnd(t *testing.T) {
	l, path := openTempFSLog(t)
	defer func() {
		l.Close()
		os.RemoveAll(path)
	}()
	testLogItrPastEnd(l, t)
}

func testLogItrPastEnd(l Log, t *testing.T) {
	// Check that requesting an iterator with 'start' greater than all
	// stored records returns 0 records and no errors.
	fillLog(l, 2, t)
	itr := l.GetIterator(10) // 10 is greater than any ID used by fillLog
	if itr.Next() {
		t.Errorf("Iterator should not have any elements, yet gave us %v", itr.Record())
	}

	err := itr.Close()
	if err != nil {
		t.Errorf("Unexpected error when closing iterator: %v", err)
	}
}

func TestMemLogItrWithAppend(t *testing.T) {
	l := NewMemLog()
	testLogItrWithAppend(l, t)
}

func TestFSLogItrWithAppend(t *testing.T) {
	l, path := openTempFSLog(t)
	defer func() {
		l.Close()
		os.RemoveAll(path)
	}()
	testLogItrWithAppend(l, t)
}

func testLogItrWithAppend(l Log, t *testing.T) {
	// Make sure that iterators tolerate appends while open.
	// This checks for correctness, but not crashing is probably
	// sufficient.
	itr := l.GetIterator(0)
	rec := Record{ID: 1, Data: []byte("Added after iter created")}
	err := l.Append(rec)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Call next after the append, so the new record should
	// show up.
	hasNext := itr.Next()
	if !hasNext {
		t.Fatalf("No next after append")
	}

	recResult := itr.Record()
	if recResult.ID != rec.ID {
		t.Errorf("Got back wrong record %+v != %+v", recResult, rec)
	}
}

func TestMemLogTruncate(t *testing.T) {
	l := NewMemLog()
	testLogTruncate(l, t)
}

func TestFSLogTruncate(t *testing.T) {
	l, path := openTempFSLog(t)
	defer func() {
		l.Close()
		os.RemoveAll(path)
	}()
	testLogTruncate(l, t)
}

func testLogTruncate(l Log, t *testing.T) {
	// Checks that FirstID and LastID are updated after a Truncate.
	fillLog(l, 2, t)
	err := l.Truncate(1)
	if err != nil {
		t.Fatalf("Truncate failed %v", err)
	}

	checkFirstID(l, 1, false, t)
	checkLastID(l, 1, false, t)
}

func fillLog(l Log, n uint64, t *testing.T) (records []Record) {
	for i := uint64(0); i < n; i++ {
		rec := Record{ID: i + 1, Data: []byte(fmt.Sprintf("Record %d", i))}
		err := l.Append(rec)
		if err != nil {
			t.Fatalf("Failed append %d: %v", i, err)
		}
		records = append(records, rec)
	}
	return records
}

func TestMemLogTruncatePastEnd(t *testing.T) {
	l := NewMemLog()
	testLogTruncatePastEnd(l, t)
}

func TestFSLogTruncatePastEnd(t *testing.T) {
	l, path := openTempFSLog(t)
	defer func() {
		l.Close()
		os.RemoveAll(path)
	}()
	testLogTruncatePastEnd(l, t)
}

func testLogTruncatePastEnd(l Log, t *testing.T) {
	// Checks that a truncate with an ID greater than any stored
	// record ID is a no-op.
	fillLog(l, 2, t)
	err := l.Truncate(100)
	if err != nil {
		t.Fatalf("Truncate failed %v", err)
	}

	checkFirstID(l, 1, false, t)
	checkLastID(l, 2, false, t)
}

func TestMemLogTruncateBeforeBeginning(t *testing.T) {
	l := NewMemLog()
	testLogTruncateBeforeBeginning(l, t)
}

func TestFSLogTruncateBeforeBeginning(t *testing.T) {
	l, path := openTempFSLog(t)
	defer func() {
		l.Close()
		os.RemoveAll(path)
	}()
	testLogTruncateBeforeBeginning(l, t)
}

func testLogTruncateBeforeBeginning(l Log, t *testing.T) {
	// Checks that truncate with an ID less than any stored record
	// removes all records.
	fillLog(l, 2, t)
	err := l.Truncate(0)
	if err != nil {
		t.Fatalf("Truncate failed %v", err)
	}

	checkFirstID(l, 0, true, t)
	checkLastID(l, 0, true, t)
}

func TestMemLogTrim(t *testing.T) {
	l := NewMemLog()
	testLogTrim(l, t)
}

func TestFSLogTrim(t *testing.T) {
	l, path := openTempFSLog(t)
	defer func() {
		l.Close()
		os.RemoveAll(path)
	}()
	testLogTrim(l, t)
}

func testLogTrim(l Log, t *testing.T) {
	// Checks that trim works and updates FirstID and LastID.
	fillLog(l, 2, t)
	err := l.Trim(1)
	if err != nil {
		t.Fatalf("Trim failed %v", err)
	}

	// Trim doesn't require that anything get removed (it's up to the
	// implementation. It should only not remove anything past the ID
	// requested.
	id, empty := l.FirstID()
	if empty {
		t.Errorf("Log incorrectly empty after trim")
	}
	if id > 2 {
		t.Errorf("FirstID returned %d, should be at most 1", id)
	}
}

func TestMemLogTrimBeforeBeginning(t *testing.T) {
	l := NewMemLog()
	testLogTrimBeforeBeginning(l, t)
}

func TestFSLogTrimBeforeBeginning(t *testing.T) {
	l, path := openTempFSLog(t)
	defer func() {
		l.Close()
		os.RemoveAll(path)
	}()
	testLogTrimBeforeBeginning(l, t)
}

func testLogTrimBeforeBeginning(l Log, t *testing.T) {
	// Checks that Trim with an ID less than any stored record is a no-op.
	fillLog(l, 2, t)
	err := l.Trim(0)
	if err != nil {
		t.Fatalf("Truncate failed %v", err)
	}

	checkFirstID(l, 1, false, t)
	checkLastID(l, 2, false, t)
}

func openTempFSLog(t *testing.T) (Log, string) {
	name, err := ioutil.TempDir(test.TempDir(), "WAL-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir for FS WAL: %v", err)
	}

	l, err := OpenFSLog(name)
	if err != nil {
		t.Fatalf("Failed to create FS WAL in %q: %v", name, err)
	}
	l.(*fsLog).maxFileSize = 1 // Should create a new file after each append
	return l, name
}

// FS Log-specific tests

func TestFSLogManyFiles(t *testing.T) {
	// Make sure things work with many log files, but forcing small files.
	l, path := openTempFSLog(t)
	defer func() {
		l.Close()
		os.RemoveAll(path)
	}()
	l.(*fsLog).maxFileSize = 1 // Should create a new file after each append
	recs := fillLog(l, 10, t)
	itr := l.GetIterator(0)
	checkItrMatchesSlice(itr, recs, t)
}

func TestFSOpenTwiceEmpty(t *testing.T) {
	// Make sure we can open an existing, empty log file.
	l, path := openTempFSLog(t)
	defer os.RemoveAll(path)

	l.Close()
	l, err := OpenFSLog(path)
	if err != nil {
		t.Errorf("Failed to reopen FS WALL in  %q: %v", path, err)
	}

	l.Close()
}

func TestFSOpenTwice(t *testing.T) {
	// Make sure we can open an existing log with multiple non-empty files.
	l, path := openTempFSLog(t)
	defer os.RemoveAll(path)
	recs := fillLog(l, 4, t)
	l.Close()

	l, err := OpenFSLog(path)
	if err != nil {
		t.Errorf("Failed to reopen FS WALL in  %q: %v", path, err)
	}

	itr := l.GetIterator(0)
	checkItrMatchesSlice(itr, recs, t)
	l.Close()
}

func TestFSLogGapInFiles(t *testing.T) {
	// Make sure that if a log file has been removed, we fail the open.
	l, homeDir := openTempFSLog(t)
	defer os.RemoveAll(homeDir)
	l.(*fsLog).maxFileSize = 1 // Should create a new file after each append
	fillLog(l, 4, t)
	l.Close()

	dir, err := os.Open(homeDir)
	if err != nil {
		t.Fatalf("Failed to open dir %q: %v", homeDir, err)
	}

	children, err := dir.Readdirnames(0)
	if err != nil {
		t.Fatalf("Failed to read dir %q: %v", homeDir, err)
	}

	// Find a file in the middle to delete.

	var toDelete string
	for _, child := range children {
		if strings.Contains(child, "02.log") {
			toDelete = child
			break
		}
	}

	if toDelete == "" {
		t.Fatalf("Couldn't find file to delete in %v", children)
	}

	err = os.Remove(path.Join(homeDir, toDelete))
	if err != nil {
		t.Fatalf("Failed to delete %s: %v", toDelete, err)
	}

	_, err = OpenFSLog(homeDir)
	if err == nil {
		t.Errorf("Open should have failed due to missing file")
	}
}

func TestFSLogIgnoreOtherFiles(t *testing.T) {
	// Checks that a spurious file in the log directory doesn't
	// prevent operation.
	homeDir, err := ioutil.TempDir(test.TempDir(), "WAL-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir for FS WAL: %v", err)
	}
	defer os.RemoveAll(homeDir)

	spuriousFileName := path.Join(homeDir, "notALog")
	f, err := os.Create(spuriousFileName)
	if err != nil {
		t.Fatalf("Failed to create %s: %v", spuriousFileName, err)
	}
	f.Close()

	l, err := OpenFSLog(homeDir)
	if err != nil {
		t.Errorf("Open should succeeded with spurious file, but got error: %v", err)
	}
	l.Close()
}

func TestFSLogTruncateWithItr(t *testing.T) {
	// Make sure we don't crash if clients Truncate while iterating.
	l, path := openTempFSLog(t)
	l.(*fsLog).maxFileSize = 1 // Create a new file after each append
	defer func() {
		l.Close()
		os.RemoveAll(path)
	}()
	fillLog(l, 5, t)
	itr := l.GetIterator(4)
	err := l.Truncate(2)
	if err != nil {
		t.Fatalf("Truncate whille iterating failed: %v", err)
	}

	// Make sure we get to the end of the iterator without crashing.
	// but there isn't anything particularly to assert, since the
	// behavior is undefined.
	for itr.Next() {
	}

	itr.Close()
}

func TestFSLogTrimWithItr(t *testing.T) {
	// Make sure we don't crash if clients Trim while iterating.
	l, path := openTempFSLog(t)
	l.(*fsLog).maxFileSize = 1 // Create a new file after each append
	defer func() {
		l.Close()
		os.RemoveAll(path)
	}()
	fillLog(l, 5, t)
	itr := l.GetIterator(1)
	err := l.Trim(4)
	if err != nil {
		t.Fatalf("Truncate whille iterating failed: %v", err)
	}

	// Make sure we get to the end of the iterator without crashing.
	// but there isn't anything particularly to assert, since the
	// behavior is undefined.
	for itr.Next() {
	}

	err = itr.Close()
	if err == nil {
		// Currently we do get an error in this case, which
		// seems nice.
		t.Errorf("No error on close")
	}
}

// Test trimming the whole in-memory log.
func TestMemLogTrimAll(t *testing.T) {
	l := NewMemLog()
	fillLog(l, 2, t)
	// Trim all entries.
	err := l.Trim(2)
	if err != nil {
		t.Fatalf("Trim failed %v", err)
	}

	_, empty := l.FirstID()
	if !empty {
		t.Errorf("Mem log should be empty after trimming all entries")
	}
}
