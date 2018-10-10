// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package wal

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	test "github.com/westerndigitalcorporation/blb/pkg/testutil"
)

func TestCreateAppend(t *testing.T) {
	// Test that append works on a freshly created log file.
	path := tempPath()
	lf, err := createLogFile(path)
	if err != nil {
		t.Fatalf("Failed to open log file: %v", err)
	}

	defer os.Remove(path)

	rec := Record{Data: []byte("Write me"), ID: 100}
	err = lf.Append(rec)
	if err != nil {
		t.Errorf("Append failed: %v", err)
	}

	lf.Close()
}

func TestAppendAndReopen(t *testing.T) {
	// Test that appends work after reopening.
	path := tempPath()
	lf, err := createLogFile(path)
	if err != nil {
		t.Fatalf("Failed to open log file: %v", err)
	}

	defer os.Remove(path)

	rec := Record{Data: []byte("Write me"), ID: 100}
	err = lf.Append(rec)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	lf.Close()

	lf1, err := openLogFile(path)
	if err != nil {
		t.Fatalf("Failed to reopen log file: %v", err)
	}

	rec1 := Record{Data: []byte("Write more"), ID: 101}
	err = lf1.Append(rec1)
	if err != nil {
		t.Fatalf("Failed to append after reopen: %v", err)
	}

	lf1.Close()
}

func TestAppendAndIterate(t *testing.T) {
	// Check that Iterator returns records that match what was written.
	path := tempPath()
	lf, err := createLogFile(path)
	if err != nil {
		t.Fatalf("Failed to open log file: %v", err)
	}

	defer os.Remove(path)

	recs, err := fillLogFile(lf, 8)
	if err != nil {
		t.Fatalf("Failed to append records: %v", err)
	}

	checkItrRecords(lf, recs, t)

	lf.Close()
}

func fillLogFile(lf *logFile, numRecs int) (recs []Record, err error) {
	for i := 0; i < numRecs; i++ {
		data := []byte(fmt.Sprintf("record %d", i))
		rec := Record{Data: data, ID: uint64(i)}
		recs = append(recs, rec)

	}
	err = lf.Append(recs...)
	return recs, err
}

func checkItrRecords(lf *logFile, recs []Record, t *testing.T) {
	itr := lf.GetIterator(recs[0].ID)
	i := 0
	for itr.Next() {
		rec := itr.Record()
		if rec.ID != recs[i].ID || !bytes.Equal(rec.Data, recs[i].Data) {
			t.Errorf("Mismatch in record %d from iterator: %+v vs %+v", i, rec, recs[i])
		}
		i++
	}

	if i != len(recs) {
		t.Errorf("Didn't get the expected number of records: got %d expected %d", i, len(recs))
	}

	err := itr.Close()
	if err != nil {
		t.Errorf("Error when closing iterator: %v", err)
	}
}

func TestAppendAndIterateFromMiddle(t *testing.T) {
	// Test that the 'start' parameter is respected in GetIterator().
	path := tempPath()
	lf, err := createLogFile(path)
	if err != nil {
		t.Fatalf("Failed to open log file: %v", err)
	}

	defer os.Remove(path)

	recs, err := fillLogFile(lf, 8)
	if err != nil {
		t.Fatalf("Failed to append records: %v", err)
	}

	checkItrRecords(lf, recs[3:], t)

	lf.Close()
}

func TestAppendAndIterateAfterEnd(t *testing.T) {
	// Test that GetIterator can be called with a 'start' greater than
	// and IDs in the log file.
	path := tempPath()
	lf, err := createLogFile(path)
	if err != nil {
		t.Fatalf("Failed to open log file: %v", err)
	}

	defer os.Remove(path)

	recs, err := fillLogFile(lf, 8)
	if err != nil {
		t.Fatalf("Failed to append records: %v", err)
	}

	lastID := recs[len(recs)-1].ID
	itr := lf.GetIterator(lastID + 1)
	if itr.Next() {
		t.Errorf("Got record after the end (%d): %+v", lastID, itr.Record())
	}

	lf.Close()
}

func TestTruncateEmpty(t *testing.T) {
	// Test that truncate on an empty file is a no-op.
	path := tempPath()
	lf, err := createLogFile(path)
	if err != nil {
		t.Fatalf("Failed to open log file: %v", err)
	}

	defer os.Remove(path)

	err = lf.Truncate(0)
	if err != nil {
		t.Fatalf("Failed to truncate log file: %v", err)
	}
}

func TestTruncate(t *testing.T) {
	// Test that getting an iterator after truncating reflects the change.
	path := tempPath()
	lf, err := createLogFile(path)
	if err != nil {
		t.Fatalf("Failed to open log file: %v", err)
	}

	defer os.Remove(path)

	recs, err := fillLogFile(lf, 2)
	if err != nil {
		t.Fatalf("Failed to append records: %v", err)
	}

	err = lf.Truncate(recs[0].ID)
	if err != nil {
		t.Fatalf("Failed to truncate log file: %v", err)
	}

	recs = recs[:1]
	checkItrRecords(lf, recs, t)

	lf.Close()
}

func TestTruncateEnd(t *testing.T) {
	// Tests that truncating after the last ID works.
	path := tempPath()
	lf, err := createLogFile(path)
	if err != nil {
		t.Fatalf("Failed to open log file: %v", err)
	}

	defer os.Remove(path)

	recs, err := fillLogFile(lf, 2)
	if err != nil {
		t.Fatalf("Failed to append records: %v", err)
	}

	err = lf.Truncate(recs[1].ID)
	if err != nil {
		t.Fatalf("Failed to truncate log file: %v", err)
	}

	checkItrRecords(lf, recs, t)

	lf.Close()
}

func TestOpenWithCorruption(t *testing.T) {
	path := tempPath()
	lf, err := createLogFile(path)
	if err != nil {
		t.Fatalf("Failed to open log file: %v", err)
	}
	defer os.Remove(path)

	_, err = fillLogFile(lf, 20)
	if err != nil {
		t.Fatalf("Failed to append records: %v", err)
	}

	// Change a byte.
	f, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("Failed to open %q to corrupt: %v", path, err)
	}
	fi, _ := f.Stat()
	f.Seek(fi.Size()/2, os.SEEK_SET)
	f.Write([]byte{0xbb})
	f.Close()

	// Reopening the file should fail
	lf, err = openLogFile(path)
	if err == nil {
		t.Fatalf("Opening corrupted file %q should have failed: %s", path, err)
	}
}
func TestOpenWithIncompleteRecord(t *testing.T) {
	path := tempPath()
	lf, err := createLogFile(path)
	if err != nil {
		t.Fatalf("Failed to open log file: %v", err)
	}
	defer os.Remove(path)

	_, err = fillLogFile(lf, 20)
	if err != nil {
		t.Fatalf("Failed to append records: %v", err)
	}
	if lf.lastID != 19 {
		t.Fatalf("Wrong last id: %d", lf.lastID)
	}

	// Cut a byte off the end of the file.
	f, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("Failed to open %q to truncate: %v", path, err)
	}
	fi, _ := f.Stat()
	f.Truncate(fi.Size() - 1)
	f.Close()

	// Reopening the file should succeed, with the last record missing.
	lf, err = openLogFile(path)
	if err != nil {
		t.Fatalf("Failed to open log file %q: %v", path, err)
	}
	if lf.lastID != 18 {
		t.Fatalf("Wrong last id: %d", lf.lastID)
	}
}

func tempPath() string {
	return filepath.Join(test.TempDir(), fmt.Sprintf("waltest-%d.log", os.Getpid()))
}
