// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package wal

import (
	"io"
	"os"

	log "github.com/golang/glog"
)

// logFile is a single file in WAL. It is responsible for managing the
// I/O to the file and converting between IDs and byte offsets.
// A log file contains a sequence of serialized Records. The IDs of
// consecutive Records should be sequential.
type logFile struct {
	f *os.File

	empty   bool   // Valid for all logFiles
	firstID uint64 // Valid for all logFiles
	lastID  uint64 // Only valid for logFiles opened for appending
}

// createLogFile creates a log file at the given path. The log file
// must not already exist.
func createLogFile(path string) (*logFile, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		log.Errorf("Failed to open %q: %v", path, err)
		return nil, err
	}

	// TODO: Preallocate the file. However, we call Truncate a lot,
	// and a look through the ext4 source code does not seem promising
	// that preallocations will survive that.

	log.Infof("Successfully created log file at %q", path)

	return &logFile{f: f, empty: true}, nil
}

// openLogFile opens the given, existing log file for appending.
func openLogFile(path string) (*logFile, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		log.Errorf("Failed to open %q: %v", path, err)
		return nil, err
	}

	lf := &logFile{f: f, empty: true}

	// Go through all the records until the end to find the last
	// Record so we know the last ID.
	for {
		rec, err := lf.readRecord()
		if err == io.EOF {
			// EOF means we have hit the end cleanly, so don't treat it as an error.
			break
		} else if err != nil {
			log.Errorf("Hit corruption when opening %q: %v", path, err)
			return nil, err
		}

		if lf.empty {
			lf.firstID = rec.ID
		}
		lf.lastID = rec.ID
		lf.empty = false
	}

	log.Infof("Successfully opened log file at %q", path)

	return lf, nil
}

// openLogFileForRead opens the given, existing log file for reading,
// with the offset set to the beginning.
func openLogFileForRead(path string) (*logFile, error) {
	f, err := os.Open(path)
	if err != nil {
		log.Errorf("Failed to open %q: %v", path, err)
		return nil, err
	}

	lf := &logFile{f: f, empty: true}

	// Read the first record to determine the first record ID
	// Not the most efficient, but it makes the code cleaner.
	rec, err := lf.readRecord()
	if err != nil && err != io.EOF {
		lf.f.Close()
		log.Errorf("Failed to read 1st record of %q: %v", path, err)
		return nil, err
	}

	if err == nil {
		lf.firstID = rec.ID
		lf.empty = false
	}

	if _, err = lf.f.Seek(0, os.SEEK_SET); err != nil {
		f.Close()
		log.Errorf("Failed to reset offset of %q: %v", path, err)
		return nil, err
	}

	return lf, nil

}

// Append appends a series of records to the logFile. If the append fails,
// no guarantees are made about the start of the file except that records
// previously written should still be readable.
func (lf *logFile) Append(recs ...Record) (err error) {
	for _, r := range recs {
		if err = r.serialize(lf.f); err != nil {
			log.Errorf("Record write failed on %q: %v", lf.f.Name(), err)
			return err
		}
	}

	if err = lf.f.Sync(); err != nil {
		log.Errorf("Failed to sync %q after append: %v", lf.f.Name(), err)
		return err
	}

	log.V(10).Infof("Successfully appended %d records to %q", len(recs), lf.f.Name())
	// Update bookkeeping
	if len(recs) > 0 {
		if lf.empty {
			lf.firstID = recs[0].ID
		}
		lf.empty = false

		lf.lastID = recs[len(recs)-1].ID
	}

	return nil
}

// Size returns the size of the file in bytes.
func (lf *logFile) Size() int64 {
	fi, err := lf.f.Stat()
	if err != nil {
		log.Fatalf("Failed to stat %q: %v", lf.f.Name(), err)
	}

	return fi.Size()
}

// Truncate removes any records after the one with ID lastToKeep
// and sets the file offset to the new end of the file.
func (lf *logFile) Truncate(lastToKeep uint64) error {
	if lastToKeep >= lf.lastID {
		// Truncating past the end is a no-op.
		log.Infof("Ignoring truncate at or past end (%d >= %d)", lastToKeep, lf.lastID)
		return nil
	}

	if err := lf.seekToID(lastToKeep + 1); err != nil {
		return err
	}

	if err := lf.f.Truncate(lf.getOffset()); err != nil {
		return err
	}

	// Update bookkeeping
	if lastToKeep < lf.firstID {
		// If the request if for an ID before the first in this file
		// then everything up to the first ID will be removed.
		lf.empty = true
		lf.lastID = 0
		lf.firstID = 0
	} else {
		lf.lastID = lastToKeep
	}

	return nil
}

// seekToID seeks the file to the start of the record with the given ID
func (lf *logFile) seekToID(id uint64) error {
	if _, err := lf.f.Seek(0, os.SEEK_SET); err != nil {
		log.Fatalf("Failed to seek to begining of file %q: %v", lf.f.Name(), err)
	}

	if lf.firstID >= id {
		return nil
	}

	for {
		rec, err := lf.readRecord()
		if err != nil {
			return err
		}

		// If we just read the record before the requested ID, then the
		// file position should be right.
		if rec.ID == id-1 {
			break
		}
	}
	return nil
}

func (lf *logFile) getOffset() int64 {
	n, err := lf.f.Seek(0, os.SEEK_CUR)
	if err != nil {
		// Should not happen, we are only seek to query position.
		log.Fatalf("Seek to get offset on %s failed: %v", lf.f.Name(), err)
	}
	return n
}

// GetIterator returns an iterator for accessing the Records in this file.
func (lf *logFile) GetIterator(start uint64) Iterator {
	err := lf.seekToID(start)
	return &logFileItr{lf: lf, err: err}
}

// logFileItr implements Iterator and allows sequential access to the Records
// in a log file.
type logFileItr struct {
	lf  *logFile // Use the *os.File here to keep the state
	rec Record   // current record
	err error    // last error encountered

}

// Next implements wal.Iterator
func (itr *logFileItr) Next() bool {
	if itr.err != nil {
		// Once we hit an error, we are done.
		return false
	}

	itr.rec, itr.err = itr.lf.readRecord()
	return itr.err == nil
}

// Record implements wal.Iterator
func (itr *logFileItr) Record() Record {
	return itr.rec
}

// Err implements wal.Iterator.
func (itr *logFileItr) Err() error {
	if itr.err == io.EOF {
		// EOF doesn't count as as a failure, but we are done.
		return nil
	}
	return itr.err
}

// Close implements wal.Iterator.
func (itr *logFileItr) Close() error {
	itr.lf = nil
	if itr.err == io.EOF {
		// EOF doesn't count as as a failure, but we are done.
		return nil
	}
	return itr.err
}

// Close closes the logFile.
func (lf *logFile) Close() {
	if lf.f != nil {
		if err := lf.f.Close(); err != nil {
			// Shouldn't happen since we Sync after every
			// write, so this would only indicate a programmer
			// error.
			log.Fatalf("Failed to close log file %q: %v", lf.f.Name(), err)
		}
		lf.f = nil
	}
}

// readRecord tries to read one record from the current position.
// It automatically truncates incomplete records.
func (lf *logFile) readRecord() (r Record, err error) {
	offset := lf.getOffset()
	r, err = deserializeRecord(lf.f)
	if err == io.ErrUnexpectedEOF {
		log.Errorf("Hit unexpected EOF reading from wal at offset %d. Truncating.", offset)

		if err = lf.f.Truncate(offset); err != nil {
			return
		}
		if _, err = lf.f.Seek(offset, os.SEEK_SET); err != nil {
			return
		}

		// Pretend this was a regular EOF.
		err = io.EOF
	}
	return
}
