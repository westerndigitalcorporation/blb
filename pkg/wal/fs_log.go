// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package wal

import (
	"fmt"
	"os"
	"path"
	"sort"
	"sync"

	log "github.com/golang/glog"
)

// defaultMaxFileSize is the size (in bytes) to which we try to limit
// individual log files. The bigger this value, more efficient we are in terms
// of overhead (fewer extents and inodes), but the less space we can reclaim
// on a Trim().
const defaultMaxFileSize = 4 * 1024 * 1024

// fsLog is an implementation of a WAL that uses a single local file system,
// specifically a single directory.
// Its goals are:
// 1. Durablilty and data integrity. Try as hard as possible to protect
//    data that we've promised we've written.
// 2. Low overhead: we're not storing user data, so everything we do
//    is implementation overhead. Try to minimize that overhead.
//
// The generic structure is a sequence of log files named like
// wal-<sequence_number>.log. Each file has a unique sequence number and they
// are assigned sequentially starting at zero. The sequence number has
// no correspondence to the records contained within.
// A well-formed log consists of a set of zero or more log files with
// consecutive sequence numbers; this implies a set of Records with
// consecutive IDs. This is a simple way to detect whether files
// have been lost by noticing gaps in the files' sequence numbers; however
// this does not detect if files were lost at the "ends". To detect this,
// we could add a catalog file with all files expected to be part of the
// log (like the MANIFEST in LevelDB).
// The most recent log file is used for appending, until it exceeds
// maxFileSize. Then a new file with a higher sequence number is created.
// Log files are deleted when a client indicates via Trim() that certain
// Records are no longer needed; any files containing only unneeded Records
// are then removed. Log files can also be deleted by calls to Truncate,
// if the Record ID for Truncation is before the start of the files.
//
// This is the entry point; the logic for a single file is in fs_log_file.go
// and the logic for serializing and deserializing a record is in record.go.
type fsLog struct {
	homeDir string // the path to the dir where the Log lives.

	// existingFiles contains info about the log files, sorted by
	// sequence number. It is used to determine in which file to
	// start looking for a record when iterating or Trim()ing.
	//
	// There is an entry for the current file, but its firstID
	// field may not be accurate (e.g., if the file is empty).
	existingFiles []fileInfo
	curFile       *logFile   // the file currently being appended to
	lock          sync.Mutex // guards the three above fields

	// The maximum size in bytes of a file in the log. We allow this
	// to be set to arbitrary sizes for testing purposes.
	maxFileSize int64
}

// fileInfo describes an existing log file that is part of a log.
type fileInfo struct {
	seqNum  int    // the sequence number of the file
	firstID uint64 // the ID of the first record contained in the file
}

// OpenFSLog opens a new or existing durable WAL backed by a directory
// in a filesystem.
// At most one WAL log can be stored within a directory.
func OpenFSLog(homeDir string) (Log, error) {
	logFiles, err := readExistingFiles(homeDir)
	if err != nil {
		return nil, err
	}

	l := &fsLog{
		homeDir:       homeDir,
		existingFiles: logFiles,
		maxFileSize:   defaultMaxFileSize,
	}

	if len(logFiles) == 0 {
		// Opening for the first time.
		log.Infof("Creating new WAL at %q", homeDir)
		err = l.addLogFile()
	} else {
		// Opening an existing log, open the file with the highest
		// seqeunce number.
		seqNum := logFiles[len(logFiles)-1].seqNum
		logFileName := l.generateLogFileName(seqNum)
		l.curFile, err = openLogFile(logFileName)
	}
	if err != nil {
		return nil, err
	}

	log.V(10).Infof("Opened FS log at %q", homeDir)

	return l, nil
}

// readExistingFiles populate a slice of fileInfos for all the log
// files that belong to this log, sorted by sequence number.
func readExistingFiles(homeDir string) ([]fileInfo, error) {
	// TODO: Add a file lock, to prevent multiple processes from
	// hurting each other.
	dir, err := os.Open(homeDir)
	if err != nil {
		log.Errorf("Failed to open WAL home dir %q: %v", homeDir, err)
		return nil, err
	}

	defer dir.Close()

	// Read all the directory's childen in one go (0). Could be problematic
	// for really huge logs. But with 4MB log files, even a 1000 files is
	// 4 GB, which we shouldn't reach if we are being used well.
	children, err := dir.Readdirnames(0)
	if err != nil {
		log.Errorf("Failed to read home dir %q: %v", homeDir, err)
		return nil, err
	}

	// Sort the files so they are ordered based on sequence number
	// which should be the order the records are in.
	sort.Strings(children)

	// Build our in-memory index of the log files and where the record
	// ID boundaries are.
	logFiles := []fileInfo{}
	for _, child := range children {
		var seqNum int
		if _, err = fmt.Sscanf(child, "wal-%d.log", &seqNum); err != nil {
			log.V(10).Infof("Skipping non-log file %s", child)
			continue
		}

		fullPath := path.Join(homeDir, child)
		lf, err := openLogFileForRead(fullPath)
		if err != nil {
			log.Errorf("Failed to open log file %q to get start: %v", fullPath, err)
			return nil, err
		}

		childInfo := fileInfo{
			seqNum:  seqNum,
			firstID: lf.firstID,
		}

		log.V(10).Infof("Found existing log file %+v", childInfo)
		logFiles = append(logFiles, childInfo)
		lf.Close()
	}

	// Sanity check the set of files. Their sequence numbers should be
	// sequential (no gaps), otherwise files might be missing.
	for i := 1; i < len(logFiles); i++ {
		if logFiles[i-1].seqNum+1 != logFiles[i].seqNum {
			// A file got deleted or the directory is corrupt.
			log.Errorf("Missing log file %d; set is %v", logFiles[i-1].seqNum+1, logFiles)
			return nil, fmt.Errorf("DATA LOSS")
		}
	}

	return logFiles, nil
}

// addLogFile creates a new empty log file for appending
// based on the log's current file sequence number. Returns an error
// if the file already exists, which should never happen during normal
// operation since log file sequence numbers are monotonically increasing.
func (l *fsLog) addLogFile() error {
	nextSeqNum := 0
	if len(l.existingFiles) > 0 {
		// The next file's sequence number should be one greater
		// then previous one's.
		nextSeqNum = l.existingFiles[len(l.existingFiles)-1].seqNum + 1
	}
	name := l.generateLogFileName(nextSeqNum)
	newFileInfo := fileInfo{seqNum: nextSeqNum}
	l.existingFiles = append(l.existingFiles, newFileInfo)
	nextFile, err := createLogFile(name)
	if err != nil {
		log.Errorf("Failed to create file %q: %v", name, err)
		return err
	}

	// Ensure that the data changes to the directory are synced,
	// so the file doesn't disappear after a hard crash.
	if err = l.syncHomeDir(); err != nil {
		return err
	}

	l.curFile = nextFile
	return nil
}

// generateLogFileName formats log files with sufficient zero padding on
// the sequence number that lexical sorts should agree with numerical sorts.
func (l *fsLog) generateLogFileName(seqNum int) string {
	return path.Join(l.homeDir, fmt.Sprintf("wal-%.10d.log", seqNum))
}

// FirstID implements wal.Log.
func (l *fsLog) FirstID() (id uint64, empty bool) {
	l.lock.Lock()
	defer l.lock.Unlock()
	// We don't keep the last file's firstID updated after every
	// mutation, so make sure it is updated now.
	l.existingFiles[len(l.existingFiles)-1].firstID = l.curFile.firstID
	if len(l.existingFiles) == 1 && l.curFile.empty {
		// We always have one file but it might be empty.
		return 0, true
	}
	return l.existingFiles[0].firstID, false
}

// LastID implements wal.Log.
func (l *fsLog) LastID() (id uint64, empty bool) {
	l.lock.Lock()
	defer l.lock.Unlock()
	if len(l.existingFiles) == 1 && l.curFile.empty {
		return 0, true
	}
	return l.curFile.lastID, false
}

// GetIterator implements wal.Log.
func (l *fsLog) GetIterator(start uint64) Iterator {
	l.lock.Lock()
	defer l.lock.Unlock()
	// Figure out which file to start with.
	startFI := l.getFileContainingID(start)

	itr := &fsLogItr{
		l:      l,
		seqNum: l.existingFiles[startFI].seqNum,
	}

	// Called the unlocked version of getFile since we already hold
	// the log's lock.
	file, err := l.getFileUnlocked(itr.seqNum)
	if err != nil {
		// Save the error to halt future iteration.
		itr.err = err
		return itr
	}
	if file == nil {
		log.Fatalf("getFileContainingID returned invalid index (%d not found)!", startFI)
	}

	itr.fileItr = file.GetIterator(start)
	itr.file = file

	return itr
}

// fsLogItr allows iteration of the Records in an fsLog.
type fsLogItr struct {
	// We use the state in fsLog to figure out what the next file is.
	l      *fsLog
	seqNum int // sequence number of the current file

	fileItr Iterator // delegate to the file's iterator for most of the work
	file    *logFile // open file for the iterator
	err     error    // Err from opening a log file
}

// Next implements wal.Iterator.
// We should be able to tolerate appends, unless we already reached the
// end of the file. If there is a Trim, we will be in trouble if the
// trim removes both the current file
func (itr *fsLogItr) Next() bool {
	if itr.Err() != nil {
		// We hit an error and should not continue.
		return false
	}

	if itr.fileItr.Next() {
		return true
	}

	// The current iterator is finished, but see if we have more
	// log files to iterate.
	itr.seqNum++

	nextFile, err := itr.l.getFile(itr.seqNum)
	if err != nil {
		itr.err = err
		// When itr.err is non-nil, the code in Close
		// expects that the iterator and file are closed.
		itr.fileItr.Close()
		itr.file.Close()
		return false
	}
	if nextFile == nil {
		return false
	}

	err = itr.fileItr.Close()
	itr.file.Close() // Close the file whenever we close the iterator
	if err != nil {
		itr.err = err
		return false
	}

	itr.file = nextFile
	itr.fileItr = nextFile.GetIterator(0)

	return itr.fileItr.Next()
}

// Record implements wal.Iterator.
func (itr *fsLogItr) Record() Record {
	return itr.fileItr.Record()
}

// Err implements wal.Iterator.
func (itr *fsLogItr) Err() error {
	if itr.err != nil {
		return itr.err
	}
	return itr.fileItr.Err()
}

// Close implements wal.Iterator.
func (itr *fsLogItr) Close() error {
	if itr.err != nil {
		// If there is a top-level error, we don't have a fileItr
		// to close.
		return itr.err
	}

	err := itr.fileItr.Close()
	itr.file.Close()
	return err
}

// getFile returns a pointer to a read-only log file with the given
// sequence number, or nil if no file with that sequence number exists.
func (l *fsLog) getFile(seqNum int) (f *logFile, err error) {
	l.lock.Lock()
	defer l.lock.Unlock()
	return l.getFileUnlocked(seqNum)
}

// getFileUnlocked is the same as getFile, except without the locking for
// use within fsLog methods that already hold the lock.
func (l *fsLog) getFileUnlocked(seqNum int) (f *logFile, err error) {
	// Check that the sequence number refers to a file that is
	// currently valid.
	if seqNum < l.existingFiles[0].seqNum {
		// Can only happen if the client messes with the log
		// during iteration.
		return nil, fmt.Errorf("Invalid sequence number %d < earliest %d", seqNum, l.existingFiles[0].seqNum)
	}

	if seqNum > l.existingFiles[len(l.existingFiles)-1].seqNum {
		// Hit the end of the iterator.
		return nil, nil
	}

	name := l.generateLogFileName(seqNum)
	nextFile, err := openLogFileForRead(name)
	if err != nil {
		log.Errorf("Failed to open log file %q for iterator: %v", name, err)
		return nil, err
	}

	return nextFile, nil
}

// Append implements wal.Log.
func (l *fsLog) Append(recs ...Record) error {
	l.lock.Lock()
	defer l.lock.Unlock()

	// Make sure the client is supplying consecutive Record IDs.
	lastID := l.curFile.lastID
	if l.curFile.empty && len(recs) > 0 {
		lastID = recs[0].ID - 1
	}
	for i, r := range recs {
		if r.ID != lastID+uint64(i)+1 {
			return fmt.Errorf("Invalid Record ID, expected %d", lastID+uint64(i)+1)
		}
	}

	size := l.curFile.Size()
	// It's much easier to check after the fact, but this won't work
	// when we switch to pre-allocating.
	if size >= l.maxFileSize {
		// Update the last existingFile info, since when the file is
		// closed this is the only place the data can be found.
		l.existingFiles[len(l.existingFiles)-1].firstID = l.curFile.firstID
		l.curFile.Close()
		if err := l.addLogFile(); err != nil {
			return err
		}
	}

	err := l.curFile.Append(recs...)
	return err
}

// Truncate implements wal.Log.
func (l *fsLog) Truncate(lastToKeep uint64) error {
	l.lock.Lock()
	defer l.lock.Unlock()
	// Delete any files that have a first index after lastToKeep,
	// since their Records need to be removed from the log.
	// Start from the file with the largest sequence number and work
	// backwards so we keep the set of log files valid at each step.
	lastSafeFile := l.getFileContainingID(lastToKeep)
	filesToDelete := l.existingFiles[lastSafeFile+1:]
	err := l.deleteFiles(filesToDelete, deleteFromBack, "Truncate")
	if err != nil {
		return err
	}

	if len(filesToDelete) > 0 {
		// If we deleted any files, then the log file for
		// curFile has been deleted and we need to open
		// the file that is now the last to be our current.
		l.curFile.Close()
		curFileSeqNum := l.existingFiles[len(l.existingFiles)-1].seqNum
		name := l.generateLogFileName(curFileSeqNum)
		l.curFile, err = openLogFile(name)
		if err != nil {
			log.Errorf("Failed to open log file %q for truncate: %v", name, err)
			return err
		}
	}

	return l.curFile.Truncate(lastToKeep)
}

// Trim implements wal.Log.
func (l *fsLog) Trim(discardUpTo uint64) error {
	l.lock.Lock()
	defer l.lock.Unlock()
	// We don't change any existing files, only delete entire files
	// that are before our target.
	// That means we never trim the current file.

	firstSafeFile := l.getFileContainingID(discardUpTo)
	filesToDelete := l.existingFiles[:firstSafeFile]
	err := l.deleteFiles(filesToDelete, deleteFromFront, "Trim")
	if err != nil {
		return err
	}

	return nil
}

// Close implements wal.Log.
func (l *fsLog) Close() {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.curFile.Close()
	l.curFile = nil
}

// getFileContainingID returns the index into l.existingFiles of the
// fileInfo representing the log file that contains the given ID.
// If the ID is less than those contained in any file, 0 is returned;
// if it is greater, the index of the last file is returned.
func (l *fsLog) getFileContainingID(id uint64) int {
	prev := 0
	// We don't keep the last file's firstID updated after every
	// mutation, so make sure it is updated now.
	l.existingFiles[len(l.existingFiles)-1].firstID = l.curFile.firstID
	for i := range l.existingFiles {
		if l.existingFiles[i].firstID > id {
			break
		}
		prev = i
	}

	return prev
}

const (
	deleteFromFront = iota
	deleteFromBack
)

// deleteFiles deletes log files either starting at the first log file
// and working forwards or starting at the last log files and working
// backwards. It maintains the invariant that the set of log files has
// consecutive sequence numbers.
func (l *fsLog) deleteFiles(filesToDelete []fileInfo, deleteType int, cmd string) error {
	for i := range filesToDelete {
		idx := i
		if deleteType == deleteFromBack {
			idx = len(filesToDelete) - i - 1
		}
		seqNum := filesToDelete[idx].seqNum
		name := l.generateLogFileName(seqNum)
		if err := os.Remove(name); err != nil {
			log.Errorf("Failed to remove file %s for %s: %v", name, cmd, err)
			return err
		}

		log.Infof("Deleted file %q for %s", name, cmd)

		// We have to sync here after each deletion, otherwise
		// if the directory is large, the block scheduler or the
		// device might re-order writes, and a removal in the
		// middle might be written before an earlier removal,
		// which if we then hard crash, might make the log
		// appear to have lost data.
		// Having a catalog of files could be more efficient,
		// since we could just sync that once after all changes.
		if err := l.syncHomeDir(); err != nil {
			return err
		}
	}

	// Update the bookkeeping.
	if deleteType == deleteFromFront {
		l.existingFiles = l.existingFiles[len(filesToDelete):]
	} else {
		cutAt := len(l.existingFiles) - len(filesToDelete)
		l.existingFiles = l.existingFiles[:cutAt]
	}

	return nil
}

// syncHomeDir syncs the state of the log's directory to ensure the
// set of files that compose the log is consistent and durable.
func (l *fsLog) syncHomeDir() error {
	dir, err := os.Open(l.homeDir)
	if err != nil {
		log.Errorf("Failed to open dir %q for fsync: %v", l.homeDir, err)
		return err
	}

	if err = dir.Sync(); err != nil {
		log.Errorf("Failed to fsync dir %q: %v", l.homeDir, err)
		if cerr := dir.Close(); cerr != nil {
			log.Errorf("Failed to close dir %q: %v", l.homeDir, cerr)
		}
		return err
	}

	if err = dir.Close(); err != nil {
		log.Errorf("Failed to close dir %q: %v", l.homeDir, err)
		return err
	}

	return nil
}
