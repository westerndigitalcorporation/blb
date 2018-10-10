// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package raftfs

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	log "github.com/golang/glog"
	"github.com/westerndigitalcorporation/blb/pkg/disk"
	"github.com/westerndigitalcorporation/blb/pkg/raft/raft"
)

const (
	snapshotPrefix     = "snapshot"
	snapshotTempSuffix = ".tmp"
	// Keep last 2 snapshot files instead of just the last one so we get a
	// chance to to restore Raft state even the last one gets corrupted.
	snapRetention = 2
)

// Returns true if the name is a valid snapshot file name.
func isValidSnapshotName(name string) bool {
	return strings.HasPrefix(name, snapshotPrefix+"-") && !strings.HasSuffix(name, snapshotTempSuffix)
}

// Returns true if the name is a valid temporary snapshot file name.
func isValidSnapshotTempName(name string) bool {
	return strings.HasPrefix(name, snapshotPrefix+"-") && strings.HasSuffix(name, snapshotTempSuffix)
}

// Extracts the raft meatadata from the name of a snapshot file.
// The name MUST be a valid snapshot file name.
func snapshotNameToMetadata(name string) raft.SnapshotMetadata {
	var term, index uint64
	n, err := fmt.Sscanf(name, snapshotPrefix+"-%d-%d", &term, &index)
	if n != 2 || err != nil {
		log.Fatalf("Failed to parse snapshot metadata from file name: %v", err)
	}
	return raft.SnapshotMetadata{LastTerm: term, LastIndex: index}
}

// Returns a snapshot file name given the metadata of a Raft.
func metadataToSnapshotName(meta raft.SnapshotMetadata) string {
	return fmt.Sprintf("%s-%020d-%020d", snapshotPrefix, meta.LastTerm, meta.LastIndex)
}

// fsSnapshotMgr is a durable file-based implementation of Snapshot. The cached
// metadata can never be out of sync with the on-file version because: (1) There
// is only one writer exists, and the update of the cached metadata and committment
// of the snapshot file is done atomically and serialized with all other reads;
// (2) program panics if the write ever fails.
//
// The snapshot file has the format as below(Snapshot metadata is encoded using
// Gob):
//
// ----------------------------------------------------------------
// | metadata len(4 bytes) | encoded metadata |  ... payload ...  |
// ----------------------------------------------------------------
//
// fsSnapshotMgr is implemented in a way that being accessed by one writer
// and one(or more) reader concurrently is still safe. To understand how
// the safety is guaranteed we first need to understand:
//
// The steps of openning(and reading) a latest snapshot file:
//
//  - Access the cached metadata
//  - Open the file corresponding to the cached metadata(might be read later).
//
// The steps of writing and commiting a snapshot file:
//
//  - Create a temporary file and dump state there.
//  - Commit(fsync file, rename, fsync directory) the file.
//  - Update the cached metadata to metadata of the file that was just written.
//  - Delete all previous files.
//
// So it's possible that while a reader is reading data of a snapshot file but
// a newer one gets received and committed. To guarantee the new committed one
// will not affect the previous one that is being accessed, we have to
// guarantee:
//
//  - Concurrent accesses to cached metadata is safe. This is protected by
//    locking.
//  - The step of accessing the cached metadata and opening the file
//    corresponding to the metadata must be serialized with the update of
//    the metadata and the deletion of all previous files. Otherwise it's
//    possible that a file is deleted from disk after its metadata is accessed
//    but before the file is opened.
//
// But the deletion of previous files and reading of a stale file can happen in
// parallel. Given the file has already been opened, the OS will protect the
// opened file from being deleted.
//
type fsSnapshotMgr struct {
	homeDir string // home directory on the local filesystem

	lock sync.Mutex // protects the fileds below
	// The metadata of the most recent snapshot, it will be 'NilSnapshotMetadata'
	// if no snapshot exists.
	meta raft.SnapshotMetadata
}

// NewFSSnapshotMgr creates a fsSnapshotMgr backed by a snapshot file under
// directory 'homeDir' in a filesystem.
func NewFSSnapshotMgr(homeDir string) (raft.SnapshotManager, error) {
	// Check if 'homeDir' exists. Panic if it doesn't.
	if !isExist(homeDir) {
		return nil, fmt.Errorf("homeDir %q doesn't exist", homeDir)
	}

	// Create a snapshot object.
	f := &fsSnapshotMgr{
		meta:    raft.NilSnapshotMetadata,
		homeDir: homeDir,
	}

	snapshots := f.getSnapshots()
	if len(snapshots) != 0 {
		// If there're snapshots, initializes 'f.meta' to the metadata of the most
		// recent snapshot.
		reader := f.newSnapshotFileReader(snapshots[len(snapshots)-1])
		f.meta = reader.(*snapshotFileReader).meta
		reader.Close()
	}

	// Remove all stale and temporary snapshots.
	f.cleanupSnapshots()
	return f, nil
}

// BeginSnapshot implements SnapshotManager.
func (f *fsSnapshotMgr) BeginSnapshot(meta raft.SnapshotMetadata) (raft.SnapshotFileWriter, error) {
	return f.newSnapshotFileWriter(meta)
}

// GetSnapshot implements SnapshotManager.
func (f *fsSnapshotMgr) GetSnapshot() raft.SnapshotFileReader {
	f.lock.Lock()
	defer f.lock.Unlock()
	if f.meta == raft.NilSnapshotMetadata {
		return nil
	}
	return f.newSnapshotFileReader(metadataToSnapshotName(f.meta))
}

// GetSnapshotMetadata implements SnapshotManager.
func (f *fsSnapshotMgr) GetSnapshotMetadata() (raft.SnapshotMetadata, string) {
	f.lock.Lock()
	defer f.lock.Unlock()
	return f.meta, filepath.Join(f.homeDir, metadataToSnapshotName(f.meta))
}

// cleanupSnapshots removes all temporary snapshot files and all stale snapshot
// files.
func (f *fsSnapshotMgr) cleanupSnapshots() {
	homeDir, err := os.Open(f.homeDir)
	if err != nil {
		log.Errorf("Failed to open snapshot home directory: %v", err)
		return
	}
	files, err := homeDir.Readdirnames(0)
	homeDir.Close()
	if err != nil {
		log.Errorf("Failed ot read the directory that stores snapshots")
		return
	}
	// Remove all temporary snapshot files.
	for _, file := range files {
		if isValidSnapshotTempName(file) {
			if err := os.Remove(filepath.Join(f.homeDir, file)); err != nil {
				log.Errorf("Failed to remove temporary snapshot file: %v", err)
			}
		}
	}
	snapshots := f.getSnapshots()
	if len(snapshots) <= snapRetention {
		return
	}

	// Remove all snapshot files but keep the most recent "snapRetention".
	for _, snapshot := range snapshots[:len(snapshots)-snapRetention] {
		if err := os.Remove(filepath.Join(f.homeDir, snapshot)); err != nil {
			log.Errorf("Failed to remove stale snapshot file: %v", err)
		}
	}
}

// Returns all snapshot file names sorted by name in increasing order.
func (f *fsSnapshotMgr) getSnapshots() []string {
	files, err := ioutil.ReadDir(f.homeDir)
	if err != nil {
		log.Fatalf("Failed ot read the directory that stores snapshots")
	}
	var snapshots []string
	for _, file := range files {
		if isValidSnapshotName(file.Name()) {
			snapshots = append(snapshots, file.Name())
		}
	}
	if len(snapshots) <= 1 {
		return snapshots
	}
	// Sort snapshot files by their names in increasing order.
	sort.Strings(snapshots)
	return snapshots
}

//--------------------
// snapshotFileWriter
//--------------------

// snapshotFileWriter implements SnapshotFileWriter and is used by fsSnapshotMgr
// for writing snapshot file.
type snapshotFileWriter struct {
	meta     raft.SnapshotMetadata
	writer   *disk.ChecksumFile // the handle of the temporary snapshot file
	homeDir  string             // home directory on the local filesystem
	snapFile string             // snapshot file
	temp     string             // temporary snapshot file
	// Store a reference to snapshot manager as we need to update metadata of
	// the latest snapshot file and ask it to cleanup snapshot directory after
	// committing the snapshot.
	mgr *fsSnapshotMgr
}

// newSnapshotFileWriter creates a new snapshotFileWriter. A temporary file is
// opened to write the snapshot metadata, and its handle is kept in the writer.
func (f *fsSnapshotMgr) newSnapshotFileWriter(meta raft.SnapshotMetadata) (raft.SnapshotFileWriter, error) {
	// Create a temporary file to write the snapshot. Note that we can
	// safely truncate the file if it already exists.
	snapshotFile := metadataToSnapshotName(meta)
	tmpSnapshotFile := filepath.Join(f.homeDir, snapshotFile+snapshotTempSuffix)
	temp, err := disk.NewChecksumFile(tmpSnapshotFile, os.O_CREATE|os.O_TRUNC|os.O_RDWR)
	if nil != err {
		log.Errorf("failed to create file %q: %s", tmpSnapshotFile, err)
		return nil, err
	}

	if err := encodeSnapshotMetadata(temp, meta); err != nil {
		return nil, err
	}

	// Pass the handle to the writer.
	return &snapshotFileWriter{
		meta:     meta,
		writer:   temp,
		homeDir:  f.homeDir,
		snapFile: filepath.Join(f.homeDir, snapshotFile),
		temp:     tmpSnapshotFile,
		mgr:      f,
	}, nil
}

// Write implements io.Writer. The data is written to the temporary snapshot
// file.
func (w *snapshotFileWriter) Write(p []byte) (int, error) {
	return w.writer.Write(p)
}

// Commit implements SnapshotFileWriter. This method fsyncs and closes the
// temporary file, renames it to replace the original snapshot file, and fsyncs
// the home directory. The cached metadata is also updated.
func (w *snapshotFileWriter) Commit() (err error) {
	// Sync and close the temporary file.
	if err = w.writer.Close(); nil != err {
		log.Errorf("failed to close file: %s", err)
		return err
	}

	// 'w.mgr.meta.LastIndex' stores the latest index of current "effective"
	// snapshot file and "w.meta.LastIndex" stores the index of this temporary
	// file that's going to be committed. It's users' responsibility to guarantee
	// they can not commit a snapshot file which is staler than the current one.
	if w.mgr.meta.LastIndex > w.meta.LastIndex {
		// Sanity check
		log.Fatalf("bug: newly created snapshot is staler than current one")
	}

	// Rename the temporary file to replace the original metadata file.
	if err = disk.Rename(w.temp, w.snapFile); nil != err {
		log.Errorf("failed to rename file: %s", err)
		return err
	}

	// Update cached metadata.
	w.mgr.lock.Lock()
	w.mgr.meta = w.meta
	w.mgr.lock.Unlock()

	log.Infof("snapshot committed: [LastTerm: %d, LastIndex: %d]", w.meta.LastTerm, w.meta.LastIndex)
	// Cleanup unused snapshot files, if there're any.
	w.mgr.cleanupSnapshots()
	return nil
}

// Abort implements SnapshotFileWriter. This method doesn't try to remove the
// temporary file because next time it gets truncated first when we write to it.
func (w *snapshotFileWriter) Abort() (err error) {
	log.Infof("snapshot aborted: [LastTerm: %d, LastIndex: %d]", w.meta.LastTerm, w.meta.LastIndex)
	return w.writer.Close()
}

// GetMetadata returns the metadata of the snapshot file.
func (w *snapshotFileWriter) GetMetadata() raft.SnapshotMetadata {
	return w.meta
}

//--------------------
// snapshotFileReader
//--------------------

// snapshotFileReader implements SnapshotFileReader and is used by fsSnapshot
// for reading snapshot files.
type snapshotFileReader struct {
	reader io.ReadCloser // the handle of the snapshot file
	meta   raft.SnapshotMetadata
}

func (f *fsSnapshotMgr) newSnapshotFileReader(name string) raft.SnapshotFileReader {
	file, err := disk.NewChecksumFile(filepath.Join(f.homeDir, name), os.O_RDONLY)
	if nil != err {
		log.Fatalf("failed to open file: %s", err)
	}

	var meta raft.SnapshotMetadata
	if err := decodeSnapshotMetadata(file, &meta); err != nil {
		log.Fatalf("failed to decode snapshot metadata: %v", err)
	}

	// Pass the handle to the reader.
	return &snapshotFileReader{reader: file, meta: meta}
}

// Read implements io.Reader.
func (r *snapshotFileReader) Read(p []byte) (int, error) {
	return r.reader.Read(p)
}

// Close implements io.Closer.
func (r *snapshotFileReader) Close() error {
	return r.reader.Close()
}

// GetMetadata implements SnapshotFileReader.
func (r *snapshotFileReader) GetMetadata() raft.SnapshotMetadata {
	return r.meta
}

func encodeSnapshotMetadata(writer io.Writer, meta raft.SnapshotMetadata) error {
	// Write the encoded metadata to a buffer first so we can get the length.
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	if err := enc.Encode(meta); err != nil {
		return err
	}
	metaLen := len(buffer.Bytes())
	// Write the length of encoded metadata to snapshot file.
	if err := binary.Write(writer, binary.LittleEndian, uint32(metaLen)); err != nil {
		return err
	}
	// Write the encoded metadata.
	if _, err := writer.Write(buffer.Bytes()); err != nil {
		return err
	}
	return nil
}

func decodeSnapshotMetadata(reader io.Reader, meta *raft.SnapshotMetadata) error {
	// First read the length of encoded metadata.
	var metaLen uint32
	if err := binary.Read(reader, binary.LittleEndian, &metaLen); err != nil {
		return err
	}
	// Read the encoded metadata. We need to use 'LimitReader' here otherwise
	// Gob will read past the encoded data and consume the acutal payload.
	dec := gob.NewDecoder(io.LimitReader(reader, int64(metaLen)))
	if err := dec.Decode(meta); err != nil {
		return err
	}
	return nil
}
