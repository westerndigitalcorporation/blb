// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package raftfs

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/westerndigitalcorporation/blb/pkg/disk"
	"github.com/westerndigitalcorporation/blb/pkg/raft/raft"
	test "github.com/westerndigitalcorporation/blb/pkg/testutil"
)

// testFSSnapshotEnv creates a fsSnapshot, saves a snapshot with metadata
// 'meta' and payload 'data', and checks for errors in each step.
func testFSSnapshotEnv(meta raft.SnapshotMetadata, data []byte, t *testing.T) raft.SnapshotManager {
	// Create temporary homeDir.
	homeDir, err := ioutil.TempDir(test.TempDir(), "snapshot")
	if nil != err {
		t.Fatalf("failed to create temporary directory: %s", err)
	}

	// Create a fsSnapshot.
	f, err := NewFSSnapshotMgr(homeDir)
	if nil != err {
		t.Fatalf("failed to create snapshot mgr: %s", err)
	}

	// Snapshot and its metadata should not exist.
	if reader := f.GetSnapshot(); reader != nil {
		t.Errorf("snapshot should not exist")
	}
	if meta, _ := f.GetSnapshotMetadata(); meta != raft.NilSnapshotMetadata {
		t.Errorf("snapshot metadata should not exist")
	}

	// Write the snapshot.
	writer, err := f.BeginSnapshot(meta)
	if nil != err {
		t.Fatalf("failed to create a snapshot: %s", err)
	}
	n, err := writer.Write(data)
	if nil != err {
		t.Errorf("failed to create a snapshot: %s", err)
	}
	if len(data) != n {
		t.Errorf("expected write length %d and got %d", len(data), n)
	}

	// Commit the write.
	if err = writer.Commit(); nil != err {
		t.Errorf("failed to commit the snapshot: %s", err)
	}

	return f
}

// testSnapshotMatch verifies whether the given snapshot metadata and payload
// match those returned by the given fsSnapshot.
func testSnapshotMatch(meta raft.SnapshotMetadata, data []byte, f raft.SnapshotManager, t *testing.T) {
	// Verify the metadata.
	newMeta, _ := f.GetSnapshotMetadata()
	if newMeta == raft.NilSnapshotMetadata {
		t.Error("snapshot metadata doesn't exist")
	}
	if meta != newMeta {
		t.Error("snapshot metadata doesn't match")
	}

	// Verify the payload.
	reader := f.GetSnapshot()
	if reader == nil {
		t.Error("snapshot doesn't exist")
	}
	if !reflect.DeepEqual(meta, reader.GetMetadata()) {
		t.Error("snapshot metadata doesn't match")
	}
	newData := make([]byte, len(data))
	n, err := reader.Read(newData)
	if nil != err {
		t.Errorf("failed to read snapshot file: %s", err)
	}
	if len(newData) != n {
		t.Errorf("expected read length %d and got %d", len(newData), n)
	}
	if !bytes.Equal(data, newData) {
		t.Errorf("expected %v and got %v", data, newData)
	}

	if err = reader.Close(); nil != err {
		t.Errorf("failed to close the reader: %s", reader)
	}
}

// TestFSSnapshotWriteAndRead creates a fsSnapshot, writes a snapshot, reads it
// back and verifies the metadata and payload.
func TestFSSnapshotWriteAndRead(t *testing.T) {
	meta := raft.SnapshotMetadata{LastIndex: 12, LastTerm: 8}
	data := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

	// Write the snapshot.
	f := testFSSnapshotEnv(meta, data, t)

	// Verify the write.
	testSnapshotMatch(meta, data, f, t)
}

// TestFSSnapshotMultiWrites writes muiltiple snapshots, commits half of the
// writes, aborts the other half, and verifies the results.
func TestFSSnapshotMultiWrites(t *testing.T) {
	tests := []struct {
		meta raft.SnapshotMetadata
		data []byte
	}{
		{
			meta: raft.SnapshotMetadata{
				LastIndex:  1,
				LastTerm:   0,
				Membership: &raft.Membership{Index: 1, Term: 0, Members: []string{"1"}},
			},
			data: []byte{0, 1},
		},
		{
			meta: raft.SnapshotMetadata{
				LastIndex:  1,
				LastTerm:   0,
				Membership: &raft.Membership{Index: 1, Term: 0, Members: []string{"1"}},
			},
			data: []byte{0, 1, 2},
		},
		{
			meta: raft.SnapshotMetadata{
				LastIndex:  2,
				LastTerm:   1,
				Membership: &raft.Membership{Index: 1, Term: 0, Members: []string{"1"}},
			},
			data: []byte{0, 1, 2, 3},
		},
		{
			meta: raft.SnapshotMetadata{
				LastIndex:  2,
				LastTerm:   2,
				Membership: &raft.Membership{Index: 1, Term: 0, Members: []string{"1"}},
			},
			data: []byte{0, 1, 2, 3, 4},
		},
		{
			meta: raft.SnapshotMetadata{
				LastIndex:  13,
				LastTerm:   23,
				Membership: &raft.Membership{Index: 1, Term: 0, Members: []string{"1"}},
			},
			data: []byte{0, 1, 2, 3, 4, 5, 6, 7, 8},
		},
		{
			meta: raft.SnapshotMetadata{
				LastIndex:  200,
				LastTerm:   400,
				Membership: &raft.Membership{Index: 1, Term: 0, Members: []string{"1"}},
			},
			data: []byte{'a', 'b', 'c'},
		},
	}

	// Create a fsSnapshot.
	f := testFSSnapshotEnv(raft.NilSnapshotMetadata, []byte{}, t)

	for i, test := range tests {
		writer, err := f.BeginSnapshot(test.meta)
		if nil != err {
			t.Fatalf("failed to create a snapshot: %s", err)
		}
		n, err := writer.Write(test.data)
		if nil != err {
			t.Errorf("failed to create a snapshot: %s", err)
		}
		if len(test.data) != n {
			t.Errorf("expected write length %d and got %d", len(test.data), n)
		}

		if i%2 == 0 {
			// Commit.
			if err = writer.Commit(); nil != err {
				t.Errorf("failed to commit the snapshot: %s", err)
			}

			// Verify the committed write made it to the storage.
			testSnapshotMatch(tests[i].meta, tests[i].data, f, t)
		} else {
			// Abort.
			if err = writer.Abort(); nil != err {
				t.Errorf("failed to abort the snapshot: %s", err)
			}

			// Verify the aborted write didn't make it to the
			// storage.
			testSnapshotMatch(tests[i-1].meta, tests[i-1].data, f, t)
		}
	}
}

// Tests that from a bunch of snapshot files(including temporary snapshot files)
// in home directory, we'll restore from the latest one.
func TestFSSnapshotRestoreFromDir(t *testing.T) {
	// Create temporary homeDir.
	homeDir, err := ioutil.TempDir(test.TempDir(), "snapshot")
	files := []string{
		metadataToSnapshotName(raft.SnapshotMetadata{LastTerm: 1, LastIndex: 1000}),
		metadataToSnapshotName(raft.SnapshotMetadata{LastTerm: 2, LastIndex: 1001}),
		metadataToSnapshotName(raft.SnapshotMetadata{LastTerm: 4, LastIndex: 2000}), // latest one snapshot file.
		metadataToSnapshotName(raft.SnapshotMetadata{LastTerm: 1, LastIndex: 10}),
		// the snapshot with the highest index, but it's a temporary file so it shouldn't be visible.
		metadataToSnapshotName(raft.SnapshotMetadata{LastTerm: 4, LastIndex: 3000}) + snapshotTempSuffix,
	}

	// Create all snapshot files.
	for _, fname := range files {
		fpath := filepath.Join(homeDir, fname)
		f, err := disk.NewChecksumFile(fpath, os.O_CREATE|os.O_TRUNC|os.O_RDWR)
		if nil != err {
			t.Fatalf("failed to create snapshot file: %v", err)
		}
		encodeSnapshotMetadata(f, snapshotNameToMetadata(fname))
		f.Close()
	}

	mgr, err := NewFSSnapshotMgr(homeDir)
	if err != nil {
		t.Fatalf("Failed to create snapshot manager: %v", err)
	}

	// Should restore from the lastest and effective one.
	meta, _ := mgr.GetSnapshotMetadata()
	expected := raft.SnapshotMetadata{LastTerm: 4, LastIndex: 2000}
	if meta != expected {
		t.Fatalf("expected get metadata: %+v, but got %+v", expected, meta)
	}
}

// Test that simulates:
//  (1) writing a snapshot file.
//  (2) getting the most recent snapshot file(the first one).
//  (3) writing a second snapshot file, thus the first one should be removed
//  (4) the file that was opened in step 2 should still be accessible even
//      a newer one gets committed.
func TestFSSnapshotConcurrency(t *testing.T) {
	// Create temporary homeDir.
	homeDir, err := ioutil.TempDir(test.TempDir(), "snapshot")
	mgr, err := NewFSSnapshotMgr(homeDir)
	if err != nil {
		t.Fatalf("Failed to create snapshot manager: %v", err)
	}

	// (1) Write the first snapshot
	w, err := mgr.BeginSnapshot(raft.SnapshotMetadata{LastTerm: 1,
		LastIndex: 100, Membership: nil})
	if err != nil {
		t.Fatalf("Failed to create snapshot file")
	}
	if _, err := w.Write([]byte("first hello world")); err != nil {
		t.Fatalf("Failed to write snapshot file: %v", err)
	}
	if err := w.Commit(); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// (2) get the most recent snapshot file(the first one).
	reader := mgr.GetSnapshot()
	if reader == nil {
		t.Fatalf("Expected to get the recent written snapshot")
	}
	defer reader.Close()

	// (3) Write the second snapshot(it will try to remove the first snapshot
	// file)
	w, err = mgr.BeginSnapshot(raft.SnapshotMetadata{LastTerm: 1,
		LastIndex: 200, Membership: nil})
	if err != nil {
		t.Fatalf("Failed to create snapshot file")
	}
	if _, err := w.Write([]byte("second hello world")); err != nil {
		t.Fatalf("Failed to write snapshot file: %v", err)
	}
	if err := w.Commit(); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// (4) the file that was opened in step 2 should still be accessible even
	// a newer one gets committed.
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read snapshot: %v", err)
	}
	if bytes.Compare(data, []byte("first hello world")) != 0 {
		t.Fatalf("Expected to read the first snapshot back.")
	}
}

// Tests that we'll keep last N snapshots undeleted and delete snapshots
// before that.
func TestFSSnapshotKeepLastNSnapshots(t *testing.T) {
	// Create temporary homeDir.
	homeDir, err := ioutil.TempDir(test.TempDir(), "snapshot")
	if err != nil {
		t.Fatalf("Failed to create the temp directory: %v", err)
	}

	// Start with 2 snapshots.
	files := []string{
		metadataToSnapshotName(raft.SnapshotMetadata{LastTerm: 1, LastIndex: 1000}),
		metadataToSnapshotName(raft.SnapshotMetadata{LastTerm: 1, LastIndex: 2000}),
	}
	for _, fname := range files {
		fpath := filepath.Join(homeDir, fname)
		f, err := disk.NewChecksumFile(fpath, os.O_CREATE|os.O_TRUNC|os.O_RDWR)
		if nil != err {
			t.Fatalf("failed to create snapshot file: %v", err)
		}
		encodeSnapshotMetadata(f, snapshotNameToMetadata(fname))
		f.Close()
	}

	mgr, err := NewFSSnapshotMgr(homeDir)
	if err != nil {
		t.Fatalf("Failed to create snapshot manager: %v", err)
	}

	// Check that last N snapshots on disks are kept.
	if !checkSnapshotsExists(files[len(files)-min(len(files), snapRetention):], homeDir) {
		t.Fatalf("Expected the last N snapshots exist")
	}

	// Create N new snapshots.
	for i := 0; i < snapRetention; i++ {
		meta := raft.SnapshotMetadata{LastTerm: 1, LastIndex: 2001 + uint64(i)}
		w, err := mgr.BeginSnapshot(meta)
		if err != nil {
			t.Fatalf("Failed to get snapshot writer: %v", err)
		}
		if err := w.Commit(); err != nil {
			t.Fatalf("Failed to commit snapshot: %v", err)
		}
		files = append(files, metadataToSnapshotName(meta))
	}

	// Check the last N files are kept.
	if !checkSnapshotsExists(files[len(files)-snapRetention:], homeDir) {
		t.Fatalf("Expected the last N snapshots exist")
	}

	// Check the snapshots before the last N are removed.
	for _, f := range files[:len(files)-snapRetention] {
		fpath := filepath.Join(homeDir, f)
		if _, err := os.Stat(fpath); !os.IsNotExist(err) {
			t.Fatalf("The old snapshot file is supposed to be removed")
		}
	}
}

// Return true if all snapshots eixst under 'dir'.
func checkSnapshotsExists(snapshots []string, dir string) bool {
	for _, f := range snapshots {
		fpath := filepath.Join(dir, f)
		if _, err := os.Stat(fpath); os.IsNotExist(err) {
			return false
		}
	}
	return true
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
