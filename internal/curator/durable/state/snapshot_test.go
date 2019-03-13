// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package state

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/westerndigitalcorporation/blb/internal/core"
	"github.com/westerndigitalcorporation/blb/internal/curator/durable/state/fb"
	test "github.com/westerndigitalcorporation/blb/pkg/testutil"
)

// Test snapshot.
func TestStateSnapshot(t *testing.T) {
	var buf bytes.Buffer

	s1 := getTestState(t)

	txn := s1.WriteTxn(1)
	txn.SetCuratorID(1)
	txn.PutPartition(1, &fb.Partition{})
	txn.PutPartition(3, &fb.Partition{})
	txn.PutBlob(core.BlobIDFromParts(1, 1), &fb.Blob{Repl: 3})
	txn.PutBlob(core.BlobIDFromParts(1, 2), &fb.Blob{Repl: 3})
	txn.PutBlob(core.BlobIDFromParts(3, 1), &fb.Blob{Repl: 3})
	txn.PutBlob(core.BlobIDFromParts(3, 2), &fb.Blob{Repl: 3})
	for i := 1; i < 1000; i++ {
		k := core.BlobKey(i)
		txn.PutBlob(core.BlobIDFromParts(3, k), &fb.Blob{Repl: 3})
	}
	txn.Commit()

	// Create a snapshot of s1.
	snapTxn := s1.ReadOnlyTxn()
	if _, err := snapTxn.Dump(&buf); err != nil {
		t.Fatalf("failed to dump state: %v", err)
	}
	snapTxn.Commit()

	file := filepath.Join(test.TempDir(), "new_state")
	err := ioutil.WriteFile(file, buf.Bytes(), os.FileMode(mode))
	if err != nil {
		t.Fatalf("Faled to dump state to file: %v", err)
	}

	// Open a new state database with the dumped state.
	s2 := Open(file)

	if !sameState(s1, s2) {
		t.Fatalf("after recovery s2 has different state with s1")
	}
}

// Returns true if two states are same, false otherwise.
func sameState(s1, s2 *State) bool {
	txn1 := s1.ReadOnlyTxn()
	txn2 := s2.ReadOnlyTxn()
	defer txn1.Commit()
	defer txn2.Commit()

	// Check curator IDs are same.
	if txn1.GetCuratorID() != txn2.GetCuratorID() {
		return false
	}

	// Check whether partition metadta are same.
	if !reflect.DeepEqual(txn1.GetPartitions(), txn2.GetPartitions()) {
		return false
	}

	iter1, ok1 := txn1.GetIterator(0)
	iter2, ok2 := txn2.GetIterator(0)

	// Checks whether blob metadata are same.
	for {
		if ok1 != ok2 {
			return false
		}
		if !ok1 {
			// Both have reached the end.
			break
		}

		id1, blob1 := iter1.Blob()
		id2, blob2 := iter2.Blob()
		if id1 != id2 {
			return false
		}
		if !reflect.DeepEqual(blob1, blob2) {
			return false
		}

		ok1 = iter1.Next()
		ok2 = iter2.Next()
	}

	return true
}
