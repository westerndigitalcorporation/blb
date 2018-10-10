// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package watchblb

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	client "github.com/westerndigitalcorporation/blb/client/blb"
	"github.com/westerndigitalcorporation/blb/internal/core"
	test "github.com/westerndigitalcorporation/blb/pkg/testutil"
)

// Create a SqliteDB backed by a file in a temporary directory for testing.
func getTestDB() *SqliteDB {
	path := filepath.Join(test.TempDir(), "test.db")
	os.RemoveAll(path)
	return NewSqliteDB(path)
}

// Test basic put & get operations.
func TestPutGet(t *testing.T) {
	db := getTestDB()
	defer db.Close()

	now := time.Now()
	blobCnt := 10

	// Put blobs in.
	for i := 0; i < blobCnt; i++ {
		id := client.BlobID(core.BlobIDFromParts(core.PartitionID(0), core.BlobKey(i)))
		if err := db.Put(id, now.Add(time.Duration(i))); err != nil {
			t.Fatal(err)
		}
	}

	// Get them back.
	for i := 0; i < blobCnt; i++ {
		id := client.BlobID(core.BlobIDFromParts(core.PartitionID(0), core.BlobKey(i)))
		then, err := db.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		if exp := now.Add(time.Duration(i)); !exp.Equal(then) {
			t.Fatalf("mismatched time: exp %s and got %s", exp, then)
		}
	}
}

// Test delete-if-expires operation.
func TestDeleteIfExpires(t *testing.T) {
	db := getTestDB()
	defer db.Close()

	now := time.Now()
	lifetime := time.Minute

	// Insert an "old" blob.
	oldOne := client.BlobID(core.BlobIDFromParts(core.PartitionID(0), core.BlobKey(0)))
	if err := db.Put(oldOne, now.Add(-2*lifetime)); err != nil {
		t.Fatal(err)
	}

	// Insert a "new" blob.
	newOne := client.BlobID(core.BlobIDFromParts(core.PartitionID(0), core.BlobKey(1)))
	if err := db.Put(newOne, now); err != nil {
		t.Fatal(err)
	}

	// DeleteIfExpires and verify the old one is gone and the new one stays.
	blobs, err := db.DeleteIfExpires(lifetime)
	if err != nil {
		t.Fatal(err)
	}
	if len(blobs) != 1 || blobs[0] != oldOne {
		t.Fatalf("the old blob should have been flagged for deletion")
	}
	if err := db.ConfirmDeletion(blobs); err != nil {
		t.Fatalf("failed to confirm the deletion: %s", err)
	}

	if _, err := db.Get(oldOne); err == nil {
		t.Fatal("the old blob should have been removed")
	}
	if _, err := db.Get(newOne); err != nil {
		t.Fatal(err)
	}
}

// Test confirm-deletion operation.
func TestConfirmDeletion(t *testing.T) {
	db := getTestDB()
	defer db.Close()

	// Insert a blob.
	blob := client.BlobID(core.BlobIDFromParts(core.PartitionID(0), core.BlobKey(0)))
	if err := db.Put(blob, time.Now()); err != nil {
		t.Fatal(err)
	}

	// Flag for deletion.
	if _, err := db.DeleteIfExpires(time.Duration(0)); err != nil {
		t.Fatalf("failed to flag blob for deletion: %s", err)
	}

	// We should still be able to get the blob.
	if _, err := db.Get(blob); err != nil {
		t.Fatalf("failed to get blob: %s", err)
	}

	// Confirm the deletion and the blob should be gone.
	if err := db.ConfirmDeletion([]client.BlobID{blob}); err != nil {
		t.Fatalf("failed to confirm the deletion: %s", err)
	}
	if _, err := db.Get(blob); err == nil {
		t.Fatal("the blob should have been deleted")
	}
}

// Test rand operation.
func TestRand(t *testing.T) {
	db := getTestDB()
	defer db.Close()

	now := time.Now()
	blobCnt := 10

	// Put blobs in.
	for i := 0; i < blobCnt; i++ {
		id := client.BlobID(core.BlobIDFromParts(core.PartitionID(0), core.BlobKey(i)))
		if err := db.Put(id, now); err != nil {
			t.Fatal(err)
		}
	}

	// Get a random blob from the db.
	id, err := db.Rand()
	if err != nil {
		t.Fatal(err)
	}

	// Verify the random blob exists.
	if _, err := db.Get(id); err != nil {
		t.Fatal(err)
	}

	// Remove all blobs and rand should return error.
	blobs, err := db.DeleteIfExpires(time.Duration(0))
	if err != nil {
		t.Fatalf("failed to flag for deletion: %s", err)
	}
	if len(blobs) != blobCnt {
		t.Fatal("all blobs should have expired")
	}
	if err := db.ConfirmDeletion(blobs); err != nil {
		t.Fatalf("failed to confirm the deletion: %s", err)
	}
	if _, err := db.Rand(); err == nil {
		t.Fatal("all blobs should have been removed")
	}
}
