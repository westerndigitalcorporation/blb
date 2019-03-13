// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package durable

import (
	"io/ioutil"
	"path/filepath"
	"testing"
	"time"

	"github.com/westerndigitalcorporation/blb/internal/core"
	"github.com/westerndigitalcorporation/blb/internal/curator/durable/state"
	test "github.com/westerndigitalcorporation/blb/pkg/testutil"
)

func getTestState(t *testing.T) *state.State {
	dir, err := ioutil.TempDir(test.TempDir(), "state_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	return state.Open(filepath.Join(dir, "state_db"))
}

// Test that registration is idempotent and reliable.
func TestRegistration(t *testing.T) {
	d := getTestState(t)

	txn := d.WriteTxn(1)
	defer txn.Commit()

	// One must register to be registered.
	if id := txn.GetCuratorID(); id.IsValid() {
		t.Errorf("shouldn't be registered")
	}

	// Register as an elite curator.
	setCmd := SetRegistrationCommand{ID: 31337}
	if setCmd.apply(txn).ID != 31337 {
		t.Errorf("registered for first time but didn't get registration back")
	}
	if id := txn.GetCuratorID(); !id.IsValid() {
		t.Errorf("should be registered")
	}

	// Ensure idempotency.
	setCmd.ID = 100
	if setCmd.apply(txn).ID != 31337 {
		t.Errorf("registered again but didn't get ID from first time")
	}
}

// Test adding partitions.
func TestAddPartition(t *testing.T) {
	d := getTestState(t)

	txn := d.WriteTxn(1)
	defer txn.Commit()

	SetRegistrationCommand{ID: 31337}.apply(txn)

	// Get our partitions, shouldn't have any.
	if len(txn.GetPartitions()) != 0 {
		t.Errorf("expected no partitions")
	}

	// Add a partition.
	addCmd := AddPartitionCommand{ID: 7}
	addRes := addCmd.apply(txn)
	if addRes.Err != core.NoError {
		t.Errorf("error adding a partition")
	}

	// Add it again, should error.
	addRes = addCmd.apply(txn)
	if addRes.Err == core.NoError {
		t.Errorf("expected an error adding a dup partition")
	}

	// Get partitions, should find it.
	partitions := txn.GetPartitions()
	if len(partitions) != 1 {
		t.Errorf("expected no partitions")
	}
	if partitions[0].ID != 7 {
		t.Fatalf("wrong id for our partition")
	}
}

// Test sync-ing partitions.
func TestSyncPartitions(t *testing.T) {
	d := getTestState(t)

	txn := d.WriteTxn(1)
	defer txn.Commit()

	SetRegistrationCommand{ID: 31337}.apply(txn)

	// Add a partition.
	addCmd := AddPartitionCommand{ID: 7}
	addRes := addCmd.apply(txn)
	if addRes.Err != core.NoError {
		t.Errorf("error adding a partition")
	}

	// Sync with the partition we added and another.
	SyncPartitionsCommand{Partitions: []core.PartitionID{7, 2001}}.apply(txn)

	// Should see both partitions now.
	partitions := txn.GetPartitions()
	if len(partitions) != 2 {
		t.Fatalf("didn't get our partition back")
	}

	// Verify the values.
	for _, part := range partitions {
		if part.ID != 7 && part.ID != 2001 {
			t.Fatalf("wrong id for our partition")
		}
	}
}

// Create a blob, modify the partition in the durable state to pretend we've exhausted it, check that
// blob creation errors, add another partition, blob creation should work again.
func TestCreateBlob(t *testing.T) {
	d := getTestState(t)

	txn := d.WriteTxn(1)
	defer txn.Commit()

	SetRegistrationCommand{ID: 31337}.apply(txn)

	// Add a partition.  We'll later exhaust this and then add another one.
	addCmd := AddPartitionCommand{ID: 7}
	addRes := addCmd.apply(txn)
	if addRes.Err != core.NoError {
		t.Errorf("error adding a partition")
	}

	// Create the blob.
	createCmd := CreateBlobCommand{Repl: 1}
	blob1 := createCmd.apply(txn)
	if blob1.Err != core.NoError {
		t.Errorf("expected blob creation to work")
	}

	// Create another, verify the ID differs.
	blob2 := createCmd.apply(txn)
	if blob2.Err != core.NoError {
		t.Errorf("expected blob creation to work")
	}
	if blob2.ID == blob1.ID {
		t.Errorf("blob2 and blob1 have the same ID")
	}

	// Reach in and change the partition's state
	p := txn.GetPartitions()[0]
	newPart := p.P.ToStruct()
	newPart.NextBlobKey = core.MaxBlobKey
	txn.PutPartition(p.ID, newPart)

	if createCmd.apply(txn).Err == core.NoError {
		t.Errorf("expected to fail in blob creation")
	}

	// Add a new partition.
	AddPartitionCommand{ID: 2001}.apply(txn)
	blob3 := createCmd.apply(txn)
	if blob3.Err != core.NoError {
		t.Errorf("expected blob creation to work")
	}
	if blob3.ID.Partition() != core.PartitionID(2001) {
		t.Errorf("succeeded but didn't get new partition?")
	}
}

// Add some full partitions, add a non-full partition, create a blob and it
// should work.
func TestPartitionAllocation(t *testing.T) {
	d := getTestState(t)

	txn := d.WriteTxn(1)
	defer txn.Commit()

	SetRegistrationCommand{ID: 31337}.apply(txn)

	// Add a bunch of full partitions.
	for i := 0; i < 23; i++ {
		addCmd := AddPartitionCommand{ID: core.PartitionID(i + 10)}
		if addCmd.apply(txn).Err != core.NoError {
			t.Fatalf("error adding a partition")
		}

		p := txn.GetPartitions()[i]
		newPart := p.P.ToStruct()
		newPart.NextBlobKey = core.MaxBlobKey
		txn.PutPartition(p.ID, newPart)
	}

	// Add a non-full partition.
	addCmd := AddPartitionCommand{ID: core.PartitionID(88)}
	if addCmd.apply(txn).Err != core.NoError {
		t.Fatalf("error adding a partition")
	}

	p := txn.GetPartitions()[23]
	newPart := p.P.ToStruct()
	newPart.NextBlobKey = 100
	txn.PutPartition(p.ID, newPart)

	// Create should work in the non-full partition.
	createCmd := CreateBlobCommand{Repl: 1}
	blob := createCmd.apply(txn)
	if core.NoError != blob.Err {
		t.Fatalf("failed to create a blob")
	}
	if partition := blob.ID.Partition(); 88 != partition {
		t.Fatalf("expected %d and got %d", 88, partition)
	}
}

// Test that extending a blob doesn't crash when the partition and/or blob don't exist.
func TestExtendNoSuchBlob(t *testing.T) {
	d := getTestState(t)

	txn := d.WriteTxn(1)
	defer txn.Commit()

	SetRegistrationCommand{ID: 31337}.apply(txn)

	// Add a bunch of full partitions.
	for i := 0; i < 23; i++ {
		addCmd := AddPartitionCommand{ID: core.PartitionID(i + 10)}
		if addCmd.apply(txn).Err != core.NoError {
			t.Fatalf("error adding a partition")
		}

		p := txn.GetPartitions()[i]
		newPart := p.P.ToStruct()
		newPart.NextBlobKey = core.MaxBlobKey
		txn.PutPartition(p.ID, newPart)
	}

	// Add a non-full partition.
	addCmd := AddPartitionCommand{ID: core.PartitionID(88)}
	if addCmd.apply(txn).Err != core.NoError {
		t.Fatalf("error adding a partition")
	}
	SetRegistrationCommand{ID: 31337}.apply(txn)

	// Extend a blob that doesn't exist as there is no partition.  Should fail.
	id := core.BlobIDFromParts(7, 300)
	extendCmd := ExtendBlobCommand{
		ID:            id,
		FirstTractKey: 0,
		Hosts:         [][]core.TractserverID{{1}},
	}
	if extendCmd.apply(txn).Err == core.NoError {
		t.Errorf("extended a blob that doesn't exist, should'a failed")
	}

	// Add a partition that could own the blob, but should still fail, as no blobs.
	AddPartitionCommand{ID: 7}.apply(txn)
	if extendCmd.apply(txn).Err == core.NoError {
		t.Errorf("extended a blob that still doesn't exist, should'a failed")
	}

	// Actually create a blob and use that ID to extend, should work.
	blob1 := CreateBlobCommand{Repl: 1}.apply(txn)
	extendCmd.ID = blob1.ID
	if extendCmd.apply(txn).Err != core.NoError {
		t.Errorf("expected extend to work")
	}
}

// Test that extending without specifying enough hosts for the repl factor is rejected.
func TestExtendBlobReplBad(t *testing.T) {
	d := getTestState(t)

	txn := d.WriteTxn(1)
	defer txn.Commit()

	SetRegistrationCommand{ID: 31337}.apply(txn)
	AddPartitionCommand{ID: 7}.apply(txn)

	blob := CreateBlobCommand{Repl: 2}.apply(txn)
	extendCmd := ExtendBlobCommand{
		ID:            blob.ID,
		FirstTractKey: 0,
		Hosts:         [][]core.TractserverID{{1}},
	}

	// Repl=2 required but repl=1 supplied in extendCmd.Hosts[0].
	if extendCmd.apply(txn).Err == core.NoError {
		t.Errorf("extend should have been rejected as too few hosts")
	}

	// Repl=2 is now satisfied.
	extendCmd.Hosts[0] = []core.TractserverID{1, 2}
	if extendCmd.apply(txn).Err != core.NoError {
		t.Errorf("extend should have been rejected as too few hosts")
	}
}

// Test that we can't delete a blob that doesn't exist.
func TestDeleteMissingBlob(t *testing.T) {
	d := getTestState(t)

	txn := d.WriteTxn(1)
	defer txn.Commit()

	SetRegistrationCommand{ID: 31337}.apply(txn)

	// Extend a blob that doesn't exist as there is no partition.  Should fail.
	id := core.BlobIDFromParts(7, 300)
	deleteCmd := DeleteBlobCommand{ID: id}
	if deleteCmd.apply(txn).Err == core.NoError {
		t.Errorf("delete should have failed")
	}

	// Add a partition that could own the blob, but should still fail, as no blobs.
	AddPartitionCommand{ID: 7}.apply(txn)
	if deleteCmd.apply(txn).Err == core.NoError {
		t.Errorf("extended a blob that still doesn't exist, should'a failed")
	}

	// Actually create a blob and use that ID in deletion, should work.
	blob1 := CreateBlobCommand{Repl: 1}.apply(txn)
	deleteCmd.ID = blob1.ID
	if deleteCmd.apply(txn).Err != core.NoError {
		t.Errorf("expected extend to work")
	}
}

// Test basic deleting and undeleting of a blob.  We shouldn't be able to do anything with a blob
// when it's deleted, and we should be able to undelete it, and then delete it again.
func TestUndeleteBlob(t *testing.T) {
	d := getTestState(t)

	txn := d.WriteTxn(1)
	defer txn.Commit()

	SetRegistrationCommand{ID: 31337}.apply(txn)
	AddPartitionCommand{ID: 7}.apply(txn)

	// Create a blob.
	createCmd := CreateBlobCommand{Repl: 1}
	res := createCmd.apply(txn)
	if res.Err != core.NoError {
		t.Fatalf("couldn't create a blob")
	}
	id := res.ID

	// Extend it.
	extendCmd := ExtendBlobCommand{
		ID:            id,
		FirstTractKey: 0,
		Hosts:         [][]core.TractserverID{{1}, {2}, {3}, {4}},
	}
	extendRes := extendCmd.apply(txn)
	if extendRes.Err != core.NoError {
		t.Errorf("blob extension didn't work: %s", extendRes.Err)
	}

	// Delete it.
	now := time.Now()
	DeleteBlobCommand{ID: id, When: now}.apply(txn)

	// Try to get info, should fail.
	if _, _, err := txn.GetTracts(id, 0, 1000); err == core.NoError {
		t.Errorf("gettracts shouldn't work on a deleted blob")
	}

	// Try to extend, should fail.
	if extendRes = extendCmd.apply(txn); extendRes.Err == core.NoError {
		t.Errorf("blob extension shouldn't work")
	}

	// Try to stat, should fail.
	if _, err := txn.Stat(id); err == core.NoError {
		t.Errorf("stat should have failed")
	}

	// Undelete.
	undel := UndeleteBlobCommand{ID: id}
	if res := undel.apply(txn); res.Err != core.NoError {
		t.Errorf("undelete should have worked")
	}

	// Everything should work.
	if _, _, err := txn.GetTracts(id, 0, 1000); err != core.NoError {
		t.Errorf("gettracts should work")
	}
	extendCmd.FirstTractKey = 4
	if extendRes = extendCmd.apply(txn); extendRes.Err != core.NoError {
		t.Errorf("blob extension should work")
	}
	if _, err := txn.Stat(id); err != core.NoError {
		t.Errorf("stat should work")
	}

	// Delete and GC.  Undeleting shouldn't work.
	DeleteBlobCommand{ID: id, When: now}.apply(txn)
	FinishDeleteCommand{Blobs: []core.BlobID{id}}.apply(txn)

	if res := undel.apply(txn); res.Err == core.NoError {
		t.Errorf("undelete should not have worked")
	}
}

// Test stat-ing of blobs that don't exist and one that does.
func TestStatBlobCommand(t *testing.T) {
	d := getTestState(t)

	txn := d.WriteTxn(1)
	defer txn.Commit()

	SetRegistrationCommand{ID: 31337}.apply(txn)

	// Stat a blob that doesn't exist as there is no partition.  Should fail.
	id := core.BlobIDFromParts(7, 300)
	if _, err := txn.Stat(id); err == core.NoError {
		t.Errorf("stat should have failed")
	}

	// Add a partition that could own the blob, but should still fail, as no blobs.
	AddPartitionCommand{ID: 7}.apply(txn)
	if _, err := txn.Stat(id); err == core.NoError {
		t.Errorf("stat should have failed")
	}

	// Actually create a blob and use that ID to stat, should work.
	blob := CreateBlobCommand{Repl: 1}.apply(txn)
	info, err := txn.Stat(blob.ID)
	if err != core.NoError {
		t.Errorf("expected stat to work")
	}

	// Verify state.
	if info.NumTracts != 0 {
		t.Errorf("wrong numtracts")
	}
	if info.Repl != 1 {
		t.Errorf("wrong repl")
	}

	// Mutate state.
	extendCmd := ExtendBlobCommand{
		ID:            blob.ID,
		FirstTractKey: 0,
		Hosts:         [][]core.TractserverID{{1}, {2}, {3}, {4}},
	}

	extendRes := extendCmd.apply(txn)
	if extendRes.Err != core.NoError {
		t.Errorf("blob extension didn't work")
	}

	// Verify mutation.
	info, err = txn.Stat(blob.ID)
	if err != core.NoError {
		t.Errorf("expected stat to work")
	}
}

// Test error cases for getting tracts.
func TestGetTractsErrors(t *testing.T) {
	d := getTestState(t)

	txn := d.WriteTxn(1)
	defer txn.Commit()

	SetRegistrationCommand{ID: 31337}.apply(txn)

	id := core.BlobIDFromParts(7, 300)

	// No partition, should fail.
	if _, _, err := txn.GetTracts(id, 0, 0); err == core.NoError {
		t.Errorf("no partition for blob expected failure")
	}

	// Create the partition, now there is no such blob, should fail.
	AddPartitionCommand{ID: 7}.apply(txn)
	if _, _, err := txn.GetTracts(id, 0, 0); err == core.NoError {
		t.Errorf("no partition for blob expected failure")
	}

	// Create a blob we'll use to gettracts.
	blob := CreateBlobCommand{Repl: 1}.apply(txn)
	tracts, _, err := txn.GetTracts(blob.ID, 0, 0)

	if err != core.NoError {
		t.Errorf("expected to be able to getTracts an empty interval")
	}
	if len(tracts) != 0 {
		t.Errorf("no tracts expected, but tracts?")
	}

	// Now pass in some invalid arguments to the gettracts.
	if _, _, err := txn.GetTracts(blob.ID, -1, 0); err == core.NoError {
		t.Errorf("expected gettracts to be rejected")
	}

	if _, _, err := txn.GetTracts(blob.ID, 0, -1); err == core.NoError {
		t.Errorf("expected gettracts to be rejected")
	}

	// Extend so we can pass in new invalid arguments.
	extendCmd := ExtendBlobCommand{
		ID:            blob.ID,
		FirstTractKey: 0,
		Hosts:         [][]core.TractserverID{{1}, {2}, {3}, {4}},
	}

	extendRes := extendCmd.apply(txn)
	if extendRes.Err != core.NoError {
		t.Errorf("blob extension didn't work")
	}

	// Start must be before end.
	if _, _, err := txn.GetTracts(blob.ID, 2, 1); err == core.NoError {
		t.Errorf("expected gettracts to be rejected")
	}

	// Start must be a valid tract.
	if _, _, err := txn.GetTracts(blob.ID, 80, 100); err == core.NoError {
		t.Errorf("expected gettracts to be rejected")
	}
}

// Test non-error GetTracts behavior.
func TestGetTractsNormal(t *testing.T) {
	d := getTestState(t)

	txn := d.WriteTxn(1)
	defer txn.Commit()

	SetRegistrationCommand{ID: 31337}.apply(txn)
	AddPartitionCommand{ID: 7}.apply(txn)

	// Make a blob ID.
	blob := CreateBlobCommand{Repl: 1}.apply(txn)

	// Add some tracts for us to get.
	extendCmd := ExtendBlobCommand{
		ID:            blob.ID,
		FirstTractKey: 0,
		Hosts:         [][]core.TractserverID{{1}, {2}, {3}, {4}},
	}

	extendRes := extendCmd.apply(txn)
	if extendRes.Err != core.NoError {
		t.Errorf("blob extension didn't work")
	}

	// Getting tracts [0, 0) should work fine.
	tracts, _, err := txn.GetTracts(blob.ID, 0, 0)
	if err != core.NoError || len(tracts) != 0 {
		t.Errorf("unexpected return from gettracts")
	}

	// Get tracts [0, some big number) to check capping the end.
	tracts, _, err = txn.GetTracts(blob.ID, 0, 1000)
	if err != core.NoError {
		t.Errorf("expected gettracts to work")
	}
	if len(tracts) != extendRes.NewSize {
		t.Errorf("gettracts didn't return all tracts")
	}
	for i, tract := range tracts {
		tid := core.TractID{Blob: blob.ID, Index: core.TractKey(i)}
		if tract.Version != 1 {
			t.Errorf("wrong version for tract")
		}
		if tract.Tract != tid {
			t.Errorf("wrong tract ID")
		}
		if len(tract.TSIDs) != 1 {
			t.Fatalf("wrong number of hosts")
		}
		if tract.TSIDs[0] != extendCmd.Hosts[i][0] {
			t.Fatalf("wrong host in tract %s %d", tract.Hosts[0], extendCmd.Hosts[i][0])
		}
	}

	// Get tracts [1, 3) to check non-capping.
	tracts, _, err = txn.GetTracts(blob.ID, 1, 3)
	if err != core.NoError {
		t.Errorf("expected gettracts to work")
	}
	if len(tracts) != 2 {
		t.Errorf("gettracts didn't return all tracts")
	}
	for i := 1; i < 3; i++ {
		tract := tracts[i-1]
		tid := core.TractID{Blob: blob.ID, Index: core.TractKey(i)}
		if tract.Version != 1 {
			t.Errorf("wrong version for tract")
		}
		if tract.Tract != tid {
			t.Errorf("wrong tract ID")
		}
		if len(tract.TSIDs) != 1 {
			t.Fatalf("wrong number of hosts")
		}
		if tract.TSIDs[0] != extendCmd.Hosts[i][0] {
			t.Fatalf("wrong host in tract")
		}
	}
}

// Test tract changing error cases and the non-error case.
func TestChangeTract(t *testing.T) {
	d := getTestState(t)

	txn := d.WriteTxn(1)
	defer txn.Commit()

	SetRegistrationCommand{ID: 31337}.apply(txn)
	AddPartitionCommand{ID: 7}.apply(txn)

	// This blob doesn't exist.
	cmd := ChangeTractCommand{ID: core.TractID{Blob: 1, Index: core.TractKey(1)}, NewVersion: 2}
	if err := cmd.apply(txn); err == core.NoError {
		t.Errorf("change should have failed, didn't")
	}

	// This blob exists, but this tract doesn't exist.
	blob := CreateBlobCommand{Repl: 2}.apply(txn)
	cmd.ID.Blob = blob.ID
	if err := cmd.apply(txn); err == core.NoError {
		t.Errorf("change didn't fail but tract doesn't exist")
	}

	// This tract exists but the version isn't one more than the current version.
	extendCmd := ExtendBlobCommand{
		ID:            blob.ID,
		FirstTractKey: 0,
		Hosts:         [][]core.TractserverID{{1, 2}, {3, 4}},
	}
	if res := extendCmd.apply(txn); res.Err != core.NoError {
		t.Errorf("blob extension didn't work")
	}

	// Right number of hosts
	cmd.NewHosts = []core.TractserverID{4, 5}
	// Right version.
	cmd.NewVersion = 2
	if err := cmd.apply(txn); err != core.NoError {
		t.Errorf("change should have not failed")
	}
}

/*
// Test that scanning tracts probably works.  Construct the state directly and iterate over it.
func TestTractScanner(t *testing.T) {
	s := State{
		ID: 1,
		Partitions: []*Partition{
			// Partition that is empty.
			{ID: core.PartitionID(1), NextBlobKey: core.BlobKey(1)},
			// Blob that is empty, with a full blob after it.
			{ID: core.PartitionID(2), NextBlobKey: core.BlobKey(2),
				Blobs: []Blob{
					{Key: core.BlobKey(1), Repl: 3, Tracts: []Tract{}},
					{Key: core.BlobKey(100), Repl: 3, Tracts: []Tract{
						{Version: 1, Hosts: []core.TractserverID{1, 2, 3}},
						{Version: 3, Hosts: []core.TractserverID{4, 2, 5}},
					}},
					{Key: core.BlobKey(2000), Repl: 3, Tracts: []Tract{}},
				}},
			// Another empty partition.
			{ID: core.PartitionID(3), NextBlobKey: core.BlobKey(1)},
			{ID: core.PartitionID(4), NextBlobKey: core.BlobKey(3),
				Blobs: []Blob{
					{Key: core.BlobKey(1), Repl: 3, Tracts: []Tract{
						{Version: 1, Hosts: []core.TractserverID{1, 2, 3}},
						{Version: 2, Hosts: []core.TractserverID{4, 2, 5}},
					}},
					{Key: core.BlobKey(2), Repl: 3, Tracts: []Tract{}},
					{Key: core.BlobKey(3), Repl: 3, Tracts: []Tract{
						{Version: 1, Hosts: []core.TractserverID{1, 2, 3}},
						{Version: 3, Hosts: []core.TractserverID{4, 2, 5}},
					}},
				}},
			{ID: core.PartitionID(5), NextBlobKey: core.BlobKey(4),
				Blobs: []Blob{
					{Key: core.BlobKey(1), Repl: 3, Tracts: []Tract{
						{Version: 1, Hosts: []core.TractserverID{1, 2, 3}},
						{Version: 2, Hosts: []core.TractserverID{4, 2, 5}},
					}},
					{Key: core.BlobKey(2), Repl: 3, Tracts: []Tract{
						{Version: 1, Hosts: []core.TractserverID{1, 2, 3}},
						{Version: 3, Hosts: []core.TractserverID{4, 2, 5}},
					}},
					{Key: core.BlobKey(3), Repl: 3, Tracts: []Tract{
						{Version: 1, Hosts: []core.TractserverID{6, 9, 8}},
					}},
				}},
		},
	}
	ts := startTractScanner(&s)
	seen := make(map[core.TractID]bool)
	for {
		id, _, _, ok := ts.next()
		if !ok {
			break
		}
		seen[id] = true
	}
	if len(seen) != 11 {
		t.Errorf("expected to see 11 tracts, saw %d", len(seen))
	}
}
*/
