// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package master

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/westerndigitalcorporation/blb/internal/core"
	"github.com/westerndigitalcorporation/blb/internal/master/durable"
	"github.com/westerndigitalcorporation/blb/pkg/raft/raft"
)

// same returns true if 'a' and 'b' are slices that contain the same
// strings, false otherwise.  As a side-effect may sort both 'a' and 'b'.
// Used to verify that the list of hosts we expect are the list of hosts we
// receive.
func same(a []string, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	sort.Sort(sort.StringSlice(a))
	sort.Sort(sort.StringSlice(b))
	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// newTestMaster creates a master instance with single node raft.
func newTestMaster() *Master {
	master := NewMaster(
		DefaultConfig,
		durable.DefaultStateConfig,
		// Generate a unique cluster name by reading current timestamp.
		raft.NewTestRaftNode("master_test", fmt.Sprintf("%d", time.Now().UnixNano())),
	)
	master.stateHandler.ProposeInitialMembership([]string{"master_test"})
	// Wait until it becomes Raft leader.
	<-master.leaderCh
	return master
}

const (
	// This isn't validated so the contents don't really matter.
	testAddr = "somehost:someport"
)

// Test basic curator registration, heartbeat, partition-getting.
func TestCuratorRegistration(t *testing.T) {
	m := newTestMaster()

	// Register a curator.
	id, err := m.registerCurator(testAddr)
	if core.NoError != err {
		t.Fatal(err.Error())
	}

	// Send a heartbeat.
	partitions, err := m.curatorHeartbeat(id, testAddr)
	if core.NoError != err {
		t.Fatal("got an error from curatorHeartbeat: " + err.String())
	}
	if nil != partitions {
		t.Fatal("should have no partition assigned yet")
	}
}

// Test invalid heartbeating.
func TestInvalidHeartbeat(t *testing.T) {
	m := newTestMaster()

	// This ID is bogus.
	if _, err := m.curatorHeartbeat(0, testAddr); core.NoError == err {
		t.Fatal("no error for invalid ID with heartbeat")
	}

	// This ID doesn't exist yet but could.
	if _, err := m.curatorHeartbeat(0, testAddr); core.NoError == err {
		t.Fatal("no error for invalid ID with heartbeat")
	}

	// Get a valid ID.
	id, err := m.registerCurator(testAddr)
	if core.NoError != err {
		t.Fatal(err.Error())
	}

	// Heartbeat with the valid ID.
	if _, err := m.curatorHeartbeat(id, testAddr); core.NoError != err {
		t.Fatal("error for valid heartbeat")
	}

	// This ID doesn't exist, but there is still an ID.
	if _, err := m.curatorHeartbeat(id+1, testAddr); core.NoError == err {
		t.Fatal("no error for invalid ID with heartbeat")
	}
}

// Test error cases for partition allocation.
func TestNewPartitionError(t *testing.T) {
	m := newTestMaster()

	// This ID doesn't exist yet.
	if _, err := m.newPartition(0); core.NoError == err {
		t.Fatal("no error for trying to allocate a partition to a fake curator")
	}

	// Get a valid ID.
	id, err := m.registerCurator(testAddr)
	if core.NoError != err {
		t.Fatal(err.Error())
	}

	// Heartbeat with the valid ID.
	partitions, err := m.curatorHeartbeat(id, testAddr)
	if core.NoError != err {
		t.Fatal("error for valid heartbeat")
	}
	if nil != partitions {
		t.Fatal("should have no partition assigned yet")
	}

	// Get some partitions.
	for i := 0; i < newPartitionUpperLimit; i++ {
		partitionID, err := m.newPartition(id)
		if core.NoError != err {
			t.Fatal("error getting new partition: " + err.String())
		}
		partitions = append(partitions, partitionID)
	}

	// Send another heartbeat and verify the partition assignment.
	got, err := m.curatorHeartbeat(id, testAddr)
	if core.NoError != err {
		t.Fatal("error for valid heartbeat")
	}
	if len(partitions) != len(got) {
		t.Fatalf("partitions don't match, expected %v and got %v", partitions, got)
	}

	for i := range partitions {
		if partitions[i] != got[i] {
			t.Fatalf("partitions don't match, expected %v and got %v", partitions, got)
		}
	}

	// Try to get another partition. This should fail as we have already
	// reached the quota.
	if _, err := m.newPartition(id); core.NoError == err {
		t.Fatalf("should have failed to get a new partition")
	}
}

// Test blobid -> curator mapping.
func TestBlobLookup(t *testing.T) {
	m := newTestMaster()

	// Create a curator, get a partition.
	curator, err := m.registerCurator(testAddr)
	if core.NoError != err {
		t.Fatal(err.Error())
	}

	// Get a partition.
	partitionID, err := m.newPartition(curator)
	if core.NoError != err {
		t.Fatal("error getting new partition: " + err.String())
	}

	// Ask where a blob belonging to this partition is. We should
	// get back the curator's information.
	res, err := m.lookup(partitionID)
	if core.NoError != err {
		t.Fatal(err.Error())
	}
	if !same(res, []string{testAddr}) {
		t.Fatal("didn't get the hostname for the curator that owns the partition")
	}

	// There is no partition assigned here so there will be no curators for it.
	if _, err = m.lookup(partitionID + 1); core.ErrNoSuchBlob != err {
		t.Fatal("got hosts for partition, shouldn't have")
	}
}

// Test blob placement requests.
func TestBlobPlacement(t *testing.T) {
	m := newTestMaster()

	// There are no curators so we can't put a blob anywhere.
	if _, err := m.newBlob(); core.NoError == err {
		t.Fatal("no curators but somewhere to place a blob?")
	}

	// Create a curator.
	curator, err := m.registerCurator(testAddr)
	if core.NoError != err {
		t.Fatal(err.Error())
	}

	// Get a partition.
	if _, err := m.newPartition(curator); core.NoError != err {
		t.Fatal("error getting new partition: " + err.String())
	}

	// Blobs should go to the only curator.
	curators, err := m.newBlob()
	if core.NoError != err {
		t.Fatal(err.Error())
	}
	if !same(curators, []string{testAddr}) {
		t.Fatal("didn't get the hostname for the only curator")
	}
}

// Test basic tractserver registration, heartbeat.
func TestTractserverRegistration(t *testing.T) {
	m := newTestMaster()

	// Register a tractserver.
	id, err := m.registerTractserver(testAddr)
	if core.NoError != err {
		t.Fatal(err.Error())
	}

	// Send a heartbeat.
	curators, err := m.tractserverHeartbeat(id, testAddr, nil)
	if core.NoError != err {
		t.Fatal("got an error from tractserverHeartbeat: " + err.String())
	}
	if nil != curators {
		t.Fatal("should have no curator registered yet")
	}
}

// Test getHeartbeats returns correct heartbeat info.
func TestGetHeartbeats(t *testing.T) {
	m := newTestMaster()

	// Register some curators and send heartbeats.
	curatorCount := 2
	for i := 0; i < curatorCount; i++ {
		curatorAddr := fmt.Sprint("a-curator-$d", i)
		curatorID, err := m.registerCurator(curatorAddr)
		if err != core.NoError {
			t.Fatal(err.Error())
		}
		if _, err := m.curatorHeartbeat(curatorID, curatorAddr); err != core.NoError {
			t.Fatal("got an error from curatorHeartbeat: " + err.String())
		}
	}

	// Register some tractservers and send heartbeats.
	tsCount := 10
	for i := 0; i < tsCount; i++ {
		tsAddr := fmt.Sprintf("a-ts-%d", i)
		tsID, err := m.registerTractserver(tsAddr)
		if err != core.NoError {
			t.Fatal(err.Error())
		}
		curators, err := m.tractserverHeartbeat(tsID, tsAddr, []core.FsStatus{
			{Status: core.DiskStatus{Root: "/disk/one"}},
		})
		if err != core.NoError {
			t.Fatal(err.Error())
		}
		if len(curators) != curatorCount {
			t.Fatalf("expecting %d curators but got %d", curatorCount, len(curators))
		}
	}

	c, ts := m.getHeartbeats()
	if len(c) != curatorCount {
		t.Fatalf("expecting %d curators but got %d", curatorCount, len(c))
	}
	if len(ts) != tsCount {
		t.Fatalf("expecting %d tractservers but got %d", tsCount, len(ts))
	}
	if ts[0].Disks[0].Status.Root != "/disk/one" {
		t.Fatalf("disk info didn't get passed through")
	}
}

// Tests read-only mode.
func TestReadOnly(t *testing.T) {
	m := newTestMaster()

	// Can register.
	id, err := m.registerCurator(testAddr)
	if err != core.NoError {
		t.Fatal(err.Error())
	}

	// Set read-only.
	err = m.stateHandler.SetReadOnlyMode(true)
	if err != core.NoError {
		t.Fatal(err.Error())
	}

	// Can read.
	if m.stateHandler.ValidateCuratorID(id) != core.NoError ||
		m.stateHandler.ValidateCuratorID(5678) == core.NoError {
		t.Fatal("validation")
	}

	// Can't write
	_, err = m.registerCurator("anotheraddr:port")
	if err != core.ErrReadOnlyMode {
		t.Fatal("should have failed")
	}

	err = m.stateHandler.SetReadOnlyMode(false)
	if err != core.NoError {
		t.Fatal(err.Error())
	}

	// Can write again now.
	_, err = m.registerCurator("anotheraddr:port")
	if err != core.NoError {
		t.Fatal(err.Error())
	}
}
