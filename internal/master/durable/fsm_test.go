// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package durable

import (
	"testing"

	"github.com/westerndigitalcorporation/blb/internal/core"
)

// Test that registration doesn't return the same curator id.
func TestRegisterCurator(t *testing.T) {
	s := newState()
	req := RegisterCuratorCmd{}
	curators := make(map[core.CuratorID]bool)

	const numCurators = 1000000
	for i := 0; i < numCurators; i++ {
		id := req.apply(s)
		if curators[id] {
			t.Fatal("duplicated curator id")
		}
		curators[id] = true
	}
}

// Test that registration doesn't return the same tractserver id.
func TestRegisterTractserver(t *testing.T) {
	s := newState()
	req := RegisterTractserverCmd{}
	ts := make(map[core.TractserverID]bool)

	const numTractservers = 1000000
	for i := 0; i < numTractservers; i++ {
		id := req.apply(s)
		if ts[id] {
			t.Fatal("duplicated tractserver id")
		}
		ts[id] = true
	}
}

// Test if we can recognize invalid curator id.
func TestVerifyCuratorID(t *testing.T) {
	s := newState()

	// Guard value cannot pass.
	if core.NoError == s.verifyCuratorID(0) {
		t.Fatal("invalid curator id should fail")
	}

	// Register some curators and try different values.
	cnt := 10
	var id core.CuratorID
	for i := 0; i < cnt; i++ {
		id = RegisterCuratorCmd{}.apply(s)
	}
	if core.NoError != s.verifyCuratorID(id) {
		t.Fatal("valid curator id shouldn't fail")
	}
	if core.NoError == s.verifyCuratorID(id+1) {
		t.Fatal("invalid curator id should fail")
	}
}

// Test assigning a partition to a curator.
func TestAssignPartition(t *testing.T) {
	s := newState()

	// Try to request a partition for a non-existing curator.
	res := NewPartitionCmd{CuratorID: 1}.apply(s)
	if core.NoError == res.Err {
		t.Fatal("cannot assign partition to a non-existing curator")
	}

	// Register a curator and try again.
	id := RegisterCuratorCmd{}.apply(s)
	res = NewPartitionCmd{CuratorID: id}.apply(s)
	if core.NoError != res.Err {
		t.Fatal(res.Err.String())
	}
}

// Test partitions assigned to multiple curators are all unique.
func TestUniquePartitions(t *testing.T) {
	s := newState()

	// Assign partitions to several curators for multiple times, and the
	// partitions should all be unique.
	const numCurators = 3
	curators := make([]core.CuratorID, numCurators)
	partitions := make(map[core.PartitionID]bool)
	for i := range curators {
		curators[i] = RegisterCuratorCmd{}.apply(s)
	}

	const numPartitionsPerCurator = 1000000
	for i := 0; i < numPartitionsPerCurator; i++ {
		for _, c := range curators {
			res := NewPartitionCmd{CuratorID: c}.apply(s)
			if core.NoError != res.Err {
				t.Fatal(res.Err.String())
			}
			if partitions[res.PartitionID] {
				t.Fatal("duplicated partition id")
			}
			partitions[res.PartitionID] = true
		}
	}
}

// Test getting partition assignment for a given curator.
func TestBasicGetPartitions(t *testing.T) {
	s := newState()

	// Try to get the partitions for a non-existing curator.
	if 0 != len(s.getPartitions(1)) {
		t.Fatal("should have no partitions assigned")
	}

	// Register a curator but don't assign any partition.
	c0 := RegisterCuratorCmd{}.apply(s)
	if 0 != len(s.getPartitions(c0)) {
		t.Fatal("should have no partitions assigned")
	}
}

// Test we can get correct partitions from multiple curators.
func TestMultiGetPartitions(t *testing.T) {
	s := newState()

	// Register a set of curators and assign some number of partitions to
	// each of them.
	const numCurators = 5
	curators := make([]core.CuratorID, numCurators)
	partitions := make([][]core.PartitionID, numCurators)
	for i := range curators {
		curators[i] = RegisterCuratorCmd{}.apply(s)
	}

	const numPartitionsPerCurator = 1000000
	for i := 0; i < numPartitionsPerCurator; i++ {
		for i, c := range curators {
			res := NewPartitionCmd{CuratorID: c}.apply(s)
			if core.NoError != res.Err {
				t.Fatal(res.Err.String())
			}
			partitions[i] = append(partitions[i], res.PartitionID)
		}
	}

	// See if we can get the assigned partitions back.
	for i, c := range curators {
		res := s.getPartitions(c)
		if len(res) != len(partitions[i]) {
			t.Fatal("failed to get back the right partitions")
		}
		for j := range res {
			if res[j] != partitions[i][j] {
				t.Fatal("failed to get back the right partitions")
			}
		}
	}
}

// Test looking up a curator that is responsible for a given blob.
func TestLookup(t *testing.T) {
	s := newState()

	// Register a curator and assign several partitions to it.
	curator := RegisterCuratorCmd{}.apply(s)
	const numPartitions = 10
	partitions := make([]core.PartitionID, numPartitions)

	for i := range partitions {
		res := NewPartitionCmd{CuratorID: curator}.apply(s)
		if core.NoError != res.Err {
			t.Fatal(res.Err.String())
		}
		partitions[i] = res.PartitionID
	}

	// Look up a blob in any of the partitions should succeed.
	for _, p := range partitions {
		if c, err := s.lookup(p); core.NoError != err || curator != c {
			t.Fatal("lookup should have succeeded")
		}
	}

	// Look up a blob in none of the partitions should fail.
	nonExistentPartition := partitions[numPartitions-1] + 1
	if _, err := s.lookup(nonExistentPartition); core.NoError == err {
		t.Fatal("lookup should have failed")
	}
}
