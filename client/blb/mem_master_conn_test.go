// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package blb

import (
	"context"
	"strconv"
	"testing"

	"github.com/westerndigitalcorporation/blb/internal/core"
)

// testMemMasterConnEnv creates a MemMasterConnection managing
// 'numCurators' of curators.
func testMemMasterConnEnv(numCurators int) MasterConnection {
	var curators []string
	for i := 0; i < numCurators; i++ {
		curators = append(curators, strconv.Itoa(i))
	}
	return newMemMasterConnection(curators)
}

// TestMemMasterConnCreate creates a MemMasterConnection and requests
// to create a set of blobs.
func TestMemMasterConnCreate(t *testing.T) {
	numCurators := 20
	m := testMemMasterConnEnv(numCurators)

	// Repeat create requests and verify returned curators.
	for i := 0; i < numCurators*3; i++ {
		curator, err := m.MasterCreateBlob(context.Background())
		if core.NoError != err {
			t.Error(err)
		}
		if expected := strconv.Itoa(i % numCurators); expected != curator {
			t.Errorf("expected %s and got %s", expected, curator)
		}
	}
}

// TestMemMasterConnLookup creates a MemMasterConnection and a set of
// curators, requests to create a set of blobs, and then look them up.
func TestMemMasterConnLookup(t *testing.T) {
	numCurators := 20
	m := testMemMasterConnEnv(numCurators)

	// We assume that 'numCurators' blobs are created, each from a separate curator.
	for i := 0; i < numCurators; i++ {
		// Look up the partition.
		curator, err := m.LookupPartition(context.Background(), core.PartitionID(i))
		if core.NoError != err {
			t.Error(err)
		}
		if expected := strconv.Itoa(i); expected != curator {
			t.Errorf("expected %s! and got %s!", expected, curator)
		}
	}

	// Look up a blob in a non-existing partition returns error.
	if _, err := m.LookupPartition(context.Background(), core.PartitionID(numCurators)); core.ErrNoSuchBlob != err {
		t.Errorf("expected %s and got %s", core.ErrNoSuchBlob, err)
	}
}
