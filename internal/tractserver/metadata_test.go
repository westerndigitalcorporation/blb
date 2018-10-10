// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package tractserver

import (
	"testing"

	"github.com/westerndigitalcorporation/blb/internal/core"
)

// Make a MetadataStore, fill in a tractserver id, close it, make another one,
// and get id back.
func testSetGetTractserverID(t *testing.T, disks []Disk) {
	// Get a metadata store.
	s := NewMetadataStore()
	s.setDisks(disks)

	// We should have no tractserver id to begin with.
	if id := s.getTractserverID(); id.IsValid() {
		t.Fatalf("should have no tractserver id yet")
	}

	// Now set a tractserver id.
	id := core.TractserverID(23)
	newID, err := s.setTractserverID(id)
	if core.NoError != err {
		t.Fatal(err.String())
	}
	if id != newID {
		t.Fatalf("failed to set tractserver id")
	}

	// We cannot set it to a different value once it's been set.
	newID, err = s.setTractserverID(id + 1)
	if core.NoError != err {
		t.Fatal(err.String())
	}
	if id != newID {
		t.Fatalf("should only be able to set tractserver id once")
	}

	// Re-open the version store and pull the data out.
	s = NewMetadataStore()
	s.setDisks(disks)
	if newID := s.getTractserverID(); id != newID {
		t.Fatalf("failed to get tractserver id back, expected %d and got %d", id, newID)
	}
}

func testMerge(t *testing.T, disks []Disk) {
	// Set some initial metadata.
	s := NewMetadataStore()
	s.setDisks(disks)
	s.meta.TSID = 567
	s.saveMetadata()

	// Load it from one disk
	s = NewMetadataStore()
	s.setDisks(disks[:1])
	if s.meta.TSID != 567 {
		t.Errorf("Unexpected meta: %+v", s.meta)
	}
	// Write different TSID.
	s.meta.TSID = 999
	s.saveMetadata()

	// Load from all three again and we should get the original TSID.
	s = NewMetadataStore()
	s.setDisks(disks)
	if s.meta.TSID != 567 {
		t.Errorf("Unexpected meta: %+v", s.meta)
	}
}

func metadataTests(t *testing.T, disks []Disk) {
	testSetGetTractserverID(t, disks)
	testMerge(t, disks)
}

func TestMetadataStoreWithMemDisks(t *testing.T) {
	metadataTests(t, []Disk{
		NewMemDisk(),
		NewMemDisk(),
		NewMemDisk(),
	})
}

func TestMetadataStoreWithRealDisks(t *testing.T) {
	metadataTests(t, []Disk{
		getTestManager(t),
		getTestManager(t),
		getTestManager(t),
	})
}
