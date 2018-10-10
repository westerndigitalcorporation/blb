// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package core

import (
	"math"
	"testing"
)

// testBlobID creates a BlobID with the provided partition and id, serializes to a string,
// deserializes it, and checks that everything went OK.
func testBlobID(partition PartitionID, id BlobKey, t *testing.T) {
	b := BlobIDFromParts(partition, id)

	if b.Partition() != partition {
		t.Fatal("partition encoding failed")
	}

	if b.ID() != id {
		t.Fatal("id encoding failed")
	}

	bstr := b.String()
	newB, e := ParseBlobID(bstr)
	if nil != e {
		t.Fatal("error parsing from encoded string: " + e.Error())
	}

	if b.Partition() != newB.Partition() {
		t.Fatal("parsed partition does not match")
	}

	if b.ID() != newB.ID() {
		t.Fatal("parsed ID does not match")
	}
}

// testTractID creates a TractID with the provided values, serializes to a string,
// deserializes, and checks that everything went OK.
func testTractID(partition PartitionID, id BlobKey, index TractKey, t *testing.T) {
	tr := TractIDFromParts(BlobIDFromParts(partition, id), index)

	if tr.Blob.Partition() != partition {
		t.Fatal("partition encoding failed")
	}

	if tr.Blob.ID() != id {
		t.Fatal("id encoding failed")
	}

	tstr := tr.String()
	newT, e := ParseTractID(tstr)

	if nil != e {
		t.Fatal("error parsing from encoded string: " + e.Error())
	}

	if tr.Blob != newT.Blob {
		t.Fatalf("parsed BlobID does not match: %s and %s", tr.Blob, newT.Blob)
	}

	if tr.Index != newT.Index {
		t.Fatal("new index does not match")
	}
}

// Test various near-the-limit BlobID and TractID values.
func TestTractAndBlobIDLimits(t *testing.T) {
	// These are shared by the blob and tract IDs.
	partitionLimits := []PartitionID{1, 2, 100, math.MaxUint32 - 1, math.MaxUint32}
	idLimits := []BlobKey{1, 2, 100, math.MaxUint32 - 1, math.MaxUint32}

	// This is just for the tract ID.
	indexLimits := []TractKey{0, 1, 2, 100, math.MaxUint16 - 1, math.MaxUint16}

	for _, partition := range partitionLimits {
		for _, id := range idLimits {
			testBlobID(partition, id, t)

			for _, index := range indexLimits {
				testTractID(partition, id, index, t)
			}
		}
	}
}

// Test various invalid BlobIDs.
func TestInvalidBlobIDs(t *testing.T) {
	var blobs []BlobID

	// A set of PartitionIDs plus an invalid BlobKey.
	partitions := []PartitionID{0, 1, 2, 100, math.MaxUint32 - 1, math.MaxUint32}
	for _, p := range partitions {
		blobs = append(blobs, BlobIDFromParts(p, BlobKey(0)))
	}

	// An invalid PartitionID plus a set of BlobKeys.
	blobkeys := []BlobKey{0, 1, 2, 100, math.MaxUint32 - 1, math.MaxUint32}
	for _, bk := range blobkeys {
		blobs = append(blobs, BlobIDFromParts(PartitionID(0), bk))
	}

	// None of the BlobIDs should be valid.
	for _, b := range blobs {
		if b.IsValid() {
			t.Fatalf("invalid blob id %s", b)
		}
	}
}

func TestPartitionTypes(t *testing.T) {
	p := PartitionID(0x00001234)
	if !p.IsValidRegular() {
		t.Errorf("should be valid regular")
	}
	if p.IsValidRS() {
		t.Errorf("should not be valid parity")
	}
	if p.Type() != RegularPartition {
		t.Errorf("wrong type")
	}

	p = PartitionID(0x80001234)
	if p.IsValidRegular() {
		t.Errorf("should not be valid regular")
	}
	if !p.IsValidRS() {
		t.Errorf("should be valid parity")
	}
	if p.Type() != RSPartition {
		t.Errorf("wrong type")
	}
}

func TestRSChunkIDs(t *testing.T) {
	id := RSChunkID{0x1234, 0x5678} // not in parity partition
	if id.IsValid() {
		t.Errorf("should not be valid")
	}
	id = RSChunkID{0x1234, 0x56789abcdef1234} // too many bits
	if id.IsValid() {
		t.Errorf("should not be valid")
	}

	id = RSChunkID{0x80001234, 0x5678}
	if !id.IsValid() {
		t.Errorf("should be valid")
	}
	if !id.ToTractID().IsValid() {
		t.Errorf("should be valid")
	}

	if id.Add(1) == id {
		t.Errorf("add should do something")
	}
}
