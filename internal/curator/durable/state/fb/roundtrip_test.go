// Copyright (c) 2019 David Reiss. All rights reserved.
// SPDX-License-Identifier: MIT

package fb

import (
	"reflect"
	"testing"

	"github.com/westerndigitalcorporation/blb/internal/core"
)

// TestRoundtripPartition tests BuildPartition and PartitionF.ToStruct.
func TestRoundtripPartition(t *testing.T) {
	p := Partition{
		NextBlobKey:    456,
		NextRsChunkKey: 789,
	}
	p2 := *GetRootAsPartitionF(BuildPartition(&p), 0).ToStruct()
	if !reflect.DeepEqual(p, p2) {
		t.Errorf("partition roundtrip: %+v != %+v", p, p2)
	}
}

// TestRoundtripBlob tests BuildBlob and BlobF.ToStruct.
func TestRoundtripBlob(t *testing.T) {
	b := Blob{
		Storage: core.StorageClassRS_6_3,
		Hint:    core.StorageHintCOLD,
		Tracts: []*Tract{
			&Tract{
				Hosts:   []core.TractserverID{7, 6, 5},
				Version: 3,
			},
			&Tract{
				Hosts:   []core.TractserverID{777777, 666666},
				Version: 3,
			},
			&Tract{
				Hosts:     []core.TractserverID{999999},
				Version:   3,
				Rs83Chunk: &core.TractID{Blob: 2, Index: 3},
			},
			&Tract{
				Version:    4,
				Rs63Chunk:  &core.TractID{Blob: 22222, Index: 33},
				Rs103Chunk: &core.TractID{Blob: 55555, Index: 99},
			},
			&Tract{
				Hosts:      []core.TractserverID{23, 26, 29, 31, 33, 35, 37, 39},
				Version:    9,
				Rs125Chunk: &core.TractID{Blob: 33333, Index: 22},
			},
		},
		Repl:    3,
		Deleted: 100e9,
		Mtime:   9876e9,
		Atime:   5432e9,
		Expires: 100e9,
	}
	b2 := *GetRootAsBlobF(BuildBlob(&b), 0).ToStruct()
	if !reflect.DeepEqual(b, b2) {
		t.Errorf("blob roundtrip: %+v != %+v", b, b2)
	}

	// try without tracts
	b.Tracts = nil
	b2 = *GetRootAsBlobF(BuildBlob(&b), 0).ToStruct()
	if !reflect.DeepEqual(b, b2) {
		t.Errorf("blob-no-tracts roundtrip: %+v != %+v", b, b2)
	}
}

// TestRoundtripRSChunk tests BuildRSChunk and RSChunkF.ToStruct.
func TestRoundtripRSChunk(t *testing.T) {
	tid := func(p core.PartitionID, b core.BlobKey, t core.TractKey) core.TractID {
		return core.TractIDFromParts(core.BlobIDFromParts(p, b), t)
	}
	c := RSChunk{
		Data: []*RSC_Data{
			{Tracts: []*RSC_Tract{
				{Id: tid(123, 456, 78), Length: 100, Offset: 0},
				{Id: tid(123, 456, 99), Length: 321, Offset: 1000},
			}},
			{},
			{Tracts: []*RSC_Tract{
				{Id: tid(0xddccaabb, 0xf293d3aa, 0xeabc), Length: 0xfedcba98, Offset: 0xefcdab89},
				{Id: tid(0x33449977, 0xe93345a1, 0x53ff), Length: 0x3231cc2b, Offset: 0x7382c912},
			}},
		},
		Hosts: []core.TractserverID{9, 8, 7, 6, 5, 4, 33, 2, 111},
	}
	c2 := *GetRootAsRSChunkF(BuildRSChunk(&c), 0).ToStruct()
	if !reflect.DeepEqual(c, c2) {
		t.Errorf("rschunk roundtrip: %+v != %+v", c, c2)
	}
}
