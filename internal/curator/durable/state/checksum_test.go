// Copyright (c) 2017 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package state

import (
	"testing"

	"github.com/golang/protobuf/proto"

	"github.com/westerndigitalcorporation/blb/internal/core"
	pb "github.com/westerndigitalcorporation/blb/internal/curator/durable/state/statepb"
)

func checksum(s *State, startBlob core.BlobID, n int) uint64 {
	txn := s.ReadOnlyTxn()
	ck, _ := txn.Checksum(pos(startBlob), n)
	txn.Commit()
	return ck
}

func pos(blob core.BlobID) ChecksumPosition {
	return ChecksumPosition{Blob: blobID2Key(blob)}
}

func (cp *ChecksumPosition) blob() core.BlobID {
	if cp.Blob == nil {
		return core.BlobID(0)
	}
	return key2BlobID(cp.Blob)
}

type putRSChunkArgs struct {
	id      core.RSChunkID
	storage core.StorageClass
	hosts   []core.TractserverID
	data    [][]EncodedTract
}

func makeState(t *testing.T, id core.CuratorID, parts []pb.Partition, blobs map[core.BlobID]pb.Blob,
	chunks []putRSChunkArgs) (s *State) {

	s = getTestState(t)
	txn := s.WriteTxn(1)
	txn.SetCuratorID(id)
	for _, p := range parts {
		txn.PutPartition(&p)
	}
	for bid, blob := range blobs {
		txn.PutBlob(bid, &blob)
	}
	for _, c := range chunks {
		if e := txn.PutRSChunk(c.id, c.storage, c.hosts, c.data); e != core.NoError {
			t.Fatalf("PutRSChunk: %s", e.Error())
		}
	}
	txn.Commit()
	return
}

func makeTract(version int, hosts ...core.TractserverID) *pb.Tract {
	return &pb.Tract{
		Version: version,
		Hosts:   hosts,
	}
}

// TestChecksum defines a series of states none of which match, ensuring that
// each has a different checksum. The checksums are compared to each other: If
// the checksums are calculated from the same state, they should match;
// otherwise, they should never match.
func TestChecksum(t *testing.T) {
	// States below are different from each other.
	states := []*State{
		// 0
		makeState(t, 1, nil, nil, nil),
		// 1
		makeState(t, 2, nil, nil, nil),
		// 2
		makeState(t, 1,
			[]pb.Partition{
				{Id: proto.Uint32(1), NextBlobKey: proto.Uint32(1)},
			},
			nil,
			nil,
		),
		// 3
		makeState(t, 1,
			[]pb.Partition{
				{Id: proto.Uint32(2), NextBlobKey: proto.Uint32(1)},
			},
			nil,
			nil,
		),
		// 4
		makeState(t, 1,
			[]pb.Partition{
				{Id: proto.Uint32(1), NextBlobKey: proto.Uint32(1)},
			},
			map[core.BlobID]pb.Blob{
				core.BlobIDFromParts(1, 1): {Repl: proto.Uint32(3)},
			},
			nil,
		),
		// 5
		makeState(t, 1,
			[]pb.Partition{
				{Id: proto.Uint32(1), NextBlobKey: proto.Uint32(2)},
			},
			map[core.BlobID]pb.Blob{
				core.BlobIDFromParts(1, 1): {Repl: proto.Uint32(3)},
			},
			nil,
		),
		// 6
		makeState(t, 1,
			[]pb.Partition{
				{Id: proto.Uint32(1), NextBlobKey: proto.Uint32(2)},
			},
			map[core.BlobID]pb.Blob{
				core.BlobIDFromParts(1, 1): {Repl: proto.Uint32(3), Tracts: []*pb.Tract{
					makeTract(1, 1, 2, 3),
				}},
			},
			nil,
		),
		// 7
		makeState(t, 1,
			[]pb.Partition{
				{Id: proto.Uint32(1), NextBlobKey: proto.Uint32(2)},
			},
			map[core.BlobID]pb.Blob{
				core.BlobIDFromParts(1, 1): {Repl: proto.Uint32(3), Tracts: []*pb.Tract{
					makeTract(1, 1, 2, 3),
					makeTract(2, 4, 2, 5),
				}},
			},
			nil,
		),
		// 8
		makeState(t, 1,
			[]pb.Partition{
				{Id: proto.Uint32(1), NextBlobKey: proto.Uint32(3)},
			},
			map[core.BlobID]pb.Blob{
				core.BlobIDFromParts(1, 1): {Repl: proto.Uint32(3), Tracts: []*pb.Tract{
					makeTract(1, 1, 2, 3),
					makeTract(2, 4, 2, 5),
				}},
				core.BlobIDFromParts(1, 2): {Repl: proto.Uint32(3), Tracts: []*pb.Tract{
					makeTract(1, 1, 2, 3),
					makeTract(3, 4, 2, 5),
				}},
			},
			nil,
		),
		// 9
		makeState(t, 1,
			[]pb.Partition{
				{Id: proto.Uint32(1), NextBlobKey: proto.Uint32(3)},
				{Id: proto.Uint32(2), NextBlobKey: proto.Uint32(4)},
			},
			map[core.BlobID]pb.Blob{
				core.BlobIDFromParts(1, 1): {Repl: proto.Uint32(3), Tracts: []*pb.Tract{
					makeTract(1, 1, 2, 3),
					makeTract(2, 4, 2, 5),
				}},
				core.BlobIDFromParts(1, 2): {Repl: proto.Uint32(3), Tracts: []*pb.Tract{
					makeTract(1, 1, 2, 3),
					makeTract(3, 4, 2, 5),
				}},
				core.BlobIDFromParts(2, 1): {Repl: proto.Uint32(3), Tracts: []*pb.Tract{
					makeTract(1, 1, 2, 3),
					makeTract(2, 4, 2, 5),
				}},
				core.BlobIDFromParts(2, 2): {Repl: proto.Uint32(3), Tracts: []*pb.Tract{
					makeTract(1, 1, 2, 3),
					makeTract(3, 4, 2, 5),
				}},
				core.BlobIDFromParts(2, 3): {Repl: proto.Uint32(3), Tracts: []*pb.Tract{
					makeTract(1, 6, 9, 8),
				}},
			},
			nil,
		),
		// 10 (partitions and blobs same as 7)
		makeState(t, 1,
			[]pb.Partition{
				{Id: proto.Uint32(1), NextBlobKey: proto.Uint32(3)},
			},
			map[core.BlobID]pb.Blob{
				core.BlobIDFromParts(1, 1): {Repl: proto.Uint32(3), Tracts: []*pb.Tract{
					makeTract(1, 1, 2, 3),
					makeTract(2, 4, 2, 5),
				}},
				core.BlobIDFromParts(1, 2): {Repl: proto.Uint32(3), Tracts: []*pb.Tract{
					makeTract(1, 1, 2, 3),
					makeTract(3, 4, 2, 5),
				}},
			},
			[]putRSChunkArgs{
				{
					core.RSChunkID{Partition: 0x80000001, ID: 54321},
					core.StorageClass_RS_6_3,
					[]core.TractserverID{1, 2, 3, 4, 5, 6, 9, 8, 7},
					[][]EncodedTract{
						{{core.TractIDFromParts(core.BlobIDFromParts(1, 2), 0), 0, 100, 6}},
						{{core.TractIDFromParts(core.BlobIDFromParts(1, 1), 0), 0, 100, 4}},
						{},
						{},
						{},
						{},
					}},
			},
		),
		// 11 (partitions and blobs same as 7, 10)
		makeState(t, 1,
			[]pb.Partition{
				{Id: proto.Uint32(1), NextBlobKey: proto.Uint32(3)},
			},
			map[core.BlobID]pb.Blob{
				core.BlobIDFromParts(1, 1): {Repl: proto.Uint32(3), Tracts: []*pb.Tract{
					makeTract(1, 1, 2, 3),
					makeTract(2, 4, 2, 5),
				}},
				core.BlobIDFromParts(1, 2): {Repl: proto.Uint32(3), Tracts: []*pb.Tract{
					makeTract(1, 1, 2, 3),
					makeTract(3, 4, 2, 5),
				}},
			},
			[]putRSChunkArgs{
				{
					core.RSChunkID{Partition: 0x80000001, ID: 54321},
					core.StorageClass_RS_6_3,
					[]core.TractserverID{1, 2, 3, 4, 5, 6, 9, 8, 7},
					[][]EncodedTract{
						{{core.TractIDFromParts(core.BlobIDFromParts(1, 2), 0), 0, 100, 6},
							{core.TractIDFromParts(core.BlobIDFromParts(1, 1), 0), 0, 100, 4}},
						{},
						{},
						{},
						{},
					}},
			},
		),
	}

	for i := 0; i < len(states)-1; i++ {
		for j := i; j < len(states); j++ {
			ck1 := checksum(states[i], core.BlobID(0), 1000)
			ck2 := checksum(states[j], core.BlobID(0), 1000)
			if i != j && ck1 == ck2 {
				t.Errorf("different states have the same checksum %d, %d: %d", i, j, ck1)
				t.Errorf("state %d: %+v", i, states[i])
				t.Errorf("state %d: %+v", j, states[j])
			}
			if i == j && ck1 != ck2 {
				t.Errorf("same states have different checksums")
			}
		}
	}
}

func TestChecksumNext(t *testing.T) {
	s := getTestState(t)

	// create a state with 1000 blobs
	txn := s.WriteTxn(1)
	txn.SetCuratorID(123)
	var part core.PartitionID = 44
	txn.PutPartition(&pb.Partition{Id: proto.Uint32(uint32(part))})
	for i := 1000; i < 2000; i++ {
		txn.PutBlob(core.BlobIDFromParts(part, core.BlobKey(i)), &pb.Blob{Repl: proto.Uint32(3)})
	}
	txn.Commit()

	// get a chunk of 100
	txn = s.ReadOnlyTxn()
	_, next := txn.Checksum(pos(core.BlobID(0)), 100)
	if next.blob() != core.BlobIDFromParts(part, 1100) {
		t.Errorf("unexpected next blob: %v", next)
	}

	// another
	_, next = txn.Checksum(next, 100)
	if next.blob() != core.BlobIDFromParts(part, 1200) {
		t.Errorf("unexpected next blob: %v", next)
	}

	// the final chunk
	_, next = txn.Checksum(pos(core.BlobIDFromParts(part, 1950)), 100)
	if next.blob() != core.BlobID(0) {
		t.Errorf("unexpected next blob: %v", next)
	}

	txn.Commit()
}
