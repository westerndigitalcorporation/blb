// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package durable

import (
	"encoding/binary"
	"encoding/json"
	"hash/crc32"

	log "github.com/golang/glog"

	"github.com/westerndigitalcorporation/blb/internal/core"
	"github.com/westerndigitalcorporation/blb/pkg/failures"
)

// State is the durable state that can only be mutated by Raft. There is no need
// to maintain a lock as Raft is responsible for serializing commands.
//
// The types don't really have to be Exported but we use Gob which requires it.
// NOTE: Don't forget to update State.checksum if any change is made to State.
type State struct {
	// List of curators managing the assigned partitions.
	//
	// Note that we are currently assuming that:
	// 1. CuratorIDs never go away and
	// 2. CuratorIDs are never reused.
	//
	// Curators own metadata, and to lose a curator replica group entirely
	// would be a disaster that we would have to recover from by determining
	// which blobIDs were not managed by a curator. In this case, we might
	// as well use the same CuratorID. Also, we can change this decision later.
	//
	// The slice is indexed by the partition IDs and stores curator IDs.
	// Note that valid partition and curator IDs start from 1 and thus we
	// reserve the first spot. This layout optimizes the lookup from blob
	// (partition) to curator with O(1) cost but has O(n) cost for returning
	// the list of partitions owned by a given curator. An alternative is to
	// store the reversed mapping from curators to list of partitions, which
	// works best for the second query. We opt for this design because the
	// first query happens much more frequently than the second.
	Partitions []core.CuratorID

	// The next curator ID we assign.
	NextCuratorID core.CuratorID

	// The next tractserver ID we assign.
	NextTractserverID core.TractserverID

	// Are we currently read-only?
	ReadOnly bool
}

// newState creates a new State. All the IDs starts from 1.
func newState() *State {
	s := &State{
		// Valid partition/curator ID starts from 1.
		Partitions:        []core.CuratorID{0},
		NextCuratorID:     1,
		NextTractserverID: 1,
	}
	failures.Register("corrupt_master_state", s.corrupt)
	return s
}

// lookup returns the ID of the curator replication group that is responsible
// for a partition.
func (s *State) lookup(partition core.PartitionID) (core.CuratorID, core.Error) {
	if partition == 0 || partition >= core.PartitionID(len(s.Partitions)) {
		return 0, core.ErrNoSuchBlob
	}
	return s.Partitions[partition], core.NoError
}

// Precalculated table for CRC computation.
var crc32Table = crc32.MakeTable(crc32.Castagnoli)

// checksum computes the crc checksum.
func (s *State) checksum() (checksum uint32) {
	// Write s.NextCuratorID and s.NextTractserverID.
	checksum = crc32UpdateUint64(0, uint64(s.NextCuratorID))
	checksum = crc32UpdateUint64(checksum, uint64(s.NextTractserverID))

	// Write s.Partitions.
	checksum = crc32UpdateUint64(checksum, uint64(len(s.Partitions)))
	for partitionID, curatorID := range s.Partitions {
		checksum = crc32UpdateUint64(checksum, uint64(partitionID))
		checksum = crc32UpdateUint64(checksum, uint64(curatorID))
	}

	// Read-only
	if s.ReadOnly {
		checksum = crc32UpdateUint64(checksum, 1)
	}

	return
}

// crc32UpdateUint64 returns the result of adding the byte representation of the
// uint64 'val' to 'crc'.
func crc32UpdateUint64(crc uint32, val uint64) uint32 {
	buffer := make([]byte, 8)
	binary.LittleEndian.PutUint64(buffer, val)
	return crc32.Update(crc, crc32Table, buffer)
}

// uint32Transform does bitwise transform to 'x'. For now, it's a simple 16-bit
// rotation.
func uint32Transform(x uint32) uint32 {
	return x<<16 | x>>16
}

// corrupt makes an arbitrary change to the state, for testing with failure injection.
func (s *State) corrupt(json.RawMessage) error {
	log.Infof("CORRUPTING STATE")
	s.NextTractserverID = 678910
	return nil
}
