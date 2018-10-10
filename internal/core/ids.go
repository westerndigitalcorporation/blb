// Copyright (c) 2016 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package core

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

/*

The definitions of partition, blob and tract ID's follow the hierarchy below:

 - PartitionID is the 32 bits (or 4 bytes) ID that identifies a partition in the universe.

     +-------------------------+
     |  PartitionID (4 bytes)  |
     +-------------------------+

 - BlobKey is the 32 bits (or 4 bytes) in-partition ID that identifies a blob in a partition.
 - BlobID is the PartitionID prefix plus the BlobKey suffix, which identifies a blob in the universe.

     +-------------------------+---------------------+
     |  PartitionID (4 bytes)  |  BlobKey (4 bytes)  |
     +-------------------------+---------------------+
     |<--------------------------------------------->|
                       BlobID (8 bytes)

 - TractKey is the 16 bits (or 2 bytes) in-blob index that identifies a tract in a blob.
 - TractID is the BlobID prefix plus the TractKey suffix, which identifies a tract in the universe.

     +-------------------------+---------------------+----------------------+
     |  PartitionID (4 bytes)  |  BlobKey (4 bytes)  |  TractKey (2 bytes)  |
     +-------------------------+---------------------+----------------------+
     |<-------------------------------------------------------------------->|
                                  TractID (10 bytes)


Rationale: These sizes were chosen to work well for Upthere's hardware and
cluster size requirements at the time, keep the curator metadata working set
size small, and also leave room for growth.

*/

// ErrInvalidID is the error returned when a string representation of an ID is invalid.
var ErrInvalidID = errors.New("invalid id format")

// CuratorID is a master assigned ID to a Curator. Valid CuratorIDs start from 1.
type CuratorID uint32

// TractserverID is a master assigned ID to a Tractserver. Valid TractserverIDs
// start from 1.
type TractserverID uint32

func (t TractserverID) String() string {
	return fmt.Sprintf("%d", t)
}

// PartitionID identifies a partition that the master assigned to a
// curator. BlobIDs and TractIDs generated in this partition are all
// prefixed by it. Valid PartitionIDs start from 1.
type PartitionID uint32

// PartitionType is a type for partition IDs. PartitionIDs come in two varieties
// (so far), regular partitions and RS partitions. RS partitions are used
// for allocating IDs for RS-encoded chunks. They're differentiated by their
// uppermost two bits.
type PartitionType uint32

const (
	// RegularPartition is the type for partitions holding regular blobs and tracts.
	RegularPartition PartitionType = iota
	reservedPartitionType1
	// RSPartition is the type for partitions holding RS chunks (data and parity).
	RSPartition
	reservedPartitionType2
)

// MaxPartitionID is the maximum id of a regular partition (or the max of the id
// part of a RS partition).
const MaxPartitionID = (1 << 30) - 1

// BlobKey identifies a blob in a partition. It is the suffix part of
// a BlobID. Valid BlobKeys start from 1.
type BlobKey uint32

// BlobID refers to a Blob stored in Blb. A BlobID consists of a
// PartitionID prefix and a BlobKey suffix. Clients of Blb
// communicate in BlobIDs.
type BlobID uint64

// TractKey identifies a tract in a blob. It is the suffix part of a
// TractID. Valid TractKeys start from 0, so that we can align them with slice
// indices.
type TractKey uint16

// TractID refers to a tract stored on a Tractserver. A TractID
// consists of a BlobId prefix and a TractKey suffix. BlobIDs are
// mapped to TractIDs internally. This is not visible to the Blb client.
type TractID struct {
	// What blob does this tract belong to?
	Blob BlobID

	// What Index in the blob is this tract?
	Index TractKey
}

// RSChunkID is the id of a Reed-Solomon-encoded bunch of tracts. It is the same
// size as a tract id so that tractservers can handle it identically.
// Partition in an RSChunkID must be a RS partition, with upper two bits 10.
type RSChunkID struct {
	Partition PartitionID
	ID        uint64 // only lower 48 bits used
}

// MaxBlobSize is the maximum number of tracts can be created in a
// blob. This is determined by TractKey length (currently 2 bytes).
const MaxBlobSize = math.MaxUint16 + 1

// MaxBlobKey is the maximum value of a BlobKey.
const MaxBlobKey = math.MaxUint32

// MaxRSChunkKey is the maximum value of the key part of an RSChunkID.
const MaxRSChunkKey uint64 = (1 << 48) - 1

//-------------------
// CuratorID Methods
//-------------------

// IsValid returns if 'c' is a valid CuratorID.
func (c CuratorID) IsValid() bool {
	return c != CuratorID(0)
}

//-----------------------
// TractserverID Methods
//-----------------------

// IsValid returns if 't' is a valid TractserverID.
func (t TractserverID) IsValid() bool {
	return t != TractserverID(0)
}

//---------------------
// PartitionID Methods
//---------------------

// IsValidRegular returns true if 'p' is a valid PartitionID of a regular partition.
func (p PartitionID) IsValidRegular() bool {
	return (p&MaxPartitionID) != 0 && p.Type() == RegularPartition
}

// IsValidRS returns true if p is a valid PartitionID for a RS partition.
func (p PartitionID) IsValidRS() bool {
	return (p&MaxPartitionID) != 0 && p.Type() == RSPartition
}

// Type returns the type of the paritition (upper two bits).
func (p PartitionID) Type() PartitionType {
	return PartitionType(p >> 30)
}

// WithoutType returns the ID part of the full partition ID, that is, collapsing
// RS partitions to their corresponding regular partition.
func (p PartitionID) WithoutType() PartitionID {
	return p & 0x3fffffff
}

// FirstBlobID returns the first BlobID in the partition.
// Note we reserve the first value as guard and thus the plus one below.
func (p PartitionID) FirstBlobID() BlobID {
	return BlobID(uint64(p)<<32 + 1)
}

// LastBlobID returns the last BlobID in the partition.
func (p PartitionID) LastBlobID() BlobID {
	return BlobID(uint64(p)<<32 | uint64(math.MaxUint32))
}

//----------------
// BlobID Methods
//----------------

// ZeroBlobID is an invalid blob id for most purposes, but is used for metadata
// tracts in the tractserver.
const ZeroBlobID BlobID = 0

// IsValid returns if 'b' is a valid BlobID.
func (b BlobID) IsValid() bool {
	return b.Partition().IsValidRegular() && b.ID() > BlobKey(0)
}

// BlobIDFromParts makes a BlobID from a PartitionID and a BlobKey.
func BlobIDFromParts(pid PartitionID, bk BlobKey) BlobID {
	return BlobID(uint64(pid)<<32 | uint64(bk))
}

// Partition returns the partition ID.
func (b BlobID) Partition() PartitionID {
	return PartitionID(b >> 32)
}

// ID returns the partition-unique ID number of the blob.
func (b BlobID) ID() BlobKey {
	return BlobKey(b & math.MaxUint32)
}

// String returns a human-readable string representation of the BlobID that can also be parsed by ParseBlobID.
func (b BlobID) String() string {
	return fmt.Sprintf("%016x", uint64(b))
}

// ParseBlobID parses a BlobID from the provided string. The string must be in
// the format produced by BlobID.String(). If it is not, ErrInvalidID will be
// returned.
func ParseBlobID(s string) (BlobID, error) {
	var b BlobID
	n, e := fmt.Sscanf(s, "%016x", &b)
	if n != 1 || nil != e {
		return b, ErrInvalidID
	}
	return b, nil
}

// Next returns the next highest blob id in a sequential ordering.
func (b BlobID) Next() BlobID {
	return b + 1
}

//-----------------
// TractID Methods
//-----------------

// IsValid returns if 't' is a valid TractID.
func (t TractID) IsValid() bool {
	return t.Blob.IsValid() || t.Blob.Partition().IsValidRS()
}

// TractIDFromParts makes a TractID from a BlobID and a TractKey.
func TractIDFromParts(bid BlobID, tk TractKey) TractID {
	return TractID{Blob: bid, Index: tk}
}

// String returns a human-readable string representation of the TractID that can also be parsed by ParseTractID.
func (t TractID) String() string {
	return fmt.Sprintf("%016x:%04x", uint64(t.Blob), uint16(t.Index))
}

// ParseTractID parses a TractID from the provided string. The string must be in
// the format produced by TractID.String(). If it is not, ErrInvalidID will be
// returned.
func ParseTractID(s string) (TractID, error) {
	var t TractID

	n, e := fmt.Sscanf(s, "%016x:%04x", &t.Blob, &t.Index)
	if nil != e || 2 != n {
		return t, ErrInvalidID
	}
	return t, nil
}

// TractIDs can also be formed from RSChunkIDs. In that case, they won't
// represent a single tract from a single blob, they will represent an RS chunk
// piece. We can use these functions to handle them separately:

// IsRegular returns true if t represents a regular tract.
func (t TractID) IsRegular() bool {
	return t.Blob.Partition().Type() == RegularPartition
}

// IsRS returns true if t represents an RS chunk piece.
func (t TractID) IsRS() bool {
	return t.Blob.Partition().Type() == RSPartition
}

// For automatic marshaling in protocol buffers:

// Size returns the marshaled size of a tract id.
func (t TractID) Size() int {
	return 10
}

// MarshalTo writes this tract id to the given slice.
func (t TractID) MarshalTo(out []byte) (n int, err error) {
	binary.BigEndian.PutUint64(out[0:8], uint64(t.Blob))
	binary.BigEndian.PutUint16(out[8:10], uint16(t.Index))
	return 10, nil
}

// Unmarshal sets this tract id from the given slice.
func (t *TractID) Unmarshal(in []byte) error {
	if len(in) != 10 {
		return ErrInvalidID
	}
	t.Blob = BlobID(binary.BigEndian.Uint64(in[0:8]))
	t.Index = TractKey(binary.BigEndian.Uint16(in[8:10]))
	return nil
}

//-----------------
// RSChunkID Methods
//-----------------

// IsValid returns true if r is a valid RSChunkID.
func (r RSChunkID) IsValid() bool {
	return r.Partition.IsValidRS() && r.ID != 0 && r.ID <= MaxRSChunkKey
}

// ToTractID returns the tract id that corresponds to this RSChunkID.
func (r RSChunkID) ToTractID() TractID {
	return TractID{
		Blob:  BlobID((uint64(r.Partition) << 32) | (r.ID>>16)&math.MaxUint32),
		Index: TractKey(r.ID & math.MaxUint16),
	}
}

// Add returns the RS chunk id incremented by i.
func (r RSChunkID) Add(i int) RSChunkID {
	r.ID += uint64(i)
	return r
}

// Next returns the following RS chunk id in sequential order.
func (r RSChunkID) Next() RSChunkID {
	if r.ID < MaxRSChunkKey {
		r.ID++
	} else {
		r.Partition++
		r.ID = 0
	}
	return r
}

// String returns a string representation of this RSChunkID. Note that this is
// NOT the same representation as r.ToTractID().String().
func (r RSChunkID) String() string {
	return fmt.Sprintf("%08x:%012x", uint32(r.Partition), r.ID)
}
