// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package core

import "time"

// This file contains common structs that are used as parts of other messages.

// TractInfo describes one tract and where to find it.
type TractInfo struct {
	// The unique ID for the tract.
	Tract TractID

	// Latest version known to Curator.
	// Used to detect stale versions from failed tractservers.
	Version int

	// ==== for REPLICATED storage class:

	// List of tractservers serving this tract. Maybe be stale.
	Hosts []string

	// The corresponding tractserver IDs.
	TSIDs []TractserverID

	// ==== for RS_X classes:

	// Pointer to the tract encoded in an RS chunk.
	RS TractPointer
}

// BlobInfo is information about a blob, analogous to os.FileInfo.
type BlobInfo struct {
	// What is the replication factor of this blob? (REPLICATED storage class only)
	Repl int

	// How many tracts make up the blob?
	NumTracts int

	// When was the blob last written/read?
	MTime, ATime time.Time

	// How is this blob stored?
	Class StorageClass

	// How did the client hint that this should be stored?
	Hint StorageHint

	// Time after which this blob can be automatically deleted by the system.
	Expires time.Time
}

// TractPointer is a reference to a tract embedded in an RS chunk.
type TractPointer struct {
	Chunk  RSChunkID
	Host   string
	TSID   TractserverID
	Offset uint32
	Length uint32

	// Metadata for client-side reconstruction. These are parallel lists
	// containing the location of the other pieces of data that this tract was
	// coded with. Hosts that are known to be down may have "" / 0. If Class is
	// 0 (REPLICATED), OtherHosts/TSIDs are not valid (should be empty).
	Class      StorageClass
	BaseChunk  RSChunkID
	OtherHosts []string
	OtherTSIDs []TractserverID
}

// Present returns true if this TractPointer is not the zero value.
func (tp *TractPointer) Present() bool {
	return tp.TSID != 0
}
