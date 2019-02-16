// Copyright (c) 2019 David Reiss. All rights reserved.
// SPDX-License-Identifier: MIT

package fb

import "github.com/westerndigitalcorporation/blb/internal/core"

// Tract is the struct version of TractF.
type Tract struct {
	// Used for REPLICATED class.
	Hosts   []core.TractserverID
	Version int

	// Used for RS_X classes. There must be exactly one of these for each RS_n_m
	// enum value in core.StorageClass.
	Rs63Chunk  *core.TractID
	Rs83Chunk  *core.TractID
	Rs103Chunk *core.TractID
	Rs125Chunk *core.TractID
}

// Blob is the struct version of BlobF.
type Blob struct {
	Storage core.StorageClass // Storage class for this blob (applies to all tracts).
	Hint    core.StorageHint  // Client storage hint.
	Tracts  []*Tract
	Repl    int // Replication factor (for REPLICATED).
	// Times are in unix nanos.
	Deleted int64 // Time that this blob was deleted, or zero if it is not deleted.
	Mtime   int64 // Time of last open for write.
	Atime   int64 // Time of last open for read.
	Expires int64 // Time that this blob can be automatically deleted, or zero if it is permanent.
}

// Partition is the struct version of PartitionF.
type Partition struct {
	Id             core.PartitionID
	NextBlobKey    core.BlobKey
	NextRsChunkKey uint64 // 48 bits of "blob key" and "tract index" in one value
}

// RSChunk is the struct version of RSChunkF.
type RSChunk struct {
	Data  []*RSC_Data          // exactly N values
	Hosts []core.TractserverID // exactly N+M values
}

// RSC_Data is the struct version of RSC_DataF.
type RSC_Data struct {
	Tracts []*RSC_Tract // any number of values
}

// RSC_Tract is the struct version of RSC_TractF.
type RSC_Tract struct {
	Id     core.TractID // tract id
	Length uint32       // length of tract data
	Offset uint32       // offset within this piece
}
