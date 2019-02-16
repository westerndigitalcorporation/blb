// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package durable

import (
	"bytes"
	"encoding/gob"
	"time"

	log "github.com/golang/glog"

	"github.com/westerndigitalcorporation/blb/internal/core"
	"github.com/westerndigitalcorporation/blb/internal/curator/durable/state"
)

// Command is a command that is serialized and handed to the Raft algorithm.
// Gob requires a known type passed as an argument to serialize/deserialize,
// which is why we can't just pass the command.
type Command struct {
	Cmd interface{}
}

// init registers the various command types with the gob system so it
// can automatically encode and decode them.
func init() {
	gob.Register(SetReadOnlyModeCommand{})
	gob.Register(SetRegistrationCommand{})
	gob.Register(AddPartitionCommand{})
	gob.Register(SyncPartitionsCommand{})
	gob.Register(CreateBlobCommand{})
	gob.Register(ExtendBlobCommand{})
	gob.Register(ChangeTractCommand{})
	gob.Register(DeleteBlobCommand{})
	gob.Register(UndeleteBlobCommand{})
	gob.Register(FinishDeleteCommand{})
	gob.Register(SetMetadataCommand{})
	gob.Register(UpdateTimesCommand{})
	gob.Register(ChecksumCommand{})
	gob.Register(VerifyChecksumCommand{})
	gob.Register(AllocateRSChunkIDsCommand{})
	gob.Register(CommitRSChunkCommand{})
	gob.Register(UpdateRSHostsCommand{})
	gob.Register(UpdateStorageClassCommand{})
	gob.Register(CreateTSIDCacheCommand{})
}

// SetReadOnlyModeCommand changes the read-only mode of the curator's state.
// This is used to prevent mutations during upgrades.
type SetReadOnlyModeCommand struct {
	ReadOnly bool
}

// SetRegistrationCommand sets the flag indicating whether or not the curator has registered,
// and also sets its master-assigned ID.
type SetRegistrationCommand struct {
	ID core.CuratorID
}

// SetRegistrationResult returns the registration.  If a command is re-registering the curator,
// which is an unfortunate consequence of reading, contacting the master, and then writing,
// the first value wins.
type SetRegistrationResult struct {
	ID core.CuratorID
}

// AddPartitionCommand adds a partition to our state.
type AddPartitionCommand struct {
	ID core.PartitionID
}

// AddPartitionResult is the result of an AddPartitionCommand.
type AddPartitionResult struct {
	Err core.Error
}

// SyncPartitionsCommand syncs partition assignment.
type SyncPartitionsCommand struct {
	Partitions []core.PartitionID
}

// SyncPartitionsResult is just a placeholder for consistency.
type SyncPartitionsResult struct {
	Err core.Error
}

// CreateBlobCommand adds a blob.
type CreateBlobCommand struct {
	// The desired replication factor of this blob.
	Repl int

	// Initial value for MTime and ATime.
	InitialTime int64

	// Expiry time.
	Expires int64

	// Initial storage hint.
	Hint core.StorageHint
}

// CreateBlobResult is a reply to a CreateBlobCommand.
type CreateBlobResult struct {
	// What's the ID?
	ID core.BlobID

	// Did anything go wrong during creation?  We can successfully issue a create
	// command but not have any BlobID space.
	Err core.Error
}

// ExtendBlobCommand extends a blob, adding 'Tracts' to the blob 'ID'.
type ExtendBlobCommand struct {
	// What blob are we extending?
	ID core.BlobID

	// The tract key for the first new tract. New tracts have contigious
	// tract keys.
	FirstTractKey core.TractKey

	// Each Hosts[i] is a replication group for a new tract.
	Hosts [][]core.TractserverID
}

// ExtendBlobResult is the result of a blob extension.
type ExtendBlobResult struct {
	Err core.Error

	// What is the new number of tracts?
	NewSize int
}

// DeleteBlobCommand deletes a blob.
type DeleteBlobCommand struct {
	ID core.BlobID

	// When should this blob be considered deleted?  We keep the metadata of blobs
	// around for some time period to allow for easy recovery from administrator error.
	When time.Time
}

// DeleteBlobResult is the result of a DeleteBlobCommand.
type DeleteBlobResult struct {
	// Was there an error in executing this command?
	Err core.Error
}

// UndeleteBlobCommand undeletes a blob.
type UndeleteBlobCommand struct {
	ID core.BlobID
}

// FinishDeleteCommand finalizes the deletion of one or more blobs.
type FinishDeleteCommand struct {
	Blobs []core.BlobID
}

// SetMetadataCommand changes metadata for a blob.
type SetMetadataCommand struct {
	ID       core.BlobID
	Metadata core.BlobInfo
}

// UndeleteBlobResult is the result of an UndeleteBlobCommand.
type UndeleteBlobResult struct {
	Err core.Error
}

// ChangeTractCommand changes the replication group for a tract.
type ChangeTractCommand struct {
	// What tract are we changing?
	ID core.TractID

	// What is the new version of the tract?  The command only succeeds
	// if the existing version + 1 == NewVersion.  This is to allow only
	// one curator to succeed at bumping the version to any particular value.
	NewVersion int

	// What are the new hosts for this tract?
	NewHosts []core.TractserverID
}

// UpdateTimesCommand asks the curator to update blob mtimes/atimes in a batch.
type UpdateTimesCommand struct {
	Updates []state.UpdateTime
}

// ChecksumCommand asks the curator to compute a partial checksum of its state.
type ChecksumCommand struct {
	Start state.ChecksumPosition
	N     int
}

// ChecksumResult is the result of a ChecksumCommand.
type ChecksumResult struct {
	Next     state.ChecksumPosition
	Checksum uint64
	Index    uint64
}

// VerifyChecksumCommand asks the curator to verify a previously-computed
// checksum.
type VerifyChecksumCommand struct {
	Index    uint64
	Checksum uint64
}

// AllocateRSChunkIDsCommand asks the curator to allocate a contiguous range of
// RSChunkIDs.
type AllocateRSChunkIDsCommand struct {
	N int
}

// AllocateRSChunkIDsResult is the result of an AllocateRSChunkIDsCommand.
type AllocateRSChunkIDsResult struct {
	Err core.Error
	ID  core.RSChunkID // the first id
}

// CommitRSChunkCommand asks the curator to store an RS chunk and also update
// the metadata for all tracts contained in that chunk to point to it.
type CommitRSChunkCommand struct {
	// Base ID for the RS chunk.
	ID core.RSChunkID
	// The storage class that this chunk is encoded as.
	Storage core.StorageClass
	// The hosts that each piece is stored on, in order.
	Hosts []core.TractserverID
	// The layout of the data pieces.
	Data [][]state.EncodedTract
}

// UpdateRSHostsCommand asks the curator to update its record of the hosts that
// a chunk is stored on.
type UpdateRSHostsCommand struct {
	ID    core.RSChunkID
	Hosts []core.TractserverID
}

// UpdateStorageClassCommand asks the curator to update the storage class of a
// blob. All tracts of the blob must already be encoded with that storage class
// first, or this will fail. As part of this transaction, information in the
// tracts that apply to other storage classes will be removed. (E.g. changing
// storage class to RS_6_3 will delete replicated host and version for all
// tracts in the blob.)
type UpdateStorageClassCommand struct {
	ID      core.BlobID
	Storage core.StorageClass
}

// CreateTSIDCacheCommand tells the curator to create the TSID cache in the
// database.
type CreateTSIDCacheCommand struct {
}

// cmdToBytes wraps 'cmd' in Command and serializes it into bytes. It dies if it
// fails.
func cmdToBytes(cmd interface{}) []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(Command{Cmd: cmd}); nil != err {
		log.Fatalf("failed to encode command: %s", err)
	}
	return buf.Bytes()
}

// bytesToCmd deserializes a Command from bytes and returns the wrapped
// Command.Cmd. It dies if it fails.
func bytesToCmd(b []byte) interface{} {
	r := bytes.NewReader(b)
	dec := gob.NewDecoder(r)
	var c Command
	if err := dec.Decode(&c); nil != err {
		log.Fatalf("failed to decode command: %s", err)
		return Command{}
	}
	return c.Cmd
}
