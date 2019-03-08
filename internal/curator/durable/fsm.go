// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package durable

import (
	log "github.com/golang/glog"
	"github.com/westerndigitalcorporation/blb/internal/core"
	"github.com/westerndigitalcorporation/blb/internal/curator/durable/state"
	"github.com/westerndigitalcorporation/blb/internal/curator/durable/state/fb"
	"github.com/westerndigitalcorporation/blb/pkg/raft/raft"
)

//-------------------- raft.FSM implementation --------------------//

// Apply allows us to implement the raft.FSM interface.
// Note that raft.FSM will never issue two Apply calls in parallel.
func (h *StateHandler) Apply(ent raft.Entry) interface{} {
	// Get last applied Raft index to state.
	// TODO: we should cache this instead of fetching from disk everytime.
	rtx := h.LocalReadOnlyTxn()
	txnIndex := rtx.GetIndex()

	if ent.Index <= txnIndex {
		// The entry has already been applied to "State". This means the Raft
		// node is in recovery and reapplying committed entries in Raft log.
		// Since some of entries might already been persisted by DB we'll just
		// skip reapplying these entries.
		rtx.Commit()
		return nil
	}

	// Deserialize the command.
	cmd := bytesToCmd(ent.Cmd)

	// Checksum commands only need a read-only transaction.
	switch c := cmd.(type) {
	case ChecksumCommand:
		res := h.checksumRequest(c, rtx, ent.Index)
		rtx.Commit()
		return res
	case VerifyChecksumCommand:
		rtx.Commit()
		h.verifyChecksum(c)
		return nil
	}

	rtx.Commit()

	// Commands here are supposed to mutate state so use write transaction.
	// We need to attach the Raft index to every transaction application so that
	// we can know if the on-disk DB is staler than the raft snapshot by comparing
	// the Raft indices.
	txn := h.state.WriteTxn(ent.Index)
	defer txn.Commit()

	// First handle the command to change the read-only state itself.
	switch c := cmd.(type) {
	case SetReadOnlyModeCommand:
		txn.SetReadOnlyMode(c.ReadOnly)
		return core.NoError
	}

	// If we are in read-only mode, we can't proceed.
	if txn.GetReadOnlyMode() {
		return core.ErrReadOnlyMode
	}

	// Apply the command.
	switch c := cmd.(type) {
	case SetRegistrationCommand:
		return c.apply(txn)
	case AddPartitionCommand:
		return c.apply(txn)
	case SyncPartitionsCommand:
		return c.apply(txn)
	case CreateBlobCommand:
		return c.apply(txn)
	case DeleteBlobCommand:
		return c.apply(txn)
	case FinishDeleteCommand:
		return c.apply(txn)
	case UndeleteBlobCommand:
		return c.apply(txn)
	case SetMetadataCommand:
		return c.apply(txn)
	case ExtendBlobCommand:
		return c.apply(txn)
	case ChangeTractCommand:
		return c.apply(txn)
	case UpdateTimesCommand:
		return c.apply(txn)
	case AllocateRSChunkIDsCommand:
		return c.apply(txn)
	case CommitRSChunkCommand:
		return c.apply(txn)
	case UpdateRSHostsCommand:
		return c.apply(txn)
	case UpdateStorageClassCommand:
		return c.apply(txn)
	}

	log.Fatalf("applying unknown command %v", cmd)
	return nil
}

// OnLeadershipChange is called when our leadership status changes.
func (h *StateHandler) OnLeadershipChange(val bool, term uint64, leader string) {
	// There's only one goroutine performs the update on "isLeader", but there
	// might be other goroutines perform read on it at the same time, we still
	// have to grab the write lock.
	h.lock.Lock()
	h.isLeader = val
	h.term = term
	h.leaderID = leader
	h.lock.Unlock()
	h.config.OnLeadershipChange(val)
}

// OnMembershipChange is called to notify current members in a Raft cluster.
func (h *StateHandler) OnMembershipChange(membership raft.Membership) {
	h.lock.Lock()
	defer h.lock.Unlock()
	log.Infof("OnMembershipChange: %+v", membership)
	h.members = membership.Members
}

func addPartition(id core.PartitionID, txn *state.Txn) core.Error {
	if p := txn.GetPartition(id); p != nil {
		// Don't add a partition twice, but let the layer above choose how to error.
		return core.ErrAlreadyExists
	}
	txn.PutPartition(id, &fb.Partition{
		NextBlobKey:    1,
		NextRsChunkKey: 1,
	})
	return core.NoError
}

func (h *StateHandler) checksumRequest(c ChecksumCommand, rtx *ReadOnlyTxn, index uint64) ChecksumResult {
	cksum, next := rtx.Checksum(c.Start, c.N)
	h.checksum = cksum
	h.checksumIndex = index
	return ChecksumResult{next, cksum, index}
}

func (h *StateHandler) verifyChecksum(c VerifyChecksumCommand) {
	if h.checksumIndex != c.Index {
		// This may happen around node restarts or leader elections or membership changes.
		// It's harmless.
		log.Infof("got checksum for wrong index: %v != %v", h.checksumIndex, c.Index)
	} else if h.checksum != c.Checksum {
		log.Fatalf("@@@ consistency check failed! %v != %v", h.checksum, c.Checksum)
	} else {
		log.V(1).Infof("@@@ consistency check passed (idx %v, ck %v)", h.checksumIndex, h.checksum)
	}
}

//-------------------- Internal implementation of StateHandler.Apply --------------------//

// Sets the curator's registration to the information provided in the command.
func (cmd SetRegistrationCommand) apply(txn *state.Txn) SetRegistrationResult {
	if !txn.GetCuratorID().IsValid() {
		txn.SetCuratorID(cmd.ID)
	}
	return SetRegistrationResult{ID: txn.GetCuratorID()}
}

// Adds the partition to our set of managed partitions.
func (cmd AddPartitionCommand) apply(txn *state.Txn) AddPartitionResult {
	return AddPartitionResult{Err: addPartition(cmd.ID, txn)}
}

// Adds any partitions in 'cmd' that we don't have to our partition list.
func (cmd SyncPartitionsCommand) apply(txn *state.Txn) SyncPartitionsResult {
	for _, id := range cmd.Partitions {
		// We purposefully ignore the errors -- duplicates here are expected.
		addPartition(id, txn)
	}
	return SyncPartitionsResult{Err: core.NoError}
}

// Creates a new blob in a partition that has space for it and returns it.
func (cmd CreateBlobCommand) apply(txn *state.Txn) CreateBlobResult {
	// Find the non-full partition with the lowest id to create the blob in.
	var partition state.PartitionAndID
	for _, p := range txn.GetPartitions() {
		if p.P.NextBlobKey() != core.MaxBlobKey {
			partition = p
			break
		}
	}
	if partition.ID == 0 {
		// This is not a fatal error. The curator could/should ask for another partition
		// if memory permits.
		return CreateBlobResult{Err: core.ErrGenBlobID}
	}

	// Update the partition's durable state...
	newPart := partition.P.ToStruct()
	key := newPart.NextBlobKey
	newPart.NextBlobKey++
	txn.PutPartition(partition.ID, newPart)

	// Create the blob.
	id := core.BlobIDFromParts(partition.ID, key)
	blob := fb.Blob{
		Hint:    cmd.Hint,
		Repl:    cmd.Repl,
		Mtime:   cmd.InitialTime,
		Atime:   cmd.InitialTime,
		Expires: cmd.Expires,
		// Storage defaults to core.StorageClassREPLICATED, so leave it out here
	}
	txn.PutBlob(id, &blob)
	return CreateBlobResult{ID: id, Err: core.NoError}
}

// Marks a blob as deleted in the database, but without removing it.
func (cmd DeleteBlobCommand) apply(txn *state.Txn) DeleteBlobResult {
	return DeleteBlobResult{Err: txn.DeleteBlob(cmd.ID, cmd.When)}
}

// Undeletes a blob that was deleted, or does nothing if the blob wasn't deleted.
// Errors if there isn't a blob with that ID.
func (cmd UndeleteBlobCommand) apply(txn *state.Txn) UndeleteBlobResult {
	return UndeleteBlobResult{Err: txn.UndeleteBlob(cmd.ID)}
}

// Finalizes the deletion of a blob, deleting it permanently.
func (cmd FinishDeleteCommand) apply(txn *state.Txn) core.Error {
	return txn.FinishDeleteBlobs(cmd.Blobs)
}

// Changes metadata for a blob.
func (cmd SetMetadataCommand) apply(txn *state.Txn) core.Error {
	return txn.SetBlobMetadata(cmd.ID, cmd.Metadata)
}

// Appends tracts to the blob.
func (cmd ExtendBlobCommand) apply(txn *state.Txn) ExtendBlobResult {
	// Make sure the blob exists.
	blob := txn.GetBlob(cmd.ID)
	if blob == nil {
		return ExtendBlobResult{Err: core.ErrNoSuchBlob}
	}

	// The tract key should match the tract count.
	if int(cmd.FirstTractKey) != blob.TractsLength() {
		return ExtendBlobResult{Err: core.ErrExtendConflict}
	}

	// Make sure the user paid attention to the replication factor.
	for _, h := range cmd.Hosts {
		if len(h) != blob.Repl() {
			return ExtendBlobResult{Err: core.ErrInvalidArgument}
		}
	}

	// Add the tracts.
	bs := blob.ToStruct()
	for _, hosts := range cmd.Hosts {
		// Add one tract to the bs.
		bs.Tracts = append(bs.Tracts, &fb.Tract{
			Hosts:   hosts,
			Version: 1,
		})
	}

	// Put the modified metadata back.
	txn.PutBlob(cmd.ID, bs)

	return ExtendBlobResult{Err: core.NoError, NewSize: len(bs.Tracts)}
}

// Change the repl group of a tract. Tract versions can only increase by one from their current
// version to allow only one successful request to increase the version number per version number.
func (cmd ChangeTractCommand) apply(txn *state.Txn) core.Error {
	return txn.ChangeTract(cmd.ID, cmd.NewVersion, cmd.NewHosts)
}

// Update mtime/atime for a batch of blobs.
func (cmd UpdateTimesCommand) apply(txn *state.Txn) core.Error {
	txn.BatchUpdateTimes(cmd.Updates)
	return core.NoError
}

// Allocate a range of RSChunkIDs.
func (cmd AllocateRSChunkIDsCommand) apply(txn *state.Txn) AllocateRSChunkIDsResult {
	var partition state.PartitionAndID
	for _, p := range txn.GetPartitions() {
		if p.P.NextRsChunkKey()+uint64(cmd.N) <= core.MaxRSChunkKey {
			partition = p
			break
		}
	}
	if partition.ID == 0 {
		// The curator could/should ask for another partition if memory permits.
		return AllocateRSChunkIDsResult{Err: core.ErrGenBlobID}
	}

	newPart := partition.P.ToStruct()
	key := newPart.NextRsChunkKey
	newPart.NextRsChunkKey += uint64(cmd.N)
	txn.PutPartition(partition.ID, newPart)

	return AllocateRSChunkIDsResult{
		ID: core.RSChunkID{Partition: core.PartitionID(core.RSPartition<<30) | partition.ID, ID: key},
	}
}

func (cmd CommitRSChunkCommand) apply(txn *state.Txn) core.Error {
	return txn.PutRSChunk(cmd.ID, cmd.Storage, cmd.Hosts, cmd.Data)
}

func (cmd UpdateRSHostsCommand) apply(txn *state.Txn) core.Error {
	return txn.UpdateRSHosts(cmd.ID, cmd.Hosts)
}

func (cmd UpdateStorageClassCommand) apply(txn *state.Txn) core.Error {
	return txn.UpdateStorageClass(cmd.ID, cmd.Storage)
}
