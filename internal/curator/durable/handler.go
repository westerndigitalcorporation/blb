// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package durable

import (
	"path/filepath"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/westerndigitalcorporation/blb/internal/core"
	"github.com/westerndigitalcorporation/blb/internal/curator/durable/state"
	pb "github.com/westerndigitalcorporation/blb/internal/curator/durable/state/statepb"
	"github.com/westerndigitalcorporation/blb/pkg/raft/raft"
)

const (
	stateDB    = "state.db"
	stateDBTmp = "state.db.tmp"

	// How many blob keys to return per rpc. Blob keys are 32 bits, so this
	// works out to about 4KB per reply (without compression).
	maxListBlobResults = 1000

	// How many blobs/chunks to read per transaction when iterating.
	blobsPerTxn  = 1000
	chunksPerTxn = 500
)

// StateHandler exports an API that allows clients to easily query and mutate
// the durable state managed by Raft.
//
// All methods which will change the state of the StateHandler must accept a
// term number for leader continuity check. Term number uniquely identified
// a continuous period of leadership, and users can ask StateHandler to propose
// changes if it's still the leader of the given term. If the term is 0, no
// leader continuity check will be performed.
type StateHandler struct {
	// The raft algorithm that manages state.
	raft *raft.Raft

	// Configuration information.
	config *StateConfig

	// This is the lock that prevents all reads to the "State"
	// while recoverying from a Raft snapshot, given the recovery
	// process will delete old state DB file and create a new one
	// from the received snapshot.
	//
	// All read-only transactions have to grab this lock, but write
	// transactions don't have to given they are serialized with the
	// snapshot recovery callback.
	dbLock sync.RWMutex

	// The actual "state machine" that is managed by Raft.
	state *state.State

	// The lock to protect fields below.
	lock sync.Mutex

	// Is this node currently the leader?  Layers above need to know whether or not
	// they should send heartbeats.
	isLeader bool

	// who is the leader? This will be the Raft address of a leader, or an empty
	// string if the leader is unknown.
	leaderID string

	// Current Raft term number.
	term uint64

	// Current membership of the Raft cluster.
	members []string

	// Last computed checksum and the index it was computed at. These fields are
	// only used in Apply, which is called in a single-threaded manner by raft,
	// and so don't need locking.
	checksum      uint64
	checksumIndex uint64
}

// NewStateHandler creates a new StateHandler based on configuration 'cfg' and
// an raft instance 'raft'.
func NewStateHandler(cfg *StateConfig, raft *raft.Raft) *StateHandler {
	return &StateHandler{
		state:    state.Open(filepath.Join(cfg.DBDir, stateDB)),
		raft:     raft,
		config:   cfg,
		isLeader: false,

		checksumIndex: ^uint64(0),
	}
}

// ID returns the Raft ID of the node.
func (h *StateHandler) ID() string {
	h.lock.Lock()
	defer h.lock.Unlock()
	return h.config.ID
}

// LeaderID returns the Raft ID of the leader.
func (h *StateHandler) LeaderID() string {
	h.lock.Lock()
	defer h.lock.Unlock()
	return h.leaderID
}

// AddNode addes a node to the cluster.
func (h *StateHandler) AddNode(node string) error {
	pending := h.raft.AddNode(node)
	<-pending.Done
	return pending.Err
}

// RemoveNode removes a node from the cluster.
func (h *StateHandler) RemoveNode(node string) error {
	pending := h.raft.RemoveNode(node)
	<-pending.Done
	return pending.Err
}

// GetMembership gets the current membership of the cluster.
func (h *StateHandler) GetMembership() []string {
	h.lock.Lock()
	defer h.lock.Unlock()
	members := make([]string, len(h.members))
	copy(members, h.members)
	return members
}

// ProposeInitialMembership proposes an initial membership for the cluster.
func (h *StateHandler) ProposeInitialMembership(members []string) error {
	pending := h.raft.ProposeInitialMembership(members)
	<-pending.Done
	return pending.Err
}

// Start starts the raft instance.
func (h *StateHandler) Start() {
	h.raft.Start(h)
}

// GetClusterMembers return current members in the Raft cluster.
func (h *StateHandler) GetClusterMembers() []string {
	h.lock.Lock()
	defer h.lock.Unlock()
	members := make([]string, len(h.members))
	copy(members, h.members)
	return members
}

// SetLeadershipChange sets the callback that Raft calls when there is a
// leadership change. Must be called before ReplicatedState.Start is invoked.
func (h *StateHandler) SetLeadershipChange(f func(bool)) {
	h.config.OnLeadershipChange = f
}

// IsLeader returns true if this node is the leader currently, false otherwise.
// Note that this state can be stale.
func (h *StateHandler) IsLeader() bool {
	h.lock.Lock()
	defer h.lock.Unlock()
	return h.isLeader
}

// GetTerm returns current Raft term.
func (h *StateHandler) GetTerm() uint64 {
	h.lock.Lock()
	defer h.lock.Unlock()
	return h.term
}

// GetCuratorInfo returns the registration status and owned partitions of this curator. This read
// is processed through the Raft layer as we do not want to doubly register a curator.
//
// If a non-nil error is returned, the Raft algorithm encountered a non-fatal error.  This is normal.
func (h *StateHandler) GetCuratorInfo() (id core.CuratorID, partitions []core.PartitionID, err core.Error) {
	if err = h.readOK(); err != core.NoError {
		return
	}
	id, partitions = h.GetCuratorInfoLocal()
	return
}

// GetCuratorInfoLocal returns the registration status and owned partitions of
// this curator. This read is processed locally without verifying if the state
// is still up-to-date. The read is processed locally because it's used for
// generating status page and we need to get this info on non-leader nodes as
// well.
func (h *StateHandler) GetCuratorInfoLocal() (id core.CuratorID, partitionIDs []core.PartitionID) {
	txn := h.LocalReadOnlyTxn()
	defer txn.Commit()

	id = txn.GetCuratorID()
	partitions := txn.GetPartitions()
	partitionIDs = make([]core.PartitionID, len(partitions))
	for i, p := range partitions {
		partitionIDs[i] = core.PartitionID(p.GetId())
	}
	return
}

// ReadOnlyMode returns the current state of read-only mode.
func (h *StateHandler) ReadOnlyMode() (bool, core.Error) {
	txn, err := h.LinearizableReadOnlyTxn()
	if err != core.NoError {
		return false, err
	}
	defer txn.Commit()
	return txn.GetReadOnlyMode(), core.NoError
}

// SetReadOnlyMode changes the read-only mode in durable state. While set, no
// other commands that modify state will be accepted.
func (h *StateHandler) SetReadOnlyMode(mode bool) core.Error {
	pending := h.raft.Propose(cmdToBytes(SetReadOnlyModeCommand{mode}))
	select {
	case <-time.After(core.ProposalTimeout):
		return core.ErrRaftTimeout
	case <-pending.Done:
	}
	if nil != pending.Err {
		return core.FromRaftError(pending.Err)
	}
	return pending.Res.(core.Error)
}

// Register registers the curator with the Master-provided id 'id'.
//
// If a non-nil error is returned, the Raft algorithm encountered a non-fatal error.  This is normal.
func (h *StateHandler) Register(id core.CuratorID) (core.CuratorID, core.Error) {
	cmd := SetRegistrationCommand{ID: id}
	pending := h.raft.Propose(cmdToBytes(cmd))

	select {
	case <-time.After(core.ProposalTimeout):
		return 0, core.ErrRaftTimeout
	case <-pending.Done:
		// Fall through
	}

	if nil != pending.Err {
		return 0, core.FromRaftError(pending.Err)
	}
	if err, ok := pending.Res.(core.Error); ok {
		return 0, err
	}

	res := pending.Res.(SetRegistrationResult)
	return res.ID, core.NoError
}

// AddPartition adds a partition to the set managed by this curator.
//
// If a non-nil error is returned, the Raft algorithm encountered a non-fatal error.  This is normal.
func (h *StateHandler) AddPartition(id core.PartitionID, term uint64) core.Error {
	pending := h.raft.ProposeIfTerm(cmdToBytes(AddPartitionCommand{id}), term)

	select {
	case <-time.After(core.ProposalTimeout):
		return core.ErrRaftTimeout
	case <-pending.Done:
	}

	if nil != pending.Err {
		return core.FromRaftError(pending.Err)
	}
	if err, ok := pending.Res.(core.Error); ok {
		return err
	}

	res := pending.Res.(AddPartitionResult)
	return res.Err
}

// SyncPartitions syncs this curator's partition assignment with that
// piggybacked in heartbeat reply.
//
// If a non-nil error is returned, the Raft algorithm encountered a non-fatal error.  This is normal.
func (h *StateHandler) SyncPartitions(partitions []core.PartitionID, term uint64) core.Error {
	pending := h.raft.ProposeIfTerm(cmdToBytes(SyncPartitionsCommand{Partitions: partitions}), term)

	select {
	case <-time.After(core.ProposalTimeout):
		return core.ErrRaftTimeout
	case <-pending.Done:
	}

	if nil != pending.Err {
		return core.FromRaftError(pending.Err)
	}
	if err, ok := pending.Res.(core.Error); ok {
		return err
	}

	res := pending.Res.(SyncPartitionsResult)
	return res.Err
}

// CreateBlob creates a blob and returns its ID, or an error.
//
// If there are no partitions available to create a blob in, core.ErrGenBlobID will be returned.
//
// Returns core.NoError on success, another core.Error otherwise (including expected Raft errors).
func (h *StateHandler) CreateBlob(repl int, now, expires int64, hint core.StorageHint, term uint64) (core.BlobID, core.Error) {
	pending := h.raft.ProposeIfTerm(cmdToBytes(CreateBlobCommand{repl, now, expires, hint}), term)

	select {
	case <-time.After(core.ProposalTimeout):
		return core.BlobID(0), core.ErrRaftTimeout
	case <-pending.Done:
		// Fall through
	}

	if nil != pending.Err {
		return core.BlobID(0), core.FromRaftError(pending.Err)
	}
	if err, ok := pending.Res.(core.Error); ok {
		return 0, err
	}

	res := pending.Res.(CreateBlobResult)
	return res.ID, res.Err
}

// ExtendBlob extends a blob.
//
// Returns core.NoError on success, another core.Error otherwise (including expected Raft errors).
func (h *StateHandler) ExtendBlob(id core.BlobID, firstTractKey core.TractKey, hosts [][]core.TractserverID) (int, core.Error) {
	pending := h.raft.Propose(cmdToBytes(ExtendBlobCommand{id, firstTractKey, hosts}))

	select {
	case <-time.After(core.ProposalTimeout):
		return 0, core.ErrRaftTimeout
	case <-pending.Done:
		// Fall through
	}

	if nil != pending.Err {
		return 0, core.FromRaftError(pending.Err)
	}
	if err, ok := pending.Res.(core.Error); ok {
		return 0, err
	}

	res := pending.Res.(ExtendBlobResult)
	return res.NewSize, res.Err
}

// DeleteBlob deletes a blob.
//
// Returns information about the just-deleted blob so the caller could clean up the tracts.
// Returns core.NoError on success, another core.Error otherwise (including expected Raft errors).
func (h *StateHandler) DeleteBlob(id core.BlobID, when time.Time, term uint64) core.Error {
	pending := h.raft.ProposeIfTerm(cmdToBytes(DeleteBlobCommand{id, when}), term)

	select {
	case <-time.After(core.ProposalTimeout):
		return core.ErrRaftTimeout
	case <-pending.Done:
		// Fall through
	}

	if nil != pending.Err {
		return core.FromRaftError(pending.Err)
	}
	if err, ok := pending.Res.(core.Error); ok {
		return err
	}

	res := pending.Res.(DeleteBlobResult)
	return res.Err
}

// UndeleteBlob un-deletes a blob.
//
// If the blob could be un-deleted, returns core.NoError.  Otherwise, returns another blb error.
func (h *StateHandler) UndeleteBlob(id core.BlobID, term uint64) core.Error {
	pending := h.raft.ProposeIfTerm(cmdToBytes(UndeleteBlobCommand{id}), term)

	select {
	case <-time.After(core.ProposalTimeout):
		return core.ErrRaftTimeout
	case <-pending.Done:
		// Fall through
	}

	if nil != pending.Err {
		return core.FromRaftError(pending.Err)
	}
	if err, ok := pending.Res.(core.Error); ok {
		return err
	}

	res := pending.Res.(UndeleteBlobResult)
	return res.Err
}

// FinishDelete permanently deletes blobs from the database.
func (h *StateHandler) FinishDelete(blobs []core.BlobID) core.Error {
	pending := h.raft.Propose(cmdToBytes(FinishDeleteCommand{blobs}))
	select {
	case <-time.After(core.ProposalTimeout):
		return core.ErrRaftTimeout
	case <-pending.Done:
	}
	if nil != pending.Err {
		return core.FromRaftError(pending.Err)
	}
	return pending.Res.(core.Error)
}

// SetMetadata changes metadata for a blob.
func (h *StateHandler) SetMetadata(id core.BlobID, md core.BlobInfo) core.Error {
	pending := h.raft.Propose(cmdToBytes(SetMetadataCommand{id, md}))
	select {
	case <-time.After(core.ProposalTimeout):
		return core.ErrRaftTimeout
	case <-pending.Done:
	}
	if nil != pending.Err {
		return core.FromRaftError(pending.Err)
	}
	return pending.Res.(core.Error)
}

// GetTracts returns information about tracts in a blob.
func (h *StateHandler) GetTracts(id core.BlobID, start, end int) ([]core.TractInfo, core.StorageClass, core.Error) {
	txn, err := h.LinearizableReadOnlyTxn()
	if err != core.NoError {
		return nil, core.StorageClass_REPLICATED, err
	}
	defer txn.Commit()
	return txn.GetTracts(id, start, end)
}

// ChangeTract changes the repl group of a tract.
func (h *StateHandler) ChangeTract(id core.TractID, newVersion int, hosts []core.TractserverID, term uint64) (core.TractInfo, core.Error) {
	pending := h.raft.ProposeIfTerm(cmdToBytes(ChangeTractCommand{id, newVersion, hosts}), term)

	select {
	case <-time.After(core.ProposalTimeout):
		return core.TractInfo{}, core.ErrRaftTimeout
	case <-pending.Done:
		// Fall through
	}

	if pending.Err != nil {
		return core.TractInfo{}, core.FromRaftError(pending.Err)
	}
	if err, ok := pending.Res.(core.Error); ok {
		return core.TractInfo{}, err
	}

	res := pending.Res.(ChangeTractResult)
	// If the command was actually applied it shouldn't error.  If it does there's a programming
	// error and we should abort.
	if res.Err != core.NoError {
		log.Fatalf("change tract command applied unsuccessfully: %+v", res)
	}

	return res.Info, res.Err
}

// UpdateTimes applies a batch of time updates to the state.
func (h *StateHandler) UpdateTimes(updates []state.UpdateTime) core.Error {
	pending := h.raft.Propose(cmdToBytes(UpdateTimesCommand{updates}))
	select {
	case <-time.After(core.ProposalTimeout):
		return core.ErrRaftTimeout
	case <-pending.Done:
	}
	if pending.Err != nil {
		return core.FromRaftError(pending.Err)
	}
	return pending.Res.(core.Error)
}

// Stat stats a blob.
func (h *StateHandler) Stat(id core.BlobID) (core.BlobInfo, core.Error) {
	txn, err := h.LinearizableReadOnlyTxn()
	if err != core.NoError {
		return core.BlobInfo{}, err
	}
	defer txn.Commit()
	return txn.Stat(id)
}

// GetRSChunk looks up one RSChunk.
func (h *StateHandler) GetRSChunk(id core.RSChunkID) *pb.RSChunk {
	txn, err := h.LinearizableReadOnlyTxn()
	if err != core.NoError {
		return nil
	}
	defer txn.Commit()
	return txn.GetRSChunk(id)
}

// ListBlobs returns a range of existing blob keys in one partition.
func (h *StateHandler) ListBlobs(partition core.PartitionID, start core.BlobKey) (keys []core.BlobKey, err core.Error) {
	// TODO: we probably don't need to do linearizable read here
	txn, err := h.LinearizableReadOnlyTxn()
	if err != core.NoError {
		return nil, err
	}
	defer txn.Commit()

	iter, has := txn.GetIterator(core.BlobIDFromParts(partition, start))
	for has && len(keys) < maxListBlobResults {
		id, blob := iter.Blob()
		if id.Partition() != partition {
			// we're at the end of this partition
			break
		}
		if blob.GetDeleted() == 0 { // ignore deleted blobs
			keys = append(keys, id.ID())
		}
		has = iter.Next()
	}
	return
}

// AllocateRSChunkIDs allocates a contiguous range of n RSChunkIDs.
//
// If there are no partitions available to allocate ids in, core.ErrGenBlobID will be returned.
//
// Returns core.NoError on success, another core.Error otherwise (including expected Raft errors).
func (h *StateHandler) AllocateRSChunkIDs(n int, term uint64) (core.RSChunkID, core.Error) {
	pending := h.raft.ProposeIfTerm(cmdToBytes(AllocateRSChunkIDsCommand{n}), term)
	select {
	case <-time.After(core.ProposalTimeout):
		return core.RSChunkID{}, core.ErrRaftTimeout
	case <-pending.Done:
	}
	if pending.Err != nil {
		return core.RSChunkID{}, core.FromRaftError(pending.Err)
	}
	if err, ok := pending.Res.(core.Error); ok {
		return core.RSChunkID{}, err
	}
	res := pending.Res.(AllocateRSChunkIDsResult)
	return res.ID, res.Err
}

// CommitRSChunk records a newly-encoded RSChunk and updates all the tracts that
// are included in the chunk to point to it.
func (h *StateHandler) CommitRSChunk(id core.RSChunkID, cls core.StorageClass,
	hosts []core.TractserverID, data [][]state.EncodedTract, term uint64) core.Error {

	pending := h.raft.ProposeIfTerm(cmdToBytes(CommitRSChunkCommand{id, cls, hosts, data}), term)
	select {
	case <-time.After(core.ProposalTimeout):
		return core.ErrRaftTimeout
	case <-pending.Done:
	}
	if pending.Err != nil {
		return core.FromRaftError(pending.Err)
	}
	return pending.Res.(core.Error)
}

// UpdateRSHosts updates the record of hosts in an RS chunk.
func (h *StateHandler) UpdateRSHosts(id core.RSChunkID, hosts []core.TractserverID, term uint64) core.Error {
	pending := h.raft.ProposeIfTerm(cmdToBytes(UpdateRSHostsCommand{id, hosts}), term)
	select {
	case <-time.After(core.ProposalTimeout):
		return core.ErrRaftTimeout
	case <-pending.Done:
	}
	if pending.Err != nil {
		return core.FromRaftError(pending.Err)
	}
	return pending.Res.(core.Error)
}

// UpdateStorageClass changes the storage class of a blob and removes metadata
// related to other storage classes.
func (h *StateHandler) UpdateStorageClass(id core.BlobID, target core.StorageClass, term uint64) core.Error {
	pending := h.raft.ProposeIfTerm(cmdToBytes(UpdateStorageClassCommand{id, target}), term)
	select {
	case <-time.After(core.ProposalTimeout):
		return core.ErrRaftTimeout
	case <-pending.Done:
	}
	if pending.Err != nil {
		return core.FromRaftError(pending.Err)
	}
	return pending.Res.(core.Error)
}

// CreateTSIDCache creates the TSID cache in the state database.
func (h *StateHandler) CreateTSIDCache() core.Error {
	pending := h.raft.Propose(cmdToBytes(CreateTSIDCacheCommand{}))
	select {
	case <-time.After(core.ProposalTimeout):
		return core.ErrRaftTimeout
	case <-pending.Done:
	}
	if pending.Err != nil {
		return core.FromRaftError(pending.Err)
	}
	return pending.Res.(core.Error)
}

// GetKnownTSIDs returns all the known tractserver IDs in the database.
func (h *StateHandler) GetKnownTSIDs() ([]core.TractserverID, core.Error) {
	txn := h.LocalReadOnlyTxn()
	defer txn.Commit()
	return txn.GetKnownTSIDs()
}

// ForEachBlob calls the given function for each blob in the database.
// If includeDeleted is true, deleted blobs will be included, otherwise they'll
// be skipped.
// The iteration will be split scross multiple transactions so that we don't
// hold a single transaction open for too long. That means the caller might not
// get a consistent view of the state! Since we're giving up on consistency
// anyway, this function does not do a read-verify.
// The function will be called serially so it does not have to serialize data
// structure access. The arguments passed to the function are not allowed to be
// modified.
// The inBetween function, if given, will be called in between each transaction.
// This may be used to do some work set up by the iteration function, without
// holding a transaction open. If the inBetween function returns false,
// iteration is aborted.
func (h *StateHandler) ForEachBlob(
	includeDeleted bool,
	bfunc func(core.BlobID, *pb.Blob),
	inBetween func() bool) {

	var nextBlob core.BlobID
	for done := false; !done; {
		txn := h.LocalReadOnlyTxn()
		iter, has := txn.GetIterator(nextBlob)
		for n := blobsPerTxn; has && n > 0; n-- {
			id, blob := iter.Blob()
			if includeDeleted || blob.GetDeleted() == 0 {
				bfunc(id, blob)
			}
			nextBlob = id.Next()
			has = iter.Next()
		}
		done = !has
		txn.Commit()

		if inBetween != nil && !inBetween() {
			break
		}
	}
}

// ForEachTract calls the given function for each tract in non-deleted blobs.
// inBetween is as in ForEachBlob.
func (h *StateHandler) ForEachTract(tfunc func(core.TractID, *pb.Tract), inBetween func() bool) {
	h.ForEachBlob(false, func(id core.BlobID, blob *pb.Blob) {
		for i, tract := range blob.Tracts {
			tfunc(core.TractID{Blob: id, Index: core.TractKey(i)}, tract)
		}
	}, inBetween)
}

// ForEachRSChunk calls the given function for each RSChunk in the database.
// inBetween is as in ForEachBlob.
func (h *StateHandler) ForEachRSChunk(cfunc func(core.RSChunkID, *pb.RSChunk), inBetween func() bool) {
	var nextChunk core.RSChunkID
	for done := false; !done; {
		txn := h.LocalReadOnlyTxn()
		iter, has := txn.GetRSChunkIterator(nextChunk)
		for n := chunksPerTxn; has && n > 0; n-- {
			id, c := iter.RSChunk()
			cfunc(id, c)
			nextChunk = id.Next()
			has = iter.Next()
		}
		done = !has
		txn.Commit()

		if inBetween != nil && !inBetween() {
			break
		}
	}
}

// LinearizableReadOnlyTxn returns a read-only transaction that access a
// snapshot of curator's current state. The transaction can only be used for
// read access, write access must be initiated by Raft.
//
// NOTE: please release the transaction as soon as you are done with it. The
// read only transaction will capture a point-in-time snapshot of the state
// and data in the state can not be GC-ed until you release the transaction.
func (h *StateHandler) LinearizableReadOnlyTxn() (*ReadOnlyTxn, core.Error) {
	if err := h.readOK(); err != core.NoError {
		return nil, err
	}
	return h.LocalReadOnlyTxn(), core.NoError
}

// LocalReadOnlyTxn returns a read-only transaction without verifying
// linearizability.
//
// The transaction must be released as soon as you are done with it.
func (h *StateHandler) LocalReadOnlyTxn() *ReadOnlyTxn {
	h.dbLock.RLock()
	return &ReadOnlyTxn{Txn: h.state.ReadOnlyTxn(), dbLock: &h.dbLock}
}

// GetFreeSpace returns the number of blobs can be created in the existing
// partitions. Error is returned if raft complaints.
func (h *StateHandler) GetFreeSpace() (uint64, core.Error) {
	if err := h.readOK(); err != core.NoError {
		return 0, err
	}
	res := h.GetFreeSpaceLocal()
	return res, core.NoError
}

// GetFreeSpaceLocal returns the number of blobs can be created in the existing
// partitions by reading local state. We use it to generate status pages for
// non-leader node.
func (h *StateHandler) GetFreeSpaceLocal() uint64 {
	txn := h.LocalReadOnlyTxn()
	defer txn.Commit()

	var free uint64
	for _, p := range txn.GetPartitions() {
		free += core.MaxBlobKey - uint64(p.GetNextBlobKey())
	}
	return free
}

// CheckForGarbage looks at all tracts in 'tracts', and verifies that 'tsid' is supposed to be hosting
// that tract.  If the tractserver isn't supposed to host the tract, returns a core.TractState that
// indicates what version(s) of that tract can be safely garbage collected by any tractserver.
func (h *StateHandler) CheckForGarbage(tsid core.TractserverID, tracts []core.TractID) ([]core.TractState, []core.TractID) {
	txn, err := h.LinearizableReadOnlyTxn()
	if err != core.NoError {
		return nil, nil
	}

	defer txn.Commit()

	var gone []core.TractID
	var old []core.TractState

	for _, id := range tracts {
		if id.IsRegular() {
			// Look up the blob.  We don't want to GC blobs that are still in metadata
			// for undelete purposes, so we use getBlobAll instead of getBlob.
			blob := txn.GetBlobAll(id.Blob)

			// Blob is deleted or never existed, so the tractserver can remove the tract.
			if blob == nil {
				gone = append(gone, id)
				continue
			}

			// The blob exists, but this tract doesn't exist in the blob.
			//
			// We're possibly creating it right now, or the tract only exists because of a failed create attempt.
			// Either way, we don't GC it -- it's simpler to assume it's being actively created, and if it was a
			// failed create attempt, we assume the attempt will eventually succeed.
			if id.Index >= core.TractKey(len(blob.Tracts)) {
				continue
			}

			// See if the set of hosts includes the host that reports having a copy of the tract.
			t := blob.Tracts[id.Index]
			contains := false
			for i := range t.Hosts {
				if t.Hosts[i] == tsid {
					contains = true
					break
				}
			}

			// If not, we can tell the host to GC it.
			if !contains {
				// If the recipient of this has a higher version, it will ignore this message.
				//
				// If it has an equal version, we know for sure it shouldn't be hosting the tract -- we
				// just examined the replicas of this version.
				//
				// If it has a lower version, it can delete the tract.
				old = append(old, core.TractState{ID: id, Version: t.Version})
			}
		} else if id.IsRS() {
			// Look up the RS chunk piece.
			host, ok := txn.LookupRSPiece(id)

			// If this doesn't exist anywhere in our database, it shouldn't be
			// on the tractserver. And if the database says it should be on
			// another host, then we don't need it on this one.
			// Note: pieces that are currently in-progress are filtered out at
			// the layer above.
			if !ok || host != tsid {
				gone = append(gone, id)
			}
		}
	}
	return old, gone
}

// ConsistencyCheck runs one round of consistency checking. Curators will
// compute a checksum of their state starting with `start` and containing n
// blobs (among other data). If successful, this will return the position that
// the next round should start at, and NoError. Otherwise it will return
// `start` and an error.
func (h *StateHandler) ConsistencyCheck(start state.ChecksumPosition, n int) (state.ChecksumPosition, core.Error) {
	pending := h.raft.Propose(cmdToBytes(ChecksumCommand{
		Start: start,
		N:     n,
	}))
	select {
	case <-time.After(core.ProposalTimeout):
		return start, core.ErrRaftTimeout
	case <-pending.Done: // fallthrough
	}
	if nil != pending.Err {
		return start, core.FromRaftError(pending.Err)
	}

	res := pending.Res.(ChecksumResult)
	pending = h.raft.Propose(cmdToBytes(VerifyChecksumCommand{
		Index:    res.Index,
		Checksum: res.Checksum,
	}))
	select {
	case <-time.After(core.ProposalTimeout):
		return start, core.ErrRaftTimeout
	case <-pending.Done: // fallthrough
	}
	return res.Next, core.FromRaftError(pending.Err)
}

//
// Internals below
//

// readOK verifies with the raft layer that it is OK to read our local state and still
// provide linearizable semantics.  If we can do this, returns core.NoError.  Otherwise
// returns another core.Error.
func (h *StateHandler) readOK() core.Error {
	pending := h.raft.VerifyRead()

	select {
	case <-time.After(core.ProposalTimeout):
		return core.ErrRaftTimeout
	case <-pending.Done:
		if nil != pending.Err {
			return core.FromRaftError(pending.Err)
		}
		return core.NoError
	}
}

// ReadOnlyTxn represents a read-only transaction.
type ReadOnlyTxn struct {
	*state.Txn
	dbLock *sync.RWMutex
}

// Commit the read-only transaction. It MUST be called when you are done with
// the transaction.
func (t *ReadOnlyTxn) Commit() {
	t.Txn.Commit()
	// Release the read lock as well so that a writer could prevent all read
	// accesses to the state after grabbing the write lock.
	t.dbLock.RUnlock()
}
