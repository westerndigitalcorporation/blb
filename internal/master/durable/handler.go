// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package durable

import (
	"sync"
	"time"

	"github.com/westerndigitalcorporation/blb/internal/core"
	"github.com/westerndigitalcorporation/blb/pkg/raft/raft"
)

// StateHandler exports an API that allows clients to easily query and mutate
// the durable state managed by Raft.
type StateHandler struct {
	// Configuration of the handler.
	config *StateConfig

	raft *raft.Raft

	// The lock to protect fields below.
	lock sync.Mutex

	// Current Raft term.
	term uint64

	// Durable state managed by Raft.
	state *State

	// Leader indicator. Set by Raft.
	isLeader bool

	// Who is the leader? It will be the Raft address of the leader or an empty
	// string is the leader is unknown.
	leaderID string

	// Current members of the Raft cluster.
	members []string

	// Saved checksum
	checksum      uint32
	checksumIndex uint64
}

// String returns a string representation of the current sate

// NewStateHandler creates a handler for manipulating persistent state.
func NewStateHandler(cfg *StateConfig, raft *raft.Raft) *StateHandler {
	return &StateHandler{
		state:  newState(),
		config: cfg,
		raft:   raft,

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

// Start starts the Raft instance.
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

// SetOnLeader sets the callback that Raft calls when it becomes the leader.
// Must be called before StateHandler.Start is invoked.
func (h *StateHandler) SetOnLeader(f func()) {
	h.config.OnLeader = f
}

// IsLeader returns whether it is the leader in the replication group.
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

// ReadOnlyMode gets the current state of read-only mode.
func (h *StateHandler) ReadOnlyMode() (bool, core.Error) {
	pending := h.raft.VerifyRead()
	select {
	case <-time.After(core.ProposalTimeout):
		return false, core.ErrRaftTimeout
	case <-pending.Done:
	}
	if pending.Err != nil {
		return false, core.FromRaftError(pending.Err)
	}
	h.lock.Lock()
	defer h.lock.Unlock()
	return h.state.ReadOnly, core.NoError
}

// SetReadOnlyMode changes the read-only mode of the master.
func (h *StateHandler) SetReadOnlyMode(mode bool) core.Error {
	pending := h.raft.Propose(cmdToBytes(SetReadOnlyModeCmd{mode}))
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

// RegisterCurator registers a new curator. A curator ID is generated and persisted.
func (h *StateHandler) RegisterCurator(term uint64) (core.CuratorID, core.Error) {
	pending := h.raft.ProposeIfTerm(cmdToBytes(RegisterCuratorCmd{}), term)
	select {
	case <-time.After(core.ProposalTimeout):
		return 0, core.ErrRaftTimeout

	case <-pending.Done:
		// fallthrough
	}

	if nil != pending.Err {
		return 0, core.FromRaftError(pending.Err)
	}
	if err, ok := pending.Res.(core.Error); ok {
		return 0, err
	}
	return pending.Res.(core.CuratorID), core.NoError
}

// NewPartition assigns a new partition to a curator.
func (h *StateHandler) NewPartition(curatorID core.CuratorID, term uint64) (core.PartitionID, core.Error) {
	pending := h.raft.ProposeIfTerm(cmdToBytes(NewPartitionCmd{CuratorID: curatorID}), term)
	select {
	case <-time.After(core.ProposalTimeout):
		return core.PartitionID(0), core.ErrRaftTimeout

	case <-pending.Done:
		// fallthrough
	}

	if nil != pending.Err {
		return core.PartitionID(0), core.FromRaftError(pending.Err)
	}
	if err, ok := pending.Res.(core.Error); ok {
		return 0, err
	}
	res := pending.Res.(NewPartitionRes)
	return res.PartitionID, res.Err
}

// GetPartitions returns the partition assignment for the given curator, or zero for all.
func (h *StateHandler) GetPartitions(curatorID core.CuratorID) ([]core.PartitionID, core.Error) {
	pending := h.raft.VerifyRead()
	select {
	case <-time.After(core.ProposalTimeout):
		return nil, core.ErrRaftTimeout

	case <-pending.Done:
		// fallthrough
	}

	if nil != pending.Err {
		return nil, core.FromRaftError(pending.Err)
	}

	h.lock.Lock()
	defer h.lock.Unlock()

	return h.state.getPartitions(curatorID), core.NoError
}

// Lookup returns the ID of the curator replication group that is responsible
// for a partition.
func (h *StateHandler) Lookup(id core.PartitionID) (core.CuratorID, core.Error) {
	pending := h.raft.VerifyRead()
	select {
	case <-time.After(core.ProposalTimeout):
		return 0, core.ErrRaftTimeout

	case <-pending.Done:
		// fallthrough
	}

	if nil != pending.Err {
		return 0, core.FromRaftError(pending.Err)
	}

	h.lock.Lock()
	defer h.lock.Unlock()

	return h.state.lookup(id)
}

// ValidateCuratorID returns core.NoError if the given curator id is valid.
func (h *StateHandler) ValidateCuratorID(curatorID core.CuratorID) core.Error {
	pending := h.raft.VerifyRead()
	select {
	case <-time.After(core.ProposalTimeout):
		return core.ErrRaftTimeout

	case <-pending.Done:
		// fallthrough
	}

	if nil != pending.Err {
		return core.FromRaftError(pending.Err)
	}

	h.lock.Lock()
	defer h.lock.Unlock()

	return h.state.verifyCuratorID(curatorID)
}

// RegisterTractserver registers a new tractserver. A tractserver ID is
// generated and persisted.
func (h *StateHandler) RegisterTractserver(term uint64) (core.TractserverID, core.Error) {
	pending := h.raft.ProposeIfTerm(cmdToBytes(RegisterTractserverCmd{}), term)
	select {
	case <-time.After(core.ProposalTimeout):
		return 0, core.ErrRaftTimeout

	case <-pending.Done:
		// fallthrough
	}

	if nil != pending.Err {
		return 0, core.FromRaftError(pending.Err)
	}
	if err, ok := pending.Res.(core.Error); ok {
		return 0, err
	}
	return pending.Res.(core.TractserverID), core.NoError
}

// ConsistencyCheck runs one round of consistency checking.
func (h *StateHandler) ConsistencyCheck() core.Error {
	pending := h.raft.Propose(cmdToBytes(ChecksumRequestCmd{}))
	select {
	case <-time.After(core.ProposalTimeout):
		return core.ErrRaftTimeout
	case <-pending.Done: // fallthrough
	}
	if nil != pending.Err {
		return core.FromRaftError(pending.Err)
	}
	res := pending.Res.(ChecksumRes)
	pending = h.raft.Propose(cmdToBytes(ChecksumVerifyCmd{
		Index:    res.Index,
		Checksum: res.Checksum,
	}))
	select {
	case <-time.After(core.ProposalTimeout):
		return core.ErrRaftTimeout
	case <-pending.Done: // fallthrough
	}
	return core.FromRaftError(pending.Err)
}
