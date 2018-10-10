// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package raft

import (
	"time"
)

// NewTestRaftNode creates a Raft node that can be used for unit testing the
// applications that are built on top of Raft, not Raft itself.
//	- It's the only node in a cluster thus no other replicas are needed.
//	- It simulates time in a much faster pace so everyting happens much faster.
//	- All of its states are volatile.
func NewTestRaftNode(ID, clusterID string) *Raft {
	cfg := Config{
		ID:                   ID,
		ClusterID:            clusterID,
		FollowerTimeout:      10,
		CandidateTimeout:     10,
		HeartbeatTimeout:     3,
		RandomElectionRange:  3,
		DurationPerTick:      10 * time.Millisecond,
		MaxNumEntsPerAppEnts: 10,
		MaximumProposalBatch: 1,
	}
	r := NewRaft(cfg, newStorage(), NewMemTransport(TransportConfig{Addr: ID, MsgChanCap: 1024}))
	return r
}

// newStorage returns an in-memory storage with clean state.
func newStorage() *Storage {
	return NewStorage(NewMemSnapshotMgr(), NewMemLog(), NewMemState(0))
}
