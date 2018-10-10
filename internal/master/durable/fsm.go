// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package durable

import (
	"bytes"
	"encoding/gob"
	"io"

	log "github.com/golang/glog"

	"github.com/westerndigitalcorporation/blb/internal/core"
	"github.com/westerndigitalcorporation/blb/pkg/raft/raft"
)

// init registers commands with gob.
func init() {
	gob.Register(RegisterCuratorCmd{})
	gob.Register(RegisterTractserverCmd{})
	gob.Register(NewPartitionCmd{})
	gob.Register(ChecksumRequestCmd{})
	gob.Register(ChecksumVerifyCmd{})
	gob.Register(SetReadOnlyModeCmd{})
}

//-------------------- raft.FSM implementation --------------------//

// Apply implements raft.FSM.
func (h *StateHandler) Apply(ent raft.Entry) interface{} {
	// Deserialize the command.
	cmd := bytesToCmd(ent.Cmd)

	// Although there's only one goroutine that perform updates, there might be other
	// goroutines performing reads at the same time, so we have to grab the lock first.
	h.lock.Lock()
	defer h.lock.Unlock()

	// Handle these first. SetReadOnlyMode needs to work even if we're in
	// read-only mode, and the checksum commands don't modify state, so we can
	// allow them.
	switch c := cmd.(type) {
	case SetReadOnlyModeCmd:
		h.state.ReadOnly = c.ReadOnly
		return core.NoError
	case ChecksumRequestCmd:
		return h.checksumRequest(ent.Index)
	case ChecksumVerifyCmd:
		return h.verifyChecksum(c)
	}

	// If we're read-only, anything else is an error.
	if h.state.ReadOnly {
		return core.ErrReadOnlyMode
	}

	// Apply modifying command.
	switch c := cmd.(type) {
	case RegisterCuratorCmd:
		return c.apply(h.state)
	case RegisterTractserverCmd:
		return c.apply(h.state)
	case NewPartitionCmd:
		return c.apply(h.state)
	}

	log.Fatalf("applying unknown command %v", cmd)
	return nil
}

// OnLeadershipChange implements raft.FSM. When a master becomes the leader,
// it starts to track the heartbeats of curators. When a leader steps down, it
// stops watching for such heartbeats.
func (h *StateHandler) OnLeadershipChange(b bool, term uint64, leader string) {
	// There's only one goroutine performs the update on "isLeader", but there
	// might be other goroutines perform read on it at the same time, we still
	// have to grab the write lock.
	h.lock.Lock()
	h.isLeader = b
	h.term = term
	h.leaderID = leader
	h.lock.Unlock()
	if b && h.config.OnLeader != nil {
		h.config.OnLeader()
	}
}

// OnMembershipChange is called to notify current members in a Raft cluster.
func (h *StateHandler) OnMembershipChange(membership raft.Membership) {
	h.lock.Lock()
	defer h.lock.Unlock()
	log.Infof("OnMembershipChange: %+v", membership)
	h.members = membership.Members
}

// Snapshot implements raft.FSM.
func (h *StateHandler) Snapshot() (raft.Snapshoter, error) {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	if err := enc.Encode(h.state); err != nil {
		log.Fatalf("Serializing to in-memory buffer is not supposed to fail: %v", err)
	}
	return &masterSnapshoter{data: buffer.Bytes()}, nil
}

type masterSnapshoter struct {
	data []byte
}

func (s *masterSnapshoter) Release() {}

func (s *masterSnapshoter) Save(writer io.Writer) error {
	_, err := writer.Write(s.data)
	return err
}

// SnapshotRestore implements raft.FSM.
func (h *StateHandler) SnapshotRestore(reader io.Reader, lastIndex, lastTerm uint64) {
	dec := gob.NewDecoder(reader)
	if err := dec.Decode(h.state); nil != err {
		log.Fatalf("failed to decode snapshot: %s", err)
	}
}

func (h *StateHandler) checksumRequest(index uint64) ChecksumRes {
	h.checksum = h.state.checksum()
	h.checksumIndex = index
	return ChecksumRes{Checksum: h.checksum, Index: h.checksumIndex}
}

func (h *StateHandler) verifyChecksum(cmd ChecksumVerifyCmd) interface{} {
	if h.checksumIndex != cmd.Index {
		// This may happen around node restarts or leader elections or membership changes.
		// It's harmless.
		log.Infof("got checksum for wrong index: %v != %v", h.checksumIndex, cmd.Index)
	} else if h.checksum != cmd.Checksum {
		log.Fatalf("@@@ consistency check failed! %v != %v", h.checksum, cmd.Checksum)
	} else {
		log.V(1).Infof("@@@ consistency check passed (idx %v, ck %v)", h.checksumIndex, h.checksum)
	}
	return nil
}

//-------------------- Internal implementation of StateHandler.Apply --------------------//

// Generate and return a curator ID.
func (cmd RegisterCuratorCmd) apply(s *State) core.CuratorID {
	curatorID := s.NextCuratorID
	s.NextCuratorID++
	return curatorID
}

// Assign a new partition to the given curator.
func (cmd NewPartitionCmd) apply(s *State) (res NewPartitionRes) {
	if err := s.verifyCuratorID(cmd.CuratorID); err != core.NoError {
		res.PartitionID, res.Err = core.PartitionID(0), err
		return
	}

	if len(s.Partitions) >= core.MaxPartitionID {
		res.Err = core.ErrExceedNewPartitionQuota
		return
	}

	partition := core.PartitionID(len(s.Partitions))
	s.Partitions = append(s.Partitions, cmd.CuratorID)
	res.PartitionID, res.Err = partition, core.NoError
	return
}

// Generate and return a tractserver ID.
func (cmd RegisterTractserverCmd) apply(s *State) core.TractserverID {
	tractserverID := s.NextTractserverID
	s.NextTractserverID++
	return tractserverID
}

// getPartitions returns the partitions assigned for the given curator, or all
// if curatorID is zero.
func (s *State) getPartitions(query core.CuratorID) (res []core.PartitionID) {
	for partition, curator := range s.Partitions {
		if curator.IsValid() && (!query.IsValid() || curator == query) {
			res = append(res, core.PartitionID(partition))
		}
	}
	return
}

// verifyCuratorID verifies if given id is valid.
func (s *State) verifyCuratorID(id core.CuratorID) core.Error {
	if id <= 0 || id >= s.NextCuratorID {
		return core.ErrBadCuratorID
	}
	return core.NoError
}
