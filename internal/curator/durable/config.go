// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package durable

import (
	"path/filepath"

	"github.com/westerndigitalcorporation/blb/pkg/raft/raft"
	"github.com/westerndigitalcorporation/blb/pkg/raft/raftfs"
	"github.com/westerndigitalcorporation/blb/pkg/raft/raftrpc"
)

// DefaultStateConfig includes default values for replicated state.
var DefaultStateConfig StateConfig

func init() {
	DefaultStateConfig = StateConfig{
		Config:             raft.DefaultConfig,
		TransportConfig:    raft.DefaultTransportConfig,
		RPCTransportConfig: raftrpc.DefaultRPCTransportConfig,
		StorageConfig:      raftfs.DefaultStorageConfig,
	}

	// Below overrides Raft snapshot configuration values inside of
	// "DefaultStateConfig.Config".

	// The threshold is based on the performance of boltdb. The benchmark
	// showed that the throughput of boltdb on SSD is ~14000 txns/sec using
	// non-fsync mode. So the worst case is that we'll reapply 100000 txns after
	// recoverying from previous snapshot at startup. This will take ~7s, which is
	// acceptable.
	DefaultStateConfig.SnapshotThreshold = 100000

	// Keep at least 100000 entries that are included in snapshot untrimmed in log so
	// that we don't have to ship entire snapshot to synchronize a slow follower when
	// the missed entries can be found in log.
	//
	// Assume each log entry is around 1KB, then 100000 entries will take ~95MB
	// space on disk. We assumed that each curator will manage >10GB size of metadata
	// and after compression the size of the snapshot might be in between 500MB ~ 2GB. So
	// shipping these log entries is still cheaper than shipping entire snapshot.
	DefaultStateConfig.LogEntriesAfterSnapshot = 100000
}

// StateConfig encapsulates the parameters needed for creating a ReplicatedState.
type StateConfig struct {
	raft.Config                `json:"Config"`             // Parameters for Raft core.
	raft.TransportConfig       `json:"TransportConfig"`    // Parameters for Raft transport.
	raftrpc.RPCTransportConfig `json:"RPCTransportConfig"` // Parameters for Raft RPC transport.
	raftfs.StorageConfig       `json:"StorageConfig"`      // Parameters for Raft storage.

	DBDir string // The directory for DB file.

	// The callback that Raft calls when there is a leadership change.
	OnLeadershipChange func(bool)
}

// GetStateDBPath returns the path of state database file.
func (s *StateConfig) GetStateDBPath() string {
	return filepath.Join(s.DBDir, stateDB)
}

// GetStateTempDBPath returns the path of state temporary database file.
func (s *StateConfig) GetStateTempDBPath() string {
	return filepath.Join(s.DBDir, stateDBTmp)
}
