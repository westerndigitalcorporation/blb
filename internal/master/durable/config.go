// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package durable

import (
	"github.com/westerndigitalcorporation/blb/pkg/raft/raft"
	"github.com/westerndigitalcorporation/blb/pkg/raft/raftfs"
	"github.com/westerndigitalcorporation/blb/pkg/raft/raftrpc"
)

// StateConfig encapsulates the parameters needed for creating a StateHandler.
type StateConfig struct {
	raft.Config                `json:"Config"`            // Parameters for Raft core.
	raft.TransportConfig       `json:"TransportConfig"`   // Parameters for Raft transport.
	raftrpc.RPCTransportConfig `json:"RPCTransporConfig"` // Parameters for Raft RPC transport.
	raftfs.StorageConfig       `json:"StorageConfig"`     // Parameters for Raft storage.

	// The callback for newly elected leader. This function, if not-nil, is called
	// synchronously by Raft when a member becomes the leader of the replication group.
	OnLeader func()
}

// DefaultStateConfig includes default values for the state handler.
var DefaultStateConfig = StateConfig{
	Config:             raft.DefaultConfig,
	TransportConfig:    raft.DefaultTransportConfig,
	RPCTransportConfig: raftrpc.DefaultRPCTransportConfig,
	StorageConfig:      raftfs.DefaultStorageConfig,
}
