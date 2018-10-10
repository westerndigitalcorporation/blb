// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package curator

import (
	"github.com/westerndigitalcorporation/blb/internal/core"
)

// MasterConnection is a connection to a group of masters.  Connection
// establishment is done transparently.
type MasterConnection interface {
	// RegisterCurator sends a request to the master to register this curator.
	RegisterCurator() (core.RegisterCuratorReply, core.Error)

	// CuratorHeartbeat sends a heartbeat to the master.
	CuratorHeartbeat(core.CuratorID) (core.CuratorHeartbeatReply, core.Error)

	// NewPartition sends a request for a new partition to the master.
	NewPartition(core.CuratorID) (core.NewPartitionReply, core.Error)
}
