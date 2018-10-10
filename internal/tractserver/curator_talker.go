// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package tractserver

import "github.com/westerndigitalcorporation/blb/internal/core"

// CuratorTalker is the aggregation of connections to curators.
type CuratorTalker interface {
	// CuratorTractserverHeartbeat sends a heartbeat to the curator at 'curatorAddr'
	// to notify that this tractserver is still up.  The curator returns which partitions it is
	// in charge of so that we can promptly report corruption to that curator.
	CuratorTractserverHeartbeat(curatorAddr string, beat core.CuratorTractserverHeartbeatReq) ([]core.PartitionID, error)

	// close closes the connection to the curator addressed at 'curatorAddr'.
	Close(curatorAddr string)
}
