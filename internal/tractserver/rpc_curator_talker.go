// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package tractserver

import (
	"context"
	"time"

	"github.com/westerndigitalcorporation/blb/internal/core"
	"github.com/westerndigitalcorporation/blb/pkg/rpc"
)

const (
	// PL-1113: The RPC timeout(s) should be less than the heartbeat interval
	// so goroutines don't pile up in case of error.

	// How long will the RPCCuratorTalker wait for an RPC to finish?
	ctRPCDeadline time.Duration = 10 * time.Second

	// How long will the RPCCuratorTalker wait to connect?
	ctDialTimeout time.Duration = 10 * time.Second

	// How many connections should we cache?
	ctConnectionCacheSize = 20
)

// RPCCuratorTalker implements CuratorTalker using ConnectionCache.
type RPCCuratorTalker struct {
	cc *rpc.ConnectionCache
}

// NewRPCCuratorTalker returns a new RPCCuratorTalker.
func NewRPCCuratorTalker() *RPCCuratorTalker {
	return &RPCCuratorTalker{cc: rpc.NewConnectionCache(ctDialTimeout, ctRPCDeadline, ctConnectionCacheSize)}
}

// CuratorTractserverHeartbeat implements CuratorTalker.
func (ct *RPCCuratorTalker) CuratorTractserverHeartbeat(curatorAddr string, beat core.CuratorTractserverHeartbeatReq) ([]core.PartitionID, error) {
	var reply core.CuratorTractserverHeartbeatReply
	if err := ct.cc.Send(context.Background(), curatorAddr, core.CuratorTractserverHeartbeatMethod, beat, &reply); nil != err {
		return nil, err
	}
	if core.NoError != reply.Err {
		if core.ErrRaftNodeNotLeader == reply.Err || core.ErrRaftNotLeaderAnymore == reply.Err {
			ct.Close(curatorAddr)
		}
		return nil, reply.Err.Error()
	}
	return reply.Partitions, nil
}

// Close implements CuratorTalker.
func (ct *RPCCuratorTalker) Close(curatorAddr string) {
	ct.cc.Remove(curatorAddr)
}
