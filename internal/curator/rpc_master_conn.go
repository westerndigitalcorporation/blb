// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package curator

import (
	"context"
	"time"

	"github.com/westerndigitalcorporation/blb/internal/blbrpc"
	"github.com/westerndigitalcorporation/blb/internal/core"
)

const (
	rpcTimeout  = 10 * time.Second
	dialTimeout = 10 * time.Second
)

// RPCMasterConnection implements MasterConnection based on Go's RPC
// pacakge.
type RPCMasterConnection struct {
	addr string
	fc   *blbrpc.MasterFailoverConnection
}

// NewRPCMasterConnection creates a new RPCMasterConnection to the master set.
func NewRPCMasterConnection(addr string, masterSpec string) MasterConnection {
	mc := &RPCMasterConnection{
		addr: addr,
		fc:   blbrpc.NewMasterFailoverConnection(masterSpec, dialTimeout, rpcTimeout),
	}
	return mc
}

// RegisterCurator checks to see if we're already registered.  If not, it sends
// a registration request to the master and logs the registration information.
func (r *RPCMasterConnection) RegisterCurator() (core.RegisterCuratorReply, core.Error) {
	var reply core.RegisterCuratorReply
	req := core.RegisterCuratorReq{Addr: r.addr}
	err, _ := r.fc.FailoverRPC(context.Background(), core.RegisterCuratorMethod, req, &reply)
	return reply, err
}

// CuratorHeartbeat sends a heartbeat to the master and returns its reply.
func (r *RPCMasterConnection) CuratorHeartbeat(id core.CuratorID) (core.CuratorHeartbeatReply, core.Error) {
	var reply core.CuratorHeartbeatReply
	req := core.CuratorHeartbeatReq{
		CuratorID: id,
		Addr:      r.addr,
	}
	err, _ := r.fc.FailoverRPC(context.Background(), core.CuratorHeartbeatMethod, req, &reply)
	return reply, err
}

// NewPartition sends a request for a new partition to the master and returns
// its reply.
func (r *RPCMasterConnection) NewPartition(id core.CuratorID) (core.NewPartitionReply, core.Error) {
	var reply core.NewPartitionReply
	req := core.NewPartitionReq{CuratorID: id}
	err, _ := r.fc.FailoverRPC(context.Background(), core.NewPartitionMethod, req, &reply)
	return reply, err
}
