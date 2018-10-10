// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package tractserver

import (
	"context"
	"time"

	"github.com/westerndigitalcorporation/blb/internal/blbrpc"
	"github.com/westerndigitalcorporation/blb/internal/core"
)

const (
	dialTimeout = 5 * time.Second
	rpcTimeout  = 5 * time.Second
)

// RPCMasterConnection encapsulates a connection to a master group. It will probe
// who the leader is in the master group and send requests(for now only Heartbeat)
// to the master leader.
type RPCMasterConnection struct {
	fc *blbrpc.MasterFailoverConnection
}

// NewRPCMasterConnection creates a new connection to a master group specified in
// 'addrs'.
func NewRPCMasterConnection(masterSpec string) *RPCMasterConnection {
	return &RPCMasterConnection{fc: blbrpc.NewMasterFailoverConnection(masterSpec, dialTimeout, rpcTimeout)}
}

// MasterTractserverHeartbeat sends a heartbeat to the master and returns its reply.
func (r *RPCMasterConnection) MasterTractserverHeartbeat(ctx context.Context, id core.TractserverID, addr string, disks []core.FsStatus) (core.MasterTractserverHeartbeatReply, core.Error) {
	var reply core.MasterTractserverHeartbeatReply
	req := core.MasterTractserverHeartbeatReq{TractserverID: id, Addr: addr, Disks: disks}
	err, _ := r.fc.FailoverRPC(ctx, core.MasterTractserverHeartbeatMethod, req, &reply)
	return reply, err
}

// RegisterTractserver is called only once when a new tractserver is added to
// the cluster. It tries forever to get a tractserver ID from the master, until
// a valid tractserver ID is assigned.
func (r *RPCMasterConnection) RegisterTractserver(ctx context.Context, addr string) (core.RegisterTractserverReply, core.Error) {
	var reply core.RegisterTractserverReply
	req := core.RegisterTractserverReq{Addr: addr}
	err, _ := r.fc.FailoverRPC(ctx, core.RegisterTractserverMethod, req, &reply)
	return reply, err
}
