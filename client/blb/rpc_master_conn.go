// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package blb

import (
	"context"
	"github.com/westerndigitalcorporation/blb/internal/blbrpc"
	"github.com/westerndigitalcorporation/blb/internal/core"
)

// RPCMasterConnection implements MasterConnection based on Go's RPC
// pacakge.
type RPCMasterConnection struct {
	fc *blbrpc.MasterFailoverConnection
}

// NewRPCMasterConnection creates a new RPCMasterConnection to the master set
// with addresses in 'addrSpec'.
func NewRPCMasterConnection(addrSpec string) MasterConnection {
	return &RPCMasterConnection{fc: blbrpc.NewMasterFailoverConnection(addrSpec, dialTimeout, rpcTimeout)}
}

// MasterCreateBlob calls master 'Create' to choose a curator to create a blob.
func (r *RPCMasterConnection) MasterCreateBlob(ctx context.Context) (curator string, err core.Error) {
	req := core.MasterCreateBlobReq{}
	reply := &core.LookupCuratorReply{}

	err, _ = r.fc.FailoverRPC(ctx, core.MasterCreateBlobMethod, req, reply)
	if err == core.NoError {
		if len(reply.Replicas) > 0 {
			curator = reply.Replicas[0]
		} else {
			err = core.ErrBadHosts
		}
	}
	return
}

// LookupPartition calls master 'Lookup' to locate the curator for 'partition'.
func (r *RPCMasterConnection) LookupPartition(ctx context.Context, partition core.PartitionID) (curator string, err core.Error) {
	req := core.LookupPartitionReq{Partition: partition}
	reply := &core.LookupPartitionReply{}

	err, _ = r.fc.FailoverRPC(ctx, core.LookupPartitionMethod, req, reply)
	if err == core.NoError {
		if len(reply.Replicas) > 0 {
			curator = reply.Replicas[0]
		} else {
			err = core.ErrBadHosts
		}
	}
	return
}

// ListPartitions returns all existing partitions.
func (r *RPCMasterConnection) ListPartitions(ctx context.Context) (partitions []core.PartitionID, err core.Error) {
	req := core.ListPartitionsReq{}
	reply := &core.ListPartitionsReply{}
	err, _ = r.fc.FailoverRPC(ctx, core.ListPartitionsMethod, req, reply)
	if err == core.NoError {
		partitions = reply.Partitions
	}
	return
}

// GetTractserverInfo gets tractserver state.
func (r *RPCMasterConnection) GetTractserverInfo(ctx context.Context) (info []core.TractserverInfo, err core.Error) {
	req := core.GetTractserverInfoReq{}
	reply := &core.GetTractserverInfoReply{}
	err, _ = r.fc.FailoverRPC(ctx, core.GetTractserverInfoMethod, req, reply)
	return reply.Info, err
}
