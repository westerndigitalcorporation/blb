// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package blb

import (
	"context"
	log "github.com/golang/glog"

	"github.com/westerndigitalcorporation/blb/internal/core"
	"github.com/westerndigitalcorporation/blb/pkg/rpc"
)

const (
	// How many curators to keep open connections to.
	curatorConnectionCacheSize = 100
)

// RPCCuratorTalker implmenets CuratorTalker based on Go's RPC package.
type RPCCuratorTalker struct {
	cc *rpc.ConnectionCache
}

// NewRPCCuratorTalker creates a new RPCCuratorTalker.
func NewRPCCuratorTalker() CuratorTalker {
	return &RPCCuratorTalker{cc: rpc.NewConnectionCache(dialTimeout, rpcTimeout, curatorConnectionCacheSize)}
}

// CreateBlob implements CuratorTalker.
func (r *RPCCuratorTalker) CreateBlob(ctx context.Context, addr string, metadata core.BlobInfo) (core.BlobID, core.Error) {
	req := core.CreateBlobReq{Repl: metadata.Repl, Hint: metadata.Hint, Expires: metadata.Expires}
	var reply core.CreateBlobReply
	if err := r.cc.Send(ctx, addr, core.CreateBlobMethod, req, &reply); err != nil {
		log.Errorf("RPC-level error creating a blob: %s", err)
		return 0, core.ErrRPC
	}
	return reply.ID, reply.Err
}

// ExtendBlob implements CuratorTalker.
func (r *RPCCuratorTalker) ExtendBlob(ctx context.Context, addr string, blob core.BlobID, numTracts int) ([]core.TractInfo, core.Error) {
	req := core.ExtendBlobReq{Blob: blob, NumTracts: numTracts}
	var reply core.ExtendBlobReply
	if err := r.cc.Send(ctx, addr, core.ExtendBlobMethod, req, &reply); err != nil {
		log.Errorf("RPC-level error extending blob %s: %s", blob, err)
		return nil, core.ErrRPC
	}
	if core.NoError != reply.Err {
		log.Errorf("curator-level error extending blob %s: %s", blob, reply.Err)
	}
	return reply.NewTracts, reply.Err
}

// AckExtendBlob acks the success of extending 'blob' with the new tracts
// 'tracts' and term number 'term'.
func (r *RPCCuratorTalker) AckExtendBlob(ctx context.Context, addr string, blob core.BlobID, tracts []core.TractInfo) core.Error {
	req := core.AckExtendBlobReq{Blob: blob, Tracts: tracts}
	var reply core.AckExtendBlobReply
	if err := r.cc.Send(ctx, addr, core.AckExtendBlobMethod, req, &reply); err != nil {
		log.Errorf("RPC-level error ack'ing extended tracts %+v for blob %s: %s", tracts, blob, err)
		return core.ErrRPC
	}
	if core.NoError != reply.Err {
		log.Errorf("curator-level error ack'ing extend tracts %+v for blob %s, %s", tracts, blob, reply.Err)
	}
	return reply.Err
}

// DeleteBlob implements CuratorTalker.
func (r *RPCCuratorTalker) DeleteBlob(ctx context.Context, addr string, blob core.BlobID) core.Error {
	var reply core.Error
	if err := r.cc.Send(ctx, addr, core.DeleteBlobMethod, blob, &reply); err != nil {
		log.Errorf("RPC-level error deleting blob %s: %s", blob, err)
		return core.ErrRPC
	}
	if core.NoError != reply {
		log.Errorf("curator-level error deleting blob %s: %s", blob, reply)
	}
	return reply
}

// UndeleteBlob implements CuratorTalker.
func (r *RPCCuratorTalker) UndeleteBlob(ctx context.Context, addr string, blob core.BlobID) core.Error {
	var reply core.Error
	if err := r.cc.Send(ctx, addr, core.UndeleteBlobMethod, blob, &reply); err != nil {
		log.Errorf("RPC-level error undeleting blob %s: %s", blob, err)
		return core.ErrRPC
	}
	if core.NoError != reply {
		log.Errorf("curator-level error undeleting blob %s: %s", blob, reply)
	}
	return reply
}

// SetMetadata implements CuratorTalker.
func (r *RPCCuratorTalker) SetMetadata(ctx context.Context, addr string, blob core.BlobID, md core.BlobInfo) core.Error {
	req := core.SetMetadataReq{Blob: blob, Metadata: md}
	var reply core.Error
	if err := r.cc.Send(ctx, addr, core.SetMetadataMethod, req, &reply); err != nil {
		log.Errorf("RPC-level error setting metadata on blob %s: %s", blob, err)
		return core.ErrRPC
	}
	if core.NoError != reply {
		log.Errorf("curator-level error setting metadata on blob %s: %s", blob, reply)
	}
	return reply
}

// GetTracts implements CuratorTalker.
func (r *RPCCuratorTalker) GetTracts(ctx context.Context, addr string, blob core.BlobID, start, end int,
	forRead, forWrite bool) ([]core.TractInfo, core.Error) {
	req := core.GetTractsReq{
		Blob: blob, Start: start, End: end,
		ForRead: forRead, ForWrite: forWrite,
	}
	var reply core.GetTractsReply
	if err := r.cc.Send(ctx, addr, core.GetTractsMethod, req, &reply); err != nil {
		log.Errorf("RPC-level error getting tracts [%d, %d) from blob %s: %s", start, end, blob, err)
		return nil, core.ErrRPC
	}
	return reply.Tracts, reply.Err
}

// StatBlob implements CuratorTalker.
func (r *RPCCuratorTalker) StatBlob(ctx context.Context, addr string, blob core.BlobID) (core.BlobInfo, core.Error) {
	var reply core.StatBlobReply
	if err := r.cc.Send(ctx, addr, core.StatBlobMethod, blob, &reply); err != nil {
		log.Errorf("RPC-level error stating blob %s: %s", blob, err)
		return core.BlobInfo{}, core.ErrRPC
	}
	if core.NoError != reply.Err {
		log.Errorf("curator-level error stating blob %s: %s", blob, reply.Err)
	}
	return reply.Info, reply.Err
}

// ReportBadTS tells the curator to re-replicate 'id' replacing 'bad'.
func (r *RPCCuratorTalker) ReportBadTS(ctx context.Context, addr string, id core.TractID, bad, op string, got core.Error, couldRecover bool) core.Error {
	req := core.ReportBadTSReq{ID: id, Bad: bad, Operation: op, GotError: got, CouldRecover: couldRecover}
	var reply core.Error
	if err := r.cc.Send(ctx, addr, core.ReportBadTSMethod, req, &reply); err != nil {
		log.Errorf("RPC-level error reporting tract %s [%s %s]: %s", id, op, bad, err)
		return core.ErrRPC
	}
	return reply
}

// FixVersion asks the curator to verify the version(s) of the provided tract as we were unable
// to use the information in 'tract' to do IO with 'bad'.
func (r *RPCCuratorTalker) FixVersion(ctx context.Context, addr string, tract core.TractInfo, bad string) core.Error {
	req := core.FixVersionReq{Info: tract, Bad: bad}
	var reply core.Error
	if err := r.cc.Send(ctx, addr, core.FixVersionMethod, req, &reply); err != nil {
		log.Errorf("RPC-level error fixversion-ing %v: %v", tract, err)
		return core.ErrRPC
	}
	return reply
}

// ListBlobs gets a contiguous range of blob ids in a given partition.
func (r *RPCCuratorTalker) ListBlobs(ctx context.Context, addr string, partition core.PartitionID, start core.BlobKey) ([]core.BlobKey, core.Error) {
	req := core.ListBlobsReq{Partition: partition, Start: start}
	var reply core.ListBlobsReply
	if err := r.cc.Send(ctx, addr, core.ListBlobsMethod, req, &reply); err != nil {
		log.Errorf("RPC-level error listing blobs: %s", err)
		return nil, core.ErrRPC
	}
	return reply.Keys, reply.Err
}
