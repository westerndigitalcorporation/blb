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
	// How many tractservers to keep open connections to.
	tsConnectionCacheSize = 100
)

// RPCTractserverTalker implements TractserverTalker based on Go RPC.
type RPCTractserverTalker struct {
	cc *rpc.ConnectionCache
}

// NewRPCTractserverTalker returns a new Golang RPC based implementation of TractserverTalker.
func NewRPCTractserverTalker() TractserverTalker {
	return &RPCTractserverTalker{cc: rpc.NewConnectionCache(dialTimeout, rpcTimeout, tsConnectionCacheSize)}
}

// Create creates a new tract on the tractserver and does a write to the newly created tract.
func (r *RPCTractserverTalker) Create(ctx context.Context, addr string, tsid core.TractserverID, id core.TractID, b []byte, off int64) core.Error {
	pri := priorityFromContext(ctx)
	req := core.CreateTractReq{TSID: tsid, ID: id, Off: off, Pri: pri}
	req.Set(b, false)
	var reply core.Error
	if err := r.cc.Send(ctx, addr, core.CreateTractMethod, &req, &reply); err != nil {
		log.Errorf("CreateTract RPC error for tract (id: %s, offset: %d) on tractserver %s@%s: %s", id, off, tsid, addr, err)
		return core.ErrRPC
	}
	if reply != core.NoError {
		log.Errorf("CreateTract error for tract (id: %s, offset: %d) on tractserver %s@%s: %s", id, off, tsid, addr, reply)
	}
	return reply
}

// Write does a write to a tract on this tractserver.
func (r *RPCTractserverTalker) Write(ctx context.Context, addr string, id core.TractID, version int, b []byte, off int64) core.Error {
	pri := priorityFromContext(ctx)
	rpcid := rpc.GenID()
	req := core.WriteReq{ID: id, Version: version, Off: off, Pri: pri, ReqID: rpcid}
	req.Set(b, false)
	var reply core.Error
	cancel := rpc.CancelAction{Method: core.CancelReqMethod, Req: rpcid}
	if err := r.cc.SendWithCancel(ctx, addr, core.WriteMethod, &req, &reply, &cancel); err != nil {
		log.Errorf("Write RPC error for tract (id: %s, version: %d, offset: %d) on tractserver @%s: %s", id, version, off, addr, err)
		return core.ErrRPC
	}
	if reply != core.NoError {
		log.Errorf("Write error for tract (id: %s, version: %d, offset: %d) on tractserver @%s: %s", id, version, off, addr, reply)
	}
	return reply
}

// Read reads from a tract.
func (r *RPCTractserverTalker) Read(ctx context.Context, addr string, id core.TractID, version int, len int, off int64) ([]byte, core.Error) {
	pri := priorityFromContext(ctx)
	rpcid := rpc.GenID()
	req := core.ReadReq{ID: id, Version: version, Len: len, Off: off, Pri: pri, ReqID: rpcid}
	var reply core.ReadReply
	cancel := rpc.CancelAction{Method: core.CancelReqMethod, Req: rpcid}
	if err := r.cc.SendWithCancel(ctx, addr, core.ReadMethod, req, &reply, &cancel); err != nil {
		log.Errorf("Read RPC error for tract (id: %s, version: %d, length: %d, offset: %d) on tractserver @%s: %s", id, version, len, off, addr, err)
		return nil, core.ErrRPC
	}
	if reply.Err != core.NoError && reply.Err != core.ErrEOF {
		log.Errorf("Read error for tract (id: %s, version: %d, length: %d, offset: %d) on tractserver @%s: %s", id, version, len, off, addr, reply.Err)
		return nil, reply.Err
	}
	return reply.B, reply.Err
}

// ReadInto reads from a tract into a provided slice without copying.
func (r *RPCTractserverTalker) ReadInto(ctx context.Context, addr, reqID string, id core.TractID, version int, b []byte, off int64) (int, core.Error) {
	pri := priorityFromContext(ctx)
	req := core.ReadReq{ID: id, Version: version, Len: len(b), Off: off, Pri: pri, ReqID: reqID}
	// Gob will decode into a provided slice if there's enough capacity. Give it b,
	// but reset the cap to len so it can't go past our segment.
	var reply core.ReadReply
	reply.Set(b[0:0:len(b)], false)
	cancel := rpc.CancelAction{Method: core.CancelReqMethod, Req: reqID}
	if err := r.cc.SendWithCancel(ctx, addr, core.ReadMethod, req, &reply, &cancel); err != nil {
		log.Errorf("Read RPC error for tract (id: %s, version: %d, length: %d, offset: %d) on tractserver @%s: %s", id, version, len(b), off, addr, err)
		return 0, core.ErrRPC
	}
	if reply.Err != core.NoError && reply.Err != core.ErrEOF {
		log.Errorf("Read error for tract (id: %s, version: %d, length: %d, offset: %d) on tractserver @%s: %s", id, version, len(b), off, addr, reply.Err)
		return 0, reply.Err
	}
	if len(reply.B) > 0 && len(b) > 0 && (&reply.B[0] != &b[0] || len(reply.B) > len(b)) {
		// Gob reallocated our slice, or the server replied with more bytes than we asked for. This should never happen.
		log.Errorf("Read error for tract (id: %s, version: %d, length: %d, offset: %d) on tractserver @%s: server returned too many bytes: %d", id, version, len(b), off, addr, len(reply.B))
		return 0, core.ErrInvalidState
	}
	return len(reply.B), reply.Err
}

// StatTract returns the number of bytes in a tract.
func (r *RPCTractserverTalker) StatTract(ctx context.Context, addr string, id core.TractID, version int) (int64, core.Error) {
	pri := priorityFromContext(ctx)
	req := core.StatTractReq{ID: id, Version: version, Pri: pri}
	var reply core.StatTractReply
	if err := r.cc.Send(ctx, addr, core.StatTractMethod, req, &reply); err != nil {
		log.Errorf("Stat RPC error for tract (id: %s, version: %d) on tractserver @%s: %s", id, version, addr, err)
		return 0, core.ErrRPC
	}
	if reply.Err != core.NoError && reply.Err != core.ErrEOF {
		log.Errorf("Stat error for tract (id: %s, version: %d) on tractserver @%s: %s", id, version, addr, reply.Err)
		return 0, reply.Err
	}
	return reply.Size, reply.Err
}

// GetDiskInfo returns a summary of disk info.
func (r *RPCTractserverTalker) GetDiskInfo(ctx context.Context, addr string) ([]core.FsStatus, core.Error) {
	req := core.GetDiskInfoReq{}
	var reply core.GetDiskInfoReply
	if err := r.cc.Send(ctx, addr, core.GetDiskInfoMethod, req, &reply); err != nil {
		log.Errorf("GetDiskInfo RPC error on tractserver %s: %s", addr, err)
		return nil, core.ErrRPC
	}
	if reply.Err != core.NoError {
		log.Errorf("GetDiskInfo error on tractserver %s: %s", addr, reply.Err)
	}
	return reply.Disks, reply.Err
}

// SetControlFlags changes control flags for a disk.
func (r *RPCTractserverTalker) SetControlFlags(ctx context.Context, addr string, root string, flags core.DiskControlFlags) core.Error {
	req := core.SetControlFlagsReq{Root: root, Flags: flags}
	var reply core.Error
	if err := r.cc.Send(ctx, addr, core.SetControlFlagsMethod, req, &reply); err != nil {
		log.Errorf("SetControlFlags RPC error on tractserver %s: %s", addr, err)
		return core.ErrRPC
	}
	if reply != core.NoError {
		log.Errorf("SetControlFlags error on tractserver %s: %s", addr, reply)
	}
	return reply
}
