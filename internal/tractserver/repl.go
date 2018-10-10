// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package tractserver

import (
	"context"
	"time"

	"github.com/westerndigitalcorporation/blb/internal/core"
	"github.com/westerndigitalcorporation/blb/pkg/rpc"
)

const (
	// How long will the RPCTractserverTalker wait for an RPC to finish?
	ttRPCDeadline time.Duration = 30 * time.Second

	// How long will the RPCTractserverTalker wait to connect?
	ttDialTimeout time.Duration = 10 * time.Second

	// How many connections should we cache? This is for re-replication and
	// erasure-coding.
	ttConnectionCacheSize = 30
)

// TractserverTalker manages talking to other tractservers to replicate data.
type TractserverTalker interface {
	// CtlRead does a normal read of the tract (id, version) from 'addr'.
	CtlRead(ctx context.Context, addr string, id core.TractID, version int, len int, offset int64) ([]byte, core.Error)

	// CtlWrite writes data to a tract or RS chunk.
	CtlWrite(ctx context.Context, addr string, id core.TractID, v int, offset int64, b []byte) core.Error
}

// RPCTractserverTalker is a Go RPC-based implementation of TractserverTalker.
type RPCTractserverTalker struct {
	cc *rpc.ConnectionCache
}

// NewRPCTractserverTalker returns a new RPCTractserverTalker.
func NewRPCTractserverTalker() TractserverTalker {
	return &RPCTractserverTalker{cc: rpc.NewConnectionCache(ttDialTimeout, ttRPCDeadline, ttConnectionCacheSize)}
}

// CtlRead reads the tract (id, v) from 'addr'.
func (t *RPCTractserverTalker) CtlRead(ctx context.Context, addr string, id core.TractID, v, len int, off int64) ([]byte, core.Error) {
	req := core.ReadReq{ID: id, Version: v, Len: len, Off: off}
	var reply core.ReadReply
	if t.cc.Send(ctx, addr, core.CtlReadMethod, req, &reply) != nil {
		return nil, core.ErrRPC
	}
	return reply.B, reply.Err
}

// CtlWrite writes to the tract (id, v) on addr at the given offset.
func (t *RPCTractserverTalker) CtlWrite(ctx context.Context, addr string, id core.TractID, v int, offset int64, b []byte) (reply core.Error) {
	req := core.WriteReq{ID: id, Version: v, B: b, Off: offset}
	if t.cc.Send(ctx, addr, core.CtlWriteMethod, &req, &reply) != nil {
		return core.ErrRPC
	}
	return reply
}
