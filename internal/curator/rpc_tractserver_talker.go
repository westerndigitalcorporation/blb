// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package curator

import (
	"context"
	"time"

	log "github.com/golang/glog"

	"github.com/westerndigitalcorporation/blb/internal/core"
	"github.com/westerndigitalcorporation/blb/pkg/rpc"
)

const (
	// How long will the RPCTractserverTalker wait for an RPC to finish?
	//
	// PL-1121
	// TODO: This might be a pretty short deadline for something that has to read from
	// one spinning disk and write to another when both are under load.  If we're going
	// to use a short deadline we should also cancel the request when it times out.  Test it.
	//
	// PL-1121
	// TODO: Do we want to split it into two RPCs, one to start and one to poll for completion?
	ttRPCDeadline time.Duration = 30 * time.Second

	// How long will the RPCTractserverTalker wait to connect?
	ttDialTimeout time.Duration = 10 * time.Second

	// How many connections should we cache?
	ttConnectionCacheSize = 100
)

// RPCTractserverTalker implements TractserverTalker using ConnectionCache.
type RPCTractserverTalker struct {
	cc *rpc.ConnectionCache
}

// NewRPCTractserverTalker returns a new Golang RPC based implementation of TractserverTalker.
func NewRPCTractserverTalker() TractserverTalker {
	return &RPCTractserverTalker{cc: rpc.NewConnectionCache(ttDialTimeout, ttRPCDeadline, ttConnectionCacheSize)}
}

// SetVersion implements TractserverTalker.SetVersion
func (t *RPCTractserverTalker) SetVersion(addr string, tsid core.TractserverID, id core.TractID, newVersion int, conditionalStamp uint64) core.Error {
	req := core.SetVersionReq{TSID: tsid, ID: id, NewVersion: newVersion, ConditionalStamp: conditionalStamp}
	var reply core.SetVersionReply
	if err := t.cc.Send(context.Background(), addr, core.SetVersionMethod, req, &reply); err != nil {
		log.Errorf("SetVersion for tract %s failed on tractserver %d (@%s): %s", id, tsid, addr, err)
		return core.ErrRPC
	}
	return reply.Err
}

// PullTract implements TractserverTalker.PullTract
func (t *RPCTractserverTalker) PullTract(addr string, tsid core.TractserverID, from []string, id core.TractID, version int) core.Error {
	req := core.PullTractReq{TSID: tsid, From: from, ID: id, Version: version}
	var reply core.Error
	if err := t.cc.Send(context.Background(), addr, core.PullTractMethod, req, &reply); err != nil {
		log.Errorf("PullTract for tract %s failed on tractserver %d (@%s): %s", id, tsid, addr, err)
		return core.ErrRPC
	}
	return reply
}

// CheckTracts implements TractserverTalker.CheckTracts
func (t *RPCTractserverTalker) CheckTracts(addr string, tsid core.TractserverID, tracts []core.TractState) core.Error {
	req := core.CheckTractsReq{TSID: tsid, Tracts: tracts}
	var reply core.CheckTractsReply
	if err := t.cc.Send(context.Background(), addr, core.CheckTractsMethod, req, &reply); err != nil {
		log.Errorf("CheckTracts failed on tractserver %d (@%s): %s", tsid, addr, err)
		return core.ErrRPC
	}
	return reply.Err
}

// GCTract implements TractserverTalker.GCTract.
func (t *RPCTractserverTalker) GCTract(addr string, tsid core.TractserverID, old []core.TractState, gone []core.TractID) core.Error {
	req := core.GCTractReq{TSID: tsid, Old: old, Gone: gone}
	var reply core.Error
	if err := t.cc.Send(context.Background(), addr, core.GCTractMethod, req, &reply); err != nil {
		log.Errorf("GCTract failed on tractserver %d (@%s): %s", tsid, addr, err)
		return core.ErrRPC
	}
	return reply
}

// CtlStatTract implements TractserverTalker.CtlStatTract.
func (t *RPCTractserverTalker) CtlStatTract(addr string, tsid core.TractserverID, id core.TractID, version int) (reply core.StatTractReply) {
	req := core.StatTractReq{ID: id, Version: version}
	if err := t.cc.Send(context.Background(), addr, core.CtlStatTractMethod, req, &reply); err != nil {
		log.Errorf("CtlStatTract failed on tractserver %d (@%s): %s", tsid, addr, err)
		reply.Err = core.ErrRPC
	}
	return
}

// PackTracts implements TractserverTalker.PackTracts.
func (t *RPCTractserverTalker) PackTracts(
	addr string, tsid core.TractserverID, length int, tracts []*core.PackTractSpec, id core.RSChunkID) (reply core.Error) {
	req := core.PackTractsReq{TSID: tsid, Length: length, Tracts: tracts, ChunkID: id}
	if err := t.cc.Send(context.Background(), addr, core.PackTractsMethod, req, &reply); err != nil {
		log.Errorf("PackTracts failed on tractserver %d (@%s): %s", tsid, addr, err)
		reply = core.ErrRPC
	}
	return
}

// RSEncode implements TractserverTalker.RSEncode.
func (t *RPCTractserverTalker) RSEncode(
	addr string, tsid core.TractserverID, id core.RSChunkID, length int, srcs, dests []core.TSAddr, im []int) (reply core.Error) {
	req := core.RSEncodeReq{TSID: tsid, ChunkID: id, Length: length, Srcs: srcs, Dests: dests, IndexMap: im}
	if err := t.cc.Send(context.Background(), addr, core.RSEncodeMethod, req, &reply); err != nil {
		log.Errorf("RSEncode failed on tractserver %d (@%s): %s", tsid, addr, err)
		reply = core.ErrRPC
	}
	return
}
