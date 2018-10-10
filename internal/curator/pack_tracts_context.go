// Copyright (c) 2017 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package curator

import (
	"github.com/westerndigitalcorporation/blb/internal/core"
	"github.com/westerndigitalcorporation/blb/internal/curator/durable/state"
)

// tpContext is a type that defines the interface between tractPacker and the
// rest of the curator, to allow for better testing.
type tpContext interface {
	// RPC functions:
	SetVersion(addr string, tsid core.TractserverID, id core.TractID, newVersion int, conditionalStamp uint64) core.Error
	DelTract(addr string, tsid core.TractserverID, tid core.TractID) core.Error
	CtlStatTract(addr string, tsid core.TractserverID, id core.TractID, version int) core.StatTractReply
	PackTracts(addr string, tsid core.TractserverID, length int, tracts []*core.PackTractSpec, id core.RSChunkID) core.Error
	RSEncode(addr string, tsid core.TractserverID, id core.RSChunkID, length int, srcs, dests []core.TSAddr) core.Error

	// Volatile state functions:
	AllocateTS(num int) (addrs []string, ids []core.TractserverID)
	MarkPending(id core.RSChunkID, n int)
	UnmarkPending(id core.RSChunkID, n int)

	// Durable state functions:
	AllocateRSChunkIDs(n int) (core.RSChunkID, core.Error)
	CommitRSChunk(id core.RSChunkID, cls core.StorageClass, hosts []core.TractserverID, data [][]state.EncodedTract) core.Error

	// Other curator functions (mixed tractserver and durable state):
	SuggestFixVersion(id core.TractID, version int, badHost string)
}

// curatorTPContext is a very thin tpContext implemented in terms of the
// curator, used outside of tests.
type curatorTPContext struct {
	c    *Curator
	term uint64 // current raft term, used to enforce that tract packing happens within one term
}

func (c *curatorTPContext) SetVersion(addr string, tsid core.TractserverID, id core.TractID, newVersion int, conditionalStamp uint64) core.Error {
	return c.c.tt.SetVersion(addr, tsid, id, newVersion, conditionalStamp)
}

func (c *curatorTPContext) DelTract(addr string, tsid core.TractserverID, tid core.TractID) core.Error {
	return c.c.tt.GCTract(addr, tsid, nil, []core.TractID{tid})
}

func (c *curatorTPContext) CtlStatTract(addr string, tsid core.TractserverID, id core.TractID, version int) core.StatTractReply {
	return c.c.tt.CtlStatTract(addr, tsid, id, version)
}

func (c *curatorTPContext) PackTracts(addr string, tsid core.TractserverID, length int, tracts []*core.PackTractSpec, id core.RSChunkID) core.Error {
	// Asking a tractserver to pack this chunk will result in it requesting
	// approximately length bytes over the network.
	c.c.rsEncodeBwLim.Take(float32(length))
	return c.c.tt.PackTracts(addr, tsid, length, tracts, id)
}

func (c *curatorTPContext) RSEncode(addr string, tsid core.TractserverID, id core.RSChunkID, length int, srcs, dests []core.TSAddr) core.Error {
	// We're going to ask one TS to read length bytes from each of N other TSs,
	// then write out length bytes to itself and M-1 others.
	c.c.rsEncodeBwLim.Take(float32(length * (len(srcs) + len(dests) - 1)))
	return c.c.tt.RSEncode(addr, tsid, id, length, srcs, dests, nil)
}

func (c *curatorTPContext) AllocateTS(num int) (addrs []string, ids []core.TractserverID) {
	return c.c.allocateTS(num, nil, nil)
}

func (c *curatorTPContext) MarkPending(id core.RSChunkID, n int) {
	c.c.markPendingPieces(id, n)
}

func (c *curatorTPContext) UnmarkPending(id core.RSChunkID, n int) {
	c.c.unmarkPendingPieces(id, n)
}

func (c *curatorTPContext) AllocateRSChunkIDs(n int) (core.RSChunkID, core.Error) {
	return c.c.stateHandler.AllocateRSChunkIDs(n, c.term)
}

func (c *curatorTPContext) CommitRSChunk(id core.RSChunkID, cls core.StorageClass, hosts []core.TractserverID, data [][]state.EncodedTract) core.Error {
	return c.c.stateHandler.CommitRSChunk(id, cls, hosts, data, c.term)
}

func (c *curatorTPContext) SuggestFixVersion(id core.TractID, version int, badHost string) {
	go c.c.fixVersion(id, version, badHost) // ignore error
}
