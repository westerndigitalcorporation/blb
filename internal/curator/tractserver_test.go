// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package curator

import (
	"sync"

	"github.com/westerndigitalcorporation/blb/internal/core"
)

// testTractserverTalker records the data passed to calls of its methods, and returns
// whatever the specific test tells it to.
type testTractserverTalker struct {
	setVersionCalls   map[string][]core.SetVersionReq
	setVersionReplies map[string][]core.SetVersionReply

	pullTractCalls   map[string][]core.PullTractReq
	pullTractReplies map[string][]core.Error

	gcTractCalls    map[string][]core.GCTractReq
	gcTractCallChan chan core.GCTractReq
	gcTractReplies  map[string][]core.Error

	checkTractCalls map[string][]core.CheckTractsReq

	rsEncodeCalls   map[string][]core.RSEncodeReq
	rsEncodeReplies map[string][]core.Error

	lock sync.Mutex
}

func newTestTractserverTalker() *testTractserverTalker {
	return &testTractserverTalker{
		setVersionCalls:   make(map[string][]core.SetVersionReq),
		setVersionReplies: make(map[string][]core.SetVersionReply),

		pullTractCalls:   make(map[string][]core.PullTractReq),
		pullTractReplies: make(map[string][]core.Error),

		gcTractCalls:    make(map[string][]core.GCTractReq),
		gcTractCallChan: make(chan core.GCTractReq, 20),
		gcTractReplies:  make(map[string][]core.Error),

		checkTractCalls: make(map[string][]core.CheckTractsReq),

		rsEncodeCalls:   make(map[string][]core.RSEncodeReq),
		rsEncodeReplies: make(map[string][]core.Error),
	}
}

func (tt *testTractserverTalker) addSetVersionReply(addr string, msg core.SetVersionReply) {
	tt.setVersionReplies[addr] = append(tt.setVersionReplies[addr], msg)
}

func (tt *testTractserverTalker) addPullTractReply(addr string, err core.Error) {
	tt.pullTractReplies[addr] = append(tt.pullTractReplies[addr], err)
}

func (tt *testTractserverTalker) addRSEncodeReply(addr string, err core.Error) {
	tt.rsEncodeReplies[addr] = append(tt.rsEncodeReplies[addr], err)
}

func (tt *testTractserverTalker) SetVersion(addr string, tsid core.TractserverID, id core.TractID, newVersion int, conditionalStamp uint64) core.Error {
	tt.lock.Lock()
	defer tt.lock.Unlock()

	// Store the request for later.
	msg := core.SetVersionReq{ID: id, NewVersion: newVersion}
	tt.setVersionCalls[addr] = append(tt.setVersionCalls[addr], msg)

	// If we're out of replies, pretend we have an RPC issue.
	if len(tt.setVersionReplies[addr]) == 0 {
		return core.ErrRPC
	}

	// Otherwise dequeue the reply and return it.
	ret := tt.setVersionReplies[addr][0]
	tt.setVersionReplies[addr] = tt.setVersionReplies[addr][1:]
	return ret.Err
}

func (tt *testTractserverTalker) PullTract(addr string, tsid core.TractserverID, from []string, id core.TractID, version int) core.Error {
	tt.lock.Lock()
	defer tt.lock.Unlock()

	msg := core.PullTractReq{From: from, ID: id, Version: version}
	tt.pullTractCalls[addr] = append(tt.pullTractCalls[addr], msg)

	if len(tt.pullTractReplies[addr]) == 0 {
		return core.ErrRPC
	}

	ret := tt.pullTractReplies[addr][0]
	tt.pullTractReplies[addr] = tt.pullTractReplies[addr][1:]
	return ret
}

func (tt *testTractserverTalker) GCTract(addr string, tsid core.TractserverID, old []core.TractState, gone []core.TractID) core.Error {
	tt.lock.Lock()
	defer tt.lock.Unlock()

	req := core.GCTractReq{TSID: tsid, Old: old, Gone: gone}
	tt.gcTractCalls[addr] = append(tt.gcTractCalls[addr], req)
	tt.gcTractCallChan <- req
	tt.gcTractReplies[addr] = append(tt.gcTractReplies[addr], core.NoError)

	return core.NoError
}

func (tt *testTractserverTalker) CheckTracts(addr string, tsid core.TractserverID, tracts []core.TractState) core.Error {
	tt.lock.Lock()
	tt.checkTractCalls[addr] = append(tt.checkTractCalls[addr], core.CheckTractsReq{TSID: tsid, Tracts: tracts})
	tt.lock.Unlock()
	return core.NoError
}

func (tt *testTractserverTalker) CtlStatTract(addr string, tsid core.TractserverID, id core.TractID, version int) core.StatTractReply {
	return core.StatTractReply{Err: core.ErrNotYetImplemented}
}

func (tt *testTractserverTalker) PackTracts(addr string, tsid core.TractserverID, length int, tracts []*core.PackTractSpec, id core.RSChunkID) core.Error {
	return core.ErrNotYetImplemented
}

func (tt *testTractserverTalker) RSEncode(addr string, tsid core.TractserverID, id core.RSChunkID, length int, srcs, dests []core.TSAddr, im []int) core.Error {
	tt.lock.Lock()
	defer tt.lock.Unlock()

	msg := core.RSEncodeReq{TSID: tsid, ChunkID: id, Length: length, Srcs: srcs, Dests: dests, IndexMap: im}
	tt.rsEncodeCalls[addr] = append(tt.rsEncodeCalls[addr], msg)

	if len(tt.rsEncodeReplies[addr]) == 0 {
		return core.ErrRPC
	}

	ret := tt.rsEncodeReplies[addr][0]
	tt.rsEncodeReplies[addr] = tt.rsEncodeReplies[addr][1:]
	return ret
}
