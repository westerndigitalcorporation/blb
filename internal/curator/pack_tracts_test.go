// Copyright (c) 2017 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package curator

import (
	"testing"

	"github.com/westerndigitalcorporation/blb/pkg/testutil"

	"github.com/westerndigitalcorporation/blb/internal/core"
	"github.com/westerndigitalcorporation/blb/internal/curator/durable/state"
	"github.com/westerndigitalcorporation/blb/internal/curator/storageclass"
	"github.com/westerndigitalcorporation/blb/internal/server"
)

type mockTPContext struct {
	*testutil.GenericMock
}

func newMockTPContext(t *testing.T) *mockTPContext {
	return &mockTPContext{testutil.NewGenericMock(t)}
}

func (m *mockTPContext) SetVersion(addr string, tsid core.TractserverID, id core.TractID, newVersion int, conditionalStamp uint64) core.Error {
	return m.GetResult("SetVersion", addr, tsid, id, newVersion, conditionalStamp).(core.Error)
}

func (m *mockTPContext) DelTract(addr string, tsid core.TractserverID, tid core.TractID) core.Error {
	return m.GetResult("DelTract", addr, tsid, tid).(core.Error)
}

func (m *mockTPContext) addCtlStatTract(addr string, tsid core.TractserverID, id core.TractID, version int, res core.StatTractReply) {
	m.AddCall("CtlStatTract", res, addr, tsid, id, version)
}

func (m *mockTPContext) CtlStatTract(addr string, tsid core.TractserverID, id core.TractID, version int) core.StatTractReply {
	return m.GetResult("CtlStatTract", addr, tsid, id, version).(core.StatTractReply)
}

func (m *mockTPContext) PackTracts(addr string, tsid core.TractserverID, length int, tracts []*core.PackTractSpec, id core.RSChunkID) core.Error {
	return m.GetResult("PackTracts", addr, tsid, length, tracts, id).(core.Error)
}

func (m *mockTPContext) RSEncode(addr string, tsid core.TractserverID, id core.RSChunkID, length int, srcs []core.TSAddr, dests []core.TSAddr) core.Error {
	return m.GetResult("RSEncode", addr, tsid, id, length, srcs, dests).(core.Error)
}

type allocateTSReturn struct {
	addrs []string
	ids   []core.TractserverID
}

func (m *mockTPContext) AllocateTS(num int) (addrs []string, ids []core.TractserverID) {
	ret := m.GetResult("AllocateTS", num).(allocateTSReturn)
	return ret.addrs, ret.ids
}

func (m *mockTPContext) MarkPending(id core.RSChunkID, n int) {
	// ignore for now
}

func (m *mockTPContext) UnmarkPending(id core.RSChunkID, n int) {
	// ignore for now
}

type allocateRSChunkIDsReturn struct {
	id  core.RSChunkID
	err core.Error
}

func (m *mockTPContext) AllocateRSChunkIDs(n int) (core.RSChunkID, core.Error) {
	ret := m.GetResult("AllocateRSChunkIDs", n).(allocateRSChunkIDsReturn)
	return ret.id, ret.err
}

func (m *mockTPContext) CommitRSChunk(id core.RSChunkID, cls core.StorageClass, hosts []core.TractserverID, data [][]state.EncodedTract) core.Error {
	return m.GetResult("CommitRSChunk", id, cls, hosts, data).(core.Error)
}

func (m *mockTPContext) addSuggestFixVersion(id core.TractID, version int, badHost string) {
	m.AddCall("SuggestFixVersion", nil, id, version, badHost)
}

func (m *mockTPContext) SuggestFixVersion(id core.TractID, version int, badHost string) {
	m.GetResult("SuggestFixVersion", id, version, badHost)
}

var testOpMetric = server.NewOpMetric("test_internal_ops", "op")

func newTestTractPacker(t *testing.T) (*mockTPContext, *tractPacker) {
	mc := newMockTPContext(t)
	cls := storageclass.AllRS[0] // RS(6, 3)
	n, m := cls.RSParams()
	return mc, makeTractPacker(mc, testOpMetric, cls.ID(), n, m, RSPieceLength)
}

func TestTPCallsFixVersion(t *testing.T) {
	mc, tp := newTestTractPacker(t)

	t1id := core.TractIDFromParts(core.BlobIDFromParts(1, 1), 0)
	t1addrs := []core.TSAddr{{ID: 1, Host: "addr1"}, {ID: 2, Host: "addr2"}}
	t1ver := 3

	t2id := core.TractIDFromParts(core.BlobIDFromParts(1, 1), 1)
	t2addrs := []core.TSAddr{{ID: 3, Host: "addr3"}, {ID: 4, Host: "addr4"}}
	t2ver := 5
	t2len := 54321

	// Expect four stat calls from addTract:
	mc.addCtlStatTract("addr1", 1, t1id, t1ver, core.StatTractReply{Err: core.ErrVersionMismatch})
	mc.addCtlStatTract("addr2", 2, t1id, t1ver, core.StatTractReply{Err: core.ErrVersionMismatch})
	mc.addCtlStatTract("addr3", 3, t2id, t2ver, core.StatTractReply{Size: int64(t2len)})
	mc.addCtlStatTract("addr4", 4, t2id, t2ver, core.StatTractReply{Size: int64(t2len)})

	// Expect one call to SuggestFixVersion for t1:
	mc.addSuggestFixVersion(t1id, t1ver, "addr2")

	// Add the tracts:
	tp.addTract(t1id, t1addrs, t1ver)
	tp.addTract(t2id, t2addrs, t2ver)
	tp.doneAdding()

	mc.NoMoreCalls()

	if tp.tracts[0].Length != -1 {
		t.Error("t1 should be bad")
	}
	if tp.tracts[1].Length != t2len {
		t.Error("t2 has wrong len")
	}
}
