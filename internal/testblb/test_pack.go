// Copyright (c) 2017 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package testblb

import (
	"bytes"
	"context"
	"fmt"
	"github.com/westerndigitalcorporation/blb/pkg/rpc"
	"time"

	client "github.com/westerndigitalcorporation/blb/client/blb"
	"github.com/westerndigitalcorporation/blb/internal/core"
)

// TestPack tests that a tractserver can pack tracts.
func (tc *TestCase) TestPack() error {
	// Create a bunch of blobs with data.
	_, ts1, d1 := createAndFill1(tc, 3, 100000)
	_, ts2, d2 := createAndFill1(tc, 3, 20000)
	_, ts3, d3 := createAndFill1(tc, 3, 70000)

	dest := core.RSChunkID{Partition: 0x80000555, ID: 5555}

	// Corrupt all but one copies of the second blob.
	tc.corruptTract(ts2[0].Tract, tc.bc.FindByServiceAddress(ts2[0].Hosts[0]), 123)
	tc.corruptTract(ts2[0].Tract, tc.bc.FindByServiceAddress(ts2[0].Hosts[1]), 123)

	// Pick a tractserver and pack.
	t0 := tc.bc.Tractservers()[0].ServiceAddr()
	err := packTracts(t0, core.PackTractsReq{
		Length:  200000,
		ChunkID: dest,
		Tracts: []*core.PackTractSpec{
			{ID: ts1[0].Tract, From: tiToAddrs(ts1[0]), Version: ts1[0].Version, Offset: 0, Length: len(d1)},
			{ID: ts2[0].Tract, From: tiToAddrs(ts2[0]), Version: ts2[0].Version, Offset: len(d1), Length: len(d2)},
			{ID: ts3[0].Tract, From: tiToAddrs(ts3[0]), Version: ts3[0].Version, Offset: len(d1) + len(d2), Length: len(d3)},
		},
	})
	if err != core.NoError {
		return err.Error()
	}

	// Read it back.
	packed, err := ctlRead(t0, dest.ToTractID(), core.RSChunkVersion, 1000000, 0)
	if err != core.ErrEOF {
		return err.Error()
	}
	if len(packed) != 200000 {
		return fmt.Errorf("wrong len: %d", len(packed))
	}
	if !bytes.Equal(packed[0:len(d1)], d1) ||
		!bytes.Equal(packed[len(d1):len(d1)+len(d2)], d2) ||
		!bytes.Equal(packed[len(d1)+len(d2):len(d1)+len(d2)+len(d3)], d3) {
		return fmt.Errorf("bad data")
	}

	return nil
}

func createAndFill1(tc *TestCase, repl, length int) (b *client.Blob, tracts []core.TractInfo, data []byte) {
	var err error
	data = makeRandom(length)
	b, err = tc.c.Create(client.ReplFactor(repl))
	if err != nil {
		panic(err)
	}
	var n int
	if n, err = b.Write(data); err != nil || n != len(data) {
		panic(err)
	}
	if tracts, err = tc.c.GetTracts(context.Background(), b.ID(), 0, 100); err != nil {
		panic(err)
	}
	return
}

func tiToAddrs(ts core.TractInfo) []core.TSAddr {
	out := make([]core.TSAddr, len(ts.Hosts))
	for i := range ts.Hosts {
		out[i] = core.TSAddr{Host: ts.Hosts[i], ID: ts.TSIDs[i]}
	}
	return out
}

func packTracts(addr string, req core.PackTractsReq) (reply core.Error) {
	cc := rpc.NewConnectionCache(10*time.Second, 10*time.Second, 0)
	if cc.Send(context.Background(), addr, core.GetTSIDMethod, struct{}{}, &req.TSID) != nil || req.TSID < 1 {
		reply = core.ErrRPC
	}
	if cc.Send(context.Background(), addr, core.PackTractsMethod, req, &reply) != nil {
		reply = core.ErrRPC
	}
	return
}

func ctlRead(addr string, id core.TractID, v, len int, off int64) ([]byte, core.Error) {
	cc := rpc.NewConnectionCache(10*time.Second, 10*time.Second, 0)
	var reply core.ReadReply
	if cc.Send(context.Background(), addr, core.CtlReadMethod, core.ReadReq{ID: id, Version: v, Len: len, Off: off}, &reply) != nil {
		reply.Err = core.ErrRPC
	}
	return reply.B, reply.Err
}
