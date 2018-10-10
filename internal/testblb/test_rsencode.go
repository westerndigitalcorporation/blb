// Copyright (c) 2017 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package testblb

import (
	"context"
	"fmt"
	"github.com/westerndigitalcorporation/blb/pkg/rpc"
	"math/rand"
	"time"

	"github.com/klauspost/reedsolomon"

	"github.com/westerndigitalcorporation/blb/internal/core"
)

// TestRSEncode tests that a tractserver can RSEncode from/to other tractservers.
func (tc *TestCase) TestRSEncode() error {
	// We'll make three data chunks and two parity chunks.
	N, M := 3, 2
	// Each data chunk will have 4 tracts packed together.
	T := 4
	L := T * core.TractLength

	base := core.RSChunkID{Partition: 0x80000555, ID: 5555}

	tss := tc.bc.Tractservers()

	// Make some blobs.
	var allTractInfo []core.TractInfo
	var allLens []int
	for i := 0; i < N; i++ {
		lastLen := 1 + rand.Intn(core.TractLength) // [1, core.TractLength]
		_, ts, _ := createAndFill1(tc, 3, core.TractLength*(T-1)+lastLen)
		if len(ts) != T {
			return fmt.Errorf("wrong length")
		}
		allTractInfo = append(allTractInfo, ts...)
		for j := 0; j < T-1; j++ {
			allLens = append(allLens, core.TractLength)
		}
		allLens = append(allLens, lastLen)
	}

	// Pick hosts for chunks.
	addrs := make([]core.TSAddr, N+M)
	for i := range addrs {
		addrs[i].Host = tss[rand.Intn(len(tss))].ServiceAddr()
	}
	fillInTSIDS(addrs)

	// Pack them into data chunks.
	p := rand.Perm(len(allTractInfo)) // random order
	for i := range addrs[:N] {
		tracts := make([]*core.PackTractSpec, T)
		off := 0
		for j := range tracts {
			idx := p[0]
			ti := allTractInfo[idx]
			p = p[1:]
			tracts[j] = &core.PackTractSpec{
				ID:      ti.Tract,
				From:    tiToAddrs(ti),
				Version: ti.Version,
				Offset:  off,
				Length:  allLens[idx],
			}
			off += tracts[j].Length
		}

		err := packTracts(addrs[i].Host, core.PackTractsReq{
			Length:  L,
			ChunkID: base.Add(i),
			Tracts:  tracts,
		})
		if err != core.NoError {
			return err.Error()
		}
	}

	// Encode.
	rsAddr := tss[rand.Intn(len(tss))].ServiceAddr()
	err := rsEncode(rsAddr, core.RSEncodeReq{
		ChunkID: base,
		Length:  L,
		Srcs:    addrs[:N],
		Dests:   addrs[N:],
	})
	if err != core.NoError {
		return err.Error()
	}

	// Read them all back out.
	data := make([][]byte, N+M)
	for i := range addrs {
		data[i], err = ctlRead(addrs[i].Host, base.Add(i).ToTractID(), core.RSChunkVersion, L, 0)
		if err != core.NoError {
			return err.Error()
		}
	}

	// Verify.
	enc, _ := reedsolomon.New(N, M)
	ok, e := enc.Verify(data)
	if e != nil || !ok {
		return fmt.Errorf("RS verify failed: %v, %v", e, ok)
	}

	return nil
}

func rsEncode(addr string, req core.RSEncodeReq) (reply core.Error) {
	cc := rpc.NewConnectionCache(10*time.Second, 10*time.Second, 0)
	if cc.Send(context.Background(), addr, core.GetTSIDMethod, struct{}{}, &req.TSID) != nil || req.TSID < 1 {
		reply = core.ErrRPC
	}
	if cc.Send(context.Background(), addr, core.RSEncodeMethod, req, &reply) != nil {
		reply = core.ErrRPC
	}
	return
}

func fillInTSIDS(addrs []core.TSAddr) {
	cc := rpc.NewConnectionCache(10*time.Second, 10*time.Second, 0)
	for i := range addrs {
		if cc.Send(context.Background(), addrs[i].Host, core.GetTSIDMethod, struct{}{}, &addrs[i].ID) != nil || addrs[i].ID < 1 {
			panic("GetTSID failed")
		}
	}
}
