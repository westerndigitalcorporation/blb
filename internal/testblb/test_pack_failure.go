// Copyright (c) 2017 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package testblb

import (
	"fmt"

	"github.com/westerndigitalcorporation/blb/internal/core"
)

// TestPackFailure tests that PackTracts fails if sources are corrupted.
func (tc *TestCase) TestPackFailure() error {
	// Create a bunch of blobs with data.
	_, ts1, d1 := createAndFill1(tc, 3, 100000)
	_, ts2, d2 := createAndFill1(tc, 3, 20000)
	_, ts3, d3 := createAndFill1(tc, 3, 70000)

	// Corrupt all copies of the second blob.
	tc.corruptTract(ts2[0].Tract, tc.bc.FindByServiceAddress(ts2[0].Hosts[0]), 123)
	tc.corruptTract(ts2[0].Tract, tc.bc.FindByServiceAddress(ts2[0].Hosts[1]), 456)
	tc.corruptTract(ts2[0].Tract, tc.bc.FindByServiceAddress(ts2[0].Hosts[2]), 789)

	// Pick a tractserver and pack.
	t0 := tc.bc.Tractservers()[0].ServiceAddr()
	err := packTracts(t0, core.PackTractsReq{
		Length:  200000,
		ChunkID: core.RSChunkID{Partition: 0x80000555, ID: 5555},
		Tracts: []*core.PackTractSpec{
			{ID: ts1[0].Tract, From: tiToAddrs(ts1[0]), Version: ts1[0].Version, Offset: 0, Length: len(d1)},
			{ID: ts2[0].Tract, From: tiToAddrs(ts2[0]), Version: ts2[0].Version, Offset: len(d1), Length: len(d2)},
			{ID: ts3[0].Tract, From: tiToAddrs(ts3[0]), Version: ts3[0].Version, Offset: len(d1) + len(d2), Length: len(d3)},
		},
	})
	if err != core.ErrRPC {
		return fmt.Errorf("PackTracts should have failed")
	}

	return nil
}
