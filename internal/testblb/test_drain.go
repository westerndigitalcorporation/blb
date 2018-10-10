// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package testblb

import (
	"context"
	"fmt"
	"time"

	log "github.com/golang/glog"
	client "github.com/westerndigitalcorporation/blb/client/blb"
	"github.com/westerndigitalcorporation/blb/internal/core"
)

// TestDrain tests that we can set control flags on a disk on a tractserver and
// have it drain (curator replicates tracts off of it).
func (tc *TestCase) TestDrain() error {
	// We need at least four tractservers.
	if tc.clusterCfg.Tractservers < 4 {
		return fmt.Errorf("need at least four tractservers for TestCorruptTract")
	}

	// Create a blob and write some data to it.
	blob, err := tc.c.Create()
	if err != nil {
		return err
	}
	if n, err := blob.Write(makeRandom(mb)); err != nil || n != mb {
		return err
	}

	// Get tract info about the first tract.
	tracts, err := tc.c.GetTracts(context.Background(), blob.ID(), 0, 1)
	if err != nil {
		return err
	}
	if len(tracts) != 1 {
		return fmt.Errorf("unexpected tract count")
	}

	addr := tracts[0].Hosts[1]

	// We don't know what disk it's on, so figure it out.
	tt := client.NewRPCTractserverTalker()
	root := ""
	// This data is refreshed every 10s, loop until we see a tract.
	for root == "" {
		root = findDiskWithTract(tt, addr)
		time.Sleep(time.Second)
	}

	cap := tc.captureLogs()

	flags := core.DiskControlFlags{Drain: 100}
	berr := tt.SetControlFlags(context.Background(), addr, root, flags)
	if berr != core.NoError {
		return berr.Error()
	}

	// We expect rereplication to happen now.
	log.Infof("waiting for rerepl")
	cap.WaitFor(
		fmt.Sprintf("t:@@@ marking .* tracts for draining: .*%v", tracts[0].Tract),
		fmt.Sprintf("c:@@@ rerepl %v succeeded", tracts[0].Tract),
	)

	// The tract should be removed from that tractserver eventually.
	log.Infof("waiting for gc")
	for root != "" {
		root = findDiskWithTract(tt, addr)
		time.Sleep(time.Second)
	}

	return nil
}

func findDiskWithTract(tt client.TractserverTalker, addr string) string {
	disks, berr := tt.GetDiskInfo(context.Background(), addr)
	if berr != core.NoError {
		panic(berr)
	}
	for _, disk := range disks {
		if disk.NumTracts > 0 {
			return disk.Status.Root
		}
	}
	return ""
}
