// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package testblb

import (
	"context"
	"fmt"
	"math/rand"
	"os"

	log "github.com/golang/glog"
	"github.com/westerndigitalcorporation/blb/internal/core"
)

// TestMissingTract tests that a tract exists in curator's state but missing
// on one of tract servers. This should be detected by curator and the tract
// will be re-replicated.
func (tc *TestCase) TestMissingTract() error {
	// We need at least four tractservers.
	if tc.clusterCfg.Tractservers < 4 {
		return fmt.Errorf("need at least four tractservers for TestMissingTract")
	}

	//	1. Create a blob and write some data to it.

	// Create a blob and fill it with one tract of data.
	blob, err := tc.c.Create()
	if err != nil {
		return err
	}

	data := makeRandom(1 * mb)
	blob.Seek(0, os.SEEK_SET)
	if n, err := blob.Write(data); err != nil || n != len(data) {
		return err
	}

	//	2. Pick a tract server that owns the first tract and delete it.

	// Get tract info about the first tract.
	tracts, err := tc.c.GetTracts(context.Background(), blob.ID(), 0, 1)
	if err != nil {
		return err
	}
	if len(tracts) == 0 {
		return fmt.Errorf("The blob has no tracts")
	}

	firstTract := tracts[0]
	hostToDelete := firstTract.Hosts[int(rand.Uint32())%len(firstTract.Hosts)]
	name := tc.bc.FindByServiceAddress(hostToDelete)

	captureCorrupt := tc.captureLogs()
	if err := tc.deleteTract(firstTract.Tract, name); err != nil {
		return err
	}

	//	3. Wait until the tract server detects it and the tract gets re-replicated.

	log.Infof("Wait until the corrupted tract %s to be re-replicated", firstTract.Tract)

	// Wait until the tract server detects the corruption and curator re-replicates
	// the corrupted tract.
	captureCorrupt.WaitFor(
		fmt.Sprintf("t:@@@ check tract failed on tract %s.", firstTract.Tract),
		fmt.Sprintf("c:@@@ rerepl %v succeeded", firstTract.Tract),
	)

	// If we are here it means the re-replication is done.
	return nil
}

// Delete a tract file from a tract server that has it.
func (tc *TestCase) deleteTract(tract core.TractID, tsName string) error {
	path := tc.findTract(tract, tsName)
	if path == "" {
		return fmt.Errorf("Can't find the tract file on tract server.")
	}
	log.Infof("Removing tract file %s", path)
	return os.Remove(path)
}
