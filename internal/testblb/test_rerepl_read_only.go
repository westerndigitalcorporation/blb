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

// TestRereplReadOnly verifies read-only tract can be properly handled for
// writes. Writing to a read-only tract should fail and trigger rereplication.
// After that, further write should succeed.
//
//	1. Create a blob and write some data to it.
//	2. Pick a replica of the first tract and make it read-only.
//	3. Wait until the tract gets rereplicated and verify with a write.
//
func (tc *TestCase) TestRereplReadOnly() error {
	// We need at least four tractservers.
	if tc.clusterCfg.Tractservers < 4 {
		return fmt.Errorf("need at least four tractservers for TestCorruptTract")
	}

	// Create a blob with one tract.
	blob, err := tc.c.Create()
	if err != nil {
		return err
	}

	data := makeRandom(1 * mb)
	blob.Seek(0, os.SEEK_SET)
	if n, err := blob.Write(data); err != nil || n != len(data) {
		return err
	}

	// Get tract info about the first tract.
	tracts, err := tc.c.GetTracts(context.Background(), blob.ID(), 0, 1)
	if err != nil {
		return err
	}
	if len(tracts) == 0 {
		return fmt.Errorf("The blob has no tracts")
	}
	firstTract := tracts[0]

	// Make a replica read-only.
	readOnlyHost := firstTract.Hosts[int(rand.Uint32())%len(firstTract.Hosts)]
	name := tc.bc.FindByServiceAddress(readOnlyHost)

	if err := tc.readOnlyTract(firstTract.Tract, name); err != nil {
		return err
	}

	capture := tc.captureLogs()

	// Trigger a rereplication request.
	if err := tc.triggerRereplRequest(firstTract.Tract); err != nil {
		return err
	}

	// Wait for a curator to log success.
	log.Infof("waiting for rerepl...")
	if err := capture.WaitFor(
		fmt.Sprintf("c:@@@ rerepl %v succeeded", firstTract.Tract)); err != nil {
		return err
	}

	// Now a write should succeed.
	blob.Seek(0, os.SEEK_SET)
	n, werr := blob.Write([]byte("spicy sichuan food"))
	if werr != nil {
		return werr
	}
	log.Infof("wrote %d bytes", n)

	return nil
}

// Make a given tract on a given tractserver read-only.
func (tc *TestCase) readOnlyTract(tract core.TractID, tsName string) error {
	path := tc.findTract(tract, tsName)
	if path == "" {
		return fmt.Errorf("Can't find the tract file on tractserver.")
	}
	log.Infof("making tract %s on tractserver %s read-only", tract, tsName)
	return os.Chmod(path, os.FileMode(0400))
}
