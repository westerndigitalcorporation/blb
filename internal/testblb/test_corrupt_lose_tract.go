// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package testblb

import (
	"context"
	"fmt"
	"math/rand"
	"os"

	log "github.com/golang/glog"
)

// TestCorruptLoseTract tests that a tract can be successfully rereplicated in
// the case of mixed data corruption and server lost failure, as long as there
// is at least one replica available.
func (tc *TestCase) TestCorruptLoseTract() error {
	// We need at least five tractservers.
	if tc.clusterCfg.Tractservers < 5 {
		return fmt.Errorf("need at least five tractservers for TestCorruptTract")
	}

	// Create a blob and write some data to it.
	blob, err := tc.c.Create()
	if err != nil {
		return err
	}

	data := makeRandom(1 * mb)
	blob.Seek(0, os.SEEK_SET)
	if n, err := blob.Write(data); err != nil || n != len(data) {
		return err
	}

	// Pick two random tractservers that own the first tract. We will do
	// something evil to them.
	tracts, err := tc.c.GetTracts(context.Background(), blob.ID(), 0, 1)
	if err != nil {
		return err
	}
	if len(tracts) == 0 {
		return fmt.Errorf("The blob has no tracts")
	}
	firstTract := tracts[0]
	firstTS := int(rand.Uint32()) % len(firstTract.Hosts)
	secondTS := (firstTS + 1) % len(firstTract.Hosts)

	capture := tc.captureLogs()

	// Corrupt the replica on the first picked ts.
	if err := tc.corruptTract(firstTract.Tract, tc.bc.FindByServiceAddress(firstTract.Hosts[firstTS]), 1234); err != nil {
		return err
	}
	// Kill the second picked ts.
	proc, perr := tc.getTractserverProc(firstTract.Tract, secondTS)
	if perr != nil {
		return perr
	}
	proc.Stop()

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
