// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package testblb

import (
	"context"
	"fmt"
	"os"

	log "github.com/golang/glog"
)

// TestOnlyOneHost tests that a tract can be written to (eventually) even if every replica except one
// is down.
func (tc *TestCase) TestOnlyOneHost() error {
	repl := 3
	needed := 2*repl - 1

	if tc.clusterCfg.Tractservers < uint(needed) {
		return fmt.Errorf("need at least %d tractservers for test", needed)
	}

	// Create a tract in a blob.
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
	if len(tracts) != 1 {
		return fmt.Errorf("The blob has no tracts")
	}

	firstTract := tracts[0]
	if len(firstTract.Hosts) != repl {
		return fmt.Errorf("don't have enough tract replicas")
	}

	capture := tc.captureLogs()

	// Kill all but one host.
	for i := 0; i < repl-1; i++ {
		proc, perr := tc.getTractserverProc(firstTract.Tract, i)
		if perr != nil {
			return perr
		}
		proc.Stop()
	}

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
