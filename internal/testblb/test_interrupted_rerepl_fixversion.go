// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package testblb

import (
	"fmt"
	"os"

	log "github.com/golang/glog"
	"github.com/westerndigitalcorporation/blb/internal/core"
)

// TestInterruptedRereplFixversion is similar to TestInterruptedRerepl with the
// distinction that in a replication group of 3, we crash one host, bump the
// version on the second (simulating the curator crashed in the middle of
// re-replication), and only leave the third intact. Writting to the tract
// should eventually success after all background rereplication and version
// fixing.
func (tc *TestCase) TestInterruptedRereplFixversion() error {
	// We need at least four tractservers.
	if tc.clusterCfg.Tractservers < 4 {
		return fmt.Errorf("need at least four tractservers for TestRerepl")
	}

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

	// Kill the first host holding this blob. This will trigger
	// rereplication in the write below.
	tid := core.TractIDFromParts(core.BlobID(blob.ID()), 0)
	proc, perr := tc.getTractserverProc(tid, 0)
	if nil != perr {
		return perr
	}
	proc.Stop()

	// Pretend that we have an interrupted rereplication by bumping version
	// directly on the second host.
	if err := tc.bumpVersion(tid, 1); nil != err {
		return err
	}

	// Trigger a rereplication request.
	capture := tc.captureLogs()
	if err := tc.triggerRereplRequest(tid); err != nil {
		return err
	}

	// Wait for a curator to log success.
	log.Infof("waiting for rerepl...")
	if err := capture.WaitFor("c:@@@ rerepl .* succeeded"); err != nil {
		return err
	}

	// Now a write should succeed.
	blob.Seek(0, os.SEEK_SET)
	n, werr := blob.Write(data)
	if werr != nil {
		return werr
	}
	log.Infof("wrote %d bytes", n)

	return nil
}
