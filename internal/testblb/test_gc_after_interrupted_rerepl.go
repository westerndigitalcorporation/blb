// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package testblb

import (
	"context"
	"fmt"
	"os"

	log "github.com/golang/glog"
	"github.com/westerndigitalcorporation/blb/internal/core"
)

// TestGCAfterInterruptedRerepl exercises gc-ing a up-to-date tract on a
// tractserver not in the repl group due to interrupted rerepl.
func (tc *TestCase) TestGCAfterInterruptedRerepl() error {
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

	// Pretend that we have an interrupted rereplication by bumping versions
	// directly on all the tractservers.
	for host := 0; host < 3; host++ {
		if err := tc.bumpVersion(core.TractIDFromParts(core.BlobID(blob.ID()), 0), host); nil != err {
			return err
		}
	}

	// Kill the first host holding this blob. This will trigger
	// rereplication in the write below.
	tid := core.TractIDFromParts(core.BlobID(blob.ID()), 0)
	proc, perr := tc.getTractserverProc(tid, 0)
	if nil != perr {
		return perr
	}
	proc.Stop()

	// Trigger a rereplication request.
	captureRerepl := tc.captureLogs()
	if err := tc.triggerRereplRequest(tid); err != nil {
		return err
	}

	// Wait for a curator to log success.
	log.Infof("waiting for rerepl...")
	if err := captureRerepl.WaitFor("c:@@@ rerepl .* succeeded"); err != nil {
		return err
	}

	// Now a write should succeed.
	blob.Seek(0, os.SEEK_SET)
	n, werr := blob.Write(data)
	if werr != nil {
		return werr
	}
	log.Infof("wrote %d bytes", n)

	// Get the current version.
	tracts, err := tc.c.GetTracts(context.Background(), blob.ID(), 0, 1)
	if err != nil {
		return err
	}
	v := tracts[0].Version

	// Restart the killed host and wait for it to log gc operation.
	captureGC := tc.captureLogs()
	if serr := proc.Start(); nil != serr {
		return serr
	}
	// The version should be equal to the current value. It's just the
	// tractserver not in the repl group.
	log.Infof("waiting for gc...")
	msg := fmt.Sprintf("%s:@@@ gc-ing tract %s, old: %d <= %d", proc.Name(), tid, v, v)
	if err := captureGC.WaitFor(msg); err != nil {
		return err
	}

	return nil
}
