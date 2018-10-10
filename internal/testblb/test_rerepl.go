// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package testblb

import (
	"bytes"
	"context"
	"fmt"
	"os"

	log "github.com/golang/glog"
	client "github.com/westerndigitalcorporation/blb/client/blb"
	"github.com/westerndigitalcorporation/blb/internal/cluster"
	"github.com/westerndigitalcorporation/blb/internal/core"
)

// TestRerepl exercises a single rereplicate operation.
func (tc *TestCase) TestRerepl() error {
	// We need at least four tractservers.
	if tc.clusterCfg.Tractservers < 4 {
		return fmt.Errorf("need at least four tractservers for TestRerepl")
	}

	// Create a blob and fill it with one tract of data.
	blob, err := tc.c.Create()
	if err != nil {
		return err
	}

	buf := make([]byte, 1*mb)
	data := makeRandom(1 * mb)
	blob.Seek(0, os.SEEK_SET)
	if n, err := blob.Write(data); err != nil || n != len(data) {
		return err
	}

	// Read the data once just to check.
	blob.Seek(0, os.SEEK_SET)
	if n, err := blob.Read(buf); err != nil {
		return err
	} else if n != len(data) || bytes.Compare(buf, data) != 0 {
		return fmt.Errorf("data mismatch")
	}

	// Kill one of the tractservers holding this blob.
	tid := core.TractIDFromParts(core.BlobID(blob.ID()), 0)
	proc, perr := tc.getTractserverProc(tid, 0)
	if nil != perr {
		return perr
	}
	proc.Stop()

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
	n, werr := blob.Write(buf)
	if werr != nil {
		return werr
	}
	log.Infof("wrote %d bytes", n)

	return nil
}

func (tc *TestCase) triggerRereplRequest(tract core.TractID) error {
	// The easiest way to do this is to just try a write: the client will fail
	// and tell the curator that it failed. Use a non-retrying client since we
	// just want it to happen once.
	nrBlob, err := tc.noRetryC.Open(client.BlobID(tract.Blob), "w")
	if err != nil {
		return err
	}
	nrBlob.Seek(int64(tract.Index)*core.TractLength, os.SEEK_SET)
	if _, err := nrBlob.Write([]byte{0}); err.Error() != core.ErrRPC.String() && err.Error() != core.ErrCorruptData.String() {
		return fmt.Errorf("expected write to fail, got %s", err)
	}
	return nil
}

// Return the info of the give tract.
func (tc *TestCase) getTract(tract core.TractID) (core.TractInfo, error) {
	tracts, err := tc.c.GetTracts(context.Background(), client.BlobID(tract.Blob), int(tract.Index), int(tract.Index)+1)
	if err != nil {
		return core.TractInfo{}, err
	}
	if len(tracts) != 1 {
		return core.TractInfo{}, fmt.Errorf("wrong tract length")
	}
	return tracts[0], nil
}

// Return the proc of the 'host'-th tractserver that hosts the give 'tract'.
func (tc *TestCase) getTractserverProc(tract core.TractID, host int) (cluster.Proc, error) {
	ti, err := tc.getTract(tract)
	if nil != err {
		return nil, err
	}
	log.Infof("tract ended up on %+v", ti.Hosts)

	if host >= len(ti.Hosts) {
		return nil, fmt.Errorf("out of bound %d not in [0, %d)", host, len(ti.Hosts))
	}
	name := tc.bc.FindByServiceAddress(ti.Hosts[host])
	if name == "" {
		return nil, fmt.Errorf("failed to look up process from address %s", ti.Hosts[host])
	}
	proc := tc.bc.FindProc(name)
	if proc == nil {
		return nil, fmt.Errorf("can't find proc %s", name)
	}

	return proc, nil
}
