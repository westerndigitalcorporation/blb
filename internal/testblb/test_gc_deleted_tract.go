// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package testblb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	log "github.com/golang/glog"

	"github.com/westerndigitalcorporation/blb/internal/core"
	"github.com/westerndigitalcorporation/blb/pkg/rpc"
)

// TestGCDeletedTract exercises gc-ing tracts in a deleted blob.
func (tc *TestCase) TestGCDeletedTract() error {
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

	// Pretend that another blob was created and got deleted, by creating a
	// tract directly on a tractserver. Wait for it to be GC-ed.
	tracts, err := tc.c.GetTracts(context.Background(), blob.ID(), 0, 1)
	if err != nil {
		return err
	}
	ts := tracts[0].Hosts[0]
	// Make sure a valid partition is used for the new blob otherwise the
	// tractserver won't include it in the heartbeat to curator.
	bid := core.BlobIDFromParts(core.BlobID(blob.ID()).Partition(), 45)
	tid := core.TractIDFromParts(bid, 66)
	captureGC := tc.captureLogs()
	if err := createTract(ts, tid); err != nil {
		return err
	}
	log.Infof("waiting for gc...")
	msg := fmt.Sprintf("%s:@@@ gc-ing tract %s, gone", tc.bc.FindByServiceAddress(ts), tid)
	if err := captureGC.WaitFor(msg); err != nil {
		return err
	}

	return nil
}

// createTract directly creates a tract with 'id' on the tractserver at 'addr'
// without going through a curator. As a result, the created tract would appear
// non-existing on the curator side.
func createTract(addr string, id core.TractID) error {
	cc := rpc.NewConnectionCache(10*time.Second, 10*time.Second, 0)

	var tsid core.TractserverID
	if cc.Send(context.Background(), addr, core.GetTSIDMethod, struct{}{}, &tsid) != nil || tsid < 1 {
		return errors.New("wrong tract server")
	}

	log.Infof("creating a tract %s on %s", id, addr)
	req := core.CreateTractReq{TSID: tsid, ID: id}
	var reply core.Error
	if cc.Send(context.Background(), addr, core.CreateTractMethod, &req, &reply) != nil {
		return errors.New("RPC error")
	}
	return reply.Error()
}
