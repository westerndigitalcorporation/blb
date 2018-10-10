// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package testblb

import (
	"context"
	"fmt"
	"os"
	"time"

	log "github.com/golang/glog"
	"github.com/westerndigitalcorporation/blb/internal/core"
	"github.com/westerndigitalcorporation/blb/pkg/rpc"
)

// TestInterruptedRerepl exercises the situation that might result if a curator
// crashed in the middle of a re-replication, after it had bumped the version on
// some tractservers, but maybe not all, and its durable version number has not
// been updated.
func (tc *TestCase) TestInterruptedRerepl() error {
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
	// directly on a couple tractservers.
	for host := 0; host < 2; host++ {
		if err := tc.bumpVersion(core.TractIDFromParts(core.BlobID(blob.ID()), 0), host); nil != err {
			return err
		}
	}

	// Now try a write. This will fail initially, but it will have called
	// FixVersion on the curator, which will bump the version on the remaining
	// host and update its durable state, so that a retry will succeed.
	blob.Seek(0, os.SEEK_SET)
	if n, err := blob.Write(data); err != nil || n != len(data) {
		log.Infof("write failed: %s", err)
		return err
	}

	return nil
}

func setVersion(addr string, id core.TractID, newVersion int) error {
	cc := rpc.NewConnectionCache(10*time.Second, 10*time.Second, 0)

	var tsid core.TractserverID
	if cc.Send(context.Background(), addr, core.GetTSIDMethod, struct{}{}, &tsid) != nil || tsid < 1 {
		return core.ErrWrongTractserver.Error()
	}

	log.Infof("bumping version of %s on %s to %d", id, addr, newVersion)
	req := core.SetVersionReq{TSID: tsid, ID: id, NewVersion: newVersion}
	var reply core.SetVersionReply
	if cc.Send(context.Background(), addr, core.SetVersionMethod, req, &reply) != nil {
		return core.ErrRPC.Error()
	}
	return reply.Err.Error()
}

// Bump the version of the given 'tract' on the 'host'-th tractserver.
func (tc *TestCase) bumpVersion(tract core.TractID, host int) error {
	ti, err := tc.getTract(tract)
	if nil != err {
		return err
	}
	log.Infof("tract ended up on %+v", ti.Hosts)

	if host >= len(ti.Hosts) {
		return fmt.Errorf("out of bound %d not in [0, %d)", host, len(ti.Hosts))
	}
	if err := setVersion(ti.Hosts[host], ti.Tract, ti.Version+1); err != nil {
		return err
	}
	return nil
}
