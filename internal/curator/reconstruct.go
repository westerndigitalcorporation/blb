// Copyright (c) 2017 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package curator

import (
	log "github.com/golang/glog"

	"github.com/westerndigitalcorporation/blb/internal/core"
	"github.com/westerndigitalcorporation/blb/internal/curator/durable/state/fb"
)

// reconstructChunk sends an RPC to one tractserver asking it to reconstruct the
// missing pieces of an RS chunk, and updates durable state if it succeeds.
func (c *Curator) reconstructChunk(id core.RSChunkID, badIds []core.TractserverID) core.Error {
	term := c.stateHandler.GetTerm()
	chunk := c.stateHandler.GetRSChunk(id)
	if chunk == nil {
		return core.ErrInvalidArgument
	}

	n := chunk.DataLength()
	m := chunk.HostsLength() - n

	// Figure out what TSIDs we're keeping in the set. Note that this will end
	// up in the same order as Hosts, data followed by parity.
	var okIds []core.TractserverID
	var okIdx, dstIdx []int
	for i := 0; i < chunk.HostsLength(); i++ {
		id := core.TractserverID(chunk.Hosts(i))
		if contains(badIds, id) {
			dstIdx = append(dstIdx, i)
		} else {
			okIds = append(okIds, id)
			okIdx = append(okIdx, i)
		}
	}

	// We shouldn't have gotten here (recovery loop should filter this out).
	if len(okIds) < n {
		log.Errorf("not enough good pieces to recover rs chunk %s", id)
		return core.ErrAllocHost
	}

	// If we're all good, there's nothing to do.
	if len(dstIdx) == 0 {
		log.Errorf("no work to do for recovering rs chunk %s", id)
		return core.ErrInvalidArgument
	}

	// Use the first n good tractservers as sources.
	srcIds := okIds[:n]
	srcIdx := okIdx[:n]
	srcHosts, missing := c.tsMon.getTractserverAddrs(srcIds)
	if missing > 0 {
		log.Errorf("couldn't get addrs for good replicas for %s (%v)", id, srcHosts)
		return core.ErrHostNotExist
	}

	// We need to get back to n+m. dstIdx has the indexes of the bad pieces.
	dstHosts, dstIds := c.allocateTS(len(dstIdx), okIds, badIds)
	if dstHosts == nil {
		log.Errorf("couldn't allocate TSs to replace bad pieces for %s (%v)", id, badIds)
		return core.ErrAllocHost
	}

	// Pad to up m dests.
	for len(dstIds) < m {
		dstHosts = append(dstHosts, "")
		dstIds = append(dstIds, 0)
		dstIdx = append(dstIdx, -1)
	}

	// Ensure the new data doesn't get GC-ed before we commit the host change.
	c.markPendingPieces(id, n+m)
	defer c.unmarkPendingPieces(id, n+m)

	// Ask the first one to do the operation.
	indexMap := append(srcIdx, dstIdx...)
	err := c.tt.RSEncode(
		dstHosts[0], dstIds[0],
		id, RSPieceLength,
		zipAddrs(srcIds, srcHosts),
		zipAddrs(dstIds, dstHosts),
		indexMap)
	if err != core.NoError {
		return err
	}

	// Commit new hosts.
	newHosts := fb.HostsList(chunk)
	for i, idx := range dstIdx {
		if idx >= 0 {
			newHosts[idx] = dstIds[i]
		}
	}
	err = c.stateHandler.UpdateRSHosts(id, newHosts, term)
	if err != core.NoError {
		return err
	}

	log.Infof("@@@ reconstruction %v succeeded", id)
	return core.NoError
}
