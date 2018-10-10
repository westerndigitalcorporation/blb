// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package curator

import (
	"time"

	log "github.com/golang/glog"
	"github.com/westerndigitalcorporation/blb/internal/core"
	pb "github.com/westerndigitalcorporation/blb/internal/curator/durable/state/statepb"
)

const (
	// We need to iterate tracts metadata in curator's state to find out which
	// tracts are missing on tract servers. We'll aim to send RPCs with
	// approximately this many tract/piece ids.
	//
	// Assume we have 8M tracts per TS and check 800 every 2 seconds. Then we'll
	// finish a full round of checking every ~6 hours.
	checksAtOnce = 800

	// The interval of between batches of CheckTracts calls.
	iterationInterval = 2 * time.Second
)

func (c *Curator) checkTractsLoop() {
	ticker := time.NewTicker(c.config.SyncInterval)

	// Wait on startup for heartbeats to come in.
	time.Sleep(15 * time.Second)

	for {
		c.blockIfNotLeader()

		op := c.internalOpM.Start("CheckAllTracts")
		// Perform one round of check on the snapshot of current curator's state.
		err := c.checkTracts()
		op.EndWithBlbError(&err)
		if err != core.NoError {
			log.Errorf("Got error while checking tracts: %s", err)
		}

		// Don't spin if there isn't much work to be done.
		<-ticker.C
	}
}

// checkTracts checks if tracts that exist in curator's state also exist on
// tract servers. The check will be performed on a snapshot of curator's state
// and it returns either the curator steps down as leader or the whole snapshot
// of state has been checked.
func (c *Curator) checkTracts() core.Error {
	var healthy []core.TSAddr
	var checks map[core.TractserverID][]core.TractState

	// We only try to sync with healthy tractservers.
	init := func() {
		healthy = c.tsMon.getHealthy()
		checks = make(map[core.TractserverID][]core.TractState, len(healthy))
		for _, s := range healthy {
			checks[s.ID] = nil
		}
	}
	init()

	// We want to aim for approximately checksAtOnce tract/piece ids per RPC, so
	// keep adding ids until we have more than checksAtOnce per tractserver.
	// This assumes that tracts and pieces are roughly equally distributed
	// across tractservers.
	nChecks := 0

	addTracts := func(id core.TractID, tract *pb.Tract) {
		ts := core.TractState{ID: id, Version: tract.Version}
		for _, host := range tract.Hosts {
			// Only check for tract servers that are marked as healthy.
			if have, ok := checks[host]; ok {
				checks[host] = append(have, ts)
				nChecks++
			}
		}
	}

	addPieces := func(baseID core.RSChunkID, tract *pb.RSChunk) {
		for i, host := range tract.Hosts {
			ts := core.TractState{ID: baseID.Add(i).ToTractID(), Version: core.RSChunkVersion}
			// Only check for tract servers that are marked as healthy.
			if have, ok := checks[host]; ok {
				checks[host] = append(have, ts)
				nChecks++
			}
		}
	}

	doCalls := func() bool {
		if !c.stateHandler.IsLeader() {
			// Stop checking tracts once the curator steps down as leader.
			return false
		}
		if nChecks >= 0 && nChecks < checksAtOnce*len(healthy) {
			return true
		}
		nChecks = 0

		// Doing CheckTracts RPC in parallel.
		for tsid, tracts := range checks {
			if len(tracts) == 0 {
				// No tracts needs to be checked on this tract server.
				continue
			}

			go func(tsid core.TractserverID, tracts []core.TractState, healthy []core.TSAddr) {
				// Find out the address according to the tract server ID.
				addr := lookupAddrByID(healthy, tsid)
				if addr == "" {
					return
				}

				log.Infof("sending ts %d@%s %d tracts to check", tsid, addr, len(tracts))

				// Ask the tractserver if it's got the tracts. The check will be done asynchronously
				// on tract servers and the results will be piggybacked in heartbeat messages from
				// tract servers to curators.
				if err := c.tt.CheckTracts(addr, tsid, tracts); err != core.NoError {
					log.Errorf("error sending CheckTracts to tsid %d at %s: %v", tsid, addr, err)
				}
			}(tsid, tracts, healthy)
		}

		time.Sleep(iterationInterval)

		// Reinitialize healthy and checks from tsmon state.
		init()
		return true
	}

	c.stateHandler.ForEachTract(addTracts, doCalls)
	c.stateHandler.ForEachRSChunk(addPieces, doCalls)

	// Finish final round.
	nChecks = -1
	doCalls()

	return core.NoError
}

func lookupAddrByID(pairs []core.TSAddr, id core.TractserverID) string {
	for _, s := range pairs {
		if s.ID == id {
			return s.Host
		}
	}
	return ""
}
