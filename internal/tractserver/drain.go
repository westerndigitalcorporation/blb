// Copyright (c) 2017 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package tractserver

import (
	"time"

	log "github.com/golang/glog"

	"github.com/westerndigitalcorporation/blb/internal/core"
)

// drainLoop controls remote and local draining for a disk. The drain behavior is
// determined by fields in DiskControlFlags. It will read updates to the control flags
// over a channel, and perform draining at the specified rates.
func (s *Store) drainLoop(diskIndex int, d Disk, dch chan core.DiskControlFlags) {
	const batch = 10

	var flags core.DiskControlFlags
	var open bool
	var ids []core.TractID

	for {
		// Pick up new flags if we have them.
		for {
			select {
			case flags, open = <-dch:
				if !open {
					return // disk was removed
				}
				continue
			default:
			}
			break
		}

		// If nothing to drain, block on an update.
		if flags.Drain == 0 && flags.DrainLocal == 0 {
			ids = nil // ensure that we refresh ids if we start draining again
			flags, open = <-dch
			if !open {
				return // disk was removed
			}
			continue
		}

		if flags.Drain > 0 {
			// If we don't have tracts yet (e.g. we just started draining), get some. Note
			// that if Drain > 0, the store should not be allocating any more tracts to
			// this disk, so no new ones should appear after we grab this list. If there
			// are no tracts left, we'll sleep and try again in a little while.
			if len(ids) == 0 {
				ids = s.GetTractsByDisk(diskIndex)
			}
			// Drain 'batch' seconds worth of tracts.
			toDrain := batch * flags.Drain
			if toDrain > len(ids) {
				toDrain = len(ids)
			}
			s.markTractsDraining(ids[:toDrain])
			ids = ids[toDrain:]
		} else if flags.DrainLocal > 0 {
			log.Errorf("TODO: Local drain not implemented yet")
		}

		time.Sleep(batch * time.Second)
	}
}

// markTractsDraining sets a fake error for the given tracts, causing them to be reported
// to curators as corrupt, which will cause the curator to rereplicate or recover them.
func (s *Store) markTractsDraining(ids []core.TractID) {
	log.Infof("@@@ marking %d tracts for draining: %v", len(ids), ids)
	s.lock.Lock()
	for _, t := range ids {
		s.failures[t] = core.ErrDrainDisk
	}
	s.lock.Unlock()
}
