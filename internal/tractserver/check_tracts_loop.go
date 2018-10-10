// Copyright (c) 2016 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package tractserver

import (
	"context"
	"os"

	log "github.com/golang/glog"
	"github.com/westerndigitalcorporation/blb/internal/core"
)

func (s *Store) checkTractsLoop() {
	for {
		tracts := <-s.checkTractsCh
		missing := s.Check(tracts)

		log.Infof("Checked tracts. %d out of %d are missing.", len(missing), len(tracts))

		s.lock.Lock()
		// Simply mark missing tracts as corrupted. Tract server will ask curator
		// to re-replicate them.
		for _, t := range missing {
			s.failures[t.ID] = core.ErrCorruptData
		}
		s.lock.Unlock()
	}
}

// Check verifies that we have 'tracts', and our version for the tracts is at least the version provided.
// If we're missing the tract or our version is too old, we report it as missing. PL-1114
func (s *Store) Check(tracts []core.TractState) (missing []core.TractState) {
	for _, t := range tracts {
		// TODO: We're not really checking the tract if we can't eventually lock it.
		// If it's locked, we have it, but it's not clear if we have the right version.
		if s.tryLockTract(t.ID, READ) {
			et := s.openExistingTract(context.TODO(), t.ID, os.O_RDONLY)
			v := et.getVersion()
			s.closeErrTract(et)
			s.unlock(t.ID, READ)

			// Note that version can be >= t.Version.  This suggests that the curator failed
			// while bumping the version on the tract, or that this Check message was very
			// delayed & the version has changed multiple times.
			if et.err != core.NoError || v < t.Version {
				log.Errorf("@@@ check tract failed on tract %s.", t.ID)
				missing = append(missing, t)
			}
		}
	}
	return missing
}
