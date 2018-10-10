// Copyright (c) 2017 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package curator

import (
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/westerndigitalcorporation/blb/internal/core"
	pb "github.com/westerndigitalcorporation/blb/internal/curator/durable/state/statepb"
	"github.com/westerndigitalcorporation/blb/internal/curator/storageclass"
)

const (
	// RSPieceLength is how large we should aim to make each chunk piece. This
	// funny value will ensure that each piece comes out <= 64MiB after
	// ChecksumFile and any future overhead (let's reserve 64 bytes per 64KB).
	// But it means that we can only fit 7.992 tracts in a chunk, so we'll need
	// to have some partial tracts to make best use of our space.
	RSPieceLength = 64*1024*1024 - 64*1024 - 64

	// REPLICATED is just copied here to make some code slightly shorter.
	REPLICATED = core.StorageClass_REPLICATED
)

// Picks a storage class for the blob. We can change this over time.
func targetClass(blob *pb.Blob, now int64, delay time.Duration) core.StorageClass {
	// If the blob has been created or written to recently, keep it replicated.
	if now-blob.GetMtime() < int64(delay) {
		return REPLICATED
	}

	// Otherwise, look at the hint.
	switch blob.GetHint() {
	case core.StorageHint_HOT:
		return REPLICATED
	case core.StorageHint_WARM:
		return core.StorageClass_RS_6_3
	case core.StorageHint_COLD:
		return core.StorageClass_RS_8_3
	}

	return REPLICATED
}

// Looks for blobs that should change storage class and migrates them.
func (c *Curator) storageClassLoop() {
	for {
		c.blockIfNotLeader()

		op := c.internalOpM.Start("StorageClassLoop")

		var wg sync.WaitGroup
		now := time.Now().UnixNano()
		term := c.stateHandler.GetTerm()
		packers := c.makePackers(term)
		var cleanedUp, alreadyDone, committed int

		c.stateHandler.ForEachBlob(false, func(id core.BlobID, blob *pb.Blob) {
			current := blob.GetStorage()
			target := targetClass(blob, now, c.config.WriteDelay)
			if current == target {
				// The blob is stored correctly. We might need to clean up old
				// tract storage.
				if hasExtraStorage(blob, current) {
					wg.Add(1)
					// Updating storage class to the same thing will also clean
					// up old storage.
					go c.updateStorageClass(id, current, &wg, term)
					cleanedUp++
				}
			} else if tractsAreStoredAsClass(blob, target) {
				// We've already done the migration work for all the tracts,
				// update the blob's class now.
				wg.Add(1)
				go c.updateStorageClass(id, target, &wg, term)
				alreadyDone++
			} else if current == REPLICATED && target != REPLICATED {
				// Migrating from replicated to RS.
				c.addTractsToPacker(id, blob, packers[target])
			} else if current != REPLICATED && target == REPLICATED {
				// Migrating from RS back to replicated.
				log.Infof("migrating from RS to replicated is not implemented yet (%s)", id)
			} else {
				// Migrating from one RS class to another.
				log.Infof("migrating from RS to RS is not implemented yet (%s)", id)
			}
		}, c.stateHandler.IsLeader)

		// Kick off all RS encode operations.
		for _, p := range packers {
			if p != nil {
				p.doneAdding()
				p.packTracts()
				p.packChunks()
			}
		}
		// Wait for them to finish.
		for _, p := range packers {
			if p != nil {
				committed += p.waitForPacking()
			}
		}
		// Also wait for updateStorageClass calls to finish.
		wg.Wait()

		op.End()

		if cleanedUp+alreadyDone+committed == 0 {
			// If we didn't do anything, sleep a little so we don't spin.
			time.Sleep(10 * time.Second)
		} else {
			log.Infof("storage class loop: committed %d rs chunks, "+
				"updated storage class of %d, cleaned up %d", committed, alreadyDone, cleanedUp)
		}
	}
}

func (c *Curator) makePackers(term uint64) (ps []*tractPacker) {
	ctx := &curatorTPContext{c: c, term: term}
	ps = make([]*tractPacker, len(core.StorageClass_name))
	for _, cls := range storageclass.AllRS {
		N, M := cls.RSParams()
		ps[cls.ID()] = makeTractPacker(ctx, c.internalOpM, cls.ID(), N, M, RSPieceLength)
	}
	return
}

// Returns true if any tract in the blob has storage that it doesn't need.
func hasExtraStorage(blob *pb.Blob, cls core.StorageClass) bool {
	for _, tract := range blob.Tracts {
		for _, c := range storageclass.All {
			if c.ID() != cls && c.Has(tract) {
				return true
			}
		}
	}
	return false
}

// Returns true if all tracts in the blob support the given storage class.
func tractsAreStoredAsClass(blob *pb.Blob, cls core.StorageClass) bool {
	c := storageclass.Get(cls)
	for _, tract := range blob.Tracts {
		if !c.Has(tract) {
			return false
		}
	}
	return true
}

func (c *Curator) updateStorageClass(id core.BlobID, target core.StorageClass, wg *sync.WaitGroup, term uint64) {
	err := c.stateHandler.UpdateStorageClass(id, target, term)
	if err != core.NoError {
		log.Errorf("error updating storage class of %s to %s: %s", id, target, err)
	} else {
		log.Infof("updated storage class of blob %s to %s", id, target)
	}
	wg.Done()
}

func (c *Curator) addTractsToPacker(id core.BlobID, blob *pb.Blob, packer *tractPacker) {
	cls := storageclass.Get(packer.cls)
	for i, t := range blob.Tracts {
		if cls.Has(t) {
			continue // Already stored as this class, ignore.
		}
		tid := core.TractIDFromParts(id, core.TractKey(i))
		from := c.tsMon.makeTSAddrs(t.Hosts)
		packer.addTract(tid, from, t.Version)
	}
}
