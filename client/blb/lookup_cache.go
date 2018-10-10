// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package blb

import (
	"sync"

	"github.com/golang/groupcache/lru"

	"github.com/westerndigitalcorporation/blb/internal/core"
)

type lookupCache struct {
	lock    sync.Mutex
	forward *lru.Cache
	back    map[string]map[core.PartitionID]bool
}

func lookupCacheNew(maxEntries int) *lookupCache {
	lc := &lookupCache{
		back: make(map[string]map[core.PartitionID]bool),
	}
	lc.forward = lru.New(maxEntries)
	lc.forward.OnEvicted = lc.evicted
	return lc
}

// evicted is called when a (key, value) pair is removed from the lru cache,
// either by getting bumped, or explicit removal. We use it to keep our
// backwards map up to date. Since it's only called while the cache is being
// modified, the lock must already be held, so we don't lock here.
func (lc *lookupCache) evicted(ikey lru.Key, ivalue interface{}) {
	key := ikey.(core.PartitionID)
	value := ivalue.(string)
	otherParts := lc.back[value]
	delete(otherParts, key)
	if len(otherParts) == 0 {
		delete(lc.back, value)
	}
}

// put adds a partition->curator mapping to the cache.
func (lc *lookupCache) put(partition core.PartitionID, curator string) {
	lc.lock.Lock()
	defer lc.lock.Unlock()

	// Remove first so that the reverse map gets updated properly.
	lc.forward.Remove(partition)

	// Add to the forward map (cache).
	lc.forward.Add(partition, curator)

	// Update reverse map too.
	if parts, ok := lc.back[curator]; ok {
		parts[partition] = true
	} else {
		lc.back[curator] = map[core.PartitionID]bool{partition: true}
	}
}

// get looks up the curator that we have cached for a given partition.
func (lc *lookupCache) get(partition core.PartitionID) (curator string, ok bool) {
	lc.lock.Lock()
	defer lc.lock.Unlock()
	if v, ok := lc.forward.Get(partition); ok {
		return v.(string), true
	}
	return "", false
}

// invalidate invalidates the mapping for the given partition, and also for all
// other partitions that map to the same curator.
func (lc *lookupCache) invalidate(partition core.PartitionID) {
	lc.lock.Lock()
	defer lc.lock.Unlock()

	// If we had a curator cached for this partition, chances are it's changed
	// for all partitions that we know about. So we can invalidate those also to
	// avoid some failed calls. Note that we don't just change them to this
	// curator, if we did that, and a partition got moved from one curator to
	// another, we would continuously flip back and forth.
	prevCurator, ok := lc.forward.Get(partition)
	if !ok {
		return
	}

	// Copy to slice so we can iterate while modifying the map.
	allPartsMap := lc.back[prevCurator.(string)]
	allParts := make([]core.PartitionID, 0, len(allPartsMap))
	for part := range allPartsMap {
		allParts = append(allParts, part)
	}

	// Note that partition itself must be in allParts, so we'll delete it here.
	for _, part := range allParts {
		// Remove will call lc.evicted and update lc.back here.
		lc.forward.Remove(part)
	}

	// The map should be empty here.
	if len(allPartsMap) != 0 || lc.back[prevCurator.(string)] != nil {
		panic("internal error: reverse map not empty")
	}
}
