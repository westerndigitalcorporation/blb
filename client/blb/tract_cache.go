// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package blb

import (
	"sort"
	"sync"

	"github.com/golang/groupcache/lru"

	"github.com/westerndigitalcorporation/blb/internal/core"
)

type tractInfoSlice []core.TractInfo

type tractCache struct {
	lock  sync.Mutex
	cache *lru.Cache
}

func tractCacheNew(maxEntries int) *tractCache {
	return &tractCache{cache: lru.New(maxEntries)}
}

func (lc *tractCache) put(blob core.BlobID, tracts []core.TractInfo) {
	lc.lock.Lock()
	defer lc.lock.Unlock()

	v, ok := lc.cache.Get(blob)
	if !ok {
		// Nothing there yet, just drop them in.
		lc.cache.Add(blob, tractInfoSlice(tracts))
	} else {
		// Merge in using a map.
		cache := v.(tractInfoSlice)
		cacheMap := make(map[core.TractKey]core.TractInfo, len(cache)+len(tracts))
		for _, ti := range cache {
			cacheMap[ti.Tract.Index] = ti
		}
		for _, ti := range tracts {
			cacheMap[ti.Tract.Index] = ti
		}

		newCache := make(tractInfoSlice, 0, len(cacheMap))
		for _, v := range cacheMap {
			newCache = append(newCache, v)
		}
		sort.Sort(newCache)

		lc.cache.Add(blob, newCache)
	}
}

func (lc *tractCache) get(blob core.BlobID, start, end int) (out []core.TractInfo, ok bool) {
	lc.lock.Lock()
	defer lc.lock.Unlock()

	v, ok := lc.cache.Get(blob)
	if !ok {
		// Definitely not in the cache.
		return nil, false
	}

	// We have some tracts cached, but maybe not these.

	// Find potential start.
	cache := v.(tractInfoSlice)
	idx := sort.Search(len(cache), func(i int) bool {
		return int(cache[i].Tract.Index) >= start
	})
	// Enough room?
	if len(cache)-idx < end-start {
		return nil, false
	}
	// They have to be contiguous, so we can just check tract keys.
	for i := start; i < end; i++ {
		if int(cache[idx+i-start].Tract.Index) != i {
			return nil, false
		}
	}

	// We can satisfy the whole request.
	return cache[idx : idx+end-start], true
}

func (lc *tractCache) invalidate(blob core.BlobID) {
	lc.lock.Lock()
	defer lc.lock.Unlock()
	lc.cache.Remove(blob)
}

func (p tractInfoSlice) Len() int           { return len(p) }
func (p tractInfoSlice) Less(i, j int) bool { return p[i].Tract.Index < p[j].Tract.Index }
func (p tractInfoSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
