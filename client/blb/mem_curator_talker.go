// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package blb

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strconv"
	"sync"

	"github.com/westerndigitalcorporation/blb/internal/core"
)

// memBlobInfo holds the data for one blob in memory.
type memBlobInfo struct {
	repl   int              // Replication factor
	tracts []core.TractInfo // Where are my tracts
}

// memCurator simulates a fake curator in memory.
type memCurator struct {
	partition core.PartitionID              // What partition am I responsible for
	nextBlob  core.BlobKey                  // Next unused blob key
	blobs     map[core.BlobKey]*memBlobInfo // Tract location data
	nextTSID  core.TractserverID            // Next unused tractserver id
}

// extendTo ensures that the given blob has at least numTracts tracts.
func (bi *memBlobInfo) extendTo(id core.BlobID, numTracts int, tsid core.TractserverID) (newTracts []core.TractInfo) {
	for i := 0; i < numTracts-len(bi.tracts); i++ {
		tractID := core.TractID{Blob: id, Index: core.TractKey(len(bi.tracts) + i)}
		var tsids []core.TractserverID
		var hosts []string
		for r := 0; r < bi.repl; r++ {
			tsids = append(tsids, tsid)
			tsid++
			// Use a fixed naming scheme for hosts:
			hosts = append(hosts, fmt.Sprintf("ts-%s-%d", tractID, r))
		}
		ti := core.TractInfo{Tract: tractID, Version: 1, Hosts: hosts, TSIDs: tsids}
		newTracts = append(newTracts, ti)
	}
	return
}

// ackExtend commit the new tracts to the give blob.
func (bi *memBlobInfo) ackExtend(id core.BlobID, newTracts []core.TractInfo) {
	bi.tracts = append(bi.tracts, newTracts...)
}

// memCuratorTalker manages a set of fake curators in memory.
// The addresss used must use the same convention as in newMemMasterConnection:
// they should be an integer that represents the partition id the curator will
// be responsible for.
type memCuratorTalker struct {
	lock     sync.Mutex
	curators map[string]*memCurator
}

// CreateBlob creates a new blob.
func (cc *memCuratorTalker) CreateBlob(ctx context.Context, addr string, metadata core.BlobInfo) (core.BlobID, core.Error) {
	cc.lock.Lock()
	defer cc.lock.Unlock()
	tc := cc.getCurator(addr)

	blobKey := tc.nextBlob
	blob := core.BlobIDFromParts(tc.partition, blobKey)
	tc.nextBlob++

	bi := &memBlobInfo{repl: metadata.Repl}
	tc.blobs[blobKey] = bi

	return blob, core.NoError
}

// ExtendBlob ensures that the given blob has at least numTracts tracts.
func (cc *memCuratorTalker) ExtendBlob(ctx context.Context, addr string, blob core.BlobID, numTracts int) ([]core.TractInfo, core.Error) {
	cc.lock.Lock()
	defer cc.lock.Unlock()
	tc := cc.getCurator(addr)

	bi, ok := tc.blobs[blob.ID()]
	if !ok {
		return nil, core.ErrNoSuchBlob
	}
	tsid := tc.nextTSID
	tc.nextTSID += core.TractserverID(numTracts)
	return bi.extendTo(blob, numTracts, tsid), core.NoError
}

func (cc *memCuratorTalker) AckExtendBlob(ctx context.Context, addr string, blob core.BlobID, tracts []core.TractInfo) core.Error {
	cc.lock.Lock()
	defer cc.lock.Unlock()
	tc := cc.getCurator(addr)

	bi, ok := tc.blobs[blob.ID()]
	if !ok {
		return core.ErrNoSuchBlob
	}
	bi.ackExtend(blob, tracts)
	return core.NoError
}

// DeleteBlob deletes the given blob.
func (cc *memCuratorTalker) DeleteBlob(ctx context.Context, addr string, blob core.BlobID) core.Error {
	cc.lock.Lock()
	defer cc.lock.Unlock()
	tc := cc.getCurator(addr)

	if _, ok := tc.blobs[blob.ID()]; !ok {
		return core.ErrNoSuchBlob
	}
	delete(tc.blobs, blob.ID())
	return core.NoError
}

// UndeleteBlob always fails to undelete the blob.
func (cc *memCuratorTalker) UndeleteBlob(ctx context.Context, addr string, blob core.BlobID) core.Error {
	return core.ErrNoSuchBlob
}

func (cc *memCuratorTalker) SetMetadata(ctx context.Context, addr string, blob core.BlobID, md core.BlobInfo) core.Error {
	return core.ErrNotYetImplemented
}

// GetTracts returns the tract location for the given range.
func (cc *memCuratorTalker) GetTracts(ctx context.Context, addr string, blob core.BlobID, start, end int,
	forRead, forWrite bool) ([]core.TractInfo, core.Error) {
	cc.lock.Lock()
	defer cc.lock.Unlock()
	tc := cc.getCurator(addr)

	bi, ok := tc.blobs[blob.ID()]
	if !ok {
		return nil, core.ErrNoSuchBlob
	}
	if start < 0 || end < 0 {
		return nil, core.ErrInvalidArgument
	}
	clip := func(a int) int {
		if a > len(bi.tracts) {
			return len(bi.tracts)
		}
		return a
	}
	return bi.tracts[clip(start):clip(end)], core.NoError
}

// StatBlob returns the number of tracts in a blob.
func (cc *memCuratorTalker) StatBlob(ctx context.Context, addr string, blob core.BlobID) (core.BlobInfo, core.Error) {
	cc.lock.Lock()
	defer cc.lock.Unlock()
	tc := cc.getCurator(addr)

	bi, ok := tc.blobs[blob.ID()]
	if !ok {
		return core.BlobInfo{}, core.ErrNoSuchBlob
	}
	return core.BlobInfo{Repl: bi.repl, NumTracts: len(bi.tracts)}, core.NoError
}

// ReportBadTS does nothing.
func (cc *memCuratorTalker) ReportBadTS(ctx context.Context, addr string, id core.TractID, bad, op string, got core.Error, couldRecover bool) core.Error {
	return core.ErrNotYetImplemented
}

// FixVersion also does nothing.
func (cc *memCuratorTalker) FixVersion(ctx context.Context, addr string, tract core.TractInfo, bad string) core.Error {
	return core.ErrNotYetImplemented
}

// getCurator returns a fake curator that keeps its data in memory.
func (cc *memCuratorTalker) getCurator(addr string) *memCurator {
	if tc, ok := cc.curators[addr]; ok {
		return tc
	}
	partition, err := strconv.Atoi(addr)
	if err != nil {
		log.Fatalf("%s cannot be converted to integer", addr)
	}
	tc := &memCurator{
		partition: core.PartitionID(partition),
		nextBlob:  1,
		blobs:     make(map[core.BlobKey]*memBlobInfo),
	}
	cc.curators[addr] = tc
	return tc
}

func (cc *memCuratorTalker) ListBlobs(ctx context.Context, addr string, partition core.PartitionID, start core.BlobKey) (keys []core.BlobKey, err core.Error) {
	cc.lock.Lock()
	defer cc.lock.Unlock()
	tc := cc.getCurator(addr)
	if tc.partition == partition {
		for key := range tc.blobs {
			if key >= start {
				keys = append(keys, key)
			}
		}
	}
	sort.Sort(bkSlice(keys))
	// return only three at a time to exercise more logic
	if len(keys) > 3 {
		keys = keys[:3]
	}
	return
}

func newMemCuratorTalker() CuratorTalker {
	return &memCuratorTalker{curators: make(map[string]*memCurator)}
}

type bkSlice []core.BlobKey

func (p bkSlice) Len() int           { return len(p) }
func (p bkSlice) Less(i, j int) bool { return p[i] < p[j] }
func (p bkSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
