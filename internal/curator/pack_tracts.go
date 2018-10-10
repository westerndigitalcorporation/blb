// Copyright (c) 2017 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package curator

import (
	"sort"
	"sync"

	log "github.com/golang/glog"
	"github.com/westerndigitalcorporation/blb/internal/server"

	"github.com/westerndigitalcorporation/blb/internal/core"
	"github.com/westerndigitalcorporation/blb/internal/curator/durable/state"
)

const (
	// CtlStatTract rpcs to do at once.
	maxStatsInFlight = 100

	// Accept a chunk if it has less than this much empty space.
	// TODO: make dynamically tunable?
	acceptSlop = 0.10

	// Length to pad packed tracts up to. Should probably be equal to the
	// checksumfile block data size.
	padToLength = 64*1024 - 4
)

// Lifecycle:
// Create a new one with makeTractPacker.
// Add tracts with addTract.
// Call doneAdding.
// Call packTracts.
// Call packChunks.
// Call waitForPacking.
type tractPacker struct {
	c       tpContext
	cls     core.StorageClass
	n, m    int
	target  int
	statSem server.Semaphore
	tracts  []*core.PackTractSpec
	stamps  map[tractOnHost]uint64
	stampsL sync.Mutex
	chunks  []packedChunk
	sizeWg  sync.WaitGroup
	encWg   sync.WaitGroup
	done    int
	doneL   sync.Mutex
	opm     *server.OpMetric
}

func makeTractPacker(c tpContext, opm *server.OpMetric, cls core.StorageClass, n, m, target int) *tractPacker {
	ss := server.NewSemaphore(maxStatsInFlight)
	stamps := make(map[tractOnHost]uint64)
	return &tractPacker{c: c, cls: cls, n: n, m: m, target: target, statSem: ss, stamps: stamps, opm: opm}
}

func (tp *tractPacker) addTract(tid core.TractID, from []core.TSAddr, version int) {
	pts := &core.PackTractSpec{
		ID:      tid,
		From:    from,
		Version: version,
		Offset:  -1,
		Length:  -1,
	}
	tp.tracts = append(tp.tracts, pts)
	tp.sizeWg.Add(1)
	go tp.doStat(pts)
}

func (tp *tractPacker) doStat(pts *core.PackTractSpec) {
	defer tp.sizeWg.Done()
	tp.statSem.Acquire()
	defer tp.statSem.Release()

	var versionMismatchHost string

	// Try to stat as many replicas as we can, to collect mod stamps and check
	// that the lengths match. If we can't reach one replica, that's ok, skip
	// it. (It doesn't affect correctness.)
	for _, addr := range pts.From {
		stat := tp.c.CtlStatTract(addr.Host, addr.ID, pts.ID, pts.Version)
		if stat.Err == core.ErrVersionMismatch {
			versionMismatchHost = addr.Host
		}
		if stat.Err != core.NoError {
			log.Infof("tractPacker: error statting tract %s on %s: %s", pts.ID, addr.Host, stat.Err)
			continue
		}
		// Compare subsequent results to first one.
		if pts.Length >= 0 && int(stat.Size) != pts.Length {
			log.Errorf("tractPacker: length mismatch when packing tract %+v", pts)
			pts.Length = -1 // mark as invalid
			break
		}
		pts.Length = int(stat.Size)
		tp.stampsL.Lock()
		tp.stamps[tractOnHost{pts.ID, addr.ID}] = stat.ModStamp
		tp.stampsL.Unlock()
	}

	if pts.Length < 0 && versionMismatchHost != "" {
		// To get here:
		//   1. A tract is available for packing.
		//   2. We couldn't stat the tract at any replica.
		//   3. But at least one replica returned ErrVersionMismatch.
		// This could happen if, for example, we attempted to pack this tract
		// before, we bumped the version on all replicas, but failed to commit
		// our metadata. If we do get in this state, we'd be stuck until a
		// client tries to read or write the tract. To avoid that, we can just
		// do a FixVersion ourselves.
		tp.c.SuggestFixVersion(pts.ID, pts.Version, versionMismatchHost)
	}
}

func (tp *tractPacker) doneAdding() {
	// Wait for all size calls to finish.
	tp.sizeWg.Wait()
	// Don't need lock to read from tp.stamps after this.
}

func (tp *tractPacker) packTracts() {
	if len(tp.tracts) == 0 {
		return
	}

	log.Infof("tractPacker: considering %d tracts", len(tp.tracts))

	// Bin packing using first-fit-decreasing.
	// TODO: should we use a better algorithm here?
	var chunks []packedChunk
	sort.Sort(ptsByLen(tp.tracts)) // Sort by size, largest to smallest.
Outer:
	for _, t := range tp.tracts {
		if t.Length < 0 {
			break // we couldn't get the size of this tract, skip it (and rest because they're sorted)
		}
		paddedLength := ((t.Length + padToLength - 1) / padToLength) * padToLength
		for j := range chunks {
			if chunks[j].length+paddedLength <= tp.target {
				t.Offset = chunks[j].length
				chunks[j].length += paddedLength
				chunks[j].tracts = append(chunks[j].tracts, t)
				continue Outer
			}
		}
		// need to make a new chunk
		t.Offset = 0
		chunks = append(chunks, packedChunk{
			length: paddedLength,
			tracts: append(make([]*core.PackTractSpec, 0, tp.target/core.TractLength+3), t),
		})
	}

	// Sort chunks by how well-packed they are.
	sort.Sort(pcByLen(chunks))

	// Only take ones that are at least 90% full. The rest can wait until the
	// next round.
	slop := int(float32(tp.target) * acceptSlop)
	idx := sort.Search(len(chunks), func(i int) bool {
		return (tp.target - chunks[i].length) > slop
	})
	tp.chunks = chunks[:idx]

	log.Infof("tractPacker: packed into %d chunks, %d are sufficient size", len(chunks), idx)
}

func (tp *tractPacker) packChunks() {
	// Allocate a bunch of chunk ids all at once.
	count := len(tp.chunks) / tp.n
	if count == 0 {
		return
	}
	idsNeeded := count * (tp.n + tp.m)
	baseChunkID, err := tp.c.AllocateRSChunkIDs(idsNeeded)
	if err != core.NoError {
		log.Errorf("tractPacker: couldn't get %d rs chunk ids", idsNeeded)
		return
	}

	tp.c.MarkPending(baseChunkID, idsNeeded)

	// Work on each RS chunk in parallel.
	tp.encWg.Add(count)
	for len(tp.chunks) >= tp.n {
		op := &rsEncodeOp{
			base: baseChunkID,
			data: tp.chunks[:tp.n],
		}
		tp.chunks = tp.chunks[tp.n:]
		baseChunkID = baseChunkID.Add(tp.n + tp.m)
		go tp.doEncode(op)
	}
}

func (tp *tractPacker) waitForPacking() int {
	tp.encWg.Wait()
	// Don't need lock to read from tp.done after this.
	return tp.done
}

func (tp *tractPacker) doEncode(op *rsEncodeOp) (err core.Error) {
	defer tp.encWg.Done()
	defer tp.c.UnmarkPending(op.base, tp.n+tp.m)

	log.Infof("tractPacker: starting work on RS chunk %s", op.base)
	measurer := tp.opm.Start("RSEncode")

	stages := []func(op *rsEncodeOp) core.Error{
		tp.encPick,   // pick tractservers
		tp.encPack,   // call PackTracts on the data tracts
		tp.encEncode, // call RSEncode on one to compute parity
		tp.encBump,   // check that nothing has been modified and bump versions
		tp.encCommit, // commit to raft
	}
	for _, f := range stages {
		if err = f(op); err != core.NoError {
			measurer.EndWithBlbError(&err)
			tp.encCleanupAfterFailure(op)
			return
		}
	}

	measurer.End()
	log.Infof("tractPacker: committed RS chunk %s", op.base)
	tp.doneL.Lock()
	tp.done++
	tp.doneL.Unlock()
	return
}

func (tp *tractPacker) encPick(op *rsEncodeOp) (err core.Error) {
	op.hosts, op.tsids = tp.c.AllocateTS(tp.n + tp.m)
	if op.hosts == nil || op.tsids == nil {
		log.Errorf("tractPacker: couldn't allocate ts ids")
		return core.ErrAllocHost
	}
	return core.NoError
}

func (tp *tractPacker) encPack(op *rsEncodeOp) (lastErr core.Error) {
	// Do all the PackTracts.
	log.Infof("tractPacker: starting packing tracts for %s", op.base)

	measurer := tp.opm.Start("RSEncode/pack")
	defer measurer.EndWithBlbError(&lastErr)

	errs := make([]core.Error, len(op.data))
	var wg sync.WaitGroup
	wg.Add(len(op.data))
	for i := range op.data {
		go func(i int) {
			chunk := op.data[i]

			log.Infof("tractPacker: starting packing tracts for %s on %s (%d)", op.base, op.hosts[i], len(chunk.tracts))
			errs[i] = tp.c.PackTracts(op.hosts[i], op.tsids[i], tp.target, chunk.tracts, op.base.Add(i))
			log.Infof("tractPacker: finished packing tracts for %s on %s", op.base, op.hosts[i])

			wg.Done()
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != core.NoError {
			log.Errorf("tractPacker: error from PackTracts to %s: %s", op.hosts[i], err)
			lastErr = err
		}
	}
	log.Infof("tractPacker: finished packing tracts for %s", op.base)
	return
}

func (tp *tractPacker) encEncode(op *rsEncodeOp) (err core.Error) {
	measurer := tp.opm.Start("RSEncode/encode")
	defer measurer.EndWithBlbError(&err)

	// Do the encode.
	tsAddrs := op.zipAddrs()
	dest0 := tsAddrs[tp.n] // first parity chunk destination
	log.Infof("tractPacker: started RSEncode for %s on %s", op.base, dest0.Host)
	err = tp.c.RSEncode(dest0.Host, dest0.ID, op.base, tp.target, tsAddrs[:tp.n], tsAddrs[tp.n:])
	if err != core.NoError {
		log.Errorf("tractPacker: error from RSEncode for %s on %s: %s", op.base, dest0.Host, err)
	} else {
		log.Infof("tractPacker: finished RSEncode for %s on %s", op.base, dest0.Host)
	}
	return
}

func (tp *tractPacker) encBump(op *rsEncodeOp) (err core.Error) {
	measurer := tp.opm.Start("RSEncode/bump")
	defer measurer.EndWithBlbError(&err)

	// Double-check that none of these tracts were written to, and bump version
	// at the same time to prevent future write with stale metadata from
	// succeeding.
	var wg sync.WaitGroup
	errs := make([]core.Error, len(tp.stamps))
	i := 0

	for _, chunk := range op.data {
		for _, tract := range chunk.tracts {
			thisTractIdx := i
			for _, addr := range tract.From {
				if stamp, ok := tp.stamps[tractOnHost{tract.ID, addr.ID}]; ok {
					// If we collected a stamp from this host, then do a
					// conditional version bump here.
					wg.Add(1)
					go tp.encBumpOne(&wg, &errs[i], addr, tract, stamp)
					i++
				}
			}
			if thisTractIdx == i {
				// We must bump at least one replica for each tract. This is
				// just a sanity check: if we didn't have any sources for
				// this tract we shouldn't have gotten here at all.
				log.Errorf("tractPacker: didn't find any replicas to bump for %s", tract.ID)
				wg.Wait()
				err = core.ErrUnknown
				return
			}
		}
	}
	wg.Wait()

	for _, err = range errs[:i] {
		if err != core.NoError {
			return
		}
	}
	return
}

func (tp *tractPacker) encBumpOne(wg *sync.WaitGroup, ret *core.Error, addr core.TSAddr, pts *core.PackTractSpec, stamp uint64) {
	defer wg.Done()
	tp.statSem.Acquire()
	defer tp.statSem.Release()
	*ret = tp.c.SetVersion(addr.Host, addr.ID, pts.ID, pts.Version+1, stamp)
}

func (tp *tractPacker) encCommit(op *rsEncodeOp) (err core.Error) {
	data := make([][]state.EncodedTract, len(op.data))
	for i, c := range op.data {
		ets := make([]state.EncodedTract, len(c.tracts))
		for j, t := range c.tracts {
			ets[j] = state.EncodedTract{ID: t.ID, Offset: t.Offset, Length: t.Length, NewVersion: t.Version + 1}
		}
		data[i] = ets
	}
	err = tp.c.CommitRSChunk(op.base, tp.cls, op.tsids, data)
	if err != core.NoError {
		log.Errorf("tractPacker: error committing RS chunk %s: %s", op.base, err)
	}
	return
}

func (tp *tractPacker) encCleanupAfterFailure(op *rsEncodeOp) {
	// If we failed to pack, encode, and commit, then the packed data and parity
	// chunks are useless. Try to delete them from tractservers. If this fails
	// or we crash, GC will take care of it later.
	log.Infof("tractPacker: cleaning up after failure for %s", op.base)
	for i := range op.hosts {
		go tp.c.DelTract(op.hosts[i], op.tsids[i], op.base.Add(i).ToTractID())
	}
}

// Sort by size, largest to smallest.
type ptsByLen []*core.PackTractSpec

func (s ptsByLen) Len() int           { return len(s) }
func (s ptsByLen) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s ptsByLen) Less(i, j int) bool { return s[i].Length > s[j].Length }

type packedChunk struct {
	tracts []*core.PackTractSpec
	length int
}

// Sort by length, largest to smallest.
type pcByLen []packedChunk

func (s pcByLen) Len() int           { return len(s) }
func (s pcByLen) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s pcByLen) Less(i, j int) bool { return s[i].length > s[j].length }

type rsEncodeOp struct {
	base  core.RSChunkID
	data  []packedChunk
	tsids []core.TractserverID
	hosts []string
}

func (op *rsEncodeOp) zipAddrs() []core.TSAddr {
	return zipAddrs(op.tsids, op.hosts)
}

func zipAddrs(tsids []core.TractserverID, hosts []string) (out []core.TSAddr) {
	out = make([]core.TSAddr, len(tsids))
	for i := range tsids {
		out[i].ID = tsids[i]
		out[i].Host = hosts[i]
	}
	return
}

type tractOnHost struct {
	tract core.TractID
	tsid  core.TractserverID
}
