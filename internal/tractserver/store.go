// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package tractserver

import (
	"context"
	"errors"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/klauspost/reedsolomon"

	log "github.com/golang/glog"
	"github.com/westerndigitalcorporation/blb/internal/core"
	"github.com/westerndigitalcorporation/blb/pkg/rpc"
)

var (
	ErrDiskExists   = errors.New("A disk with that root already exists")
	ErrDiskNotFound = errors.New("A disk with that root doesn't exist")
	ErrTooManyDisks = errors.New("Too many disks!")
)

// Store is a tract store.  A tract store is a replication layer on top of a durable local disk layer.
type Store struct {
	// The disks we're storing data on.
	disks [maxDisks]diskInfo

	// The next disk to create a tract on.
	nextDisk int

	// What's busy?
	// We keep this separate from 'tracts' because sometimes we want to lock
	// a tract that doesn't exist (create), and/or have the lock last for longer
	// than the tract (pull tract overwrite).
	// Positive value is number of active readers, -1 means one active writer,
	// any other value is invalid.
	busy     map[core.TractID]int32
	busyLock sync.Mutex
	busyCond sync.Cond

	// Per-tract data. What disk is a tract on, modification stamp.
	tracts map[core.TractID]tractData

	// Failures that we've seen.
	// We report failures to the curator until we're told to GC the tract.
	failures map[core.TractID]core.Error

	// Synchronizes disks, nextDisk, and all maps above that don't have their own lock.
	lock sync.Mutex

	// Used to contact other tractservers for re-replication.
	tt TractserverTalker

	// Some information about this tractserver.
	metadata *MetadataStore

	// Global configuration for a tract server.
	// Value is immutable, this pointer is protected by lock.
	config *Config

	// Channel for checking tracts request.
	checkTractsCh chan []core.TractState

	// Initial value of tract mod stamps.
	initialStamp uint64
}

type diskInfo struct {
	root    string
	d       Disk // nil if unused
	drainCh chan core.DiskControlFlags
}

const (
	// We combine a disk index and mod stamp into a 64 bit value to space space.
	// Using six bits for the index means we can have up to 64 disks in total.
	diskBits = 6
	maxDisks = 1 << diskBits
	diskMask = ^uint64(0) >> (64 - diskBits)
)

// Lower diskBits is index into disks, upper bits are mod stamp.
type tractData uint64

func makeTractData(disk int, stamp uint64) tractData {
	return tractData((stamp << diskBits) | (uint64(disk) & diskMask))
}

func (td tractData) disk() int {
	return int(uint64(td) & diskMask)
}

func (td tractData) stamp() uint64 {
	return uint64(td) >> diskBits
}

// NewStore returns a new tract store.
// The caller must provide a way to store data and a way to track versions.
func NewStore(tt TractserverTalker, m *MetadataStore, config *Config) *Store {
	store := &Store{
		busy:          make(map[core.TractID]int32),
		tracts:        make(map[core.TractID]tractData),
		failures:      make(map[core.TractID]core.Error),
		tt:            tt,
		metadata:      m,
		config:        config,
		checkTractsCh: make(chan []core.TractState, 5),
		initialStamp:  uint64(time.Now().UnixNano()),
	}
	store.busyCond.L = &store.busyLock
	go store.checkTractsLoop()
	return store
}

// Config returns the current configuration.
func (s *Store) Config() *Config {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.config
}

// Call with s.lock held.
func (s *Store) getDisks() (out []Disk) {
	out = make([]Disk, 0, maxDisks)
	for _, di := range s.disks[:] {
		if di.d != nil {
			out = append(out, di.d)
		}
	}
	return
}

// DiskCount the number of disks the tractserver has.
func (s *Store) DiskCount() (t int) {
	s.lock.Lock()
	defer s.lock.Unlock()
	for _, di := range s.disks[:] {
		if di.d != nil {
			t++
		}
	}
	return
}

// DiskExists returns true if a disk with the given root exists.
func (s *Store) DiskExists(root string) bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	for _, d := range s.disks[:] {
		if d.root == root {
			return true
		}
	}
	return false
}

// AddDisk adds a disk to the store. If AddDisk returns nil, the disk is added and belongs
// to the store. If this returns an error, the caller should probably Stop the disk.
func (s *Store) AddDisk(disk Disk) error {
	// First read all the tracts ids on this disk so that we can add them
	// atomically with the disk table update. This will take a while.
	tids, err := readTractIDs(disk)
	if err != nil {
		return err
	}

	// Lock so we can update the disk table and tract map.
	s.lock.Lock()

	root := disk.Status().Root

	// Check if the root exists.
	var i int
	for i = 0; i < maxDisks; i++ {
		if s.disks[i].root == root {
			s.lock.Unlock()
			return ErrDiskExists
		}
	}

	// Find a free slot.
	for i = 0; s.disks[i].d != nil && i < maxDisks; i++ {
	}

	if i == maxDisks {
		s.lock.Unlock()
		return ErrTooManyDisks
	}

	s.disks[i].root = root
	s.disks[i].d = disk
	dch := make(chan core.DiskControlFlags, 2)
	s.disks[i].drainCh = dch

	// Add tracts to map.
	var conflicts []conflict
	for _, tid := range tids {
		if ti, ok := s.tracts[tid]; ok {
			conflicts = append(conflicts, conflict{tid, ti.disk(), i, s.disks[ti.disk()].d, disk})
		} else {
			s.tracts[tid] = makeTractData(i, s.initialStamp)
		}
	}

	allDisks := s.getDisks()

	// We're going to read/write to the disk now, so don't hold the main lock.
	s.lock.Unlock()

	s.metadata.setDisks(allDisks)

	s.resolveConflicts(conflicts)

	go s.scrubDisk(i, disk)
	go s.drainLoop(i, disk, dch)

	return nil
}

func readTractIDs(disk Disk) (out []core.TractID, err error) {
	d, e := disk.OpenDir()
	if e != core.NoError {
		log.Errorf("OpenDir error loading initial tracts: %s", e)
		return nil, e.Error()
	}
	for {
		tracts, e := disk.ReadDir(d)
		if e != core.NoError {
			if e != core.ErrEOF {
				log.Errorf("ReadDir error loading initial tracts: %s", e)
			}
			break
		}
		out = append(out, tracts...)
	}
	disk.CloseDir(d)
	return
}

// Sometimes we find a tract on a disk that we're adding, but we already have it
// in the tract map, on another disk.
//
// This could happen in the following situation: a disk dies temporarily, and
// the curator rereplicates the tract on another machine. Then that machine or
// disk dies and the curator rereplicates it to this machine again. Then the
// original disk comes back from the dead. Then maybe the tractserver restarts.
// In this case, the two tracts will have different versions, and we can discard
// the older one.
//
// Or another situation: a client sends a create tract rpc. The tract is written
// to disk (version 1), but then the tractserver crashes before responding to
// the rpc. When it comes back, that disk is not available. The client times out
// and retries the create, and happens to get the same tractserver. It gets
// written to a different disk (also version 1). Then the original disk comes
// back. In this case, both have version 1 so we can't tell which the correct
// one is. We have to discard them both and let the curator rereplicate.
//
// This struct holds the details of a conflict to be resolved later (outside of
// s.lock).
type conflict struct {
	id     core.TractID
	i1, i2 int
	d1, d2 Disk
}

func (s *Store) resolveConflicts(conflicts []conflict) {
	var newConflicts []conflict

	roots := make(map[Disk]string)
	root := func(d Disk) string {
		if _, ok := roots[d]; !ok {
			roots[d] = d.Status().Root
		}
		return roots[d]
	}

	fixTract := func(id core.TractID, goodDisk, badDisk Disk, goodIdx, badIdx int) {
		s.lock.Lock()
		// We think that goodDisk has the latest version of this tract. First check that
		// the disk hasn't been removed.
		if s.disks[goodIdx].d == goodDisk {
			// Then check the tract map again. We expect to see either goodIdx or badIdx.
			if ti, ok := s.tracts[id]; ok {
				if ti.disk() == goodIdx {
					// Nothing to do.
				} else if ti.disk() == badIdx && s.disks[badIdx].d == badDisk {
					// Replace with good.
					s.tracts[id] = makeTractData(goodIdx, ti.stamp()+1)
				} else {
					// It's yet another disk! Try again.
					newConflicts = append(newConflicts, conflict{id, goodIdx, ti.disk(), goodDisk, s.disks[ti.disk()].d})
				}
			} else {
				log.Errorf("tract conflict for %s, but tract was deleted from map", id)
				// This is unexpected. Maybe someone just deleted it right after we read the
				// version? We can just put it back in the map. If it's really not supposed to
				// be there, it'll get GC-ed again later.
				s.tracts[id] = makeTractData(goodIdx, s.initialStamp)
			}
		} else {
			log.Errorf("disk at index %d is no longer %s, it's %s", goodIdx, root(goodDisk),
				root(s.disks[goodIdx].d))
		}
		s.lock.Unlock()
		// In any case, we've seen a newer version of this, so the version on badDisk is
		// definitely bad.
		badDisk.Delete(id)
	}

	delTract := func(c conflict) {
		s.lock.Lock()
		// Double check that the tract was actually on one of the disks we looked at.
		if ti, ok := s.tracts[c.id]; ok {
			if (ti.disk() == c.i1 && s.disks[c.i1].d == c.d1) ||
				(ti.disk() == c.i2 && s.disks[c.i2].d == c.d2) {
				delete(s.tracts, c.id)
				delete(s.failures, c.id)
			} else {
				log.Errorf("tract conflict for %s, but current disk %d is neither %d nor %d",
					c.id, ti.disk(), c.i1, c.i2)
				// This is unexpected. We should probably just leave it there.
			}
		} else {
			log.Errorf("tract conflict for %s, but tract was already deleted from map", c.id)
		}
		s.lock.Unlock()
		// In any case, we can delete it from these disks.
		c.d1.Delete(c.id)
		c.d2.Delete(c.id)
	}

	cfg := s.Config()

	for _, c := range conflicts {
		// Get version of both tracts. An error will show up as version 0.
		et1 := s.openErrTract(context.Background(), c.d1, c.id, os.O_RDONLY, cfg)
		v1 := et1.getVersion()
		s.closeErrTract(et1)

		et2 := s.openErrTract(context.Background(), c.d2, c.id, os.O_RDONLY, cfg)
		v2 := et2.getVersion()
		s.closeErrTract(et2)

		log.Infof("found tract %s on two disks! %q (ver %d) and %q (ver %d)",
			c.id, root(c.d1), v1, root(c.d2), v2)

		if v1 > v2 {
			// Disk 1 wins.
			fixTract(c.id, c.d1, c.d2, c.i1, c.i2)
		} else if v2 > v1 {
			// Disk 2 wins.
			fixTract(c.id, c.d2, c.d1, c.i2, c.i1)
		} else {
			// Everyone loses. Give up and let the curator sort it out.
			delTract(c)
		}
	}

	if len(newConflicts) > 0 {
		s.resolveConflicts(newConflicts)
	}
}

// RemoveDisk removes a disk from the store, identified by its root. This will Stop the
// disk. All disk-related goroutines and resources may not be freed by the time this
// returns, but they should be soon after.
func (s *Store) RemoveDisk(root string) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	// Find disk
	var i int
	for i = 0; s.disks[i].root != root && i < maxDisks; i++ {
	}

	if i == maxDisks {
		return ErrDiskNotFound
	}

	// This will stop any goroutines internal to the disk, and cause further calls to
	// return ErrDiskRemoved, which will cause the scrub goroutine to exit.
	s.disks[i].d.Stop()

	// This will cause the drain goroutine to exit.
	close(s.disks[i].drainCh)

	// Replace the entry with the zero value.
	s.disks[i] = diskInfo{}

	// Remove tracts on this disk.
	for tid, td := range s.tracts {
		if td.disk() == i {
			delete(s.tracts, tid)
			delete(s.failures, tid)
		}
	}

	return nil
}

// SetConfig notifies the Store of a new configuration. Not all fields support
// dynamic configuration changes.
func (s *Store) SetConfig(ncfg Config) {
	log.Infof("got new dynamic config: %+v", ncfg)

	s.lock.Lock()
	s.config = &ncfg
	disks := s.getDisks()
	s.lock.Unlock()

	// Pass to existing disks.
	for _, d := range disks {
		if d, ok := d.(*Manager); ok {
			d.SetConfig(&ncfg)
		}
	}
}

// HasID returns true if this tractserver has a valid ID and it matches the provided 'id', false otherwise.
func (s *Store) HasID(id core.TractserverID) bool {
	return id.IsValid() && (s.metadata.getTractserverID() == id)
}

// GetID returns the tractserver's ID.
func (s *Store) GetID() core.TractserverID {
	return s.metadata.getTractserverID()
}

// SetID sets the tractserver's id to 'id'.
func (s *Store) SetID(id core.TractserverID) (core.TractserverID, core.Error) {
	return s.metadata.setTractserverID(id)
}

// getStatus returns the status of every disk.
func (s *Store) getStatus() []core.FsStatus {
	var wg sync.WaitGroup

	s.lock.Lock()
	disks := s.getDisks()
	s.lock.Unlock()

	ret := make([]core.FsStatus, len(disks))

	// Statfs might take a while if a disk has large number of tracts, so
	// call it in parallel.
	for i := range disks {
		wg.Add(1)
		go func(i int) {
			ret[i] = disks[i].Statfs()
			wg.Done()
		}(i)
	}
	wg.Wait()

	// Pull out just the flags into a map.
	latestFlags := make(map[Disk]core.DiskControlFlags)
	for i, status := range ret {
		latestFlags[disks[i]] = status.Status.Flags
	}

	// Send flags to drain channels. It's possible that some disks have been removed since
	// we called statfs, so look at the disks we have.
	go func() {
		s.lock.Lock()
		defer s.lock.Unlock()
		for _, di := range s.disks[:] {
			if flags, ok := latestFlags[di.d]; ok {
				select {
				case di.drainCh <- flags:
				default:
				}
			}
		}
	}()

	return ret
}

// SetControlFlags changes control flags on one disk (specified by root).
// If the given root is unknown, it returns ErrFileNotFound.
func (s *Store) SetControlFlags(root string, flags core.DiskControlFlags) (err core.Error) {
	// Linear search is acceptable since this is very rare.
	s.lock.Lock()
	for _, di := range s.disks[:] {
		if di.root == root {
			s.lock.Unlock()
			return di.d.SetControlFlags(flags)
		}
	}
	s.lock.Unlock()
	return core.ErrFileNotFound
}

// Create creates a tract. This involves creating an empty file on disk, then
// adding a version for it.
func (s *Store) Create(ctx context.Context, id core.TractID, b []byte, off int64) core.Error {
	// Mark the tract as busy.
	if !s.tryLockTract(id, WRITE) {
		return core.ErrTooBusy
	}
	defer s.unlock(id, WRITE)
	initialVersion := 1
	if id.Blob.Partition().Type() == core.RSPartition {
		initialVersion = core.RSChunkVersion
	}
	err := s.doCreate(ctx, id, initialVersion, b, off)
	if err == core.ErrAlreadyExists {
		// This can happen if an AckExtendBlob to a curator fails and the client
		// retries. We can convert it to a write with the initial version. If it
		// was the original client retrying, it'll get what it wants. If it was
		// a different client, we'll clobber their data, but we don't guarantee
		// anything for concurrent writes anyway.
		err = s.doWrite(ctx, id, initialVersion, b, off)
	}
	return err
}

func (s *Store) pickDiskForNewTract() (int, Disk, *Config) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// Get indexes of good disks.
	indexes := make([]int, 0, maxDisks)
	for i, di := range s.disks[:] {
		if di.d != nil {
			indexes = append(indexes, i)
		}
	}

	// rand.Intn will freak out if given an argument <= 0.
	if len(indexes) == 0 {
		return -1, nil, nil
	}

	// We pick the best of N disks.
	N := 2

	// N must be <= num of disks.
	if N > len(indexes) {
		N = len(indexes)
	}

	// Pick a random size-N subset.
	try := make(map[int]bool)
	for len(try) < N {
		try[rand.Intn(len(indexes))] = true
	}

	// Choose the disk w/the best expected wait (avg * queue len).
	// Start bestWait at something big.
	var bestWait int = 1e8
	var bestDisk Disk
	var bestIndex int

	for i := range try {
		d := s.disks[indexes[i]].d
		status := d.Status()
		if canAllocate(status) {
			// Add 1 to queue length so we're not multiplying by zero and disregarding
			// the avg wait time if the queue just happens to be ready.
			wait := (1 + status.QueueLen) * status.AvgWaitMs
			if wait < bestWait {
				bestWait = wait
				bestDisk = d
				bestIndex = indexes[i]
			}
		}
	}

	if bestDisk != nil {
		return bestIndex, bestDisk, s.config
	}

	// Edge case: there are not many good disks / we got really unlucky. Just linearly scan
	// from where we started looking before.
	for inc := range indexes {
		i := (s.nextDisk + inc) % len(indexes)
		d := s.disks[indexes[i]].d
		status := d.Status()
		if canAllocate(status) {
			s.nextDisk = (i + 1) % len(indexes)
			return indexes[i], d, s.config
		}
	}

	return -1, nil, nil
}

// doCreate creates a new tract. Must call with tract locked for write.
func (s *Store) doCreate(ctx context.Context, id core.TractID, version int, b []byte, off int64) core.Error {
	// Make sure it doesn't exist already.
	// We don't have to hold this lock as s.tracts[id] is already protected by s.lock(id, WRITE).
	if _, _, _, ok := s.lookup(id); ok {
		log.Errorf("tract %s already exists", id)
		return core.ErrAlreadyExists
	}

	i, disk, cfg := s.pickDiskForNewTract()
	if disk == nil {
		log.Errorf("no disk is free or healthy enough to create tract %+v", id)
		return core.ErrNoSpace
	}

	// Create the file on disk. This will fail if it already exists.
	t := s.openErrTract(ctx, disk, id, os.O_CREATE|os.O_EXCL|os.O_RDWR, cfg)
	// Set version.
	t.setVersion(version)
	// Write bytes.
	t.write(b, off)
	s.closeErrTract(t)

	if t.err == core.NoError {
		// If everything went OK, note the location & version for later use.
		s.lock.Lock()
		s.tracts[id] = makeTractData(i, s.initialStamp)
		s.lock.Unlock()
	} else {
		// Otherwise, try to remove it.
		disk.Delete(id)
	}

	return t.err
}

// Write is called from the RPC layer to write to a tract.
// See curator/rereplicate.go for the details of how writes and version numbers
// interact.
func (s *Store) Write(ctx context.Context, id core.TractID, version int, b []byte, off int64) core.Error {
	// Serialize writes.
	if !s.tryLockTract(id, WRITE) {
		return core.ErrTooBusy
	}
	defer s.unlock(id, WRITE)
	return s.doWrite(ctx, id, version, b, off)
}

// doWrite performs one write. Must call with tract locked for write.
func (s *Store) doWrite(ctx context.Context, id core.TractID, version int, b []byte, off int64) core.Error {
	// Open and write.
	t := s.openExistingTractAndBumpStamp(ctx, id, os.O_RDWR)
	t.checkVersion(version)
	t.write(b, off)
	s.closeErrTract(t)

	return t.err
}

// Read is called from the RPC layer to perform a read.
func (s *Store) Read(ctx context.Context, id core.TractID, version int, length int, off int64) ([]byte, core.Error) {
	if !s.tryLockTract(id, READ) {
		return nil, core.ErrTooBusy
	}
	defer s.unlock(id, READ)

	t := s.openExistingTract(ctx, id, os.O_RDONLY)
	t.checkVersion(version)
	b := t.read(length, off)
	s.closeErrTract(t)

	if t.err != core.NoError {
		return nil, t.err
	} else if len(b) != length {
		return b, core.ErrEOF
	}
	return b, core.NoError
}

// Stat is called from the RPC layer to get the size and mod stamp of a tract.
func (s *Store) Stat(ctx context.Context, id core.TractID, version int) (int64, uint64, core.Error) {
	if !s.tryLockTract(id, READ) {
		return 0, 0, core.ErrTooBusy
	}
	defer s.unlock(id, READ)

	t, stamp := s.openExistingTractWithStamp(ctx, id, os.O_RDONLY)
	t.checkVersion(version)
	size := t.size()
	s.closeErrTract(t)
	return size, stamp, t.err
}

// SetVersion tries to set the version for an existing tract 'id' to
// 'newVersion'. The rule is that 'newVersion' must be <= ourVersion+1. Then the
// version is set to max(ourVersion, newVersion) and this value is returned.
//
// On success, returns the version number of the tract (may be unmodified) and
// core.NoError. On failure, returns another core.Error.
func (s *Store) SetVersion(id core.TractID, newVersion int, conditionalStamp uint64) (finalVersion int, err core.Error) {
	if newVersion <= 1 {
		return 0, core.ErrBadVersion
	}

	if !s.tryLockTract(id, WRITE) {
		return 0, core.ErrTooBusy
	}
	defer s.unlock(id, WRITE)

	t, stamp := s.openExistingTractWithStamp(context.TODO(), id, os.O_RDWR)

	if conditionalStamp != 0 && conditionalStamp != stamp {
		return 0, core.ErrStampChanged
	}

	t.bumpVersion(newVersion)
	s.closeErrTract(t)
	return newVersion, t.err
}

// PullTract attempts to read the tract (id, version) from the given 'sources'.
// It tries each source sequentially and stops if it succeeds early. It returns
// the latest error if pulling from all source hosts fails.
func (s *Store) PullTract(ctx context.Context, sources []string, id core.TractID, version int) (err core.Error) {
	if !s.tryLockTract(id, LONG_WRITE) {
		return core.ErrTooBusy
	}
	defer s.unlock(id, LONG_WRITE)

	for _, from := range sources {
		if err = s.pullTractOnce(ctx, from, id, version); core.NoError == err {
			return
		}
		log.Errorf("failed to pull tract from %s: %s", from, err)
	}
	log.Errorf("failed to pull tract from any of the given source hosts")
	return
}

// pullTractOnce reads the tract (id, version) from the tractserver serving on 'from' and copies
// it to this store, making it available for serving. Returns core.NoError on success, another error otherwise.
//
// The flow is described as follows:
// (1) Check if the file already exists:
//   - Any error other than ErrNoSuchTract: return error directly.
//   - ErrNoSuchTract: no existing file, proceed to step (4).
//   - NoError: existing file, proceed to step (2).
// (2) Check the version:
//   - Any error other than ErrNoAttribute: return error directly.
//   - ErrNoAttribute: no version, proceed to step (3).
//   - NoError: compare myVersion against the provided version.
//     -- myVersion > version: this shouldn't happen, return ErrInvalidState.
//     -- myVersion == version: possibly from a failed pull, proceed to step (3).
//     -- myVersion < version: my data is outdated, proceed to step (3).
// (3) Remove the existing file.
// (4) Create a new file, pull data from remote host and write to the new file.
// (5) Create a version for the new file.
func (s *Store) pullTractOnce(ctx context.Context, from string, id core.TractID, version int) core.Error {
	// See if the tract exists already.
	if _, disk, cfg, ok := s.lookup(id); ok {
		// If the tract exists on this server already we look at its version.
		t := s.openErrTract(ctx, disk, id, os.O_RDONLY, cfg)
		v := t.getVersion()
		s.closeErrTract(t)

		if t.err == core.NoError && (v > version) {
			log.Errorf("asked to pulltract id=%v, version=%v from %v, but already have version %v", id, version, from, v)
			return core.ErrInvalidState
		}

		// There was an error, or the on-disk version is <= the new version.
		//
		// if there was an error, just delete the file and overwrite it.
		//
		// myVersion can be == version if:
		//   0. The curator is doing a re-replication
		//   1. The curator asks us to pull the tract from a bumped good host.
		//   2. The curator crashes before logging the new repl set.
		//   3. The curator repeats the above operations on start-up.
		// This is OK and we just clobber the failed pull.
		//
		// myVersion being < version means we have outdated data and we can just remove it.
		if e := s.removeTract(id); e != core.NoError {
			return e
		}
	}

	// Read the data from the other tractserver.  EOF is fine -- the tract can be half-written.
	data, err := s.tt.CtlRead(ctx, from, id, version, core.TractLength, 0)
	defer rpc.PutBuffer(data, true)
	if err != core.NoError && err != core.ErrEOF {
		return err
	}

	// If we're here, the tract never existed or we deleted it.
	// Create the tract and write the data into it.
	if err = s.doCreate(ctx, id, version, data, 0); err != core.NoError {
		// If we failed to pull the tract, delete it.
		if e := s.removeTract(id); e != core.NoError {
			log.Errorf("error pulling data & setting version (err=%s), additional error rm-ing tract=%s", err, e)
		}
	}

	return err
}

// GetSomeTractsByPartition returns tract ids grouped by partition. To reduce the need to
// allocate very large slices of tract ids, it returns only a subset of the known tracts
// with each call, controlled by the low bits of 'shard'. To ensure that all tracts are
// visited, a caller should loop and increment shard with each call.
func (s *Store) GetSomeTractsByPartition(shard uint64) map[core.PartitionID][]core.TractID {
	ret := make(map[core.PartitionID][]core.TractID)

	s.lock.Lock()

	// To pick a subset of tracts, we'll take the ones whose lower bits of blob id are
	// equal to the corresponding lower bits of 'shard'. Since 'shard' keeps getting
	// incremented by one, we'll eventually return them all. The number of bits to
	// consider determines what fraction of tracts we return: 0 -> 100%, 1 -> 50%, 2 ->
	// 25%, etc. We'll pick the number of bits in the mask based on the total number of
	// tracts such that the returned tracts use less than targetBytes memory (not counting
	// slice overhead). If the number of tracts changes between calls such that the mask
	// changes, we might duplicate some tracts in consecutive calls, or skip some.
	// Duplication is acceptable, and skipping is ok because we'll catch them the next
	// time around.
	const targetBytes = 10 << 20 // aim to stay under this much memory
	var mask uint64
	for total := len(s.tracts) * core.SizeofTractID; total > targetBytes; total >>= 1 {
		mask = (mask << 1) | 1
	}

	shard = shard & mask // pre-mask so we can just use == below

	for t := range s.tracts {
		// Assume the lower bits of blob ids are uniformly distributed. Maybe not quite
		// true but close enough.
		if uint64(t.Blob)&mask == shard {
			pid := t.Blob.Partition().WithoutType()
			ret[pid] = append(ret[pid], t)
		}
	}

	s.lock.Unlock()
	return ret
}

// GetTractsByDisk returns all tracts on the given disk (by index).
func (s *Store) GetTractsByDisk(i int) (out []core.TractID) {
	s.lock.Lock()
	for tid, td := range s.tracts {
		if td.disk() == i {
			out = append(out, tid)
		}
	}
	s.lock.Unlock()
	return
}

// GetBadTracts returns the tracts that we know are missing or corrupt for the provided partition(s).
func (s *Store) GetBadTracts(parts []core.PartitionID, limit int) (bad []core.TractID) {
	s.lock.Lock()
	defer s.lock.Unlock()
	// For each failure, see if the failed tract is in one of the partitions in 'parts'.  If so, return it.
	for id := range s.failures {
		for _, p := range parts {
			if p == id.Blob.Partition().WithoutType() {
				bad = append(bad, id)
				limit--
				if limit <= 0 {
					return
				}
				break
			}
		}
	}
	return
}

// Remove bad tracts from failures map. This is called when we believe that the
// given 'tracts' have already been taken good care of (reported to curator for
// rereplication).
func (s *Store) removeTractsFromFailures(tracts []core.TractID) {
	s.lock.Lock()
	for _, t := range tracts {
		delete(s.failures, t)
	}
	s.lock.Unlock()
}

// GCTracts reads the tract state returned from the curator and decides if a
// tract is stale. If so, it removes the tract from 's.Disk'. Error is returned
// if we fail to remove it.
//
// We can delete our copy of the tract if it has version <= the version in the GC message.
func (s *Store) GCTracts(old []core.TractState, gone []core.TractID) {
	for _, tract := range old {
		s.maybeGCTract(tract)
	}
	for _, tract := range gone {
		log.Infof("@@@ gc-ing tract %s, gone", tract)
		s.removeTract(tract)
	}
}

// Remove the provided tract if our version is less than or equal to the provided version.
func (s *Store) maybeGCTract(tract core.TractState) core.Error {
	if !s.tryLockTract(tract.ID, WRITE) {
		return core.ErrTooBusy
	}
	defer s.unlock(tract.ID, WRITE)

	t := s.openExistingTract(context.TODO(), tract.ID, os.O_RDONLY)
	v := t.getVersion()
	s.closeErrTract(t)

	if t.err != core.NoError {
		return t.err
	}

	if v > tract.Version {
		return core.ErrHaveNewerVersion
	}

	log.Infof("@@@ gc-ing tract %s, old: %d <= %d", tract.ID, v, tract.Version)
	return s.removeTract(tract.ID)
}

// PackTracts reads tracts from other tractservers and writes them to a local RS
// data chunk.
func (s *Store) PackTracts(ctx context.Context, length int, srcs []*core.PackTractSpec, dest core.RSChunkID) core.Error {
	if !dest.IsValid() || !checkTractSpec(srcs, length) {
		return core.ErrInvalidArgument
	}
	destTract := dest.ToTractID()

	if !s.tryLockTract(destTract, LONG_WRITE) {
		return core.ErrTooBusy
	}
	defer s.unlock(destTract, LONG_WRITE)

	// Delete the tract if it exists.
	if err := s.removeTract(destTract); err != core.NoError {
		return err
	}

	// Open the dest tract for writing.
	i, disk, cfg := s.pickDiskForNewTract()
	if disk == nil {
		log.Errorf("no disk is free or healthy enough to create packed tract")
		return core.ErrNoSpace
	}

	// Create the file on disk. This will fail if it already exists.
	t := s.openErrTract(ctx, disk, destTract, os.O_CREATE|os.O_EXCL|os.O_RDWR, cfg)
	t.setVersion(core.RSChunkVersion)

	// Write sources. Do this sequentially so that we do all the writes sequentially and
	// minimize seeking. We don't have to be particularly fast here.
SourceLoop:
	for _, src := range srcs {
		if t.err != core.NoError {
			break // avoid extra work if we've already failed
		}
		for _, from := range src.From {
			// Ask for core.TractLength and compare the length with src.Length so that we
			// catch tracts that are an unexpected length.
			b, err := s.tt.CtlRead(ctx, from.Host, src.ID, src.Version, core.TractLength, 0)
			if (err == core.NoError || err == core.ErrEOF) && len(b) == src.Length {
				t.write(b, int64(src.Offset))
				rpc.PutBuffer(b, true)
				continue SourceLoop
			}
			log.Errorf("failed to pull tract %s from %s: %s (len %d, expected %d)", src.ID, from.Host, err, len(b), src.Length)
			rpc.PutBuffer(b, true)
		}
		log.Errorf("Failed to pull tract %s from all sources when packing", src.ID)
		t.err = core.ErrRPC // force an error in t
	}

	// Pad if necessary.
	if len(srcs) > 0 {
		lastPos := srcs[len(srcs)-1].Offset + srcs[len(srcs)-1].Length
		if length > lastPos && t.err == core.NoError {
			t.write(make([]byte, length-lastPos), int64(lastPos))
		}
	}

	// Close and collect errors.
	s.closeErrTract(t)

	if t.err == core.NoError {
		// If everything went OK, note the location & version for later use.
		s.lock.Lock()
		s.tracts[destTract] = makeTractData(i, s.initialStamp)
		s.lock.Unlock()
	} else {
		// Otherwise, try to remove it.
		disk.Delete(destTract)
	}

	return t.err
}

// checkTractSpec checks that the constituent tracts are in order, don't
// overlap, and are within the total length.
func checkTractSpec(srcs []*core.PackTractSpec, length int) bool {
	end := 0
	for _, src := range srcs {
		if !src.ID.IsValid() || len(src.From) < 1 || src.Offset < end {
			return false
		}
		end = src.Offset + src.Length
	}
	if length < end {
		return false
	}
	return true
}

// RSEncode performs the RS parity computation, reading data and writing parity
// to other tractservers.
func (s *Store) RSEncode(ctx context.Context, baseid core.RSChunkID, length int, srcs, dests []core.TSAddr, indexMap []int) (err core.Error) {
	N, M := len(srcs), len(dests)
	increment := s.Config().EncodeIncrementSize

	if !baseid.IsValid() || !baseid.Add(N+M-1).IsValid() {
		return core.ErrInvalidArgument
	}

	enc, e := reedsolomon.New(N, M)
	if e != nil {
		log.Errorf("couldn't create RS encoder: %s", e)
		return core.ErrInvalidArgument
	}

	off := 0
	for length > 0 {
		l := min(length, increment)
		err = s.rsEncodeOne(ctx, baseid, off, l, srcs, dests, indexMap, enc)
		if err != core.NoError {
			break
		}
		length -= l
		off += l
	}

	return err
}

func (s *Store) rsEncodeOne(ctx context.Context, baseid core.RSChunkID, offset, length int, srcs, dests []core.TSAddr, indexMap []int, enc reedsolomon.Encoder) core.Error {
	N, M := len(srcs), len(dests)

	data := make([][]byte, N+M)
	errs := make([]core.Error, N+M)

	defer func() {
		for _, b := range data {
			rpc.PutBuffer(b, true) // everything in data is exclusively owned, so we can reuse buffers
		}
	}()

	encode := false
	if len(indexMap) == 0 {
		encode = true
		// Use identity map for encoding.
		indexMap = make([]int, N+M)
		for i := range indexMap {
			indexMap[i] = i
		}
	} else if len(indexMap) != N+M {
		return core.ErrInvalidArgument
	}

	// Pull all the data that we need.
	var wg sync.WaitGroup
	wg.Add(N)
	for srcI, dataI := range indexMap[:N] {
		go func(srcI, dataI int) {
			var err core.Error
			id := baseid.Add(dataI).ToTractID()
			data[dataI], err = s.tt.CtlRead(ctx, srcs[srcI].Host, id, core.RSChunkVersion, length, int64(offset))
			if err != core.NoError && err != core.ErrEOF {
				errs[srcI] = err
			} else if len(data[dataI]) != length {
				errs[srcI] = core.ErrVersionMismatch
			} else {
				errs[srcI] = core.NoError
			}
			wg.Done()
		}(srcI, dataI)
	}
	wg.Wait()

	// Check errors.
	for _, err := range errs[:N] {
		if err != core.NoError {
			return err
		}
	}

	var err error
	if encode {
		// For encoding, RS wants us to allocate empty slices.
		for _, dataI := range indexMap[N:] {
			data[dataI] = rpc.GetBuffer(length)
		}
		err = enc.Encode(data)
	} else {
		// For reconstruction, RS wants the missing pieces left as nil.
		err = reconstructAndVerify(enc, data)
	}
	if err != nil {
		log.Errorf("RS encoding failed: %s", err)
		return core.ErrUnknown
	}

	// Write out results.
	for destI, dataI := range indexMap[N:] {
		if dataI >= 0 && dests[destI].ID != 0 {
			wg.Add(1)
			go func(destI, dataI int) {
				id := baseid.Add(dataI).ToTractID()
				errs[N+destI] = s.tt.CtlWrite(ctx, dests[destI].Host, id, core.RSChunkVersion, int64(offset), data[dataI])
				wg.Done()
			}(destI, dataI)
		}
	}
	wg.Wait()

	// Check errors.
	for _, err := range errs[N:] {
		if err != core.NoError {
			return err
		}
	}

	return core.NoError
}

func reconstructAndVerify(enc reedsolomon.Encoder, data [][]byte) error {
	if err := enc.Reconstruct(data); err != nil {
		return err
	}
	if ok, err := enc.Verify(data); err != nil {
		return err
	} else if !ok {
		return errVerifyFailed
	}
	return nil
}

var errVerifyFailed = errors.New("verification failed")

func canAllocate(status core.DiskStatus) bool {
	return !status.Full &&
		status.Healthy &&
		!status.Flags.StopAllocating &&
		status.Flags.Drain == 0 &&
		status.Flags.DrainLocal == 0
}
