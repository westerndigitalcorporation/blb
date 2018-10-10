// Copyright (c) 2016 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package tractserver

import (
	"context"
	"encoding/binary"

	log "github.com/golang/glog"
	"github.com/westerndigitalcorporation/blb/internal/core"
	libdisk "github.com/westerndigitalcorporation/blb/pkg/disk"
	"github.com/westerndigitalcorporation/blb/pkg/rpc"
)

const (
	// WRITE indicates that the tract should be accessed exclusively, but for a
	// short time (typical disk latency). Waiters might want to block.
	WRITE = iota

	// LONG_WRITE indicates the tract will be accessed exclusively for an
	// indeterminate time.
	LONG_WRITE

	// READ indicates that the tract can be shared with other readers.
	READ
)

// Modes maps above constants to strings.
var Modes = []string{"WRITE", "LONG_WRITE", "READ"}

// Returns true if 'id' could be locked in 'mode', false otherwise.
// If locked, caller must call unlock(id, mode).
func (s *Store) tryLockTract(id core.TractID, mode int) bool {
	s.busyLock.Lock()
	for {
		locked, shouldWait := s.tryLockTractOnce(id, mode)
		if locked {
			s.busyLock.Unlock()
			log.V(10).Infof("locked tract %s for %s", id, Modes[mode])
			return true
		} else if !shouldWait {
			s.busyLock.Unlock()
			log.V(10).Infof("failed to lock tract %s for %s", id, Modes[mode])
			return false
		}
		log.V(10).Infof("waiting to lock tract %s for %s", id, Modes[mode])
		s.busyCond.Wait()
	}
}

// Must be called with s.busyLock held.
func (s *Store) tryLockTractOnce(id core.TractID, mode int) (locked bool, shouldWait bool) {
	// See what the lock's state is.
	state, ok := s.busy[id]

	// Not busy?  We get it.
	if !ok {
		if mode == WRITE {
			s.busy[id] = -1
		} else if mode == LONG_WRITE {
			s.busy[id] = -2
		} else {
			s.busy[id] = 1
		}
		return true, false
	}

	// Someone is using the tract.  Only possible compatibility
	// is shared requests w/other shared requests.
	if mode == READ && state > 0 {
		s.busy[id] = state + 1
		return true, false
	}

	// Wait for READ or WRITE, but not LONG_WRITE
	return false, state != LONG_WRITE
}

// Unlock 'id' which was locked in 'mode'.
func (s *Store) unlock(id core.TractID, mode int) {
	s.busyLock.Lock()
	state, ok := s.busy[id]
	if !ok {
		log.Fatalf("programming error: unlock without matching lock")
	}

	if mode == WRITE {
		if state != -1 {
			log.Fatalf("programmer error: wrong unlock type: mode %v, state %v", mode, state)
		}
		delete(s.busy, id)
	} else if mode == LONG_WRITE {
		if state != -2 {
			log.Fatalf("programmer error: wrong unlock type: mode %v, state %v", mode, state)
		}
		delete(s.busy, id)
	} else {
		if state < 1 {
			log.Fatalf("programmer error: wrong unlock type: mode %v, state %v", mode, state)
		} else if state == 1 {
			delete(s.busy, id)
		} else {
			s.busy[id] = state - 1
		}
	}
	log.V(10).Infof("unlocked tract %s from %s", id, Modes[mode])
	s.busyLock.Unlock()
	s.busyCond.Broadcast()
}

// errTract is an implementation of this pattern: https://blog.golang.org/errors-are-values
// Each method on errTract will only manipulate the underlying tract if no error
// has been encountered yet.
type errTract struct {
	// What tract did we open?
	id core.TractID
	// What disk owns this tract?
	disk Disk
	// Was it actually opened?
	opened bool
	// If so, what's the file?
	f interface{}
	// Any error we've encountered thus far.
	err core.Error
	// The context of the request.
	ctx context.Context
}

// The extended attribute name we stuff the version into.
const versionXattr = "v"

func (t *errTract) setVersion(version int) {
	if t.err != core.NoError {
		return
	}

	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(version))
	t.err = t.disk.Setxattr(t.f, versionXattr, b[:])
}

func (t *errTract) write(b []byte, off int64) {
	if t.err != core.NoError {
		return
	}
	_, t.err = t.disk.Write(t.ctx, t.f, b, off)
}

func (t *errTract) read(length int, off int64) []byte {
	if t.err != core.NoError {
		return nil
	}

	b := rpc.GetBuffer(length + libdisk.ExtraRoom)[:length]
	n, err := t.disk.Read(t.ctx, t.f, b, off)
	// Truncate to what was actually read.
	b = b[:n]

	// EOF isn't a "real" error.  We make the caller detect a short read.
	if err != core.ErrEOF {
		t.err = err
	}
	return b
}

func (t *errTract) size() int64 {
	if t.err != core.NoError {
		return 0
	}
	var n int64
	n, t.err = t.disk.Size(t.f)
	return n
}

func (t *errTract) getVersion() int {
	if t.err != core.NoError {
		return 0
	}

	b, err := t.disk.Getxattr(t.f, versionXattr)
	if err != core.NoError {
		t.err = err
		return 0
	}
	return int(binary.LittleEndian.Uint64(b))
}

// Check that the version of the tract is equal to 'target'.
// Enters the error condition if it's not.
func (t *errTract) checkVersion(target int) {
	cur := t.getVersion()
	if t.err != core.NoError {
		return
	}
	if target != cur {
		t.err = core.ErrVersionMismatch
	}
}

// bumpVersion succeeds if we're already at newVersion (or higher), or we're going from
// newVersion-1 to newVersion.  Otherwise it fails.
func (t *errTract) bumpVersion(newVersion int) {
	cur := t.getVersion()
	if t.err != core.NoError {
		return
	}

	// t.err == core.NoError is true.

	if cur >= newVersion {
		return
	}
	if cur+1 != newVersion {
		t.err = core.ErrVersionMismatch
		return
	}
	t.setVersion(newVersion)
}

// opens the tract 'id' on the provided disk (with the provided mode) and returns an errTract
// wrapper around it.  The tract does not have to exist.
func (s *Store) openErrTract(ctx context.Context, disk Disk, id core.TractID, mode int, cfg *Config) *errTract {
	if cfg.DropCache {
		mode |= libdisk.O_DROPCACHE
	}
	f, e := disk.Open(ctx, id, mode)
	opened := e == core.NoError
	return &errTract{id: id, disk: disk, opened: opened, f: f, err: e, ctx: ctx}
}

// Opens an already existing tract and returns an errTract wrapper around it.
func (s *Store) openExistingTract(ctx context.Context, id core.TractID, mode int) *errTract {
	_, disk, cfg, ok := s.lookup(id)
	if !ok {
		return &errTract{opened: false, err: core.ErrNoSuchTract}
	}
	return s.openErrTract(ctx, disk, id, mode, cfg)
}

// Opens an already existing tract and returns an errTract wrapper around it,
// and the modification stamp of the tract.
func (s *Store) openExistingTractWithStamp(ctx context.Context, id core.TractID, mode int) (*errTract, uint64) {
	data, disk, cfg, ok := s.lookup(id)
	if !ok {
		return &errTract{opened: false, err: core.ErrNoSuchTract}, 0
	}
	return s.openErrTract(ctx, disk, id, mode, cfg), data.stamp()
}

// Opens an already existing tract and returns an errTract wrapper around it.
// Also bumps the modification stamp of the tract.
func (s *Store) openExistingTractAndBumpStamp(ctx context.Context, id core.TractID, mode int) *errTract {
	_, disk, cfg, ok := s.lookupAndBumpStamp(id)
	if !ok {
		return &errTract{opened: false, err: core.ErrNoSuchTract}
	}
	return s.openErrTract(ctx, disk, id, mode, cfg)
}

func (s *Store) closeErrTract(t *errTract) {
	// We only close the file if it was actually opened in the first place.
	if t.opened {
		if cerr := t.disk.Close(t.f); cerr != core.NoError {
			// We keep the first t.err for no reason in particular.
			if t.err == core.NoError {
				t.err = cerr
			} else {
				log.Errorf("tract %v: previous error %v, additional error closing: %v", t.id, t.err, cerr)
				s.maybeReportError(t.id, cerr)
			}
		}
	}

	s.maybeReportError(t.id, t.err)
}

// Looks up the tract 'id' in our in-memory cache of what tracts we own.
// If it exists, returns the disk that contains it and true.
// Otherwise returns an empty value and false.
func (s *Store) lookup(id core.TractID) (tractData, Disk, *Config, bool) {
	var d Disk
	s.lock.Lock()
	info, ok := s.tracts[id]
	if ok {
		d = s.disks[info.disk()].d
	}
	cfg := s.config
	s.lock.Unlock()
	return info, d, cfg, ok
}

// Looks up the tract 'id' in our in-memory cache of what tracts we own.
// Also bumps the modification stamp.
func (s *Store) lookupAndBumpStamp(id core.TractID) (tractData, Disk, *Config, bool) {
	var d Disk
	s.lock.Lock()
	data, ok := s.tracts[id]
	if ok {
		d = s.disks[data.disk()].d
		s.tracts[id] = makeTractData(data.disk(), data.stamp()+1)
	}
	cfg := s.config
	s.lock.Unlock()
	return data, d, cfg, ok
}

// removeTract removes the tract 'id'.
// The caller is responsible for calling tryLockTract and checking that we actually *should* remove the tract.
func (s *Store) removeTract(id core.TractID) core.Error {
	_, disk, _, ok := s.lookup(id)
	if !ok {
		return core.NoError
	}

	if err := disk.Delete(id); err != core.NoError {
		log.Errorf("failed to remove tract %s: %s", id, err)
		return err
	}

	s.lock.Lock()
	delete(s.failures, id)
	delete(s.tracts, id)
	s.lock.Unlock()

	return core.NoError
}

// maybeReportError checks to see if the error 'err' encountered while manipulating
// the tract 'id' indicates corruption.  Logs and returns true if so.  Does nothing
// and returns false if the tract is fine.
func (s *Store) maybeReportError(id core.TractID, err core.Error) bool {
	if err == core.ErrCorruptData || err == core.ErrIO || err == core.ErrNoAttribute {
		s.lock.Lock()
		s.failures[id] = err
		s.lock.Unlock()
		// NOTE: testblb relies this line of log output. Changing the output might
		// cause testblb to fail.
		log.Errorf("@@@ tract %v has corruption error %v", id, err)
		return true
	}
	return false
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
