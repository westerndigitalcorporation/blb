// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package server

import (
	"sync"

	"github.com/westerndigitalcorporation/blb/internal/core"
)

// LockManager provides exclusive access to a given blob or tract.
// We use leader continuity check to guard against conflict changes from
// different nodes, but within a same node we still need lock to guard
// against conflict changes from different goroutines.
type LockManager interface {
	// LockBlock acquires a lock of exclusive access to a given blob.
	LockBlob(core.BlobID)

	// UnlockBlob releases the lock on a given blob.
	UnlockBlob(core.BlobID)

	// LockTract  acquires a lock of exclusive access to a given tract.
	LockTract(core.TractID)

	// UnlockTract releases the lock on a given tract.
	UnlockTract(core.TractID)
}

// FineGrainedLock implements LockManager.
type FineGrainedLock struct {
	// Protects cond and things.
	lock sync.Mutex

	// Signals when something is unlocked.
	cond sync.Cond

	// Holds lock state for blobs and tracts. If present, the object is locked.
	things map[interface{}]bool
}

// NewFineGrainedLock creates a new FineGrainedLock.
func NewFineGrainedLock() LockManager {
	f := new(FineGrainedLock)
	f.cond.L = &f.lock
	f.things = make(map[interface{}]bool)
	return f
}

func (f *FineGrainedLock) lockThing(thing interface{}) {
	f.lock.Lock()
	defer f.lock.Unlock()
	for f.things[thing] {
		f.cond.Wait()
	}
	f.things[thing] = true
}

func (f *FineGrainedLock) unlockThing(thing interface{}) {
	f.lock.Lock()
	defer f.lock.Unlock()
	if !f.things[thing] {
		panic("wasn't locked!")
	}
	delete(f.things, thing)
	f.cond.Broadcast()
}

// LockBlob locks a blob.
func (f *FineGrainedLock) LockBlob(blobID core.BlobID) {
	f.lockThing(blobID)
}

// UnlockBlob unlocks a blob.
func (f *FineGrainedLock) UnlockBlob(blobID core.BlobID) {
	f.unlockThing(blobID)
}

// LockTract locks a tract.
func (f *FineGrainedLock) LockTract(tractID core.TractID) {
	f.lockThing(tractID)
}

// UnlockTract unlocks a tract
func (f *FineGrainedLock) UnlockTract(tractID core.TractID) {
	f.unlockThing(tractID)
}
