// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package tractserver

import (
	"bytes"
	"context"
	"encoding/gob"
	"os"
	"sync"

	log "github.com/golang/glog"

	"github.com/westerndigitalcorporation/blb/internal/core"
)

// We store our metadata in gob format in this fake "tract". Note that a blob id of
// zero is not a valid blob id, because both valid partitions and blob keys start from
// one. (If we need other "meta" files in the future, we can increment index.)
// We don't use versions on the meta tract.
var metaTractID = core.TractID{Blob: core.ZeroBlobID, Index: 0}

const maxMetaSize = 128 * 1024

type metadata struct {
	// What is my tractserver id?
	TSID core.TractserverID
}

// MetadataStore stores the metadata (master assigned id) of the tractserver.
// MetadataStore is thread-safe.
type MetadataStore struct {
	// Disks to store metadata on.
	disks []Disk

	// All my metadata.
	meta metadata

	// Protect md and disks.
	lock sync.Mutex
}

// NewMetadataStore creates a new metadata store. Metadata will be read from and
// all 'disks' and merged on startup, and written to all 'disks'.
func NewMetadataStore() *MetadataStore {
	return &MetadataStore{}
}

func (ms *MetadataStore) setDisks(disks []Disk) {
	ms.lock.Lock()
	ms.disks = disks
	// Only load metadata if we don't have an ID yet, so that we don't change
	// the ID after it's set.
	if ms.meta.TSID == 0 {
		ms.loadMetadata()
	}
	ms.saveMetadata()
	ms.lock.Unlock()
}

// Load and merge metadata from all our disks.
// Call with lock held.
// For TSID, we take the most popular value.
func (ms *MetadataStore) loadMetadata() {
	meta := metadata{
		// Valid TractserverIDs start from 1. We set an invalid id here
		// so that the registration process can pick it up later.
		TSID: 0,
	}

	ctx := context.Background()
	buf := make([]byte, maxMetaSize)
	tsidVotes := make(map[core.TractserverID]int)

	for _, d := range ms.disks {
		root := d.Status().Root

		f, err := d.Open(ctx, metaTractID, os.O_RDONLY)
		if err != core.NoError {
			log.Errorf("Error opening meta tract on disk %s: %s", root, err)
			continue
		}
		n, err := d.Read(ctx, f, buf, 0)
		d.Close(f)
		if err != core.NoError && err != core.ErrEOF {
			log.Errorf("Error reading meta tract on disk %s: %s", root, err)
			continue
		}

		var m metadata
		dec := gob.NewDecoder(bytes.NewReader(buf[:n]))
		if err := dec.Decode(&m); err != nil {
			log.Errorf("Error decoding meta tract on disk %s: %s", root, err)
			continue
		}

		// Vote:
		tsidVotes[m.TSID] = tsidVotes[m.TSID] + 1
	}

	// Pick id based on votes.
	best := 0
	for id, count := range tsidVotes {
		if count > best {
			best = count
			meta.TSID = id
		}
	}

	if best > 0 {
		log.Infof("Loaded TSID %d from metadata (%d disks)", meta.TSID, best)
	} else {
		log.Infof("TSID uninitialized, will register with master")
	}

	ms.meta = meta
}

// setTractserverID sets the master assigned tractserver id if it hasn't been
// set yet. If the tractserver id has already been set, the id is returned.
// Error is returned if we fail to commit the change to persistent storage.
func (ms *MetadataStore) setTractserverID(id core.TractserverID) (core.TractserverID, core.Error) {
	// We are suppose to do this only when we register the tractserver with
	// the master. No client should try to access the metadata store at the
	// same time and thus we don't optimize the locking mechanism below for
	// disk IOs.
	ms.lock.Lock()
	defer ms.lock.Unlock()

	// If a parallel thread has already set the id, return it.
	if ms.meta.TSID.IsValid() {
		return ms.meta.TSID, core.NoError
	}

	// Set it.
	ms.meta.TSID = id
	if err := ms.saveMetadata(); err != core.NoError {
		return 0, err
	}

	return ms.meta.TSID, core.NoError
}

// Call with lock held.
func (ms *MetadataStore) saveMetadata() core.Error {
	ctx := context.Background()

	// Serialize the data in gob format.
	buf := bytes.NewBuffer(nil)
	enc := gob.NewEncoder(buf)
	if err := enc.Encode(ms.meta); nil != err {
		log.Errorf("Error encoding metadata: %s", err)
		return core.ErrIO
	}
	mdBytes := buf.Bytes()

	// Write to all disks.
	success := 0
	var lastErr core.Error
	for _, d := range ms.disks {
		f, err := d.Open(ctx, metaTractID, os.O_CREATE|os.O_TRUNC|os.O_RDWR)
		if err != core.NoError {
			log.Errorf("Error opening meta tract on disk %s: %s", d, err)
			lastErr = err
			continue
		}
		n, err := d.Write(ctx, f, mdBytes, 0)
		d.Close(f)
		if err != core.NoError && n == len(mdBytes) {
			log.Errorf("Error writing meta tract on disk %s: %s", d, err)
			lastErr = err
			continue
		}
		success++
	}

	if success == 0 {
		// Return an error only if we couldn't write to any disks.
		return lastErr
	}

	return core.NoError
}

// getTractserverID returns the master assigned tractserver id, which may not be valid if it's not set yet.
func (ms *MetadataStore) getTractserverID() core.TractserverID {
	ms.lock.Lock()
	defer ms.lock.Unlock()
	return ms.meta.TSID
}
