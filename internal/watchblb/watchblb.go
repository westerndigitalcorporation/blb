// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package watchblb

import (
	"context"
	"time"

	log "github.com/golang/glog"

	client "github.com/westerndigitalcorporation/blb/client/blb"
	"github.com/westerndigitalcorporation/blb/internal/core"
	"github.com/westerndigitalcorporation/blb/internal/server"
)

// BlbWatcher watches the status of a blb cluster, proactively verifies its
// healthiness via background read/write traffic, and triggers alerts when
// something goes wrong.
type BlbWatcher struct {
	cfg Config         // Configuration parameters.
	cli *client.Client // blb client.
	db  *SqliteDB      // The database for storing blob ids.

	// Per operation stats.
	opm *server.OpMetric
}

// NewBlbWatcher creates a new BlbWatcher with the given client.
func NewBlbWatcher(cfg Config) *BlbWatcher {
	return &BlbWatcher{
		cfg: cfg,
		cli: client.NewClient(client.Options{Cluster: cfg.Cluster, RetryTimeout: 5 * time.Second}),
		db:  NewSqliteDB(cfg.TableFile),
		opm: server.NewOpMetric("watchblb", "op"),
	}
}

// Start starts the BlbWatcher. This is blocking.
func (bw *BlbWatcher) Start() {
	defer bw.db.Close()

	// Clean up possibly leaked blobs due to failures in a previous
	// BlbWatcher.clean operation.
	toremove, err := bw.db.GetDeleted()
	if err != nil {
		log.Fatalf("failed to retrieve possibly leaked blobs: %s", err)
	}
	bw.remove(toremove)

	// Create a buffer to use for all read/write.
	b := make([]byte, bw.cfg.WriteSize)

	// Start looping forever.
	writeC := time.Tick(bw.cfg.WriteInterval)
	readC := time.Tick(bw.cfg.ReadInterval)
	cleanC := time.Tick(bw.cfg.CleanInterval)
	for {
		select {
		// Make sure that cleanC is picked first if it has ticked.
		case <-cleanC:
			bw.clean()
		default:
			select {
			case <-cleanC:
				bw.clean()
			case <-writeC:
				bw.write(b)
			case <-readC:
				bw.read(b)
			}
		}
	}
}

// Create and write a blob.
func (bw *BlbWatcher) write(b []byte) {
	op := bw.opm.Start("write")

	// Create a blob.
	blob, cerr := bw.cli.Create(client.ReplFactor(bw.cfg.ReplFactor))
	if cerr != nil {
		log.Errorf("failed to create a blob: %s", cerr)
		op.Failed()
		return
	}

	// Write data to the blob.
	fillBytes(blob.ID(), 0, b)
	if n, err := blob.Write(b); err != nil {
		log.Errorf("failed to write to blob %s: %s", blob.ID(), err)
		op.Failed()
		return
	} else if n != len(b) {
		log.Errorf("short write to blob %s: expected %d and got %d", blob.ID(), len(b), n)
		op.Failed()
		return
	}
	op.End()

	// Write the blob id into db.
	if err := bw.db.Put(blob.ID(), time.Now()); err != nil {
		log.Errorf("failed to log blob %s to db: %s", blob.ID(), err)
	}

	log.Infof("successfully created blob %s", blob.ID())
}

// Read and verify a random blob created before.
func (bw *BlbWatcher) read(b []byte) {
	// Get one random blob id from db.
	id, err := bw.db.Rand()
	if err == errNoBlobs {
		log.Infof("no previously written blobs, skip read")
		return
	}
	if err != nil {
		log.Errorf("failed to retrieve blob id from db: %s", err)
		return
	}

	op := bw.opm.Start("read")
	defer op.End()

	// Open the blob.
	blob, oerr := bw.cli.Open(id, "r")
	if oerr != nil {
		log.Errorf("failed to open blob %s: %s", id, oerr)
		op.Failed()
		return
	}

	// Verify the length.
	if length, err := blob.ByteLength(); err != nil {
		log.Errorf("failed to get blob length: %s", err)
		op.Failed()
		return
	} else if length != bw.cfg.WriteSize {
		log.Errorf("wrong size: expected %d and got %d", bw.cfg.WriteSize, length)
		op.Failed()
		return
	}

	// Read data from the blob and verify.
	if n, err := blob.Read(b); err != nil {
		log.Errorf("failed to read from blob %s: %s", id, err)
		op.Failed()
		return
	} else if n != len(b) {
		log.Errorf("short read from blob %s: expected %d and got %d", id, len(b), n)
		op.Failed()
		return
	}
	if !verifyBytes(id, 0, b) {
		log.Fatalf("mismatched data, data corruption?")
	}

	log.Infof("successfully verified blob %s", id)
}

// Remove blobs that have reached the end of their lifetime.
func (bw *BlbWatcher) clean() {
	// Retrieve expired blobs from db.
	blobs, err := bw.db.DeleteIfExpires(bw.cfg.CleanInterval)
	if err != nil {
		log.Errorf("failed to get expired blobs: %s", err)
		return
	}
	bw.remove(blobs)
}

// Remove the given blobs from the blb cluster and the db.
func (bw *BlbWatcher) remove(blobs []client.BlobID) {
	// Remove them from the blb cluster.
	var removed []client.BlobID
	for _, blob := range blobs {
		op := bw.opm.Start("clean")
		if err := bw.cli.Delete(context.Background(), blob); err != nil && err.Error() != core.ErrNoSuchBlob.Error().Error() {
			log.Errorf("failed to remove blob %s: %s", blob, err)
			op.Failed()
		} else {
			removed = append(removed, blob)
			log.Infof("successfully removed expired blob %s", blob)
		}
		op.End()
	}

	// Remove from the db.
	if err := bw.db.ConfirmDeletion(removed); err != nil {
		log.Errorf("failed to remove blobs from the db: %s", err)
	}
}

//====== Helpers ======//

// fillBytes fills 'buf' with deterministic bytes. The first byte is computed
// from 'computeBase' and each following byte equals the previous one plus one.
func fillBytes(id client.BlobID, off int64, buf []byte) {
	base := computeBase(id, off)
	for i := range buf {
		buf[i] = base
		base++
	}
}

// verifyBytes verifies bytes in 'buf' is computed from 'fillBytes'.
func verifyBytes(id client.BlobID, off int64, buf []byte) bool {
	base := computeBase(id, off)
	for _, b := range buf {
		if b != base {
			return false
		}
		base++
	}
	return true
}

// A deterministic mapping from (blob, offset) to a byte.
func computeBase(id client.BlobID, off int64) byte {
	bid := core.BlobID(id)
	x := uint32(bid.Partition()) ^ uint32(bid.ID()) ^ uint32(off>>32) ^ uint32(off)
	return byte(x) ^ byte(x>>8) ^ byte(x>>16) ^ byte(x>>24)
}
