// Copyright (c) 2017 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package testblb

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"time"

	log "github.com/golang/glog"

	client "github.com/westerndigitalcorporation/blb/client/blb"
	"github.com/westerndigitalcorporation/blb/internal/core"
)

// TestStorageMigration tests that the curator can erasure-code data.
func (tc *TestCase) TestStorageMigration() error {
	// Make some blobs. The curator currently implements "warm" as RS(6, 3),
	// with a 64MB chunk size, so we need approximately 6 * 64MB = 384MB of data
	// to trigger coding a full set. We'll create 35 blobs of 16-32MB each (2-4
	// tracts), all in the same partition (so they end up on the same curator).
	N := 35
	low, high := 16000000, 32000000
	seed := int64(97531)
	buf := make([]byte, high)

	var blobs []*client.Blob
	var sizes []int
	for len(blobs) < N {
		b, _ := tc.c.Create(client.StorageWarm)
		if core.BlobID(b.ID()).Partition() != core.PartitionID(1) {
			// just drop blobs that aren't in partition 1
			tc.c.Delete(context.Background(), b.ID())
			continue
		}

		size := low + rand.Intn(high-low+1)
		data := buf[:size]
		rand.New(rand.NewSource(seed * int64(len(blobs)+1))).Read(data)
		b.Write(data)
		blobs = append(blobs, b)
		sizes = append(sizes, size)
	}

	capture := tc.captureLogs()
	var waitFor []string
	migrated := 0
	timeout := time.Now().Add(10 * time.Minute)

	// Wait for a few to be migrated.
	log.Infof("waiting for migration...")
	for time.Now().Before(timeout) && migrated < 3 {
		time.Sleep(5 * time.Second)
		migrated = 0
		waitFor = nil
		for _, b := range blobs {
			bi, _ := b.Stat()
			if bi.Class == core.StorageClass_RS_6_3 {
				migrated++
				// Wait for each replica of each tract to be GC-ed
				for tk := 0; tk < bi.NumTracts; tk++ {
					tid := core.TractIDFromParts(core.BlobID(b.ID()), core.TractKey(tk))
					for i := 0; i < bi.Repl; i++ {
						waitFor = append(waitFor, "t:@@@ gc-ing tract "+tid.String())
					}
				}
			}
		}
	}
	if migrated < 3 {
		return fmt.Errorf("timed out waiting for migration")
	}

	// Old tracts should be GC-ed.
	log.Infof("waiting for gc...")
	capture.WaitFor(waitFor...)

	// Read all data back.
	rbuf := make([]byte, high)
	for i, b := range blobs {
		n, err := b.ReadAt(buf, 0)
		if err != nil && err != io.EOF {
			return err
		}
		if n != sizes[i] {
			return fmt.Errorf("wrong size")
		}
		if bl, err := b.ByteLength(); err != nil {
			return err
		} else if int(bl) != n {
			return fmt.Errorf("wrong ByteLength")
		}
		data := buf[:n]
		rdata := rbuf[:n]

		rand.New(rand.NewSource(seed * int64(i+1))).Read(rdata)
		if !bytes.Equal(data, rdata) {
			return fmt.Errorf("data mismatch for blob %s", b.ID())
		}
	}

	return nil
}
