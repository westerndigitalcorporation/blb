// Copyright (c) 2017 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package testblb

import (
	"bytes"
	"context"
	"fmt"
	log "github.com/golang/glog"
	"io"
	"math/rand"
	"strings"
	"time"

	client "github.com/westerndigitalcorporation/blb/client/blb"
	"github.com/westerndigitalcorporation/blb/internal/cluster"
	"github.com/westerndigitalcorporation/blb/internal/core"
)

// TestRSRecovery tests that the curator can recover RS data from various types
// of failures. We test a bunch of things serially to amoritize the setup time.
func (tc *TestCase) TestRSRecovery() error {
	// Make some blobs. The curator currently implements "warm" as RS(6, 3),
	// with a 64MB chunk size, so we need approximately 6 * 64MB = 384MB of data
	// to trigger coding a full set. We'll create 20 blobs of 16-32MB each (2-4
	// tracts), all in the same partition (so they end up on the same curator).
	N := 20
	low, high := 16000000, 32000000
	seed := int64(97531)
	buf := make([]byte, high)
	rbuf := make([]byte, len(buf))

	var blobs []*client.Blob
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
	}

	// Wait for one to be migrated.
	var rsBlob *client.Blob
	var rsIdx int
outer:
	for {
		for i, b := range blobs {
			bi, _ := b.Stat()
			if bi.Class == core.StorageClass_RS_6_3 {
				rsBlob = b
				rsIdx = i
				break outer
			}
		}
		time.Sleep(5 * time.Second)
	}

	// Figure out where the first tract is.
	rsTract, tsName, tsProc, err := tc.findRSTract(rsBlob.ID())
	if err != nil {
		return err
	}

	// Test 1: Kill that tractserver.
	capture := tc.captureLogs()
	tsProc.Stop()
	// If we have enough free tractservers, we can kill up to two more.
	// Triggering this requires running testblb with -tractservers=12 or more.
	// (-6-3 for RS(6, 3), -1 for the 1 we already killed)
	allTs := tc.bc.Tractservers()
	for i := 0; i < len(allTs)-6-3-1 && i < 2; i++ {
		allTs[rand.Intn(len(allTs))].Stop()
	}

	// Reading it should fail.
	shortBuf := buf[:100000]
	_, err = tc.tryReadDirect(rsBlob.ID(), shortBuf)
	if err == nil || err == io.EOF {
		return fmt.Errorf("read after killing should not have succeeded")
	}

	// Read with reconstructing client should work.
	rand.New(rand.NewSource(seed * int64(rsIdx+1))).Read(buf)
	// within one tract
	if err = tc.tryReconstructingRead(rsBlob.ID(), buf, 4000, 123000); err != nil {
		return err
	}
	// cross tracts and unaligned offset
	if err = tc.tryReconstructingRead(rsBlob.ID(), buf, core.TractLength-56789, 98765); err != nil {
		return err
	}

	log.Infof("waiting for reconstruction 1...")
	if err = capture.WaitFor("c:@@@ reconstruction .* succeeded"); err != nil {
		return err
	}

	// Read all data back.
	if err = readAllData(blobs, buf, rbuf, seed); err != nil {
		return err
	}

	// Test 2: Start the old TS back up again and make sure that piece is GC-ed.
	capture = tc.captureLogs()
	tsProc.Start()
	log.Infof("waiting for gc...")
	if err := capture.WaitFor(tsName + ":@@@ gc-ing tract " + rsTract.Chunk.ToTractID().String()); err != nil {
		return err
	}

	// Test 3: Find the new home of the data and corrupt it.
	if rsTract, tsName, tsProc, err = tc.findRSTract(rsBlob.ID()); err != nil {
		return err
	}

	capture = tc.captureLogs()
	tc.corruptTract(rsTract.Chunk.ToTractID(), tsName, int64(rsTract.Offset))

	// Instead of waiting for the scrub to find it, read the data, which will
	// fail and trigger a corruption report to the curator.
	_, err = tc.tryReadDirect(rsBlob.ID(), shortBuf)
	if err == nil || err == io.EOF {
		return fmt.Errorf("read after corrupting should not have succeeded")
	}
	log.Infof("waiting for reconstruction 2...")
	if err := capture.WaitFor("c:@@@ reconstruction .* succeeded"); err != nil {
		return err
	}

	// Test 4: Find the new home again, and this time delete the file.
	if rsTract, tsName, tsProc, err = tc.findRSTract(rsBlob.ID()); err != nil {
		return err
	}

	capture = tc.captureLogs()
	tc.deleteTract(rsTract.Chunk.ToTractID(), tsName)

	// Read to force a corruption report.
	_, err = tc.tryReadDirect(rsBlob.ID(), shortBuf)
	if err == nil || err == io.EOF {
		return fmt.Errorf("read after deletion should not have succeeded")
	}
	log.Infof("waiting for reconstruction 3...")
	if err := capture.WaitFor("c:@@@ reconstruction .* succeeded"); err != nil {
		return err
	}

	// Read all data back.
	if err = readAllData(blobs, buf, rbuf, seed); err != nil {
		return err
	}

	return nil
}

func (tc *TestCase) findRSTract(id client.BlobID) (rsTract core.TractPointer, tsName string, tsProc cluster.Proc, err error) {
	var tracts []core.TractInfo
	tracts, err = tc.noRetryC.GetTracts(context.Background(), id, 0, 1)
	if err != nil {
		return
	}
	rsTract = tracts[0].RS
	if !rsTract.Present() {
		err = fmt.Errorf("RS pointer not present")
		return
	}
	tsName = tc.bc.FindByServiceAddress(rsTract.Host)
	tsProc = tc.bc.FindProc(tsName)
	return
}

func readAllData(blobs []*client.Blob, buf, rbuf []byte, seed int64) error {
	for i, b := range blobs {
		n, err := b.ReadAt(buf, 0)
		if err != nil && err != io.EOF {
			return err
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

func (tc *TestCase) tryReadDirect(id client.BlobID, buf []byte) (int, error) {
	b, err := tc.noRetryC.Open(id, "r")
	if err != nil {
		return 0, err
	}
	return b.ReadAt(buf, 0)
}

func (tc *TestCase) tryReconstructingRead(id client.BlobID, exp []byte, offset, length int) error {
	cli := client.NewClient(client.Options{
		Cluster:             strings.Join(tc.bc.MasterAddrs(), ","),
		DisableRetry:        true,
		ReconstructBehavior: client.ReconstructBehavior{Enabled: true},
	})
	b, err := cli.Open(id, "r")
	if err != nil {
		return fmt.Errorf("reconstructing read open failed: %s", err)
	}
	buf := make([]byte, length)
	n, err := b.ReadAt(buf, int64(offset))
	if err != nil || n < length {
		return fmt.Errorf("reconstructing read failed: %d, %s", n, err)
	}
	if !bytes.Equal(buf, exp[offset:offset+length]) {
		return fmt.Errorf("data mismatch for reconstructing read")
	}
	return nil
}
