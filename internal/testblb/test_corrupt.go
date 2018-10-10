// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package testblb

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"

	log "github.com/golang/glog"
	"github.com/westerndigitalcorporation/blb/internal/core"
)

// TestCorruptTract corrupts a tract on a tract server that owns it. The tract
// server should detect that and finally the curator should re-replicate that
// tract.
//
//	1. Create a blob and write some data to it.
//	2. Pick a tract server that owns the first tract and corrupt it.
//	3. Wait until the tract server detects it and the tract gets re-replicated.
//
func (tc *TestCase) TestCorruptTract() error {
	// We need at least four tractservers.
	if tc.clusterCfg.Tractservers < 4 {
		return fmt.Errorf("need at least four tractservers for TestCorruptTract")
	}

	//	1. Create a blob and write some data to it.

	// Create a blob and fill it with one tract of data.
	blob, err := tc.c.Create()
	if err != nil {
		return err
	}

	data := makeRandom(1 * mb)
	blob.Seek(0, os.SEEK_SET)
	if n, err := blob.Write(data); err != nil || n != len(data) {
		return err
	}

	//	2. Pick a tract server that owns the first tract and corrupt it.

	// Get tract info about the first tract.
	tracts, err := tc.c.GetTracts(context.Background(), blob.ID(), 0, 1)
	if err != nil {
		return err
	}
	if len(tracts) == 0 {
		return fmt.Errorf("The blob has no tracts")
	}

	firstTract := tracts[0]
	hostToCorrupt := firstTract.Hosts[int(rand.Uint32())%len(firstTract.Hosts)]
	name := tc.bc.FindByServiceAddress(hostToCorrupt)

	captureCorrupt := tc.captureLogs()
	if err := tc.corruptTract(firstTract.Tract, name, 1234); err != nil {
		return err
	}

	//	3. Wait until the tract server detects it and the tract gets re-replicated.

	log.Infof("Wait until the corrupted tract %s to be re-replicated", firstTract.Tract)

	// Wait until the tract server detects the corruption and curator re-replicates
	// the corrupted tract.
	captureCorrupt.WaitFor(
		fmt.Sprintf("%s:@@@ tract .* has corruption error", name),
		fmt.Sprintf("c:@@@ rerepl %v succeeded", firstTract.Tract),
	)

	// If we are here it means the re-replication is done.
	return nil
}

func (tc *TestCase) findTract(tract core.TractID, tsName string) string {
	// From manager.go:40
	const tractDir = "data"

	// Get the root path of disk aggregator of the tract server.
	aggRoot := filepath.Join(tc.bc.GetConfig().RootDir, tsName)

	// We have no idea which disk owns that tract, so we'll search different
	// directories ourselves.
	for i := 0; i < int(tc.clusterCfg.DisksPerTractserver); i++ {
		tractPath := filepath.Join(aggRoot, fmt.Sprintf("disk%d", i), tractDir, tract.String())

		if _, err := os.Stat(tractPath); err == nil {
			return tractPath
		}
	}
	return ""
}

// Corrupt a given tract on a given tract server that owns it.
func (tc *TestCase) corruptTract(tract core.TractID, tsName string, offset int64) error {
	path := tc.findTract(tract, tsName)
	if path == "" {
		return fmt.Errorf("Can't find the tract file on tract server.")
	}

	// Great, the tract file is found, we gonna corrupt it.
	f, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		return err
	}

	log.Infof("writing garbage to %s", path)
	defer f.Close()

	// Convert offset to checksumfile offset
	offset += 4 * (offset / 65532)

	// Corrupt the file.
	f.Seek(offset, os.SEEK_SET)
	_, err = f.Write([]byte("bad data"))
	return err
}
