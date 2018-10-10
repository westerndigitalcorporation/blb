// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package testblb

import (
	"bytes"
	"fmt"
	"os"

	log "github.com/golang/glog"
)

// TestWriteReadRaftFailure tests that replicas of master and curator can
// fail arbitrarily but as long as a quorum of master or curator are up the
// whole Blb cluster should still function.
//
//	1. Create blob and write some data to it.
//	2. Kill all master and curator replicas and restart a quorum of instance.
//	3. Read back the original blob so we know the blob metadata is still
//	   accessible.
//	4. Create, write and verify a new Blob so we know masters & curators are still
//	   writable.
//
func (tc *TestCase) TestWriteReadRaftFailure() error {

	//	1. Create a blob and write some data to it.

	// Create a blob and fill it with one tract of data.
	blob, err := tc.c.Create()
	if err != nil {
		return err
	}

	buf := make([]byte, 1*mb)
	data := makeRandom(1 * mb)
	blob.Seek(0, os.SEEK_SET)
	if n, err := blob.Write(data); err != nil || n != len(data) {
		return err
	}

	//	2. Kill all master and curator replicas and restart a quorum of instance.
	masters := tc.bc.Masters()
	curatorGroups := tc.bc.Curators()

	/// Kill all of master replicas.
	for _, m := range masters {
		m.Stop()
	}
	/// Kill all of curator replicas.
	for _, group := range curatorGroups {
		for _, c := range group {
			c.Stop()
		}
	}

	captureNewLeader := tc.captureLogs()

	// Restart a quorum of master and curator replicas.
	for i := 0; i < len(masters)/2+1; i++ {
		masters[i].Start()
	}
	for _, group := range curatorGroups {
		for i := 0; i < len(group)/2+1; i++ {
			group[i].Start()
		}
	}

	//	3. Read back the original blob so we know the blob metadata is still
	//	   accessible.

	log.Infof("Wait for new leaders of master and curator get elected.")
	waitFor := []string{"m.:@@@ became leader"}
	for i := 0; i < int(tc.clusterCfg.CuratorGroups); i++ {
		waitFor = append(waitFor, fmt.Sprintf("c%d:@@@ leadership changed: true", i))
	}
	if err := captureNewLeader.WaitFor(waitFor...); err != nil {
		return err
	}

	// Read the data once just to check.
	blob.Seek(0, os.SEEK_SET)
	if n, err := blob.Read(buf); err != nil {
		return err
	} else if n != len(data) || bytes.Compare(buf, data) != 0 {
		return fmt.Errorf("data mismatch")
	}

	//	4. Create, write and verify a new Blob so we know masters & curators are still
	//	   writable.

	tc.TestWriteReadNoFailure()

	return nil
}
