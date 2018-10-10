// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package testblb

import (
	"os"

	log "github.com/golang/glog"
	"github.com/westerndigitalcorporation/blb/internal/core"
)

// TestInterruptedRereplRead exercises the situation that might result if a
// curator crashed in the middle of a re-replication, after it had bumped the
// version on all tractservers but not updated its durable state yet.
func (tc *TestCase) TestInterruptedRereplRead() error {
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

	// Pretend that we have an interrupted rereplication by bumping versions
	// directly on all the tractservers.
	for host := 0; host < 3; host++ {
		if err := tc.bumpVersion(core.TractIDFromParts(core.BlobID(blob.ID()), 0), host); nil != err {
			return err
		}
	}

	// Now try a read. This will fail initially, but it will have called
	// FixVersion on the curator, which will bump the version on the remaining
	// host and update its durable state, so that a retry will succeed.
	blob.Seek(0, os.SEEK_SET)
	if n, err := blob.Read(data); err != nil || n != len(data) {
		log.Infof("read failed: %s", err)
		return err
	}

	return nil
}
