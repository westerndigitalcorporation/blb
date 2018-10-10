// Copyright (c) 2017 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package state

/*
 * If we have another bug where the database size blows up, this might be useful again.
 *

import (
	"os"

	"github.com/boltdb/bolt"

	log "github.com/golang/glog"
)

func maybeCompactDb(path string) {
	const (
		sizeToCompact = 15 << 30
		txSize        = 1 << 20
	)

	fi, err := os.Stat(path)
	if err != nil || fi.Size() < sizeToCompact {
		return
	}

	log.Infof("compacting state database...")

	dstpath := path + ".compacted"
	os.Remove(dstpath)

	src, err := bolt.Open(path, mode, nil)
	if err != nil {
		log.Fatalf("couldn't open old db: %s", err)
	}
	dst, err := bolt.Open(dstpath, mode, nil)
	if err != nil {
		log.Fatalf("couldn't open new db: %s", err)
	}

	stx, err := src.Begin(false)
	if err != nil {
		log.Fatalf("couldn't get read tx: %s", err)
	}

	copyBucket := func(b []byte, fillPct float64) {
		dtx, err := dst.Begin(true)
		if err != nil {
			log.Fatalf("couldn't get write tx: %s", err)
		}

		bkt, err := dtx.CreateBucket(b)
		if err != nil {
			log.Fatalf("create bucket error: %s", err)
		}
		bkt.FillPercent = fillPct

		size := 0
		stx.Bucket(b).ForEach(func(k, v []byte) error {
			bkt.Put(k, v)

			size += len(k) + len(v)
			if size >= txSize {
				// reopen transaction if we've added enough data
				if err := dtx.Commit(); err != nil {
					log.Fatalf("commit error: %s", err)
				}
				dtx, err = dst.Begin(true)
				if err != nil {
					log.Fatalf("couldn't get write tx: %s", err)
				}
				bkt = dtx.Bucket(b)
				bkt.FillPercent = fillPct
				size = 0
			}
			return nil
		})

		// final commit
		if err := dtx.Commit(); err != nil {
			log.Fatalf("commit error: %s", err)
		}
	}

	copyBucket(partitionBucket, defaultFillPct)
	copyBucket(metaBucket, defaultFillPct)
	copyBucket(blobBucket, blobFillPct)
	copyBucket(rschunkBucket, rsChunkFillPct)

	stx.Rollback()

	src.Close()
	dst.Close()

	if err := os.Rename(dstpath, path); err != nil {
		log.Fatalf("rename error: %s", err)
	}
	log.Infof("compacting done")
}
*/
