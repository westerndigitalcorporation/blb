// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package state

import (
	"hash/crc64"

	"github.com/boltdb/bolt"
)

// ChecksumPosition records the starting or endpoing point of a checksum iteration.
type ChecksumPosition struct {
	Blob []byte
	RS   []byte
}

var crcTable = crc64.MakeTable(crc64.ECMA)

// Checksum computes a checksum of a portion of the state. Partitions and
// metadata are always included, and a subset of the blob and RS data (since
// that can be very large).
func (t *Txn) Checksum(start ChecksumPosition, n int) (checksum uint64, next ChecksumPosition) {
	crc := checksumWholeBucket(0, t.txn.Bucket(metaBucket))
	crc = checksumWholeBucket(crc, t.txn.Bucket(partitionBucket))
	crc, next.Blob = checksumPartialBucket(crc, t.txn.Bucket(blobBucket), start.Blob, n)
	crc, next.RS = checksumPartialBucket(crc, t.txn.Bucket(rschunkBucket), start.RS, n)
	return crc, next
}

func checksumWholeBucket(crc uint64, b *bolt.Bucket) uint64 {
	b.ForEach(func(k, v []byte) error {
		crc = crc64.Update(crc, crcTable, k)
		crc = crc64.Update(crc, crcTable, v)
		return nil
	})
	return crc
}

func checksumPartialBucket(crc uint64, b *bolt.Bucket, start []byte, n int) (uint64, []byte) {
	c := b.Cursor()
	var k, v []byte
	if start == nil {
		k, v = c.First()
	} else {
		k, v = c.Seek(start)
	}
	for k != nil && n > 0 {
		crc = crc64.Update(crc, crcTable, k)
		crc = crc64.Update(crc, crcTable, v)
		k, v = c.Next()
		n--
	}
	return crc, copySlice(k)
}

func copySlice(b []byte) []byte {
	if b == nil {
		return nil
	}
	o := make([]byte, len(b))
	copy(o, b)
	return o
}
