// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package state

import (
	"encoding/binary"
	"log"
	"unsafe"

	"github.com/golang/protobuf/proto"

	"github.com/westerndigitalcorporation/blb/internal/core"
)

// Converts a partition ID to a unique key in DB.
func partitionID2Key(id core.PartitionID) []byte {
	var key [4]byte
	binary.BigEndian.PutUint32(key[:], uint32(id))
	return key[:]
}

// Converts a blob ID to a unique key in DB.
func blobID2Key(id core.BlobID) []byte {
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], uint64(id))
	return key[:]
}

// Converts a key in DB to a blob ID. This is the reverse of blobID2Key.
func key2BlobID(key []byte) (id core.BlobID) {
	if len(key) != int(unsafe.Sizeof(id)) {
		log.Fatalf("wrong format of key for blob ID")
	}
	id = core.BlobID(binary.BigEndian.Uint64(key[:]))
	return id
}

// Converts a key to a partition ID.
func key2PartitionID(key []byte) (id core.PartitionID) {
	if len(key) != int(unsafe.Sizeof(id)) {
		log.Fatalf("wrong format of key for partition ID")
	}
	id = core.PartitionID(binary.BigEndian.Uint32(key[:]))
	return id
}

func tractID2Key(id core.TractID) []byte {
	var key [10]byte
	binary.BigEndian.PutUint64(key[0:8], uint64(id.Blob))
	binary.BigEndian.PutUint16(key[8:10], uint16(id.Index))
	return key[:]
}

func rschunkID2Key(id core.RSChunkID) []byte {
	var key [10]byte
	// put id first, overwrite first two bytes with partition
	binary.BigEndian.PutUint64(key[2:10], uint64(id.ID))
	binary.BigEndian.PutUint32(key[0:4], uint32(id.Partition))
	return key[:]
}

func key2rschunkID(key []byte) (c core.RSChunkID) {
	c.Partition = core.PartitionID(binary.BigEndian.Uint32(key[0:4]))
	c.ID = uint64(binary.BigEndian.Uint16(key[4:6]))<<32 | uint64(binary.BigEndian.Uint32(key[6:10]))
	return
}

func mustMarshal(pb proto.Message) []byte {
	b, e := proto.Marshal(pb)
	if e != nil {
		log.Fatalf("Error marshaling proto message: %v", e)
	}
	return b
}

func mustUnmarshal(buf []byte, pb proto.Message) {
	e := proto.Unmarshal(buf, pb)
	if e != nil {
		log.Fatalf("Error unmarshaling proto message: %v", e)
	}
}
