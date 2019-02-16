// Copyright (c) 2017 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package fb

import (
	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/westerndigitalcorporation/blb/internal/core"
)

// BuildPartition encodes a Partition from a struct.
func BuildPartition(p *Partition) []byte {
	bu := flatbuffers.NewBuilder(64)
	PartitionFStart(bu)
	PartitionFAddId(bu, uint32(p.Id))
	PartitionFAddNextBlobKey(bu, uint32(p.NextBlobKey))
	PartitionFAddNextRsChunkKey(bu, p.NextRsChunkKey)
	bu.Finish(PartitionFEnd(bu))
	return bu.FinishedBytes()
}

// BuildBlob encodes a Blob from a struct.
func BuildBlob(b *Blob) []byte {
	bu := flatbuffers.NewBuilder(128)

	putTid := func(tid *core.TractID) flatbuffers.UOffsetT {
		if tid == nil {
			return 0 // default value, will make flatbuffers not add field
		}
		return PutTractID(bu, *tid)
	}

	putTract := func(t *Tract) flatbuffers.UOffsetT {
		hosts012, hosts3p := TractFSetupHosts(bu, t.Hosts)
		TractFStart(bu)
		TractFAddHosts012(bu, hosts012)
		TractFAddHosts3p(bu, hosts3p)
		TractFAddVersion(bu, uint32(t.Version))
		TractFAddRs63Chunk(bu, putTid(t.Rs63Chunk))
		TractFAddRs83Chunk(bu, putTid(t.Rs83Chunk))
		TractFAddRs103Chunk(bu, putTid(t.Rs103Chunk))
		TractFAddRs125Chunk(bu, putTid(t.Rs125Chunk))
		return TractFEnd(bu)
	}

	putTracts := func(tracts []*Tract) flatbuffers.UOffsetT {
		tLen := len(tracts)
		if tLen == 0 {
			return 0
		}

		tOffs := make([]flatbuffers.UOffsetT, tLen)
		for i := tLen - 1; i >= 0; i-- {
			tOffs[tLen-i-1] = putTract(tracts[i])
		}

		BlobFStartTractsVector(bu, tLen)
		for _, off := range tOffs {
			bu.PrependUOffsetT(off)
		}
		return bu.EndVector(tLen)
	}

	tVec := putTracts(b.Tracts)

	BlobFStart(bu)
	BlobFAddPackedMeta(bu, PackMeta(b.Storage, b.Hint, int(b.Repl)))
	BlobFAddTracts(bu, tVec)
	BlobFAddDeleted(bu, b.Deleted)
	BlobFAddMtime(bu, b.Mtime)
	BlobFAddAtime(bu, b.Atime)
	BlobFAddExpires(bu, b.Expires)
	bu.Finish(BlobFEnd(bu))
	return bu.FinishedBytes()
}

// BuildRSChunk encodes an RSChunk from a struct.
func BuildRSChunk(c *RSChunk) []byte {
	bu := flatbuffers.NewBuilder(256)

	putHostsVec := func(hosts []core.TractserverID) flatbuffers.UOffsetT {
		RSChunkFStartHostsVector(bu, len(hosts))
		for i := len(hosts) - 1; i >= 0; i-- {
			bu.PrependUint32(uint32(hosts[i]))
		}
		return bu.EndVector(len(hosts))
	}

	putData := func(tracts []*RSC_Tract) flatbuffers.UOffsetT {
		RSC_DataFStartTractsVector(bu, len(tracts))
		for i := len(tracts) - 1; i >= 0; i-- {
			t := tracts[i]
			CreateRSC_TractFWithID(bu, t.Id, t.Length, t.Offset)
		}
		vec := bu.EndVector(len(tracts))
		RSC_DataFStart(bu)
		RSC_DataFAddTracts(bu, vec)
		return RSC_DataFEnd(bu)
	}

	putDataVec := func(data []*RSC_Data) flatbuffers.UOffsetT {
		dLen := len(data)
		dOffs := make([]flatbuffers.UOffsetT, dLen)
		for i := dLen - 1; i >= 0; i-- {
			dOffs[dLen-i-1] = putData(data[i].Tracts)
		}
		RSChunkFStartDataVector(bu, dLen)
		for _, off := range dOffs {
			bu.PrependUOffsetT(off)
		}
		return bu.EndVector(dLen)
	}

	dVec := putDataVec(c.Data)
	hVec := putHostsVec(c.Hosts)
	RSChunkFStart(bu)
	RSChunkFAddData(bu, dVec)
	RSChunkFAddHosts(bu, hVec)
	bu.Finish(RSChunkFEnd(bu))
	return bu.FinishedBytes()
}
