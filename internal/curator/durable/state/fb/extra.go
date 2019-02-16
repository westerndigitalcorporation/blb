// Copyright (c) 2017 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package fb

import (
	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/westerndigitalcorporation/blb/internal/core"
)

// Helpers for meta in BlobF:

const (
	metaStorage = iota * 8
	metaHint
	metaRepl
)

func (b *BlobF) Storage() core.StorageClass {
	return core.StorageClass((b.PackedMeta() >> metaStorage) & 0xff)
}

func (b *BlobF) Hint() core.StorageHint {
	return core.StorageHint((b.PackedMeta() >> metaHint) & 0xff)
}

func (b *BlobF) Repl() int {
	return int((b.PackedMeta() >> metaRepl) & 0xff)
}

func PackMeta(storage core.StorageClass, hint core.StorageHint, repl int) uint32 {
	return (uint32(storage)&0xff)<<metaStorage |
		(uint32(hint)&0xff)<<metaHint |
		(uint32(repl)&0xff)<<metaRepl
}

// Expand/truncate unix nanos/seconds so we can work with nanos in the rest of
// the codebase:

func (b *BlobF) Deleted() int64 { return int64(b.Deletedsec()) * 1e9 }
func (b *BlobF) Mtime() int64   { return int64(b.Mtimesec()) * 1e9 }
func (b *BlobF) Atime() int64   { return int64(b.Atimesec()) * 1e9 }
func (b *BlobF) Expires() int64 { return int64(b.Expiressec()) * 1e9 }

func (b *BlobF) MutateDeleted(n int64) bool { return b.MutateDeletedsec(uint32(n / 1e9)) }
func (b *BlobF) MutateMtime(n int64) bool   { return b.MutateMtimesec(uint32(n / 1e9)) }
func (b *BlobF) MutateAtime(n int64) bool   { return b.MutateAtimesec(uint32(n / 1e9)) }
func (b *BlobF) MutateExpires(n int64) bool { return b.MutateExpiressec(uint32(n / 1e9)) }

func BlobFAddDeleted(b *flatbuffers.Builder, n int64) { BlobFAddDeletedsec(b, uint32(n/1e9)) }
func BlobFAddMtime(b *flatbuffers.Builder, n int64)   { BlobFAddMtimesec(b, uint32(n/1e9)) }
func BlobFAddAtime(b *flatbuffers.Builder, n int64)   { BlobFAddAtimesec(b, uint32(n/1e9)) }
func BlobFAddExpires(b *flatbuffers.Builder, n int64) { BlobFAddExpiressec(b, uint32(n/1e9)) }

// Helpers for TractIDF:

func (t *TractIDF) TractID() core.TractID {
	bid := uint64(t.B5())<<48 | uint64(t.B4())<<32 | uint64(t.B3())<<16 | uint64(t.B2())
	return core.TractID{Blob: core.BlobID(bid), Index: core.TractKey(t.B1())}
}

func (t *TractIDF) RSChunkID() core.RSChunkID {
	pid := uint32(t.B5())<<16 | uint32(t.B4())
	id := uint64(t.B3())<<32 | uint64(t.B2())<<16 | uint64(t.B1())
	return core.RSChunkID{Partition: core.PartitionID(pid), ID: id}
}

func PutTractID(bu *flatbuffers.Builder, t core.TractID) flatbuffers.UOffsetT {
	return CreateTractIDF(bu,
		uint16(t.Index),
		uint16(t.Blob>>0),
		uint16(t.Blob>>16),
		uint16(t.Blob>>32),
		uint16(t.Blob>>48))
}

func PutRSChunkID(bu *flatbuffers.Builder, c core.RSChunkID) flatbuffers.UOffsetT {
	return CreateTractIDF(bu,
		uint16(c.ID>>0),
		uint16(c.ID>>16),
		uint16(c.ID>>32),
		uint16(c.Partition>>0),
		uint16(c.Partition>>16))
}

// Helpers for RSC_TractF:

func (t *RSC_TractF) TractID() core.TractID {
	var tid TractIDF
	return t.Id(&tid).TractID()
}

func CreateRSC_TractFWithID(bu *flatbuffers.Builder, t core.TractID, length uint32, offset uint32) flatbuffers.UOffsetT {
	return CreateRSC_TractF(bu,
		uint16(t.Index),
		uint16(t.Blob>>0),
		uint16(t.Blob>>16),
		uint16(t.Blob>>32),
		uint16(t.Blob>>48),
		length, offset)
}

// Helpers for TractF:

// HostsLength makes Hosts in TractF look like a normal field.
func (t *TractF) HostsLength() (count int) {
	hosts012 := t.Hosts012()
	if hosts012&(0xfffff<<0) == 0 {
		return 0
	}
	if hosts012&(0xfffff<<20) == 0 {
		return 1
	}
	if hosts012&(0xfffff<<40) == 0 {
		return 2
	}
	return 3 + t.Hosts3pLength()
}

// Hosts makes Hosts in TractF look like a normal field.
func (t *TractF) Hosts(j int) uint32 {
	switch j {
	case 0:
		return uint32((t.Hosts012() >> 0) & 0xfffff)
	case 1:
		return uint32((t.Hosts012() >> 20) & 0xfffff)
	case 2:
		return uint32((t.Hosts012() >> 40) & 0xfffff)
	default:
		return t.Hosts3p(j - 3)
	}
}

// MutateHostsFromSlice handles mutation of the hosts values split over two
// fields. It cannot change the length of hosts.
func (t *TractF) MutateHostsFromSlice(hosts []core.TractserverID) (ok bool) {
	if len(hosts) != t.HostsLength() {
		return false
	}
	new012, _ := TractFSetupHosts(nil, hosts)
	ok = t.MutateHosts012(new012)
	if len(hosts) > 3 {
		for i, h := range hosts[3:] {
			ok = ok && t.MutateHosts3p(i, uint32(h))
		}
	}
	return
}

// TractFSetupHosts returns values that should be passed to TractFAddHosts012
// and TractFAddHosts3p to encode the given host list. Note that zero is an
// invalid TractserverID and will not be handled correctly.
func TractFSetupHosts(bu *flatbuffers.Builder, hosts []core.TractserverID) (hosts012 uint64, hosts3p flatbuffers.UOffsetT) {
	hLen := len(hosts)
	if hLen > 3 && bu != nil {
		TractFStartHosts3pVector(bu, hLen-3)
		for j := hLen - 1; j >= 3; j-- {
			bu.PrependUint32(uint32(hosts[j]))
		}
		hosts3p = bu.EndVector(hLen - 3)
	}
	if hLen > 2 {
		hosts012 |= uint64(hosts[2]&0xfffff) << 40
	}
	if hLen > 1 {
		hosts012 |= uint64(hosts[1]&0xfffff) << 20
	}
	if hLen > 0 {
		hosts012 |= uint64(hosts[0]&0xfffff) << 0
	}
	return
}

// Helpers for stuff with hosts lists:

// HostLister is a flatbuffer that contains a list of hosts, currently: TractF and RSChunkF.
type HostsLister interface {
	HostsLength() int
	Hosts(j int) uint32
}

// HostsList returns the list of hosts as a slice.
func HostsList(l HostsLister) []core.TractserverID {
	out := make([]core.TractserverID, l.HostsLength())
	for i := range out {
		out[i] = core.TractserverID(l.Hosts(i))
	}
	return out
}

// HostsContains returns true if this list contains the given host.
func HostsContains(hl HostsLister, host core.TractserverID) bool {
	l := hl.HostsLength()
	for i := 0; i < l; i++ {
		if core.TractserverID(hl.Hosts(i)) == host {
			return true
		}
	}
	return false
}

// Mutation helpers:

// CloneBuffer copies the entire buffer backing the given FlatBuffer and points it to the
// new buffer. You have to use this before calling any of the Mutate methods if the
// FlatBuffer points into BoltDB-managed memory. Even though this copies data, it doesn't
// have to parse it.
func CloneBuffer(obj flatbuffers.FlatBuffer) []byte {
	t := obj.Table()
	b := make([]byte, len(t.Bytes))
	copy(b, t.Bytes)
	obj.Init(b, t.Pos)
	return b
}
