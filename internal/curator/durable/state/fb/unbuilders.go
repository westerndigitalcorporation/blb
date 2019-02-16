// Copyright (c) 2019 David Reiss. All rights reserved.
// SPDX-License-Identifier: MIT

package fb

import "github.com/westerndigitalcorporation/blb/internal/core"

// ToStruct creates a Tract from a TractF.
func (t *TractF) ToStruct() (o *Tract) {
	o = new(Tract)
	o.Hosts = HostsList(t)
	o.Version = int(t.Version())
	f := func(id *TractIDF) *core.TractID {
		if id == nil {
			return nil
		}
		tid := id.TractID()
		return &tid
	}
	var id TractIDF
	o.Rs63Chunk = f(t.Rs63Chunk(&id))
	o.Rs83Chunk = f(t.Rs83Chunk(&id))
	o.Rs103Chunk = f(t.Rs103Chunk(&id))
	o.Rs125Chunk = f(t.Rs125Chunk(&id))
	return
}

// ToStruct creates a Blob from a BlobF.
func (b *BlobF) ToStruct() (o *Blob) {
	o = new(Blob)
	o.Storage = b.Storage()
	o.Hint = b.Hint()
	o.Tracts = make([]*Tract, b.TractsLength())
	var t TractF
	for i := range o.Tracts {
		b.Tracts(&t, i)
		o.Tracts[i] = t.ToStruct()
	}
	o.Repl = b.Repl()
	o.Deleted = b.Deleted()
	o.Mtime = b.Mtime()
	o.Atime = b.Atime()
	o.Expires = b.Expires()
	return
}

// ToStruct creates a Blob from a BlobF.
func (p *PartitionF) ToStruct() (o *Partition) {
	o = new(Partition)
	o.Id = core.PartitionID(p.Id())
	o.NextBlobKey = core.BlobKey(p.NextBlobKey())
	o.NextRsChunkKey = p.NextRsChunkKey()
	return
}

// ToStruct creates a RSChunk from a RSChunkF.
func (r *RSChunkF) ToStruct() (o *RSChunk) {
	o = new(RSChunk)
	o.Data = make([]*RSC_Data, r.DataLength())
	var d RSC_DataF
	for i := range o.Data {
		r.Data(&d, i)
		o.Data[i] = d.ToStruct()
	}
	o.Hosts = HostsList(r)
	return
}

// ToStruct creates a RSC_Data from a RSC_DataF.
func (d *RSC_DataF) ToStruct() (o *RSC_Data) {
	o = new(RSC_Data)
	o.Tracts = make([]*RSC_Tract, d.TractsLength())
	var t RSC_TractF
	for i := range o.Tracts {
		d.Tracts(&t, i)
		o.Tracts[i] = t.ToStruct()
	}
	return
}

// ToStruct creates a RSC_Tract from a RSC_TractF.
func (t *RSC_TractF) ToStruct() (o *RSC_Tract) {
	o = new(RSC_Tract)
	var id TractIDF
	t.Id(&id)
	o.Id = id.TractID()
	o.Length = t.Length()
	o.Offset = t.Offset()
	return
}
