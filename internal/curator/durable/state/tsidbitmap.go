// Copyright (c) 2017 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package state

import (
	"encoding/binary"

	"github.com/westerndigitalcorporation/blb/internal/core"
)

// tsidbitmap is a simple bitmap representing a set of tractserver IDs that can
// be easily serialized to store in the database.
type tsidbitmap []uint64

// Set adds an id to the bitmap and returns a possibly-modified bitmap. (The
// receiver is invalid after, like append).
func (bm tsidbitmap) Set(id core.TractserverID) tsidbitmap {
	idx := uint64(id) / 64
	off := uint64(id) % 64
	for uint64(len(bm)) <= idx {
		bm = append(bm, 0)
	}
	bm[idx] |= uint64(1) << off
	return bm
}

// Merge merges another bitmap into this one. The receiver is invalid after
// (like append), but the other is not modified. The second return value will be
// true if any new bits were set.
func (bm tsidbitmap) Merge(other tsidbitmap) (tsidbitmap, bool) {
	for len(bm) < len(other) {
		bm = append(bm, 0)
	}
	dirty := false
	for i, v := range other {
		dirty = dirty || (bm[i]|v != bm[i])
		bm[i] |= v
	}
	return bm, dirty
}

// ToSlice returns the IDs as separate values in a slice.
func (bm tsidbitmap) ToSlice() (out []core.TractserverID) {
	for idx, v := range bm {
		for off := uint64(0); off < 64; off++ {
			if v&(uint64(1)<<off) != 0 {
				out = append(out, core.TractserverID(uint64(idx)*64+off))
			}
		}
	}
	return
}

// ToBytes returns the bitmap as a []byte for serialization. Note that this is
// just a memcpy.
func (bm tsidbitmap) ToBytes() []byte {
	// Use little endian so layout is the same no matter what word size we're using.
	out := make([]byte, 8*len(bm))
	for i, v := range bm {
		binary.LittleEndian.PutUint64(out[i*8:], v)
	}
	return out
}

// tsidbitmapFromBytes returns a bitmap initialized from a serialized value.
// Note that this is just a memcpy.
func tsidbitmapFromBytes(in []byte) tsidbitmap {
	for len(in)%8 != 0 {
		in = append(in, 0)
	}
	bm := make([]uint64, len(in)/8)
	for i := range bm {
		bm[i] = binary.LittleEndian.Uint64(in[i*8:])
	}
	return tsidbitmap(bm)
}
