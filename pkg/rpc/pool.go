// Copyright (c) 2017 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT
//
// Specialized pools for a few sizes: 8MB (tract length), 4MB (RS encoding block
// size), and 1MB (smaller buffers), all including one checksumblock overhead
// for ChecksumFile.ReadAt.

package rpc

import (
	"sync"

	"github.com/westerndigitalcorporation/blb/pkg/disk"
)

const (
	buf8MBSize = 8<<20 + disk.ExtraRoom
	buf4MBSize = 4<<20 + disk.ExtraRoom
	buf1MBSize = 1<<20 + disk.ExtraRoom
)

var (
	buf8MBPool = sync.Pool{New: func() interface{} { b := make([]byte, buf8MBSize); return &b }}
	buf4MBPool = sync.Pool{New: func() interface{} { b := make([]byte, buf4MBSize); return &b }}
	buf1MBPool = sync.Pool{New: func() interface{} { b := make([]byte, buf1MBSize); return &b }}
)

// GetBuffer returns a []byte with length n and capacity >= n.
// The buffer may not be zeroed!
func GetBuffer(n int) []byte {
	if n <= 128*1024+disk.ExtraRoom {
		// Don't bother with pools for small buffers.
		return make([]byte, n)
	} else if n <= buf1MBSize {
		return (*buf1MBPool.Get().(*[]byte))[:n]
	} else if n <= buf4MBSize {
		return (*buf4MBPool.Get().(*[]byte))[:n]
	} else if n <= buf8MBSize {
		return (*buf8MBPool.Get().(*[]byte))[:n]
	}
	// Or large ones.
	return make([]byte, n)
}

// PutBuffer returns a buffer to the pool. It's okay to call this on any buffer
// that isn't going to be used again, whether it came from GetBuffer or not.
// 'exclusive' indicates whether the caller is the exclusive owner of the
// buffer. (If exclusive is false, obviously, the buffer cannot be put in a
// pool. PutBuffer has this signature to be able to use it conveniently with
// BulkData.Get)
func PutBuffer(b []byte, exclusive bool) {
	if !exclusive {
		return
	}
	if cap(b) == buf8MBSize {
		buf8MBPool.Put(&b)
	} else if cap(b) == buf4MBSize {
		buf4MBPool.Put(&b)
	} else if cap(b) == buf1MBSize {
		buf1MBPool.Put(&b)
	}
}
