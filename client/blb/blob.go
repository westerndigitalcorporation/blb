// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package blb

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"time"

	log "github.com/golang/glog"
	"github.com/westerndigitalcorporation/blb/internal/core"
)

// Blob is an implementation of the 'io.ReadWriteSeeker' interface and is built
// on top of the client driver. It provides an object handle for the stored
// blobs. This is not thread-safe.
// PL-1157:
// (1) Should we make it thread-safe or leave the user to handle it?
// (2) Optimize the implementation to reduce number of rpcs to the curators.
type Blob struct {
	// A client driver instance used to create this blob.
	cli *Client

	// BlobID that references this blob.
	id core.BlobID

	// Offset for the next Read or Write.
	offset int64

	// Whether reading or writing is allowed.
	allowRead, allowWrite bool

	// A Blob does network requests that we would like to be cancellable and
	// support other nice contexty stuff, but the Read and Write functions in
	// ReadWriteSeeker don't accept contexts. So we reuse the context from the
	// Create or Open call, on the assumption that usage of a Blob will
	// generally follow a single-threaded and well-scoped control flow.
	ctx context.Context
}

// Read reads up to 'len(p)' bytes from 'b' into 'p'. It returns the number of
// bytes read and any error encountered.
func (b *Blob) Read(p []byte) (int, error) {
	n, err := b.ReadAt(p, b.offset)
	if err == nil || err == io.EOF {
		b.offset += int64(n)
	}
	return n, err
}

// ReadAt reads up to 'len(p)' bytes from the 'offset' of 'b' into 'p'. It
// returns the number of bytes read and any error encountered. Different from
// Read, this method doesn't change the internal offset of 'b' for the next Read
// or Write.
func (b *Blob) ReadAt(p []byte, offset int64) (int, error) {
	if !b.allowRead {
		return 0, core.ErrInvalidState.Error()
	}

	var n int
	var err core.Error

	st := time.Now()

	read := func(seq int) bool {
		level := log.Level(1)
		if seq > 0 {
			level = 0
		}
		log.V(level).Infof("read blob %s from offset %d, attempt #%d", b.id, offset, seq)
		n, err = b.cli.readAt(b.ctx, b.id, p, offset)
		return !core.IsRetriableError(err)
	}

	b.cli.retrier.Do(b.ctx, read)

	b.cli.metricReadBytes.Add(float64(n))
	b.cli.metricReadSizes.Observe(float64(n))
	b.cli.metricReadDurations.Observe(float64(time.Since(st)) / 1e9)

	return n, err.Error()
}

// Write writes 'len(p)' bytes to 'b'. It returns the number of bytes written
// from 'p' and any error encountered that caused the write to stop early.
func (b *Blob) Write(p []byte) (int, error) {
	n, err := b.WriteAt(p, b.offset)
	if err == nil {
		b.offset += int64(n)
	}
	return n, err
}

// WriteAt writes 'len(p)' bytes to 'b' starting at the 'offset'. It returns the
// number of bytes written from 'p' and any error encountered that caused the
// write to stop early. Different from Write, this method doesn't change the
// internal offset of 'b' for the next Write or Read.
func (b *Blob) WriteAt(p []byte, offset int64) (int, error) {
	if !b.allowWrite {
		return 0, core.ErrInvalidState.Error()
	}

	var n int
	var err core.Error

	st := time.Now()

	write := func(seq int) bool {
		level := log.Level(1)
		if seq > 0 {
			level = 0
		}
		log.V(level).Infof("write blob %s from offset %d, attempt #%d", b.id, offset, seq)
		n, err = b.cli.writeAt(b.ctx, b.id, p, offset)
		return !core.IsRetriableError(err)
	}

	b.cli.retrier.Do(b.ctx, write)

	b.cli.metricWriteBytes.Add(float64(n))
	b.cli.metricWriteSizes.Observe(float64(n))
	b.cli.metricWriteDurations.Observe(float64(time.Since(st)) / 1e9)

	return n, err.Error()
}

// ByteLength returns the length of the blob in bytes.
func (b *Blob) ByteLength() (int64, error) {
	var n int64
	var berr core.Error

	bl := func(seq int) bool {
		level := log.Level(1)
		if seq > 0 {
			level = 0
		}
		log.V(level).Infof("get byte length of blob %s, attempt #%d", b.id, seq)
		n, berr = b.cli.byteLength(b.ctx, b.id)
		return !core.IsRetriableError(berr)
	}

	b.cli.retrier.Do(b.ctx, bl)
	return n, berr.Error()
}

// Stat returns returns stat(2)-ish info about the blob.
func (b *Blob) Stat() (core.BlobInfo, error) {
	var info core.BlobInfo
	var berr core.Error

	stat := func(seq int) bool {
		level := log.Level(1)
		if seq > 0 {
			level = 0
		}
		log.V(level).Infof("get stat of blob %s, attempt #%d", b.id, seq)
		_, info, berr = b.cli.statBlob(b.ctx, b.id)
		return !core.IsRetriableError(berr)
	}

	b.cli.retrier.Do(b.ctx, stat)
	return info, berr.Error()
}

// Seek sets the offset for the next Read or Write to 'offset', interpreted
// according to 'whence': 0 means relative to the origin of the blob, 1 means
// relative to the current offset, and 2 means relative to the end. Seeking with
// respect to the end is supported but in no means optimized. Seek returns
// the new offset and an error, if any.
//
// Seeking to a negative offset is an error. Seeking to any positive offset is
// legal, but subsequent I/O operations on the underlying object can return
// errors if the offset is out of bound.
func (b *Blob) Seek(offset int64, whence int) (int64, error) {
	var newOffset int64
	switch whence {
	case os.SEEK_SET:
		// Relative to the origin.
		newOffset = offset
	case os.SEEK_CUR:
		// Relative to the current offset.
		newOffset = b.offset + offset
	case os.SEEK_END:
		// Relative to the end.
		end, err := b.ByteLength()
		if err != nil {
			return -1, err
		}
		newOffset = end + offset
	default:
		return 0, fmt.Errorf("invalid argument %d, must be in [0, 2]", whence)
	}
	if newOffset < 0 {
		return 0, fmt.Errorf("cannot seek to a negative offset: %d", newOffset)
	}
	b.offset = newOffset
	return newOffset, nil
}

// ID returns the BlobID of 'b'.
func (b *Blob) ID() BlobID {
	return BlobID(b.id)
}

// ReadaheadBlob wraps an existing Blob for read and tries to prefetch
// the data for better performance. It implements io.ReadSeeker interface.
type ReadaheadBlob struct {
	b       *Blob
	bufBlob *bufio.Reader
}

// NewReadaheadBlob wraps an existing blob and does readahead on top of that.
func NewReadaheadBlob(b *Blob) *ReadaheadBlob {
	return &ReadaheadBlob{
		b:       b,
		bufBlob: bufio.NewReaderSize(b, core.TractLength),
	}
}

// Read does the same thing as Blob.Read does.
func (b *ReadaheadBlob) Read(p []byte) (int, error) {
	return b.bufBlob.Read(p)
}

// ByteLength returns the length of the blob in bytes.
func (b *ReadaheadBlob) ByteLength() (int64, error) {
	return b.b.ByteLength()
}

// Stat returns returns stat(2)-ish info about the blob.
func (b *ReadaheadBlob) Stat() (core.BlobInfo, error) {
	return b.b.Stat()
}

// Seek does the same thing as Blob.Seek does. When a seek is performed
// all the prefetched data will be discarded. Since internally ReadaheadBlob
// keeps a buffer with the size of one tract so presumably it will issue just
// one read for each tract. But if you seek to the middle of a tract then
// the buffer will not be aligned with a tract anymore, thus it might end up
// issuing more than one read for a tract.
//
// TODO: we might want to optimize this by duplicating the bufio logic here.
func (b *ReadaheadBlob) Seek(offset int64, whence int) (int64, error) {
	// Discard the buffer.
	b.bufBlob.Reset(b.b)
	return b.b.Seek(offset, whence)
}

// ID returns the BlobID of 'b'.
func (b *ReadaheadBlob) ID() BlobID {
	return b.b.ID()
}
