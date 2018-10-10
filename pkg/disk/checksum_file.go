// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT
//
// The ChecksumFile is a layer on top of a normal os.File that verifies the integrity of the underlying data.
// ChecksumFile(s) should be created through NewChecksumFile but can be deleted or moved like an ordinary
// os.File.

package disk

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"

	log "github.com/golang/glog"
)

const (
	// If this flag is present, ChecksumFile will attempt to use fadvise to drop
	// file data from the buffer cache.
	O_DROPCACHE int = 0x10000000

	// ExtraRoom is how much room a caller can leave in the capacity of a slice
	// to read, to help us avoid copying data.
	ExtraRoom = blockLength
)

// A ChecksumFile supports a limited file interface but detects corruption of data.
type ChecksumFile struct {
	// What is the filesystem name of the file we're operating on?  For logging.
	path string

	// The underlying file we use to read/write data.
	file mockFile

	// The position for io.Reader/io.Writer/io.Seeker interfaces.
	pos int64

	// File flags.
	flags int
}

// ErrCorruptData is returned when a block's checksum is bad, or something else happens that corrupts
// a ChecksumFile.
var ErrCorruptData = errors.New("file is corrupt")

// ErrInvalidFlag is returned if a ChecksumFile is opened with a bad flag.
var ErrInvalidFlag = errors.New("invalid flag")

// ErrInvalidOffset is for a bad offset to Seek.
var ErrInvalidOffset = errors.New("invalid offset")

// NewChecksumFile opens (or creates) a new ChecksumFile at path 'path' and flags 'flags'.
// flags can be an OR-ing of:
// os.O_RDONLY -- file is opened only for reading
// os.O_RDWR -- file is opened for reading and writing.
// os.O_CREATE -- file should be created (but if existing file exists, returns error, an implicit or w/os.O_EXCL)
// os.O_EXCL -- if creating a file, return an error if it exists already.
// os.O_TRUNC -- truncate size to 0
// O_DROPCACHE -- use fadvise to prevent file data from lingering in the buffer cache
//
// The file is created with a default mode of 0600.
//
// WARNING: ChecksumFile is not thread safe!
func NewChecksumFile(path string, flags int) (*ChecksumFile, error) {
	// Verify the flags are OK.
	okFlags := os.O_RDONLY | os.O_RDWR | os.O_CREATE | os.O_EXCL | os.O_TRUNC | O_DROPCACHE
	if flags&(^okFlags) != 0 {
		log.Errorf("%s: invalid flags: %x", path, flags)
		return nil, ErrInvalidFlag
	}

	// Defer to the underlying file implementation.
	f, e := osFileOpener(path, flags, os.FileMode(0600))
	if e != nil {
		return nil, e
	}

	// Disable read-ahead.
	if e = f.Fadvise(0, 0, POSIX_FADV_RANDOM); e != nil {
		log.Errorf("%s: couldn't disable readahead: %+v", path, e)
	}
	return &ChecksumFile{path: path, file: f, pos: 0, flags: flags}, nil
}

// Seek implements io.Seeker.
func (f *ChecksumFile) Seek(offset int64, whence int) (int64, error) {
	ppos := f.pos
	switch whence {
	case os.SEEK_SET:
		f.pos = offset
	case os.SEEK_CUR:
		f.pos += offset
	case os.SEEK_END:
		end, err := f.Size()
		if err != nil {
			return 0, err
		}
		f.pos = end + offset
	}
	if f.pos < 0 {
		f.pos = ppos
		return 0, ErrInvalidOffset
	}
	return f.pos, nil
}

// Scrub verifies that the file isn't corrupt.
func (f *ChecksumFile) Scrub() (int64, error) {
	// We just read blocks until we're out of blocks.
	blockNo := 0

	// How many bytes did we read?
	var bytes int64

	block := getChecksumBlock()
	defer returnChecksumBlock(block)

	for {
		if err := f.readBlock(block, blockNo); err == io.EOF {
			return bytes, nil
		} else if err != nil {
			return bytes, err
		}
		bytes += int64(block.len())
		blockNo++
	}
}

// Size returns the number of bytes of data that the user has written to the ChecksumFile.  Note that this
// will be smaller than the on-disk size as it does not account for checksum or header overhead.
func (f *ChecksumFile) Size() (bytes int64, err error) {
	// Figure out the on-disk size in bytes.
	var stat os.FileInfo
	if stat, err = f.file.Stat(); nil != err {
		log.Errorf("%s: error stat-ing: %+v", f.path, err)
		return 0, err
	}

	// No data means no checksum, so don't return -blockChecksumLength in that case (as the
	// math below would calculate).
	if 0 == stat.Size() {
		return 0, nil
	}

	// If 0 < size <= blockChecksumLength, the file is not a valid ChecksumFile
	if stat.Size() <= blockChecksumLength {
		log.Errorf("%s: file is smaller than checksum length", f.path)
		return 0, ErrCorruptData
	}

	// Working backwards from the on-disk size, figure out how many of those bytes are user data.
	numBlocks := (stat.Size() - headerLength) / blockLength
	leftover := (stat.Size() - headerLength) % blockLength
	// If there's a partial block...
	if 0 != leftover {
		// It needs to be longer than a checksum.
		if leftover <= blockChecksumLength {
			return 0, ErrCorruptData
		}
		// Account for the checksum.
		leftover -= blockChecksumLength
	}
	return blockDataLength*numBlocks + leftover, nil
}

// Close the open ChecksumFile.
func (f *ChecksumFile) Close() (err error) {
	// Ensure the data is durable.
	if e := f.file.Sync(); e != nil {
		log.Errorf("%s: error syncing: %+v", f.path, e)
		err = e
	}

	if f.flags&O_DROPCACHE != 0 {
		// Have the OS throw any data we have left out of the buffer cache.
		// ReadAt throws out its own data but WriteAt does not.
		// Passing the 'len' parameter as zero applies the fadvise to the whole file.
		//
		// The error conditions that result in errors from posix_fadvise
		// would also come from calls to close or surface, so we pass those
		// errors up.
		if e := f.file.Fadvise(0, 0, POSIX_FADV_DONTNEED); e != nil {
			log.Errorf("%s: fadvise returned an error on close: %+v", f.path, e)
		}
	}

	if e := f.file.Close(); e != nil {
		if err == nil {
			err = e
			log.Errorf("%s: error closing: %+v", f.path, e)
		} else {
			log.Errorf("%s: error on sync (%+v) additional error on close: %+v", f.path, err, e)
		}
	}

	return err
}

// Read reads len(b) bytes from the file's current position
// Returns number of bytes written and any error encountered.
// The position for the next Read or Write is updated on success.
func (f *ChecksumFile) Read(b []byte) (n int, err error) {
	n, e := f.ReadAt(b, f.pos)
	f.pos += int64(n)
	return n, e
}

// Write reads len(b) bytes from 'f.pos' via WriteAt.
// Returns number of bytes written and any error encountered.
// The position for the next Read or Write is updated on success.
func (f *ChecksumFile) Write(b []byte) (n int, err error) {
	n, e := f.WriteAt(b, f.pos)
	f.pos += int64(n)
	return n, e
}

// ReadAt reads len(b) bytes from 'off', verifying that the data is not corrupted.
// Returns an error if the underlying file returned an error, or data is corrupted.
// ReadAt will try to read data directly into b to minimize copying. You can
// help it by leaving a little extra space (ExtraRoom) in the capacity of b.
func (f *ChecksumFile) ReadAt(b []byte, off int64) (n int, err error) {
	n = 0
	var block *checksumBlock

	// Read into 'b' block by block.
	for len(b) > 0 {
		blockNo := int(off / blockDataLength)
		start := int(off % blockDataLength)
		var nb int
		// Try fast path:
		if start == 0 && cap(b) >= blockLength {
			if nb, err = f.readBlockInPlace(b, blockNo); err != nil {
				return n, err
			}
			if nb > len(b) {
				nb = len(b)
			}
		} else {
			if block == nil {
				block = getChecksumBlock()
				defer returnChecksumBlock(block)
			}

			// This is the block we're currently reading.
			if err = f.readBlock(block, blockNo); nil != err {
				return n, err
			}

			// What data in this block are we interested in?
			if block.length <= start {
				// The offset we want to read is in this block but past the end of the
				// available data in the block.  Stop here.
				break
			}

			// Copy as much of the block's data as we can into the caller's buffer.
			nb = copy(b, block.data()[start:])
		}
		off += int64(nb)
		b = b[nb:]
		n += nb
	}

	// If we're here, there wasn't an error reading blocks, but we might not have read every block.
	if len(b) != 0 {
		// We couldn't read as much as the user wanted.  We follow the convention of
		// os.ReadAt and return io.EOF.
		return n, io.EOF
	}

	// We read everything the user wanted, no error.
	return n, nil
}

// pad fills the file with 'num' 0s.
func (f *ChecksumFile) pad(num int) error {
	if num < 0 {
		panic("bug: asked to pad <0 bytes")
	} else if 0 == num {
		return nil
	}

	// This is intentionally not optimized.
	// zeroes is already set to 0 as that's the default byte value.
	zeros := make([]byte, blockDataLength)

	// Write out 'num' bytes len(zeros) at a time (at most).
	// This is kind of inefficient but we're not optimizing
	// for hole-filling in files.
	for num > 0 {
		if num < len(zeros) {
			zeros = zeros[0:num]
		}

		n, e := f.append(zeros)
		if nil != e {
			return e
		}
		if len(zeros) != n {
			panic("bad typing: append returned no error but did a short write")
		}

		num -= n
	}

	return nil
}

// change mutates len(b) bytes at 'off' in the file.  The file must already contain data
// at the offsets this function mutates, and it is the responsibility of the caller
// to ensure this.
func (f *ChecksumFile) change(off int64, b []byte) error {
	block := getChecksumBlock()
	defer returnChecksumBlock(block)

	for len(b) > 0 {
		blockNo := int(off / blockDataLength)
		blockOff := int(off % blockDataLength)

		// 'n' is how many bytes of 'b' could fit into the block
		var n int

		// If we're not changing an entire block, we have to read in the old block.
		// This can happen on the first block written (0 != off % blockDataLength)
		// or the last block written (len(b) < blockDataLength).
		if 0 != blockOff || len(b) < blockDataLength {
			if e := f.readBlock(block, blockNo); nil != e {
				return e
			}
			// Sanity check the return of block.change.
			if n = block.change(b, blockOff); n < 0 {
				panic("bad typing: trying to change a block in a way that would cause a hole")
			}
		} else {
			// We are changing an entire block.
			n = block.append(b)
		}

		// 'wn' is how many bytes *of the block* were written.  Partial writes are OK for
		// appends, since those are space-creating and the disk could run out of space,
		// but for data mutations, we can't tolerate partial writes.
		if wn, e := f.writeBlock(block, blockNo); e != nil {
			return e
		} else if wn != block.len() {
			panic("writeBlock was short but didn't return an error")
		}

		// We put 'n' bytes of 'b' into the block that was succesfully written, so
		// advance our write.
		b = b[n:]
		off += int64(n)
		block.reset()
	}

	return nil
}

// min unsurprisingly returns the minimum of two values.
func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// WriteAt writes 'b' at offset 'off'.  Holes in files are padded with 0s
// explicitly. Failed mutations to existing data will cause file corruption.
// Writing to previously unallocated space will cause a short write.
func (f *ChecksumFile) WriteAt(b []byte, off int64) (int, error) {
	// Figure out how long the file is to see if we are creating any holes.
	var size int64
	var sizeErr error
	if size, sizeErr = f.Size(); sizeErr != nil {
		log.Errorf("%s: error determining size in WriteAt: %+v", f.path, sizeErr)
		return 0, sizeErr
	}

	// If we're starting the write after 'size', there will be a hole in the file.
	// Users shouldn't create holes in files, and actually supporting holes with ChecksumFile
	// would be complicated, so we fill the hole with 0s.  Then we can defer to Append to finish the write.
	if off > size {
		if e := f.pad(int(off - size)); e != nil {
			return 0, e
		}
		// Keeping with write(2), we don't count the padding as part of the write.
		return f.append(b)
	}

	// Writing exactly at the end of the file is an append.
	if off == size {
		return f.append(b)
	}

	// If we're here, off < size.

	// We break the write into two parts to make the error cases clearer.
	// First, we have a range that is changed in the original file.
	changeStart := off
	changeEnd := min(size, changeStart+int64(len(b)))
	changeLen := changeEnd - changeStart
	changeB := b[:changeLen]

	// If this goes wrong, the file is probably doomed.
	if e := f.change(changeStart, changeB); e != nil {
		return 0, e
	}

	// The write succeeded.
	// Is the write entirely within existing data?  If so, we're done.
	if int(changeLen) == len(b) {
		return len(b), nil
	}

	// There's some data to write after the end of the file.  We defer to Append here,
	// making sure to account for the number of bytes already written.
	n, e := f.append(b[changeLen:])
	return n + int(changeLen), e
}

// Append len(b) bytes to the end of the file 'f'.  Returns the number of bytes
// successfully written.
//
// If the append cannot write all bytes requested ErrNoSpace is returned.
//
// If ErrCorruptData is returned, at least one block in the file is corrupted.
//
// Note that Append is *NOT* synchronized!  Synchronization should be done by the caller.
func (f *ChecksumFile) append(b []byte) (n int, err error) {
	// In order to append, we need to figure out where we're writing to.
	var off int64
	if off, err = f.Size(); err != nil {
		log.Errorf("%s: error determining size in append: %+v", f.path, err)
		return 0, err
	}

	n = 0

	block := getChecksumBlock()
	defer returnChecksumBlock(block)

	// What block we are currently writing to.
	blockNo := int(off / blockDataLength)

	// If we are appending to a partially full block, we have to read existing data.
	if 0 != off%blockDataLength {
		if err = f.readBlock(block, blockNo); nil != err {
			return 0, err
		}
	}

	// Write 'b' out one block at a time.
	for n < len(b) {
		// block has data already if we're appending to a partially full block.
		priorLen := block.len()
		block.append(b[n:])

		// This write can partially succeed; when the disk is full, we may not be able
		// to write an entire block.
		var wn int
		wn, err = f.writeBlock(block, blockNo)

		// We've successfully written some bytes in this block, but some of that block
		// may have been there before.
		n += wn - priorLen

		// Stop writing on any error.
		if err != nil {
			return n, err
		}

		// We'll be writing the next block in the file.
		blockNo++

		// reset the block to an 'empty' state so we can fill it with data from 'b'.
		block.reset()
	}

	return n, nil
}

// blockPool caches allocated but unused checksum blocks for later use, relieving the pressure on the garbage
// collector.
var blockPool = &sync.Pool{New: newBlock}

// getChecksumBlock returns a checksumBlock for use.
func getChecksumBlock() *checksumBlock {
	return blockPool.Get().(*checksumBlock)
}

// returnChecksumBlock returns the block to blockPool so it can either be GC-ed or reused in future.
func returnChecksumBlock(block *checksumBlock) {
	// reset the block so it can be reused by next request.
	block.reset()
	blockPool.Put(block)
}

// A simple wrapper on "newChecksumBlock" since the signature of "Pool.New" is "
// func() interface{}".
func newBlock() interface{} {
	return newChecksumBlock()
}

// Getxattr gets an extended attribute value.
func (f *ChecksumFile) Getxattr(name string) ([]byte, error) {
	return f.file.Getxattr(name)
}

// Setxattr sets an extended attribute value.
func (f *ChecksumFile) Setxattr(name string, value []byte) error {
	return f.file.Setxattr(name, value)
}

// We mock out what we use from os.File so we can test error handling behavior.
type mockFile interface {
	ReadAt(b []byte, off int64) (n int, err error)
	WriteAt(b []byte, off int64) (n int, err error)
	Stat() (fi os.FileInfo, err error)
	Close() error
	Sync() error
	Truncate(size int64) error

	// Fadvise related.
	Fadvise(offset int64, length int64, advice int) error

	// Xattr related.
	Getxattr(name string) ([]byte, error)
	Setxattr(name string, value []byte) error
}

// An implementation of the mock file interface that just forwards to os.File.
type regularFile struct {
	*os.File
}

// Getxattr implements mockFile.
func (f *regularFile) Getxattr(name string) ([]byte, error) {
	return Fgetxattr(f.Fd(), name)
}

// Setxattr implements mockFile.
func (f *regularFile) Setxattr(name string, value []byte) error {
	return Fsetxattr(f.Fd(), name, value)
}

// Tests mutate this to inject failures into calls to os.File methods.
var osFileOpener = openOsFile

// Implementation of osFile that just defers to the os.File API. The directory
// where the file is located is synced after the file is successfully created.
func openOsFile(path string, flags int, perm os.FileMode) (mockFile, error) {
	// We don't care about atime anywhere.
	flags |= O_NOATIME

	f, e := os.OpenFile(path, flags, perm)
	if e != nil {
		return nil, e
	}

	// Sync the directory where the file is located, if the file is new.
	if flags&os.O_CREATE != 0 {
		if e := syncDir(filepath.Dir(path)); e != nil {
			return nil, e
		}
	}

	return &regularFile{f}, nil
}

//
// Utility functions
//

// syncDir syncs the given 'dir'.
func syncDir(dir string) (err error) {
	// Open.
	fd, err := os.Open(dir)
	if err != nil {
		log.Errorf("failed to open directory %s: %s", dir, err)
		return err
	}

	// Sync.
	if err = fd.Sync(); err != nil {
		log.Errorf("failed to fsync directory: %s", err)
		if cerr := fd.Close(); nil != cerr {
			log.Errorf("failed to close directory: %s", cerr)
		}
		return err
	}

	// Close.
	if err = fd.Close(); err != nil {
		log.Errorf("failed to close directory: %s", err)
		return err
	}

	return nil
}

// Rename relies on os.Remove to rename (move) a file and syncs the home
// directory of the newpath upon success.
func Rename(oldpath, newpath string) error {
	if err := os.Rename(oldpath, newpath); err != nil {
		return err
	}
	return syncDir(filepath.Dir(newpath))
}
