// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT
//
// checksumBlock is storage for a ~64k chunk of data and a checksum to validate that data.

package disk

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"syscall"

	log "github.com/golang/glog"
)

const (
	// headerLength is the length of the head every ChecksumFile has.
	headerLength = 0

	// blockDataLength is the length of data following the header.
	blockDataLength = blockLength - blockChecksumLength

	// blockChecksumLength is the length of a checksum (crc32, specifically)
	blockChecksumLength = 4

	// blockLength is the total length of 64k.
	blockLength = 64 * 1024
)

var (
	// This is opaque, pre-calculated data used by the hash/crc32 package to speed up CRC calculations.
	crc32Table = crc32.MakeTable(crc32.Castagnoli)
)

// A checksumBlock is the largest amount of user data that we compute a checksum over.
type checksumBlock struct {
	// 64k of checksum + data.
	storage [blockLength]byte

	// How much of 'storage' is data?
	length int

	// The checksum for storage[0:length].
	cksum uint32
}

// newChecksumBlock returns a new, empty checksumBlock.
func newChecksumBlock() *checksumBlock {
	return &checksumBlock{length: 0, cksum: 0}
}

// data returns a slice representing the valid data in the checksumBlock.
func (b *checksumBlock) data() []byte {
	return b.storage[0:b.length]
}

// len returns the length of the valid data in the checksumBlock.
func (b *checksumBlock) len() int {
	return b.length
}

// ok returns true if the block is not corrupt (that is, the checksum computed over the data matches
// the stored checksum).
func (b *checksumBlock) ok() bool {
	return b.cksum == crc32.Checksum(b.data(), crc32Table)
}

// reset zeros the length of the checksumBlock.
func (b *checksumBlock) reset() {
	b.cksum = 0
	b.length = 0
}

// append as much of 'b' to the block as possible, returning the number of bytes appended.
func (b *checksumBlock) append(data []byte) (n int) {
	n = copy(b.storage[b.length:blockDataLength], data)
	b.length += n
	b.cksum = crc32.Update(b.cksum, crc32Table, data[:n])
	return n
}

// change 'data' at offset 'off' in the block.  Returns how many bytes of 'data'
// could be changed.  Returns a negative number on error (changing data that doesn't exist).
func (b *checksumBlock) change(data []byte, off int) int {
	// Can only change data that is already in the block, not add data to the block.
	if off >= b.length {
		return -1
	}

	// Recall that change can only change data already in the block, hence the b.length
	// as the upper range in the slice.
	n := copy(b.storage[off:b.length], data)
	b.cksum = crc32.Checksum(b.data(), crc32Table)
	return n
}

// readBlock reads the 'blockNo'-th block from the ChecksumFile 'f' into the block 'b'.  block.length
// is set to reflect how many bytes are actually in that block.  EOF is not returned when the last
// block in the file is read.
//
// Returns an error if the underlying ReadAt encounters an error.
// Returns ErrCorruptData if the block is corrupt.
// Returns io.EOF if there is no such block.
func (f *ChecksumFile) readBlock(b *checksumBlock, blockNo int) error {
	// Clear the length and checksum.
	b.reset()

	// Try to read the entire 64k block.
	rawOff := int64(headerLength + blockLength*blockNo)
	n, e := f.file.ReadAt(b.storage[:], rawOff)

	if f.flags&O_DROPCACHE != 0 {
		// Dump data from the buffer cache after the read.
		if fe := f.file.Fadvise(rawOff, int64(len(b.storage[:])), POSIX_FADV_DONTNEED); fe != nil {
			// We still read the data if err is nil, we may just waste memory.
			// The caller doesn't really care about that, but log an error so it'll be noticed.
			log.Errorf("%s: fadvise returned an error during readblock: %+v", f.path, fe)
		}
	}

	// Non-EOF errors are problems.
	if e != nil && e != io.EOF {
		log.Errorf("%s readBlock: error from ReadAt at offset %d: %+v", f.path, rawOff, e)
		return e
	}

	// If there is a short read, ReadAt will return EOF.  This could mean there is
	// a partially full block, which isn't an error.  Or it could mean there just isn't
	// any data at all in the block, in which case we return EOF.
	if e == io.EOF && n == 0 {
		return io.EOF
	}

	// There shouldn't be a checksum without data.
	if n <= blockChecksumLength {
		return ErrCorruptData
	}

	// Store the length of the data portion of the read.
	b.length = n - blockChecksumLength

	// The checksum is stored after the data in the file.
	b.cksum = binary.LittleEndian.Uint32(b.storage[b.length:n])

	// And verify the data.
	if !b.ok() {
		return ErrCorruptData
	}

	return nil
}

// readBlockInPlace reads the 'blockNo'-th block from the ChecksumFile 'f' into
// the buffer 'b', which must have cap at least blockLength. It returns the
// number of data bytes placed in b (which will always be <= blockDataLength).
//
// Returns an error if the underlying ReadAt encounters an error.
// Returns ErrCorruptData if the block is corrupt.
// Returns io.EOF if there is no such block.
func (f *ChecksumFile) readBlockInPlace(b []byte, blockNo int) (int, error) {
	// Try to read the entire 64k block.
	rawOff := int64(headerLength + blockLength*blockNo)
	b = b[:blockLength]
	n, e := f.file.ReadAt(b, rawOff)

	if f.flags&O_DROPCACHE != 0 {
		if fe := f.file.Fadvise(rawOff, int64(len(b)), POSIX_FADV_DONTNEED); fe != nil {
			log.Errorf("%s: fadvise returned an error during readblock: %+v", f.path, fe)
		}
	}

	// Non-EOF errors are problems.
	if e != nil && e != io.EOF {
		log.Errorf("%s readBlock: error from ReadAt at offset %d: %+v", f.path, rawOff, e)
		return 0, e
	}

	// If there is a short read, ReadAt will return EOF.  This could mean there is
	// a partially full block, which isn't an error.  Or it could mean there just isn't
	// any data at all in the block, in which case we return EOF.
	if e == io.EOF && n == 0 {
		return 0, io.EOF
	}

	// There shouldn't be a checksum without data.
	if n <= blockChecksumLength {
		return 0, ErrCorruptData
	}

	// Get the length of the data portion of the read.
	length := n - blockChecksumLength

	// The checksum is stored after the data in the file.
	cksum := binary.LittleEndian.Uint32(b[length:n])

	// And verify the data.
	if cksum != crc32.Checksum(b[:length], crc32Table) {
		return 0, ErrCorruptData
	}

	return length, nil
}

// tryWrite writes the block 'b' as block number 'blockNo' of file 'f'.
// Used by writeBlock as a helper.
func (f *ChecksumFile) tryWrite(b *checksumBlock, blockNo int) (int, error) {
	// This is what we write to disk.
	writeLen := b.length + blockChecksumLength

	// Add the checksum to the data we write.
	binary.LittleEndian.PutUint32(b.storage[b.length:writeLen], b.cksum)

	// And do the write.
	return f.file.WriteAt(b.storage[0:writeLen], int64(headerLength+blockLength*blockNo))
}

// writeBlock writes the block 'b' as 'blockNo' of the file 'f'.
// Returns how many bytes of data in 'b' (not counting checksum) could be written.
func (f *ChecksumFile) writeBlock(b *checksumBlock, blockNo int) (int, error) {
	// tryWrite packs the checksum into the block and writes it via f.file.WriteAt.
	n, err := f.tryWrite(b, blockNo)
	if err == nil {
		return b.length, nil
	}

	// Error handling begins.  Pull out a sub-error type.
	switch pe := err.(type) {
	case *os.PathError:
		err = pe.Err
	case *os.SyscallError:
		err = pe.Err
	case *os.LinkError:
		err = pe.Err
	default:
		log.Errorf("%s: writeBlock encountered unrecoverable error writing block %d, err=%s", f.file, blockNo, err)
		return b.length, err
	}

	// Other errors we abort and pass up.
	if err != syscall.ENOSPC {
		log.Errorf("%s: writeBlock encountered unrecoverable error writing block %d, err=%s", f.file, blockNo, err)
		return b.length, err
	}

	// We can write some data, but not more data than the checksum takes up.  Truncate to remove the checksum
	// fragment, and tell the user we're out of space.
	if n <= blockChecksumLength {
		// If we can't remove the checksum fragment, the block is screwed up.
		if truncErr := f.file.Truncate(int64(headerLength + blockLength*blockNo)); nil != truncErr {
			log.Errorf("%s: truncate error after short write less than checksum length writing block %d: %+v", f.file, blockNo, truncErr)
			return 0, ErrCorruptData
		}
		return 0, err
	}

	// 'n' is how many bytes we successfully wrote.  Subtract the overhead for the checksum and write again,
	// including the checksum this time.
	var partialBlock checksumBlock
	partialBlock.append(b.data()[:n-blockChecksumLength])

	// This write might just succeed since we've just successfully written this much data at
	// this offset.
	_, shortE := f.tryWrite(&partialBlock, blockNo)

	// If it doesn't, not sure what other handling to do.  Report as corrupt.
	if shortE != nil {
		return 0, ErrCorruptData
	}

	// We successfully had a short write.  We still want to pass up the ENOSPC error so further blocks
	// aren't written.
	return partialBlock.length, err
}
