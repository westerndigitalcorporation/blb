// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT
//
// Tests for checksum_block.go and checksum_file.go.

package disk

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	test "github.com/westerndigitalcorporation/blb/pkg/testutil"
)

// TestChecksumBlockChange
func TestChecksumBlockChange(t *testing.T) {
	// Some data to write to the block.
	dataA := make([]byte, 100)
	dataB := make([]byte, 100)

	for i := range dataA {
		dataA[i] = 'A'
		dataB[i] = 'B'
	}

	var b checksumBlock

	// Change data that doesn't exist, shouldn't work.
	b.reset()
	b.append(dataA)
	if b.change(dataB, len(dataA)) >= 0 {
		t.Fatal("trying to change data that doesn't exist.")
	}

	// Changing data that exists works just fine.
	b.reset()
	b.append(dataA)
	if len(dataB) != b.change(dataB, 0) {
		t.Fatal("couldn't change block")
	}
	for i := range b.data() {
		if dataB[i] != b.data()[i] {
			t.Fatal("block did not change")
		}
	}

	// We only change data, not append data.
	b.reset()
	b.append(dataA)
	offset := 10
	if len(dataA)-offset != b.change(dataB, offset) {
		t.Fatal("change added data")
	}
	for i := 0; i < offset; i++ {
		if 'A' != b.data()[i] {
			t.Fatal("block wrong")
		}
	}
	for i := offset; i < len(b.data()); i++ {
		if 'B' != b.data()[i] {
			t.Fatal("block wrong")
		}
	}
}

// Make a test buffer with a repeating pattern.
func getBuffer(size int) []byte {
	buffer := make([]byte, size)
	for i := 0; i < size; i++ {
		buffer[i] = byte(i % 256)
	}
	return buffer
}

// Creates a checksum file 'size' bytes of a repeating pattern.
// Fails the test via 't' if any error encountered.
func setupChecksumFile(size int, t *testing.T) *ChecksumFile {
	var tmpChecksumFile = filepath.Join(test.TempDir(), "checksum_file_test")

	// Remove the tmpFile if it exists -- necessary for xattr tests.
	os.Remove(tmpChecksumFile)

	f, err := NewChecksumFile(tmpChecksumFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC)
	if err != nil {
		t.Fatal(err)
		return nil
	}
	n, err := f.append(getBuffer(size))
	if n != size || err != nil {
		t.Fatal("failed to append, n=", n, " size=", size, " err=", err)
	}
	return f
}

// Verify data read into 'buffer' at offset 'off' from a checksum file created
// with setupChecksumFile.
func bufIsOK(buffer []byte, off int64) (err error) {
	expected := byte(off % 256)
	for i, b := range buffer {
		if expected != b {
			err = fmt.Errorf("unexpected data at index %d", i)
			return
		}
		expected++
	}
	return nil
}

// TestChecksumFileZeroRead tests a bunch of cases where no data gets read.
func TestChecksumFileZeroRead(t *testing.T) {
	f := setupChecksumFile(1024, t)

	// Read 0 bytes. Return 0 bytes read.
	n, err := f.ReadAt(getBuffer(0), 0)
	if err != nil || n != 0 {
		t.Error("expecting zero bytes get read")
	}

	// Read non-zero bytes at invalid offset.
	n, err = f.ReadAt(getBuffer(1024), 1024+1)
	if err != io.EOF || n != 0 {
		t.Error("didn't get expected error, n=", n, " err=", err)
	}

	// Read 0 bytes at invalid offset.
	n, err = f.ReadAt(getBuffer(0), 1024+1)
	if n != 0 {
		t.Error("didn't get expected error")
	}
}

// Write various interesting lengths of data to a checksum file and ensure that the size
// is equal to what we wrote to it.
func TestChecksumFileSize(t *testing.T) {
	// For each of these sizes, we write this many bytes to the file and verify
	// that the size is reported correctly.
	sizes := []int64{0, 1, blockLength - 1, blockLength, blockLength + 1, 2 * blockLength}

	for i := range sizes {
		f := setupChecksumFile(int(sizes[i]), t)
		fs, err := f.Size()

		if nil != err {
			t.Fatal("error calling Size(): " + err.Error())
		}

		if sizes[i] != fs {
			t.Fatalf("wrote %d bytes, size reported as %d", sizes[i], fs)
		}
	}
}

var appendTests = []struct {
	// Create a file of this many bytes.  Fill it with a known repeating pattern.
	size int

	// Append data of this length.
	n int
}{
	// Append nothing.
	{blockDataLength, 0},

	// Append a bit to a block, but don't fill it.
	{1, 1},

	// Fill a block exactly with the append.
	{blockDataLength - 10, 10},

	// Add a whole block to a partial block.
	{10, blockDataLength},

	// Add a whole block to another whole block.
	{blockDataLength, blockDataLength},

	// Add multiple blocks to a partial block.
	{10, 3 * blockDataLength},

	// Add multiple blocks to a whole block.
	{0, 3 * blockDataLength},
	{blockDataLength, 3 * blockDataLength},
}

// Run the tests in 'appendTests'
func TestChecksumFileAppend(t *testing.T) {
	for _, test := range appendTests {
		f := setupChecksumFile(test.size, t)

		// Append data.
		data := make([]byte, test.n)
		for i := range data {
			data[i] = byte(i % 256)
		}
		if n, e := f.append(data); nil != e || n != len(data) {
			t.Fatal("short write or error while appending")
		}

		// Read and verify the data appended via setupChecksumFile.
		setupData := make([]byte, test.size)
		if n, e := f.ReadAt(setupData, 0); nil != e || n != len(setupData) {
			t.Fatal("short read or error while reading old data")
		}
		if e := bufIsOK(setupData, 0); nil != e {
			t.Fatal("old data corrupted, err=" + e.Error())
		}

		// Read and verify the new data.
		if n, e := f.ReadAt(data, int64(test.size)); nil != e || n != len(data) {
			t.Fatal("short read or error while reading new data")
		}
		for i := range data {
			if byte(i%256) != data[i] {
				t.Fatal("appended data corrupted")
			}
		}

		// Verify size.
		size, err := f.Size()
		if nil != err {
			t.Fatal("error getting file size")
		}
		if size != int64(test.size+test.n) {
			t.Fatal("f.Size() returned bad size")
		}
	}
}

var writeTests = []struct {
	// Create a file of this many bytes and fill it with a known repeating pattern.
	size int

	// Write a different pattern starting at this offset...
	off int

	// ...for this many bytes.
	n int
}{
	// Write a byte to an empty file.  Should be an append.
	{0, 0, 1},

	// Append to a >1 block file.
	{2 * blockDataLength, 2 * blockDataLength, 1},

	// Write with a small gap.
	{0, 10, 1},

	// Write with a small gap cross-block.
	{blockDataLength - 10, blockDataLength - 8, blockDataLength},

	// Write entirely within an existing file.
	{100, 50, 10},

	// Write many blocks entirely within an existing file.
	{5 * blockDataLength, blockDataLength - 10, 3*blockDataLength + 20},

	// Write part-in-part-out of existing file.
	{100, 90, 20},

	// Write part-in-part-out of many blocks.
	{3 * blockDataLength, 10, 3*blockDataLength - 20},
}

func TestChecksumFileWrite(t *testing.T) {
	for _, test := range writeTests {
		f := setupChecksumFile(test.size, t)
		t.Log("running test ", test)

		// Data we Write.
		buf := make([]byte, test.n)
		for i := range buf {
			buf[i] = 'W'
		}

		// This write should just work.
		n, e := f.WriteAt(buf, int64(test.off))
		if nil != e {
			t.Fatal("got non-nil error back from Write")
		}

		if len(buf) != n {
			t.Fatal("partial write")
		}

		var size int64
		if size, e = f.Size(); nil != e {
			t.Fatal("couldn't read the file size")
		}

		// Ensure that the file is what it should be.
		wholeFile := make([]byte, size)
		if n, e := f.ReadAt(wholeFile, 0); nil != e || int64(n) != size {
			t.Fatal("error reading the whole file back")
		}

		// If there is pre-existing data located earlier in the file than the write.
		if test.size > 0 && test.off > 0 && test.off > test.size {
			if nil != bufIsOK(wholeFile[0:test.size], 0) {
				t.Fatal("pre-Written data bad")
			}
		}

		// Any gap between the previous EOF and the start of the Write.
		if test.off > test.size {
			for i := test.size; i < test.off; i++ {
				if wholeFile[i] != 0 {
					t.Fatal("gap isn't 0")
				}
			}
		}

		// The write.
		for i := test.off; i < n; i++ {
			if wholeFile[i] != 'W' {
				t.Fatal("Written data bad")
			}
		}

		// After the write, if there is any data there.
		writeEnd := test.off + test.n
		if size > int64(writeEnd) {
			if nil != bufIsOK(wholeFile[writeEnd:], int64(writeEnd)) {
				t.Fatal("post-Write data bad")
			}
		}
	}
}

// This makes things easier to read below.
type readTest struct {
	// Create a file of this many bytes.  Fill it with a known repeating pattern.
	size int64
	// Do a read at this offset...
	off int64
	// ...of this length.  Since we know the repeating pattern we can validate the contents
	// of the read.
	n int
}

var readTests = []readTest{
	// Read exactly one block.
	{blockDataLength, 0, blockDataLength},

	// Read two blocks from a file with exactly one block
	{2 * blockDataLength, 0, 2 * blockDataLength},

	// Read inside one block.
	{blockDataLength, 10, 10},

	// Read past the end of the file.
	{blockDataLength*2 - 100, 10, blockDataLength * 2},

	// Read part of a file, but including an entire block.
	{3 * blockDataLength, 10, 2 * blockDataLength},
}

func init() {
	// Add lots more tests:
	var sizes []int64
	for _, blocks := range []int64{1, 3} {
		for _, delta := range []int64{-33, -4, -1, 0, 2, 7, 34} {
			sizes = append(sizes, blocks*blockDataLength+delta)
			sizes = append(sizes, blocks*blockLength+delta)
		}
	}
	for _, size := range sizes {
		for _, off := range []int64{0, 67} {
			for _, n := range sizes {
				if n < size {
					readTests = append(readTests, readTest{size, off, int(n)})
				}
			}
		}
	}
}

// TestChecksumFileRead tests different cases of read.
func TestChecksumFileRead(t *testing.T) {
	for _, test := range readTests {
		f := setupChecksumFile(int(test.size), t)

		// The number of bytes read should be min(bytes in file past read offset, bytes in buffer)
		min := func(a, b int) int {
			if a < b {
				return a
			}
			return b
		}
		expectedN := min(int(test.size-test.off), test.n)

		// Read data from the file.
		buf := make([]byte, test.n)
		n, err := f.ReadAt(buf, test.off)

		// If expectedReadLen < test.readLen, io.EOF should be returned.
		if expectedN < test.n {
			if err != io.EOF {
				t.Errorf("expecting io.EOF on short read")
			}
		} else if err != nil || n != expectedN {
			// Otherwise, there shouldn't have been an error.
			t.Errorf("failed to read %d bytes from offset %d", expectedN, test.off)
		}

		// Verify the data that were just read.
		if err := bufIsOK(buf[:expectedN], test.off); nil != err {
			t.Error(err)
			return
		}

		// Try again, but allocate extra room to test fast path:
		buf = make([]byte, test.n, test.n+ExtraRoom)
		n, err = f.ReadAt(buf, test.off)
		if expectedN < test.n {
			if err != io.EOF {
				t.Errorf("expecting io.EOF on short read")
			}
		} else if err != nil || n != expectedN {
			t.Errorf("failed to read %d bytes from offset %d", expectedN, test.off)
		}
		if err := bufIsOK(buf[:expectedN], test.off); nil != err {
			t.Error(err)
			return
		}
	}
}

// Test that the flags are screened for validity.
func TestReadFlags(t *testing.T) {
	// Open a file O_WRONLY then try to read from it.
	path := filepath.Join(test.TempDir(), "testReadFlags")
	if _, err := NewChecksumFile(path, os.O_CREATE|os.O_WRONLY); nil == err {
		t.Fatal("expected error as file opend WRONLY")
	}
}

// TestCorruptFile tests whether corrupted blocks can be detected or not.
func TestCorruptFile(t *testing.T) {
	// A file with 3 blocks is created.  The first block and the last block
	// are corrupted, and the middle block is not.
	f := setupChecksumFile(3*blockDataLength, t)

	//             the plan is this:
	//  ________________________________________
	//  |             |            |            |
	//  |     block   |    block   |   block    |
	//  |_____________|____________|____________|
	//      ^                           ^
	// corrupt 1 byte               corrupt 1 byte

	// Convert a client offset to the physical checksum file offset.
	toFileOff := func(blockNo, blockOff int64) int64 {
		return headerLength + blockNo*blockLength + blockOff
	}

	// Some random data to write to the file to corrupt it.
	data := []byte{7}

	// Corrupt data at offset 10 of the 0-th block.  We use the
	// underlying file to bypass the checksumming.
	if _, err := f.file.WriteAt(data, toFileOff(0, 10)); nil != err {
		t.Error("Failed to corrupt the first block")
		return
	}

	// Corrupt data at offset 10 of the 2nd block.
	if _, err := f.file.WriteAt(data, toFileOff(2, 10)); nil != err {
		t.Error("Failed to corrupt the third block")
		return
	}

	// Make a 4k buffer (fits inside a block) to read into.
	buffer := make([]byte, 4096)

	// Read 4k from first block, it should be corrupt.
	if _, err := f.ReadAt(buffer, 0); err != ErrCorruptData {
		t.Error("expecting first block to be corrupted")
		return
	}

	// The second block wasn't corrupted so it should be fine to read from.
	if n, err := f.ReadAt(buffer, blockDataLength); len(buffer) != n || nil != err {
		t.Error("Failed to read second block")
		return
	}

	// Read 4k from third block, should be corrupt.
	if _, err := f.ReadAt(buffer, 2*blockDataLength); err != ErrCorruptData {
		t.Error("expecting third block to be corrupted")
		return
	}

	// Read all 3 blocks.  At least one is corrupt so the read should fail.
	buffer = make([]byte, 4*blockDataLength)
	if _, err := f.ReadAt(buffer, 0); err != ErrCorruptData {
		t.Error("expecting corruption even the second block is ok")
		return
	}
}

// Do a read on a totally empty file.
func TestChecksumFileEmptyFile(t *testing.T) {
	// Make a file.
	path := filepath.Join(test.TempDir(), "testReadEmptyFile")
	_, e := os.Create(path)
	if nil != e {
		t.Fatal("couldn't create a file: " + e.Error())
	}

	// Open as a checksum file.
	var ckf *ChecksumFile
	if ckf, e = NewChecksumFile(path, os.O_RDONLY); nil != e {
		t.Fatal("couldn't open file as ChecksumFile: " + e.Error())
	}

	// Read should fail with EOF.
	if _, e = ckf.ReadAt(make([]byte, 10), 0); e != io.EOF {
		t.Fatal("expecting io.EOF")
	}
}

// Do a read on a file that's smaller than a checksum to verify that we detect that as an error.
func TestBadChecksumFileTooSmall(t *testing.T) {
	// Make a file.
	path := filepath.Join(test.TempDir(), "testBadChecksumFileTooSmall")
	f, e := os.Create(path)
	if nil != e {
		t.Fatal("couldn't create a file: " + e.Error())
	}

	// Write some nonsense into it.  We're writing <= the length of the checksum here.
	buf := make([]byte, 4)
	if _, e = f.Write(buf); nil != e {
		t.Fatal("couldn't write 2 bytes to file: " + e.Error())
	}

	// Open as a checksum file.
	var ckf *ChecksumFile
	if ckf, e = NewChecksumFile(path, os.O_RDONLY); nil != e {
		t.Fatal("couldn't open file as ChecksumFile: " + e.Error())
	}

	// Read should fail.
	if _, e = ckf.ReadAt(buf, 0); e != ErrCorruptData {
		t.Fatal("expecting corruption")
	}
}

// An implementation of the mock file interface that limits the size of the file, erroring similar to how filling a
// drive would error.
type maxLenFile struct {
	file   *os.File
	maxLen int64
}

func (f *maxLenFile) Stat() (fi os.FileInfo, err error) {
	return f.file.Stat()
}

func (f *maxLenFile) WriteAt(b []byte, off int64) (n int, err error) {
	// If the write starts after the max, reject outright.
	if off >= f.maxLen {
		return 0, &os.PathError{Op: "write", Path: "somepath", Err: syscall.ENOSPC}
	}

	// If it ends at the max, it's OK.
	if off+int64(len(b)) <= f.maxLen {
		return f.file.WriteAt(b, off)
	}

	// Otherwise, trim 'b', do the write, return ENOSPC
	max := f.maxLen - off
	n, e := f.file.WriteAt(b[0:max], off)
	if nil != e {
		panic("error during write, shouldn't happen")
	}
	return n, &os.PathError{Op: "write", Path: "somepath", Err: syscall.ENOSPC}
}

func (f *maxLenFile) ReadAt(b []byte, off int64) (n int, err error) {
	return f.file.ReadAt(b, off)
}

func (f *maxLenFile) Close() (err error) {
	return f.file.Close()
}

func (f *maxLenFile) Sync() (err error) {
	return f.file.Sync()
}

func (f *maxLenFile) Truncate(size int64) (err error) {
	return f.file.Truncate(size)
}

func (f *maxLenFile) Fadvise(offset int64, length int64, advice int) (err error) {
	return nil
}

func (f *maxLenFile) Getxattr(name string) ([]byte, error) {
	return nil, nil
}

func (f *maxLenFile) Listxattr() ([]string, error) {
	return nil, nil
}

func (f *maxLenFile) Removexattr(name string) error {
	return nil
}

func (f *maxLenFile) Setxattr(name string, value []byte) error {
	return nil
}

// Append failures.  We start with a file of a certain size, append a certain amount of data, but the write fails after
// a certain number of bytes.  The data shouldn't be corrupt though.
var appendFailTests = []struct {
	// Create a file of this many bytes.  Fill it with a known repeating pattern.
	size int

	// Append data of this length.
	n int

	// This is the maximum size that the *underlying file* will allow.
	// The units above are for the user data, not for the file size.
	maxLen int64
}{
	// Creating new block from zero partially fails.
	{0, blockDataLength / 2, blockLength / 4},

	// Extending existing block but not allocating new one.
	{1, blockDataLength / 2, blockLength / 4},

	// Extending existing block by more than one block fails in old block.
	{10, 2 * blockDataLength, blockLength / 2},

	// Extending existing block by more than one block fails in new block.
	{10, 2 * blockDataLength, blockLength + blockLength/2},

	// Adding an entirely new block entirely fails -- we can't add any of it.
	{blockDataLength, blockDataLength, blockLength},

	// Adding an entirely new block entirely fails -- we can't add the checksum.
	{blockDataLength, blockDataLength, blockLength + 2},

	// Adding an entirely new block partially fails -- we can add a bit of it.
	{blockDataLength, blockDataLength, blockLength + blockLength/4},
}

// Run the tests in appendFailTests.
func TestAppendFailures(t *testing.T) {
	// reset osFileOpener when done to not impede further testing.
	defer func() {
		osFileOpener = openOsFile
	}()

	for _, test := range appendFailTests {
		t.Log("running test ", test)
		osFileOpener = func(path string, flags int, perm os.FileMode) (mockFile, error) {
			f, e := os.OpenFile(path, flags, perm)
			if nil != e {
				return nil, e
			}
			return &maxLenFile{file: f, maxLen: test.maxLen}, nil
		}

		// Create initial file.
		f := setupChecksumFile(test.size, t)

		// Try to append 'n' bytes of a known pattern.
		b := getBuffer(test.n)
		n, e := f.append(b)

		// It should fail and it should write maxLen - size bytes.
		if e != syscall.ENOSPC {
			t.Fatal("append did not fail with no space, got ", e)
		}

		// Calculate the max number of user bytes based on the max file size.
		numBlocks := (test.maxLen - headerLength) / blockLength
		leftover := (test.maxLen - headerLength) % blockLength
		if leftover < blockChecksumLength {
			// Append won't write a corrupt block if it can avoid it.
			// If a block can't hold a checksum it will be truncated.
			leftover = 0
		} else {
			// Remove the checksum from the partial block length.
			leftover -= blockChecksumLength
		}
		maxSize := blockDataLength*numBlocks + leftover

		// initial size + bytes written successfully be the same as what we calculate
		// from the pre-determined max file size.
		if n+test.size != int(maxSize) {
			t.Fatalf("wrong size written, expected %d got %d", maxSize, n+test.size)
		}
		// ChecksumFile's self-reported size should equal the max file size.
		if cfSize, e := f.Size(); nil != e || cfSize != maxSize {
			t.Fatalf("checksumfile didn't report the size we expected")
		}

		// Ask for one more byte than what was written.
		buf := make([]byte, maxSize+1)
		n, e = f.ReadAt(buf, 0)

		// Should get EOF and read maxSize bytes.
		if io.EOF != e || int64(n) != maxSize {
			t.Fatal("did not get EOF despite reading one past end")
		}

		// Verify initial bytes.
		if e := bufIsOK(buf[0:test.size], 0); nil != e {
			t.Fatal("initial bytes not OK")
		}
		if e := bufIsOK(buf[test.size:n], 0); nil != e {
			t.Fatal("additional bytes not OK")
		}
	}
}

// Set an xattr, get it back and verify.
func TestSetGetxattr(t *testing.T) {
	f := setupChecksumFile(128, t)
	defer f.Close()

	// Set xattr.
	name := "spicy"
	in := []byte("food")
	if err := f.Setxattr(name, in); nil != err {
		t.Fatalf("failed to set xattr: %s", err)
	}

	// Get it back and verify.
	out, err := f.Getxattr(name)
	if nil != err {
		t.Fatalf("failed to get xattr: %s", err)
	}
	if 0 != bytes.Compare(in, out) {
		t.Fatalf("value doesn't match")
	}
}
