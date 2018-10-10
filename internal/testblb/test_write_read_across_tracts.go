// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package testblb

import (
	"bytes"
	"fmt"
	"os"

	log "github.com/golang/glog"
	client "github.com/westerndigitalcorporation/blb/client/blb"
)

// TestWriteReadAcrossTracts tests writing to different parts of an existing
// blob and the write might span multiple tracts. After the write the blob
// should be in an expected state.
func (tc *TestCase) TestWriteReadAcrossTracts() error {
	tests := []struct {
		// The original size of the blob.
		size int

		// The offset that we will write to.
		off int

		// ... for this many bytes.
		n int
	}{
		// Write 20MB to an empty blob.
		{0, 0, 20 * mb},

		// Append 10MB to a 20MB blob.
		{20 * mb, 20 * mb, 10 * mb},

		// Write 10MB within a blob.
		{30 * mb, 10 * mb, 10 * mb},

		// Overwrites the entire blob.
		{30 * mb, 0, 30 * mb},

		// Write extends and overwrites(partially) a blob.
		{10 * mb, 5 * mb, 20 * mb},

		// Write creates a hole within a tract.
		{5 * mb, 7 * mb, 10 * mb},

		// Write creates holes for an entire tract.
		{5 * mb, 15 * mb, 10 * mb},
	}

	for _, test := range tests {
		log.Infof("Running write&read test %+v", test)
		// Initialize a blob.
		blob, err := tc.initBlob(test.size)
		if err != nil {
			return err
		}

		// Write data to the blob.
		data := makeRandom(test.n)
		blob.Seek(int64(test.off), os.SEEK_SET)
		if n, err := blob.Write(data); err != nil {
			return err
		} else if n != len(data) {
			return fmt.Errorf("Expected the written length to be %d, but got %d", len(data), n)
		}

		// Read and verify.
		expectedLength := max(test.size, test.off+test.n)
		// Verify the blob length is expected.
		if length, err := blob.ByteLength(); err != nil {
			return err
		} else if length != int64(expectedLength) {
			return fmt.Errorf("Expect the blob length to be %d, but got %d", expectedLength, length)
		}
		wholeBlob := make([]byte, expectedLength)
		// Read the whole blob back.
		blob.Seek(0, os.SEEK_SET)
		if n, err := blob.Read(wholeBlob); err != nil || n != len(wholeBlob) {
			return fmt.Errorf("Read failed: error is %s, len is %d but expected %d", err, n, len(wholeBlob))
		}
		// Verify the blob data is expected.
		if err := verifyBlobData(wholeBlob, data, test.size, test.off); err != nil {
			return err
		}
	}
	return nil
}

// initBlob creates a blob with the given size. The data will be filled with
// byte 'x'.
func (tc *TestCase) initBlob(size int) (*client.Blob, error) {
	var err error
	var blob *client.Blob
	// Create a blob with 3 replicas.
	if blob, err = tc.c.Create(); err != nil {
		return nil, err
	}
	data := make([]byte, size)
	for i := range data {
		data[i] = 'x'
	}
	blob.Seek(0, os.SEEK_SET)
	if n, err := blob.Write(data); err != nil || n != len(data) {
		return nil, err
	}
	return blob, nil
}

// Given the blob data and the data that was written to it, verifyBlobData
// verifies the blob contains the written data and the data that is outside
// of the write range is unaffected, also if there're any holes created by
// the write, the holes should be filled with byte 0s.
func verifyBlobData(blobData, writtenData []byte, oldSize int, off int) error {
	// The data that was written must be included in 'blobData'.
	if off+len(writtenData) > len(blobData) {
		return fmt.Errorf("The written data must be included in blob data")
	}
	// We should read it back.
	if bytes.Compare(blobData[off:off+len(writtenData)], writtenData) != 0 {
		return fmt.Errorf("mismatch with written data")
	}
	// The part that belongs to original blob and haven't been modified should be
	// unaffected.
	for i := 0; i < min(off, oldSize); i++ {
		if blobData[i] != 'x' {
			return fmt.Errorf("the blob data is changed unexpectedly")
		}
	}
	// If there're some holes created by write, the hole should be filled with 0.
	for i := oldSize; i < off; i++ {
		if blobData[i] != 0 {
			return fmt.Errorf("the hole should be filled with byte 0")
		}
	}
	for i := off + len(writtenData); i < len(blobData); i++ {
		if blobData[i] != 'x' {
			return fmt.Errorf("the blob data is changed unexpectedly")
		}
	}
	return nil
}

func max(v1, v2 int) int {
	if v1 > v2 {
		return v1
	}
	return v2
}

func min(v1, v2 int) int {
	if v1 > v2 {
		return v2
	}
	return v1
}
