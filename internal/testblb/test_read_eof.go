// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package testblb

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"github.com/westerndigitalcorporation/blb/internal/core"
)

// TestReadEOF tests whether client can get EOF error after reading a whole blob.
func (tc *TestCase) TestReadEOF() error {
	// Different length of blobs.
	tests := []int64{
		core.TractLength,
		2 * core.TractLength,
		core.TractLength + 1,
		core.TractLength - 1,
	}

	for _, size := range tests {
		// Initialize a blob.
		blob, err := tc.initBlob(int(size))
		if err != nil {
			return err
		}

		blob.Seek(0, os.SEEK_SET)

		n, err := io.CopyBuffer(ioutil.Discard, blob, make([]byte, 1<<20))
		if err != nil {
			return err
		}
		if n != size {
			return fmt.Errorf("got EOF error without reading enough data(expected: %d, got: %d)", size, n)
		}
	}

	return nil
}
