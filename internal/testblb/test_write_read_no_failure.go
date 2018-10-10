// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package testblb

import (
	"bytes"
	"fmt"
	"os"
)

// TestWriteReadNoFailure tests that write and read operation should work as
// expected in case that there's no failures.
func (tc *TestCase) TestWriteReadNoFailure() error {
	// Create a blob and fill it with one tract of data.
	blob, err := tc.c.Create()
	if err != nil {
		return err
	}

	buf := make([]byte, 1*mb)
	data := makeRandom(1 * mb)
	blob.Seek(0, os.SEEK_SET)
	if n, err := blob.Write(data); err != nil || n != len(data) {
		return err
	}

	// Read the data once just to check.
	blob.Seek(0, os.SEEK_SET)
	if n, err := blob.Read(buf); err != nil {
		return err
	} else if n != len(data) || bytes.Compare(buf, data) != 0 {
		return fmt.Errorf("data mismatch")
	}
	return nil
}
