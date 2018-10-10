// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package testblb

import (
	"bytes"
	"fmt"

	client "github.com/westerndigitalcorporation/blb/client/blb"
	"github.com/westerndigitalcorporation/blb/internal/core"
)

// TestConcurrentRead tests concurrently reading from disjoint intervals in the
// same blob.
func (t *TestCase) TestConcurrentRead() error {
	tests := [][]struct {
		// write in [start, end)
		start, end int
	}{
		// Intervals are non-overlapping and in increasing order.

		// Two full tracts that are not adjacent to each other.
		{
			{0, 1 * core.TractLength},
			{2 * core.TractLength, 3 * core.TractLength},
		},
		// Three tracts (the first one is half and the others are full)
		// that are adjacent to each other.
		{
			{core.TractLength / 2, 1 * core.TractLength},
			{1 * core.TractLength, 2 * core.TractLength},
			{2 * core.TractLength, 3 * core.TractLength},
		},
		// Four tracts (some partially full) that are not adjacent to
		// each other.
		{
			{core.TractLength / 3, core.TractLength / 2},
			{core.TractLength, core.TractLength / 2 * 3},
			{2 * core.TractLength, core.TractLength / 2 * 5},
			{4 * core.TractLength, 5 * core.TractLength},
		},
		// Two disjoint intervals in the same tract.
		{
			{0, core.TractLength / 2},
			{core.TractLength / 2, core.TractLength},
		},
		// Two pairs of intervals where each pair are disjoint but in
		// the same tract. Different pairs are in different tracts.
		{
			// First pair in the first tract
			{0, core.TractLength / 2},
			{core.TractLength / 2, core.TractLength},
			// Second pair in the third tract
			{core.TractLength * 2, core.TractLength / 3 * 7},
			{core.TractLength / 2 * 5, core.TractLength * 3},
		},
	}

	for _, writes := range tests {
		// Create a blob.
		blob, err := t.c.Create()
		if nil != err {
			return err
		}

		// Create some random data to use. Again, use the last write's
		// upper bound as the byte size.
		lastOff := writes[len(writes)-1].end
		data := makeRandom(lastOff)

		// Write to the blob.
		for _, w := range writes {
			if _, err := blob.WriteAt(data[w.start:w.end], int64(w.start)); err != nil {
				return err
			}
		}

		// Concurrently read and verify all writes.
		done := make(chan error, len(writes))
		for _, w := range writes {
			go func(start, end int) {
				done <- verify(blob, start, end, data[start:end])
			}(w.start, w.end)
		}
		for i := 0; i < len(writes); i++ {
			if err := <-done; nil != err {
				return err
			}
		}
	}

	return nil
}

// verify verifies 'data' matches the [start, end) part of the given blob.
func verify(blob *client.Blob, start, end int, data []byte) error {
	if start < 0 || start >= end || len(data) != end-start {
		return fmt.Errorf("invalid argument")
	}

	read := make([]byte, len(data))
	if n, err := blob.ReadAt(read, int64(start)); len(data) != n {
		return fmt.Errorf("short read, expected %d and got %d", len(data), n)
	} else if nil != err {
		return err
	}

	if 0 != bytes.Compare(data, read) {
		return fmt.Errorf("write verification failed")
	}
	return nil
}
