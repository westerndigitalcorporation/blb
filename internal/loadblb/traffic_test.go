// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package loadblb

import (
	"bytes"
	"testing"

	client "github.com/westerndigitalcorporation/blb/client/blb"
	"github.com/westerndigitalcorporation/blb/internal/core"
)

// Test basic functionality of fillBytes and verifyBytes.
func TestFillVerifyBytes(t *testing.T) {
	numBytes := 1000
	a := make([]byte, numBytes)
	b := make([]byte, numBytes)
	id := core.BlobIDFromParts(23, 140)
	off := int64(12343)

	// The results of fillBytes should be consistent.
	fillBytes(client.BlobID(id), off, a)
	fillBytes(client.BlobID(id), off, b)
	if bytes.Compare(a, b) != 0 {
		t.Fatalf("fillBytes computes different results")
	}

	// Verify the result.
	if !verifyBytes(client.BlobID(id), off, a) {
		t.Fatalf("failed to verify bytes")
	}
}
