// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package blb

import (
	"errors"
	"fmt"

	"github.com/westerndigitalcorporation/blb/internal/core"
)

// NOTE: BlobID here is just a duplicate of common.BlobID given package common is not exposed.

// ErrInvalidBlobID is the string representation that the blob ID is invalid.
var ErrInvalidBlobID = errors.New("invalid blob ID format")

// BlobID refers to a Blob stored in Blb.
type BlobID core.BlobID

// NilBlobID means the ID is invalid.
const NilBlobID BlobID = 0

// String returns a human-readable string representation of the BlobID.
func (b BlobID) String() string {
	return core.BlobID(b).String()
}

// ParseBlobID parses a BlobID from the provided string. The string must be in
// the format produced by BlobID.String(). If it is not, ErrInvalidID will be
// returned.
func ParseBlobID(s string) (BlobID, error) {
	var b BlobID
	n, e := fmt.Sscanf(s, "%016x", &b)
	if n != 1 || nil != e {
		return b, ErrInvalidBlobID
	}
	return b, nil
}
