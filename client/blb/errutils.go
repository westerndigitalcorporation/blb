// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package blb

import (
	"github.com/westerndigitalcorporation/blb/internal/core"
)

// IsRetriableError returns if the error is a retriable error. It assumes the error
// is not nil.
func IsRetriableError(err error) bool {
	return core.IsRetriableBlbError(err)
}
