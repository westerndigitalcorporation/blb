// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package blb

import (
	"context"

	"github.com/westerndigitalcorporation/blb/internal/core"
)

// TractserverTalker manages connections to tractservers.
type TractserverTalker interface {
	// Create creates a new tract on the tractserver and does a write to the
	// newly created tract.
	Create(ctx context.Context, addr, reqID string, tsid core.TractserverID, id core.TractID, b []byte, off int64) core.Error

	// Write does a write to a tract on this tractserver.
	Write(ctx context.Context, addr, reqID string, id core.TractID, version int, b []byte, off int64) core.Error

	// Read reads from a tract.
	Read(ctx context.Context, addr string, otherHosts []string, reqID string, id core.TractID, version int, len int, off int64) ([]byte, core.Error)

	// ReadInto reads from a tract. It will try to read up to len(b) bytes and put them in b.
	// It returns the number of bytes read, as in io.Reader's Read.
	ReadInto(ctx context.Context, addr string, otherHosts []string, reqID string, id core.TractID, version int, b []byte, off int64) (int, core.Error)

	// StatTract returns the number of bytes in a tract.
	StatTract(ctx context.Context, addr string, id core.TractID, version int) (int64, core.Error)

	// GetDiskInfo returns a summary of disk info.
	GetDiskInfo(ctx context.Context, addr string) ([]core.FsStatus, core.Error)

	// SetControlFlags changes control flags for a disk.
	SetControlFlags(ctx context.Context, addr string, root string, flags core.DiskControlFlags) core.Error
}
