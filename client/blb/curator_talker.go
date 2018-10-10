// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package blb

import (
	"context"
	"github.com/westerndigitalcorporation/blb/internal/core"
)

// CuratorTalker manages connections to curators.
type CuratorTalker interface {
	// CreateBlob creates a blob. Only Repl, Hint, and Expires in options are used.
	CreateBlob(ctx context.Context, addr string, metadata core.BlobInfo) (core.BlobID, core.Error)

	// ExtendBlob extends 'blob' until it has 'numTracts' tracts, and returns
	// the new tracts.
	ExtendBlob(ctx context.Context, addr string, blob core.BlobID, numTracts int) ([]core.TractInfo, core.Error)

	// AckExtendBlob acks the success of extending 'blob' with the new
	// tracts 'tracts'.
	AckExtendBlob(ctx context.Context, addr string, blob core.BlobID, tracts []core.TractInfo) core.Error

	// DeleteBlob deletes 'blob'.
	DeleteBlob(ctx context.Context, addr string, blob core.BlobID) core.Error

	// Undelete tries to un-delete 'blob'.
	UndeleteBlob(ctx context.Context, addr string, blob core.BlobID) core.Error

	// SetMetadata changes some metadata for the given blob.
	SetMetadata(ctx context.Context, addr string, blob core.BlobID, md core.BlobInfo) core.Error

	// GetTracts retrieves the tracts ['start', 'end') for 'blob'.
	GetTracts(ctx context.Context, addr string, blob core.BlobID, start, end int, forRead, forWrite bool) ([]core.TractInfo, core.Error)

	// StatBlob gets information about a blob.
	StatBlob(ctx context.Context, addr string, blob core.BlobID) (core.BlobInfo, core.Error)

	// ReportBadTS asks the curator to re-replicate or recover the provided tract or chunk as
	// the host 'bad' is unavailable.  The curator may ignore this, or it may re-replicate.
	ReportBadTS(ctx context.Context, addr string, id core.TractID, bad, op string, got core.Error, couldRecover bool) core.Error

	// FixVersion asks the curator at 'addr' to check the version(s) of the provided
	// tract.  Sent when a client attemped an I/O to the tractserver at 'bad' with the
	// tract in 'tract' but had a version mismatch error.
	FixVersion(ctx context.Context, addr string, tract core.TractInfo, bad string) core.Error

	// ListBlobs gets a contiguous range of blob ids in a given partition, whose
	// keys (lower part of id) are >= 'start'. The server may choose how many to
	// return at once, but it will be at least one if any exist. The keys will
	// be a contiguous range of the id space, and will be in order. An empty
	// return value means that no blobs in that part of the id space exist.
	ListBlobs(ctx context.Context, addr string, partition core.PartitionID, start core.BlobKey) ([]core.BlobKey, core.Error)
}
