// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package blb

import (
	"context"
	"github.com/westerndigitalcorporation/blb/internal/core"
)

// MasterConnection is a connection to master.
type MasterConnection interface {
	// MasterCreatelob returns a proper curator to create a new blob.
	MasterCreateBlob(ctx context.Context) (curator string, err core.Error)

	// LookupPartition returns which curator is responsible for 'partition'.
	LookupPartition(ctx context.Context, part core.PartitionID) (curator string, err core.Error)

	// ListPartitions returns all existing partitions.
	ListPartitions(ctx context.Context) (partitions []core.PartitionID, err core.Error)

	// GetTractserverInfo gets tractserver state.
	GetTractserverInfo(ctx context.Context) (info []core.TractserverInfo, err core.Error)
}
