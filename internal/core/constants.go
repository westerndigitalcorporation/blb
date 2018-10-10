// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package core

import (
	"time"
)

// Global constants that several components need to agree on are defined here.
// If a constant is only needed for single component, probably it should not be
// placed here.
const (
	// TractLength is the fixed tract length of 8 MB.
	TractLength = 8 * 1024 * 1024

	// MaxReplicationFactor is the maximum allowed replication of a blob.
	MaxReplicationFactor int = 10

	// TombstoneVersion is the flag to indicate a tract should be removed.
	// When Curator is preparing a TractStateReply, it checks the version
	// numbers for tracts being asked for. If this tract doesn't exist any
	// more, TombstoneVersion is set as the version number. When
	// Tractserver receives the reply and compare the versions, it will
	// garbage collect the tract if the replied version is
	// TombstoneVersion.
	TombstoneVersion int = -1

	// ProposalTimeout is the period before a Raft application's proposal
	// times out.
	ProposalTimeout = 4 * time.Second

	// RSChunkVersion is a constant fake "version" for tracts that are actually
	// RS chunks. RS chunks are write-once, so we don't need to use versions to
	// enforce consistency.
	RSChunkVersion = -317866832
)
