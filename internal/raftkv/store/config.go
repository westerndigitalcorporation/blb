// Copyright (c) 2016 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package store

import (
	"time"

	"github.com/westerndigitalcorporation/blb/pkg/raft/raft"
)

// Config stores configuration for Store.
type Config struct {
	raft.Config
	RaftDir string
	DBDir   string

	ChecksumBatchSize int
	ChecksumInterval  time.Duration
}

// DefaultConfig provides default configurations of a Store.
var DefaultConfig = Config{
	Config: raft.Config{
		ClusterID:               "raftkv",
		FollowerTimeout:         300,
		CandidateTimeout:        500,
		HeartbeatTimeout:        100,
		LeaderStepdownTimeout:   200,
		RandomElectionRange:     300,
		DurationPerTick:         10 * time.Millisecond,
		MaxNumEntsPerAppEnts:    100,
		MaximumProposalBatch:    100,
		SnapshotTimeout:         10000,
		SnapshotThreshold:       10,
		LogEntriesAfterSnapshot: 10000,
		GenSeed:                 func(string) int64 { return time.Now().UnixNano() },
	},
	RaftDir: "",
	DBDir:   "",

	ChecksumBatchSize: 1000,
	// This is very aggressive for testing.
	// In production we would want to make it less frequent.
	ChecksumInterval: 5 * time.Second,
}
