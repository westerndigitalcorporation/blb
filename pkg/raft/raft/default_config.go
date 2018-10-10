// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package raft

import "time"

// Default configuration values. Applications should override the values
// below when appropriate.
var (
	// DefaultConfig defines the default configuration for Raft.
	DefaultConfig = Config{
		ClusterID:             "default",
		CandidateTimeout:      100,
		FollowerTimeout:       100,
		LeaderStepdownTimeout: 100,
		RandomElectionRange:   50,
		HeartbeatTimeout:      20,
		SnapshotTimeout:       1000,
		DurationPerTick:       100 * time.Millisecond,
		MaxNumEntsPerAppEnts:  1000,
		// NOTE: A good number depends on your application. But a rule of thumb is
		// that applying these number of entries should take much less time than
		// restoring from a snapshot. Otherwise we might just want to synchronize
		// and restore from a snapshot.
		LogEntriesAfterSnapshot: 10000,
		// NOTE: this can be much larger in production. A good number depends
		// on your application. The larger it is, the less frequently a snapshot will
		// be taken, but longer time an application will take in recovery. A good
		// heuristic might be the longest time you are willing to wait to apply the
		// entries in recovery.
		SnapshotThreshold:       10000,
		MaximumPendingProposals: 1000,
		MaximumProposalBatch:    100,
	}

	// DefaultTransportConfig includes default values for Raft transport.
	DefaultTransportConfig = TransportConfig{
		MsgChanCap: 1000,
	}
)
