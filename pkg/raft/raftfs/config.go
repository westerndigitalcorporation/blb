// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package raftfs

// StorageConfig stores the parameters for FSStorage.
type StorageConfig struct {
	SnapshotDir      string // Home dir for taking snapshots.
	LogDir           string // Home dir for writing logs.
	StateDir         string // Home dir for storing internal states.
	LogCacheCapacity uint   // Number of entries to cache by raft log.
}

// DefaultStorageConfig includes default values for Raft storage.
var DefaultStorageConfig = StorageConfig{
	// Home dir for taking snapshots.
	SnapshotDir: "snapshot",

	// Home dir for writing logs.
	LogDir: "log",

	// Home dir for storing internal states.
	StateDir: "state",

	// Number of entries to cache by raft log. The assumption is a command is not
	// likely to exceed 1KB so caching the last 100000 commands in log will cost
	// no more than 10MB memory.
	LogCacheCapacity: 10000,
}
