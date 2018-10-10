// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package raftfs

import (
	"fmt"

	"github.com/westerndigitalcorporation/blb/pkg/raft/raft"
)

// NewFSStorage creates a storage backed by a local filesystem.
func NewFSStorage(cfg StorageConfig) (*raft.Storage, error) {
	snapshot, err := NewFSSnapshotMgr(cfg.SnapshotDir)
	if nil != err {
		return nil, fmt.Errorf("failed to create SnapshotMgr: %s", err)
	}

	state, err := NewFSState(cfg.StateDir)
	if nil != err {
		return nil, fmt.Errorf("failed to create State: %s", err)
	}

	return raft.NewStorage(snapshot, raft.NewFSLog(cfg.LogDir, cfg.LogCacheCapacity), state), nil
}
