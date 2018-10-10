// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package curator

import (
	"fmt"
	"time"

	"github.com/westerndigitalcorporation/blb/internal/core"
)

// Config encapsulates parameters for Curator.
type Config struct {
	CuratorSeed       int64 // Seed for the random number generator.
	MaxReplFactor     int   // What is the max replication factor of a blob?
	MaxTractsToExtend int   // At most how many tracts can one extend a blob by at a time?

	MasterSpec string // How to find masters.

	Addr               string // Address for RPCs.
	UseFailure         bool   // Whether to enable the failure service.
	RejectReqThreshold int    // Pending client request limit.

	PartitionMonInterval time.Duration // At what interval do we check partitions?
	MinFreeBlobSlot      uint64        // Ask for a new partition when the number of free blob slots in existing partitions drops below this threshold.
	FreeMemLimit         uint64        // No new partition if free system memory drops below this.

	ConsistencyCheckInterval  time.Duration // How often to do a consistency check
	ConsistencyCheckBatchSize int           // How many blobs to checksum at once

	RaftACSpec string // Spec for raft autoconfig.

	// --- Master ---
	MasterHeartbeatInterval time.Duration

	// --- GC ---
	MetadataGCInterval   time.Duration
	MetadataUndeleteTime time.Duration

	// --- Tract Server Monitor ---
	// TODO(PL-1107)
	// Unhealthy is anything that hasn't beaten recently but hasn't been offline
	// long enough to be re-replicated.  Unhealthy servers won't host new data as
	// we expect writes to them to fail.
	//
	// NOTE: Needs to be related to tractserver/server.go:curatorHeartbeatInterval.
	// The idea is that if we miss a few heartbeats we'll re-replicate data to
	// unblock clients.
	TsUnhealthy time.Duration
	// Down is anything that hasn't beaten in so long that it is probably offline
	// for and the data on it should be considered gone.
	TsDown time.Duration
	// When a curator becomes a new leader tract servers might take some time to
	// know it. A newly elected curator leader will not claim any tract servers
	// unhealthy or down during the grace period.
	TsHeartbeatGracePeriod time.Duration

	// --- Tract Server ---
	// The interval of sending tracts to tract servers for finding missing tracts.
	SyncInterval time.Duration

	// --- Erasure Coding ---
	// Time after last write that a blob can be considered for erasure coding.
	WriteDelay time.Duration
}

// Validate validates the configuration object has reasonable(not obviously
// wrong) values.
func (c *Config) Validate() error {
	if c.Addr == "" {
		return fmt.Errorf("Address of the curator can not be empty")
	}
	return nil
}

// DefaultProdConfig specifies the default values for Config that is used for
// production environment.
var DefaultProdConfig = Config{
	// Seed for the random number generator.
	CuratorSeed: time.Now().UnixNano(),

	// Whether to enable the failure service.
	UseFailure: false,

	// What is the max replication factor of a blob?
	MaxReplFactor: 10,

	// At most how many tracts can one extend a blob by at a time?
	MaxTractsToExtend: 20,

	// Pending client requests are rejected after this threshold.
	RejectReqThreshold: 100,

	// At what interval do we check the fullness of our partitions?
	PartitionMonInterval: 5 * time.Minute,

	// We will ask for a new partition if the number of blobs we can create
	// in the existing partitions drops below this threshold.
	MinFreeBlobSlot: uint64(core.MaxBlobKey / 4 * 3),

	// We won't ask for a new partition if the amount of free system memory is
	// below this limit (byte). We might change its value in tests.
	//
	// We concluded in estimation (see ids.go) that the metadata size for each
	// partition should be at most 1GB. Leave some slack by doubling the amount.
	FreeMemLimit: 2 * 1024 * 1024 * 1024,

	// How often to do a consistency check
	ConsistencyCheckInterval: 1 * time.Minute,
	// How many blobs/rschunks to checksum at once
	ConsistencyCheckBatchSize: 5000,
	// This means we'll cover 7M blobs per day, 590M in a week, and 1.5B in a
	// month. When we get up to 1.5B blobs per curator, we'll probably want to
	// speed up the checksum rate so we can cover the full state in a reasonable
	// amount of time.

	// --- Master ---
	MasterHeartbeatInterval: 10 * time.Second,

	// --- GC ----
	MetadataGCInterval:   time.Hour,
	MetadataUndeleteTime: 3 * 24 * time.Hour,

	// --- Tract Server Monitor ---
	TsUnhealthy:            1 * time.Minute,
	TsDown:                 15 * time.Minute,
	TsHeartbeatGracePeriod: 3 * time.Minute,

	// --- Tract Server ---
	SyncInterval: 10 * time.Minute,

	// --- Erasure Coding ---
	// The backend can write uploaded files across seven days. (We hope
	// processed data will be written much faster.) Use eight days so that we
	// never try to EC a blob that might still be written to. Add 12 hours so
	// that the times of day that we do lots of EC will tend to be opposite from
	// the times of day that lots of data is written.
	WriteDelay: (8*24 + 12) * time.Hour,
}

// DefaultTestConfig specifies the default values for Config that is used for
// testing environment.
var DefaultTestConfig = Config{
	// Seed for the random number generator.
	CuratorSeed: 31337,

	// Whether to enable the failure service.
	UseFailure: true,

	// What is the max replication factor of a blob?
	MaxReplFactor: 10,

	// At most how many tracts can one extend a blob by at a time?
	MaxTractsToExtend: 20,

	// Pending client requests are rejected after this threshold.
	RejectReqThreshold: 100,

	// At what interval do we check the fullness of our partitions?
	PartitionMonInterval: 5 * time.Minute,

	// We will ask for a new partition if the number of blobs we can create
	// in the existing partitions drops below this threshold.
	MinFreeBlobSlot: uint64(core.MaxBlobKey / 4 * 3),

	// We won't ask for a new partition if the amount of free system memory is below
	// this limit (byte). We might change its value in tests.
	//
	// We concluded in estimation (see ids.go) that the metadata size for each
	// partition should be at most 1GB. Leave some slack by doubling the amount.
	FreeMemLimit: 2 * 1024 * 1024 * 1024,

	// How often to do a consistency check
	ConsistencyCheckInterval: 10 * time.Second,
	// How many blobs to checksum at once
	ConsistencyCheckBatchSize: 10,

	// --- Master ---
	MasterHeartbeatInterval: 10 * time.Second,

	// --- GC ----
	MetadataGCInterval:   15 * time.Minute,
	MetadataUndeleteTime: 3 * time.Hour,

	// --- Tract Server Monitor ---
	TsUnhealthy:            20 * time.Second,
	TsDown:                 30 * time.Second,
	TsHeartbeatGracePeriod: 30 * time.Second,

	// --- Tract Server ---
	SyncInterval: 5 * time.Second,

	// --- Erasure Coding ---
	WriteDelay: 30 * time.Second,
}
