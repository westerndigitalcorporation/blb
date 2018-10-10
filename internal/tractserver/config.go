// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package tractserver

import (
	"fmt"
	"time"
)

// Config encapsulates parameters for Tractserver.
type Config struct {
	MasterSpec            string // How to find masters.
	Addr                  string // Address for service.
	DiskControllerBase    string // Base directory for disk controller unix sockets.
	RejectReqThreshold    int    // Pending incoming client requests on 'Addr' are rejected after this threshold.
	RejectCtlReqThreshold int    // Pending incoming control requests on 'Addr' are rejected after this threshold.
	UseFailure            bool   // Whether to enable the failure service.

	// --- Data and Metadata ---
	DiskStatusCacheTTL time.Duration // How long a cached disk status stays valid.

	// --- Disk Scrubbing ---
	ScrubRate uint64 // How many bytes per second for data scrubbing.

	// --- Master ---
	// How long to wait after an unsuccessful registration.
	RegistrationRetry time.Duration
	// How often to send heartbeats to the master.
	MasterHeartbeatInterval time.Duration
	// How long to wait after an unsuccessful heartbeat.
	MasterHeartbeatRetry time.Duration

	// --- Curator ---
	// How often to send heartbeats to curators.
	CuratorHeartbeatInterval time.Duration
	// How many tracts do we put into each heartbeat message to the curator for them to consider GC-ing?
	TractsPerCuratorHeartbeat int
	// Max number of bad tracts to report per heartbeat.
	BadTractsPerHeartbeat int

	// --- Disk Manager ---
	// How long do we delay before _really_ deleting tracts
	UnlinkTractDelay time.Duration
	// How often do we sweep for deleted tracts.
	SweepTractInterval time.Duration
	// We treat a disk is full when the available space is found to be below this
	// value (in bytes) during Statfs.
	DiskLowThreshold uint64
	// How many IO workers per disk.
	Workers int
	// Whether to attempt to keep data out of buffer cache.
	DropCache bool

	// --- Reed-Solomon ---
	EncodeIncrementSize int

	// If OverrideID.IsValid, will skip contacting the master for an ID and just use this.
	// The default value is not valid.
	OverrideID int
}

// Validate validates the configuration object has reasonable(not obviously
// wrong) values.
func (c Config) Validate() error {
	if c.Addr == "" {
		return fmt.Errorf("Address of the tract server can not be empty")
	}
	if c.TractsPerCuratorHeartbeat == 0 {
		return fmt.Errorf("TractsPerCuratorHeartbeat can not be 0")
	}
	return nil
}

// DefaultProdConfig specifies the default values for Config that is used for
// production. TODO(PL-1113).
// Some of these values are sized for the current configuration of 12 x 4TB
// disks, holding up to 1M tracts each, and four curator groups. If we deploy
// with a significantly different configuration, we may have to revisit these.
var DefaultProdConfig = Config{
	// Disk status cache lifetime.
	DiskStatusCacheTTL: time.Minute,

	// Address for handling requests.
	Addr: "localhost:59900",

	DiskControllerBase: "/var/tmp/blb-tractserver",

	// Pending incoming requests on 'Addr' are rejected after these thresholds.
	RejectReqThreshold:    1000,
	RejectCtlReqThreshold: 1000,

	// Do not enable failure service in production?
	UseFailure: false,

	// Assuming 4TB per disk, this rate will let us read all our data once every 15.4 days.
	ScrubRate: 3 * 1000 * 1000,

	// --- Master ---
	RegistrationRetry:       5 * time.Second,
	MasterHeartbeatInterval: 10 * time.Second,
	MasterHeartbeatRetry:    5 * time.Second,

	// --- Curator ---
	CuratorHeartbeatInterval:  5 * time.Second,
	TractsPerCuratorHeartbeat: 100,  // cycle through 250K tracts in 3.5 hours
	BadTractsPerHeartbeat:     2500, // can report 250K bad tracts in 8 minutes

	// --- Disk Manager ---
	UnlinkTractDelay:   7 * 24 * time.Hour,
	SweepTractInterval: 24 * time.Hour,
	DiskLowThreshold:   1024 * 1024 * 1024,
	Workers:            1,
	DropCache:          true,

	// --- Reed-Solomon ---
	EncodeIncrementSize: 4 << 20,
}

// DefaultTestConfig specifies the default values for Config that is used for testing.
var DefaultTestConfig = Config{
	// Disk status cache lifetime.
	DiskStatusCacheTTL: 20 * time.Second,

	// Address for handling requests.
	Addr: "localhost:59900",

	DiskControllerBase: "/var/tmp/blb-tractserver",

	// Pending incoming requests on 'Addr' are rejected after these thresholds.
	RejectReqThreshold:    1000,
	RejectCtlReqThreshold: 1000,

	UseFailure: true,

	// 10 KB per second for data scrubbing.
	ScrubRate: 10 * 1024,

	// --- Master ---
	RegistrationRetry:       5 * time.Second,
	MasterHeartbeatInterval: 10 * time.Second,
	MasterHeartbeatRetry:    5 * time.Second,

	// --- Curator ---
	CuratorHeartbeatInterval:  5 * time.Second,
	TractsPerCuratorHeartbeat: 100,
	BadTractsPerHeartbeat:     2500,

	// --- Disk Manager ---
	UnlinkTractDelay:   7 * 24 * time.Hour,
	SweepTractInterval: 24 * time.Hour,
	DiskLowThreshold:   1024 * 1024 * 1024,
	Workers:            1,
	DropCache:          true,

	// --- Reed-Solomon ---
	EncodeIncrementSize: 1 << 20,
}
