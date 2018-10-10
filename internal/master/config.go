// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package master

import "time"

// Config encapsulates parameters for the server.
type Config struct {
	Addr               string // Address for answering requests / receiving heartbeats.
	UseFailure         bool   // Whether to enable the failure service.
	RejectReqThreshold int    // Pending incoming requests on 'SrvAddr' are rejected after this threshold.

	ConsistencyCheckInterval time.Duration // How often to do a consistency check

	RaftACSpec string // Spec for raft autoconfig.
}

// DefaultConfig includes default values for master server.
var DefaultConfig = Config{
	Addr:                     "localhost:55501",
	RejectReqThreshold:       1000,
	ConsistencyCheckInterval: 1 * time.Minute,
}
