// Copyright (c) 2017 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package backupblb

import "time"

// Config specifies various parameters.
type Config struct {
	// Cluster to find masters and curators, defaults to local cluster.
	Cluster string

	// Directory to put backups and state in. Backups go in a directory within
	// here named by their service. The state file is in the root.
	BaseDir string

	// How often to check for new snapshots (note that curators produce new
	// snapshots approximately every 5-6 hours), this is just the interval that
	// we check for them at.
	CheckInterval time.Duration

	// Which services do we care about (as a regexp)?
	ServiceRE string

	// Timeout to set on http requests.
	HTTPTimeout time.Duration
}

// DefaultConfig are default configuration parameters.
var DefaultConfig = Config{
	CheckInterval: 10 * time.Minute,
	ServiceRE:     "^blb-(master|curator-g.)$",
	HTTPTimeout:   2 * time.Minute,
}
