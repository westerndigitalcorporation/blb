// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package watchblb

import "time"

// Config specifies various parameters.
type Config struct {
	// Master addresses to be used by blb client.
	Cluster string

	// The persistent file for the sqlite table.
	TableFile string

	// The byte size of each write.
	WriteSize int64

	// Replication factor.
	ReplFactor int

	// How often to perform writes.
	WriteInterval time.Duration

	// How often to perform reads.
	ReadInterval time.Duration

	// How often to check blob lifetime.
	CleanInterval time.Duration

	// For how long a blob lives before getting removed.
	BlobLifetime time.Duration
}

// DefaultConfig includes default configuration parameters.
var DefaultConfig = Config{
	TableFile:  "watchblb.db",
	WriteSize:  1 * 1024 * 1024,
	ReplFactor: 3,

	// Create a new tract every minute. In total 86400 blobs are created
	// during a 60-day period, which account for 84 GB of data when each
	// blob is 1 MB in size.
	WriteInterval: time.Minute,

	ReadInterval: 20 * time.Second,

	CleanInterval: 5 * time.Minute,

	// A blob lives for 60 days.
	BlobLifetime: 1440 * time.Hour,
}
