// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package testblb

import "time"

// TestConfig includes configuration parameters for testing.
type TestConfig struct {
	TestTime        time.Duration // How long we run the test.
	WriteInterval   time.Duration // How often we write.
	FailureInterval time.Duration // How often we kill a server.
	WriteSizeLowMB  int           // The smallest write size in mb.
	WriteSizeHighMB int           // The largest write size in mb.
	MaxReplFactor   int           // Maximum replication factor.
	NumWriters      int           // Number of concurrent writers.
	DisableCache    bool          // Whether client cache is disabled.
	TestPattern     string        // Regex matching tests to run.
	TestLong        bool          // Whether tests ended with "Long" should be run (they tend to last for an extended period of time and are disabled by default).
	NumWorkers      int           // Number of test workers to run in parallel.
}
