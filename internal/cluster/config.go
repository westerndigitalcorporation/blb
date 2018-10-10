// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package cluster

import (
	"sync"
)

// Config includes configuration parameters for a cluster.
type Config struct {
	*LogConfig

	// Configurations for both RaftKV and Blb cluster.
	BinDir  string // Directory of binaries.
	RootDir string // Root directory of the cluster.

	// Configurations for Blb cluster only.
	Masters             uint // Number of master replicas.
	Curators            uint // Number of curators replicas per curator group.
	CuratorGroups       uint // Number of curator groups.
	Tractservers        uint // Number of tract servers.
	DisksPerTractserver uint // Number of disks per tract server.
}

// LogConfig includes configuration parameters for logging.
type LogConfig struct {
	termLogOn    bool       // Should we print log messages on terminal
	rewriteTimes bool       // Should we rewrite timestamps
	logPrefix    bool       // Should we print log prefix on terminal
	pattern      string     // pattern for filtering logging messages if it's not an empty string.
	lock         sync.Mutex // Used to prevent the three fields above.
}

// DefaultLogConfig returns a *LogConfig with the default settings.
func DefaultLogConfig() *LogConfig {
	return &LogConfig{}
}

// SetTermLogOn sets if we should print log messages on terminal or not.
func (c *LogConfig) SetTermLogOn(on bool) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.termLogOn = on
}

// GetTermLogOn returns if logging to terminal is turned on.
func (c *LogConfig) GetTermLogOn() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.termLogOn
}

// SetRewriteTimes sets if we should rewrite timestamps
func (c *LogConfig) SetRewriteTimes(on bool) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.rewriteTimes = on
}

// GetRewriteTimes returns whether we should rewrite timestamps
func (c *LogConfig) GetRewriteTimes() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.rewriteTimes
}

// SetLogPrefix sets if we should print log prefix on terminal or not.
func (c *LogConfig) SetLogPrefix(on bool) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.logPrefix = on
}

// GetLogPrefix returns if showing log prefix is turned on.
func (c *LogConfig) GetLogPrefix() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.logPrefix
}

// SetPattern sets the pattern for grep.
func (c *LogConfig) SetPattern(pattern string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.pattern = pattern
}

// GetPattern gets the pattern fro grep.
func (c *LogConfig) GetPattern() string {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.pattern
}
