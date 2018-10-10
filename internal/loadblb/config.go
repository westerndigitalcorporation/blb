// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package loadblb

import (
	"time"

	log "github.com/golang/glog"

	client "github.com/westerndigitalcorporation/blb/client/blb"
)

// generatorConfig includes basic parameters for a generator.
type generatorConfig struct {
	// Name of the generator.
	Name string

	// Start and end time, in terms of ticks.
	Start int64
	End   int64

	// Event arrival rate and I/O byte size.
	Rate VariateConfig
	Size VariateConfig
}

// WriteGenConfig includes parameters for a writer.
type WriteGenConfig struct {
	generatorConfig
	Readers    []string      // The names of the readers that can read the blobs written by this writer.
	ReplFactor VariateConfig // Replication factor.
}

// ReadGenConfig includes parameters for a reader.
type ReadGenConfig struct {
	generatorConfig
	RecentN int // The reader only reads from the RecentN most recently created blobs. Recent<=0 means all blobs.
}

// HandlerConfig includes parameters for a load handler.
type HandlerConfig struct {
	client.Options
	// Must be parsable by time.ParseDuration. Will be parsed and replace
	// Options.RetryTimeout. This is to facilitate writing time duration
	// value in JSON, otherwise one will need to put an value in nanosecond
	// for duration in JSON.
	RetryTimeout string

	NumWorkers int // Number of concurrent goroutines for I/O operations.

	QueueLength int // Number of maximum backlog of events.
}

// GraphConfig includes necessary parameters to define a graph.
type GraphConfig struct {
	Writers []WriteGenConfig
	Readers []ReadGenConfig
	Handler HandlerConfig

	// The time interval for one tick for generators. Must be parsable by
	// time.ParseDuration. Will be parsed into tickInterval below, which is
	// used by the graph.
	TickInterval string
	tickInterval time.Duration

	// Whether cleanup blobs that were created after the load testing is done.
	CleanupWhenDone bool
}

// To make it easy to configure durations (e.g., one can write "3s" instead of
// 3000000000), we expose a string type for each of these fields and hide the
// actual duration type (JSON encoder/decoder only touches exposed fields). This
// method parses durations from their string representation the actual
// format and assigns the values to the unexposed fields. It must be called
// before the configuration is used to create a graph.
func (g *GraphConfig) parseDuration() {
	var err error

	// Parse client retry timeout.
	options := g.Handler.Options
	if options.RetryTimeout, err = time.ParseDuration(g.Handler.RetryTimeout); err != nil {
		log.Fatalf("failed to parse RetryTimeout field: %s", err)
	}
	g.Handler = HandlerConfig{
		Options:      options,
		RetryTimeout: g.Handler.RetryTimeout,
		NumWorkers:   g.Handler.NumWorkers,
	}

	// Parse tick interval.
	if g.tickInterval, err = time.ParseDuration(g.TickInterval); err != nil {
		log.Fatalf("failed to parse Interval field: %s", err)
	}
}
