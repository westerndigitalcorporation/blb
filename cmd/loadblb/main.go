// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package main

import (
	"encoding/json"
	"flag"
	"os"
	"time"

	log "github.com/golang/glog"

	"github.com/westerndigitalcorporation/blb/internal/loadblb"
)

// Flags for config parameters.
var (
	// Where to find the blb masters.
	masters = flag.String("masters", "", "address spec for blb masters")

	// For simple tests (fixed arrival rate and size), use the following
	// three options.
	duration = flag.Duration("duration", 3*time.Minute, "duration to inject load")
	rate     = flag.Int64("rate", 2000, "fixed arrival rate of operations")
	size     = flag.Int64("size", 8*loadblb.MB, "fixed I/O size of each operation")
	repl     = flag.Int("repl", 3, "replication factor of each blob")

	// For advanced tests, use one of the two below. Configuration file
	// overwrites command-line configs.
	cfg     = flag.String("config", "", "JSON encoded configuration parameters (not recommended, use config file instead)")
	cfgFile = flag.String("config_file", "", "path for JSON encoded configuration file (overrites command-line config)")
)

func main() {
	flag.Set("logtostderr", "true")

	// Parse the flags.
	flag.Parse()

	var stats string
	var runerr error

	if *cfg == "" && *cfgFile == "" {
		// Run simple tests when appropriate.
		stats, runerr = loadblb.FixedWriteRead(*masters, *duration, *rate, *size, *repl)
	} else {
		var graphCfg loadblb.GraphConfig
		// Parse command-line config.
		if *cfg != "" {
			if err := json.Unmarshal([]byte(*cfg), &graphCfg); err != nil {
				log.Fatalf("failed to parse command-line config: %s", err)
			}
		}

		// Parse config file.
		if *cfgFile != "" {
			f, err := os.Open(*cfgFile)
			if err != nil {
				log.Fatalf("failed to open config file %s: %s", *cfgFile, err)
			}
			dec := json.NewDecoder(f)
			if err := dec.Decode(&graphCfg); err != nil {
				log.Fatalf("failed to decode config file %s: %s", *cfgFile, err)
			}
		}

		// Run the test.
		stats, runerr = loadblb.NewGraph(graphCfg).Run()
	}

	if runerr == nil {
		log.Infof("load test passed...")
		log.Infof("\n====== stats ======\n%s", stats)
	} else {
		log.Errorf("load test failed...: %s", runerr)
	}
}
