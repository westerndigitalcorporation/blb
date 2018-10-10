// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package main

import (
	"encoding/json"
	"flag"
	"os"

	log "github.com/golang/glog"

	"github.com/westerndigitalcorporation/blb/internal/tractserver"
	"github.com/westerndigitalcorporation/blb/pkg/failures"
	"github.com/westerndigitalcorporation/blb/platform/clustersniff"
	"github.com/westerndigitalcorporation/blb/platform/dyconfig"
)

/*

Configuring various parameters follows three steps:

  (1) Default config parameters are pulled from 'tractserver.DefaultConfig'.

  (2) Optional configuration files (in json format) can be specified via command-line flags '-curatorFile" and '-raftFile' to override the default values.

  (3) Optional flags can be used to override each individual parameter set in the previous two steps, e.g., '-masters="a,b,c"'.

*/

var (
	// Default configuration. This is the default configuration for production.
	cfg = tractserver.DefaultProdConfig

	// Config file name.
	tsFile = flag.String("tractserverCfg", "", "configuration file for tractserver")

	// Tractserver config parameters.
	masters            = flag.String("masters", "", "address spec for masters to talk to")
	addr               = flag.String("addr", "", "service address")
	diskControllerBase = flag.String("diskControllerBase", "", "base dir for disk controller sockets")
	useFailure         = flag.Bool("useFailure", false, "whether to enable the failure service")
	workers            = flag.Int("workers", 0, "number of io workers per disk")
	overrideID         = flag.Int("overrideID", 0, "manually specify TSID for testing purposes")
)

// Initialize config parameters. It first tries to read from configuration files
// and then applies the command-line flags to override specified values.
func init() {
	flag.Parse()

	// Read from configuration file.
	if "" != *tsFile {
		f, err := os.Open(*tsFile)
		if nil != err {
			log.Fatalf("couldn't open the provided config file: %s", err)
		}
		dec := json.NewDecoder(f)
		if err = dec.Decode(&cfg); nil != err {
			log.Fatalf("failed to decode the config file: %s", err)
		}
	}

	// Override values from command-line flags.
	// NOTE: Because of how Go's flag package works, there is no way to tell
	// if a value is set by the user or not. Therefore, we use meaningless
	// default values to check whether a particular flag is set, and only
	// override the corresponding value if so.
	if "" != *masters {
		cfg.MasterSpec = *masters
	}
	if "" != *addr {
		cfg.Addr = *addr
	}
	if *diskControllerBase != "" {
		cfg.DiskControllerBase = *diskControllerBase
	}
	if *useFailure {
		cfg.UseFailure = *useFailure
	}
	if *overrideID != 0 {
		cfg.OverrideID = *overrideID
	}
	if *workers != 0 {
		cfg.Workers = *workers
	}
}

func main() {
	if err := cfg.Validate(); err != nil {
		log.Fatalf("Failed to validate configurations: %v", err)
	}

	// Initialize failure injection service.
	if cfg.UseFailure {
		log.Infof("enabling failure service")
		failures.Init()
	}

	// Set up metadata store.
	metadata := tractserver.NewMetadataStore()

	// Set up the entire disk store.
	tt := tractserver.NewRPCTractserverTalker()
	store := tractserver.NewStore(tt, metadata, &cfg)

	// Disk controller for adding/removing disks.
	tractserver.NewDiskController(store)

	// Set up curator talker.
	ct := tractserver.NewRPCCuratorTalker()
	// Set up master connections.
	spec := cfg.MasterSpec
	if spec == "" {
		spec = clustersniff.Cluster()
		log.Infof("Defaulting to master spec %q", spec)
	}
	mc := tractserver.NewRPCMasterConnection(spec)

	// Set up dynamic configuration handler. We don't need to block on it.
	go dyconfig.Register("config", true, cfg, store.SetConfig)

	// Create server.
	server := tractserver.NewServer(store, ct, mc, &cfg)
	log.Infof("starting tractserver...")
	if e := server.Start(); nil != e {
		log.Fatalf("couldn't start tractserver: %s", e.Error())
	}
}
