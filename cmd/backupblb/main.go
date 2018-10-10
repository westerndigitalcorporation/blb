// Copyright (c) 2017 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package main

import (
	"flag"
	"os"

	log "github.com/golang/glog"
	"github.com/westerndigitalcorporation/blb/internal/backupblb"
	"github.com/westerndigitalcorporation/blb/platform/clustersniff"
)

func main() {
	cfg := backupblb.DefaultConfig
	flag.StringVar(&cfg.Cluster, "cluster", "", "cluster to back up")
	flag.StringVar(&cfg.BaseDir, "base", "", "base directory to back up to")
	flag.Parse()

	if cfg.Cluster == "" {
		cfg.Cluster = clustersniff.Cluster()
	}

	if err := os.MkdirAll(cfg.BaseDir, 0700); err != nil {
		log.Fatalf("Couldn't create base directory %q", cfg.BaseDir)
	}

	backupblb.NewBackup(cfg).Start()
}
