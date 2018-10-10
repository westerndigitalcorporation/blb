// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package main

import (
	"flag"

	"github.com/westerndigitalcorporation/blb/internal/watchblb"
	"github.com/westerndigitalcorporation/blb/platform/clustersniff"
)

func main() {
	// Parse the flags.
	cfg := watchblb.DefaultConfig
	flag.StringVar(&cfg.Cluster, "masters", "", "address spec for blb masters")
	flag.StringVar(&cfg.TableFile, "table_file", cfg.TableFile, "persistent file backing the db")
	flag.DurationVar(&cfg.BlobLifetime, "lifetime", cfg.BlobLifetime, "blob lifetime")
	flag.Int64Var(&cfg.WriteSize, "size", cfg.WriteSize, "blob size")
	flag.IntVar(&cfg.ReplFactor, "repl", cfg.ReplFactor, "replication factor")
	flag.DurationVar(&cfg.WriteInterval, "write_interval", cfg.WriteInterval, "write interval")
	flag.DurationVar(&cfg.ReadInterval, "read_interval", cfg.ReadInterval, "read interval")
	flag.DurationVar(&cfg.CleanInterval, "clean_interval", cfg.CleanInterval, "clean interval")
	flag.Parse()

	if cfg.Cluster == "" {
		cfg.Cluster = clustersniff.Cluster()
	}

	// Create and start a BlbWatcher.
	watchblb.NewBlbWatcher(cfg).Start()
}
