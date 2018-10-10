// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package main

import (
	"encoding/json"
	"flag"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	log "github.com/golang/glog"
	"github.com/westerndigitalcorporation/blb/internal/evilblb"
)

var (
	cfgFile = flag.String("config_file", "", "path for JSON encoded configuration file")
)

func main() {
	// Parse the flags.
	flag.Parse()

	var cfg evilblb.Config
	if *cfgFile == "" {
		log.Fatalf("A configuration file file must be provided.")
	}

	file, err := os.Open(*cfgFile)
	if err != nil {
		log.Fatalf("Failed to open configuration: %v", err)
	}
	defer file.Close()

	dec := json.NewDecoder(file)
	if err := dec.Decode(&cfg); err != nil {
		log.Fatalf("Failed to decode configuration file: %v", err)
	}

	// Initialize random number generator seed.
	rand.Seed(time.Now().UnixNano())
	e := evilblb.New(cfg)

	sigC := make(chan os.Signal)
	signal.Notify(sigC, syscall.SIGINT, syscall.SIGTERM)

	// Catch "SIGINT" and "SIGTERM" signals so we get a chance to revert a
	// pending evil before exiting.
	go func() {
		sig := <-sigC
		log.Infof("Signal %q has been catched", sig)
		e.Shutdown()
	}()

	log.Infof("Running EvilBlb...")
	e.Run()
	log.Infof("EvilBlb is stopped.")
}
