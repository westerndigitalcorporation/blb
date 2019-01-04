// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"strings"
	"time"

	log "github.com/golang/glog"

	"github.com/westerndigitalcorporation/blb/internal/raftkv/server"
	"github.com/westerndigitalcorporation/blb/internal/raftkv/store"
	"github.com/westerndigitalcorporation/blb/pkg/failures"
	"github.com/westerndigitalcorporation/blb/pkg/retry"
)

var (
	addr    = flag.String("addr", "", "address(host:port) for this raftkv instance")
	members = flag.String("members", "", "members is a list of host names separated by comma")
	join    = flag.String("join", "", "address of a node in the joined cluster")
	dataDir = flag.String("data-dir", "./", "persistent data directory")
)

func main() {
	flag.Parse()

	if *addr == "" {
		log.Fatalf("You must specify address of this instace.")
	}
	failures.Init()
	log.Infof("Starting server on: %s!", *addr)

	config := store.DefaultConfig
	config.ID = *addr
	config.RaftDir = *dataDir
	config.DBDir = *dataDir

	store := store.New(config)

	if *members != "" {
		store.Start(strings.Split(*members, ","))
	} else if *join != "" {
		// Issuing a request to join the cluster asynchronously.
		go joinNode(*join, *addr)
		store.Join()
	} else {
		store.Restart()
	}

	server := server.New(*addr, store)
	server.Serve()
}

// Issue a join request to "node" on behalf of "joiner".
func joinNode(node, joiner string) {
	clt := &http.Client{}
	retrier := &retry.Retrier{
		MinSleep:      500 * time.Millisecond,
		MaxSleep:      10 * time.Second,
		MaxNumRetries: 0, // Inifinite number of retries, we use timeout to abort retry.
		MaxRetry:      30 * time.Second,
	}
	success, _ := retrier.Do(context.Background(), func(seq int) bool {
		resp, err := clt.Get(fmt.Sprintf("http://%s/join?node=%s", node, joiner))
		if err != nil {
			log.Errorf("Failed to issue join request: %v", err)
			return false
		}
		if resp.StatusCode != http.StatusOK {
			log.Infof("Received %q from server, retry...", resp.Status)
			return false
		}
		return true
	})

	if !success {
		log.Fatalf("Faield to join the cluster after several retries")
		return
	}
}
