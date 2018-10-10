// Copyright (c) 2016 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

// tsspam creates a bunch of 8mb tracts on a tractserver.
// tsspam does this through issuing a create + write for a random blob's 0-th tract.
// useful for profiling a single tractserver.

package main

import (
	"flag"
	"math/rand"
	"net/rpc"
	"time"

	log "github.com/golang/glog"

	"github.com/westerndigitalcorporation/blb/internal/core"
)

var (
	tsaddr   = flag.String("tsaddr", "localhost:31337", "host:port to send RPCs to TS on")
	num      = flag.Int("num", 100, "how many tracts to add in one run")
	tsidFlag = flag.Int("tsid", 1, "what tsid to assume we're talking to?")
)

func main() {
	client, err := rpc.DialHTTP("tcp", *tsaddr)
	if err != nil {
		log.Fatalf("couldn't dial %s: %s", *tsaddr, err)
	}

	// We randomly pick blob IDs but don't want to create the same blobs for subsequent runs.
	rand.Seed(time.Now().UnixNano())

	// The TSID we're talking to.
	tsid := core.TractserverID(*tsidFlag)

	// A buffer of 0s that we use for the data to write.
	data := make([]byte, 8*1024*1024)

	for i := 0; i < *num; i++ {
		// Pick a random tract ID.
		tid := core.TractID{Blob: core.BlobID(rand.Int63()), Index: core.TractKey(0)}
		var blberr core.Error

		// Create it.
		create := core.CreateTractReq{TSID: tsid, ID: tid, B: data, Off: 0}
		if err = client.Call(core.CreateTractMethod, &create, &blberr); err != nil {
			log.Errorf("rpc error, couldn't create tract %+v: %s", tid, err)
		} else if blberr != core.NoError {
			log.Errorf("rpc success but couldn't create tract %+v: %s", tid, blberr)
		}

		log.Infof("wrote 8mb to tract %s", tid)
	}
}
