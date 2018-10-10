// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package main

import (
	"flag"
	"strings"
	"time"

	log "github.com/golang/glog"

	"github.com/westerndigitalcorporation/blb/pkg/raft/raft"
	"github.com/westerndigitalcorporation/blb/pkg/raft/raftfs"
	"github.com/westerndigitalcorporation/blb/pkg/raft/raftrpc"
)

func main() {
	cmdSize := flag.Uint("cmd_size", 1024, "Size of each command in bytes")
	ID := flag.String("id", "", "host ID")
	members := flag.String("members", "", "addresses of all members, separated by comma")
	maxAppents := flag.Uint("max_apps", 1000, "maximum number of entries allowed per AppEnts")
	maxBatchProps := flag.Uint("max_batch", 1000, "maximum number of proposals batched")
	dataDir := flag.String("data_dir", "", "data directory for Raft persistent state")
	numTxns := flag.Uint("n_txns", 50000, "Number of transactions used to benchmark")
	cacheCap := flag.Uint("cache", 10000, "Number of log entries to cache in memory")

	flag.Parse()

	config := raft.DefaultConfig
	config.SnapshotThreshold = 0 // No snapshot should be taken.
	config.ID = *ID
	config.MaxNumEntsPerAppEnts = uint32(*maxAppents)
	config.MaximumProposalBatch = uint32(*maxBatchProps)

	sMgr, err := raftfs.NewFSSnapshotMgr(*dataDir)
	if err != nil {
		log.Fatalf("Failed to create snapshot manager: %v", err)
	}
	fState, err := raftfs.NewFSState(*dataDir)
	if err != nil {
		log.Fatalf("Failed to create FSState: %v", err)
	}

	storage := raft.NewStorage(sMgr, raft.NewFSLog(*dataDir, *cacheCap), fState)
	tc := raft.TransportConfig{Addr: config.ID, MsgChanCap: 100}
	rpcc := raftrpc.RPCTransportConfig{SendTimeout: 1 * time.Second}
	transport, err := raftrpc.NewRPCTransport(tc, rpcc)
	if err != nil {
		log.Fatalf("Failed to create transport: %v", err)
	}
	transport.StartStandaloneRPCServer()
	r := raft.NewRaft(config, storage, transport)

	log.Infof("Starting benchmark on node %s, number of txns: %d, txn size %d bytes.", *ID, *numTxns, *cmdSize)
	log.Infof("max_app: %d, max_batch: %d, cache capacity: %d", *maxAppents, *maxBatchProps, *cacheCap)

	bench := newBenchmark(r, *numTxns, strings.Split(*members, ","))
	bench.start(*cmdSize, *numTxns)

	log.Infof("Benchmark is done!")
}
