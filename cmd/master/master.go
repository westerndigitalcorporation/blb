// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package main

import (
	"encoding/json"
	"flag"
	"os"

	log "github.com/golang/glog"

	"github.com/westerndigitalcorporation/blb/internal/master"
	"github.com/westerndigitalcorporation/blb/internal/master/durable"
	"github.com/westerndigitalcorporation/blb/pkg/failures"
	"github.com/westerndigitalcorporation/blb/pkg/raft/raft"
	"github.com/westerndigitalcorporation/blb/pkg/raft/raftfs"
	"github.com/westerndigitalcorporation/blb/pkg/raft/raftrpc"
)

/*

Configuring various parameters follows three steps:

  (1) Default config parameters are pulled from each individual package, e.g., 'master.DefaultConfig'.

  (2) Optional configuration files (in json format) can be specified via command-line flags '-masterCfg" and '-raftCfg' to override the default values.

  (3) Optional flags can be used to override each individual parameter set in the previous two steps, e.g., '-snapshotDir=ZZZ'.

*/

var (
	// Default configurations.
	masterCfg = master.DefaultConfig
	raftCfg   = durable.DefaultStateConfig

	// Config file names.
	masterFile = flag.String("masterCfg", "", "configuration file for master server")
	raftFile   = flag.String("raftCfg", "", "configuration file for raft instance")

	// Master config parameters.
	addr       = flag.String("addr", "", "address to listen on for requests")
	useFailure = flag.Bool("useFailure", false, "whether to enable the failure service")

	// Raft membership parameters.
	raftID = flag.String("raftID", "", "id of this raft instance, identified by the address that raft listens on")
	raftAC = flag.String("raftAC", "", "spec for raft autoconfig: cluster/user/service=n")

	// Raft storage parameters.
	snapshotDir = flag.String("snapshotDir", "", "home dir for taking snapshots")
	logDir      = flag.String("logDir", "", "home dir for writing logs")
	stateDir    = flag.String("stateDir", "", "home dir for saving raft internal states")
)

// Initialize config parameters. It first tries to read from configuration files
// and then applies the command-line flags to override specified values.
func init() {
	flag.Parse()

	// Read from configuration files.

	// Master server.
	if "" != *masterFile {
		f, err := os.Open(*masterFile)
		if nil != err {
			log.Fatalf("couldn't open the provided config file: %s", err)
		}
		dec := json.NewDecoder(f)
		if err = dec.Decode(&masterCfg); nil != err {
			log.Fatalf("failed to decode the config file: %s", err)
		}
	}

	// Raft.
	raftCfg.Config.ClusterID = "master"

	if "" != *raftFile {
		f, err := os.Open(*raftFile)
		if nil != err {
			log.Fatalf("couldn't open the provided config file: %s", err)
		}
		dec := json.NewDecoder(f)
		if err = dec.Decode(&raftCfg); nil != err {
			log.Fatalf("failed to decode the config file: %s", err)
		}
	}

	// Override values from command-line flags.
	// NOTE: Because of how Go's flag package works, there is no way to tell
	// if a value is set by the user or not. Therefore, we use meaningless
	// default values to check whether a particular flag is set, and only
	// override the corresponding value if so.

	// Master config.
	if "" != *addr {
		masterCfg.Addr = *addr
	}
	if *useFailure {
		masterCfg.UseFailure = *useFailure
	}

	// Raft membership.
	if "" != *raftID {
		raftCfg.ID = *raftID
	}
	if *raftAC != "" {
		masterCfg.RaftACSpec = *raftAC
	}

	// Raft storage.
	if "" != *snapshotDir {
		raftCfg.StorageConfig.SnapshotDir = *snapshotDir
	}
	if "" != *logDir {
		raftCfg.StorageConfig.LogDir = *logDir
	}
	if "" != *stateDir {
		raftCfg.StorageConfig.StateDir = *stateDir
	}

	// Forward raft id to its transport.
	raftCfg.Addr = raftCfg.ID
}

func main() {
	// Initialize failure injection service.
	if masterCfg.UseFailure {
		log.Infof("enabling failure service")
		failures.Init()
	}

	// Create a storage.
	storage, err := raftfs.NewFSStorage(raftCfg.StorageConfig)
	if nil != err {
		log.Fatalf("failed to create raft storage: %s", err)
	}

	// Create a transport.
	var transport raft.Transport
	transport, err = raftrpc.NewRPCTransport(raftCfg.TransportConfig, raftCfg.RPCTransportConfig)
	if nil != err {
		log.Fatalf("failed to create raft transport: %s", err)
	}
	if masterCfg.UseFailure {
		// Add message dropper so we can inject network partitions.
		transport = raft.NewMsgDropper(transport, 0, 0)
	}

	// Create a raft instance.
	r := raft.NewRaft(raftCfg.Config, storage, transport)

	// Create a Master.
	theMaster := master.NewMaster(masterCfg, raftCfg, r)

	// Create a server.
	server := master.NewServer(theMaster, masterCfg, r, storage)
	log.Infof("starting master...")
	if e := server.Start(); nil != e {
		log.Fatalf("couldn't start master server: %s", e.Error())
	}
}
