// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package evilblb

import (
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"time"

	log "github.com/golang/glog"
	"github.com/westerndigitalcorporation/blb/internal/evilblb/failimpl"
	"github.com/westerndigitalcorporation/blb/internal/evilblb/topology"
)

func init() {
	registerEvil(PartitionMasterLeader{})
	registerEvil(PartitionCuratorLeader{})
}

// PartitionMasterLeader is the evil that partitions the leader of
// a master group from the rest of the replicas.
type PartitionMasterLeader struct {
	// Length defines how long the partition will last before a revert action.
	Length Duration
}

// Duration implements EvilTask.
func (p PartitionMasterLeader) Duration() time.Duration {
	return p.Length.Duration
}

// Validate implements EvilTask.
func (p PartitionMasterLeader) Validate() error {
	if p.Length.Duration <= 0 {
		return fmt.Errorf("Length of evil must > 0")
	}
	return nil
}

// Apply implements EvilTask.  Partition a master leader from the rest of replicas.
func (p PartitionMasterLeader) Apply(topo *topology.Topology, failer *failimpl.Failer) func() error {
	leader := topo.Masters.Leader
	leaderHost, leaderPort := splitHostPort(leader)

	log.Infof("Partition master leader %s from the rest of the members.", leader)
	for _, master := range topo.Masters.Members {
		if master == leader {
			// Skip itself.
			continue
		}

		peerHost, peerPort := splitHostPort(master)

		// Block traffics from the leader to the replica.
		if err := failer.BlockOutPacketsOnPortToHost(leaderHost, peerHost, peerPort); err != nil {
			log.Fatalf("%v", err)
		}
		// Block traffics from the replica to the leader.
		if err := failer.BlockInPacketsOnPortFromHost(leaderHost, peerHost, leaderPort); err != nil {
			log.Fatalf("%v", err)
		}
	}

	return func() error {
		log.Infof("Fix the partition in master group...")
		return failer.Heal(leaderHost)
	}
}

// PartitionCuratorLeader is the evil that partitions the leader of
// a curator group from the rest of the replicas.
type PartitionCuratorLeader struct {
	// Length defines how long the partition will last before a revert action.
	Length Duration
}

// Duration implements EvilTask.
func (p PartitionCuratorLeader) Duration() time.Duration {
	return p.Length.Duration
}

// Validate implements EvilTask.
func (p PartitionCuratorLeader) Validate() error {
	if p.Length.Duration <= 0 {
		return fmt.Errorf("Length of evil must > 0")
	}
	return nil
}

// Apply implements EvilTask.  Partition a curator leader from the rest of replicas.
func (p PartitionCuratorLeader) Apply(topo *topology.Topology, failer *failimpl.Failer) func() error {
	if len(topo.CuratorGroups) == 0 {
		// Nothing needs to be done.
		return nil
	}

	// Randomly pick a curator group.
	group := topo.CuratorGroups[rand.Intn(len(topo.CuratorGroups))]

	leader := group.Addr
	leaderHost, leaderPort := splitHostPort(leader)

	log.Infof("Partition curator(group %d) leader %s from the rest of the members.", group.ID, leader)

	for _, curator := range group.Members {
		if curator == leader {
			// Skip itself.
			continue
		}

		peerHost, peerPort := splitHostPort(curator)

		// Block traffics from the leader to the replica.
		if err := failer.BlockOutPacketsOnPortToHost(leaderHost, peerHost, peerPort); err != nil {
			log.Fatalf("%v", err)
		}
		// Block traffics from the replica to the leader.
		if err := failer.BlockInPacketsOnPortFromHost(leaderHost, peerHost, leaderPort); err != nil {
			log.Fatalf("%v", err)
		}
	}

	return func() error {
		log.Infof("Fix the partition in curator group %d...", group.ID)
		return failer.Heal(leaderHost)
	}
}

func splitHostPort(addr string) (host string, port int) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		log.Fatalf("Failed to split host and port from addr %s", addr)
	}
	port, err = strconv.Atoi(portStr)
	if err != nil {
		log.Fatalf("Failed to convert port number")
	}
	return host, port
}
