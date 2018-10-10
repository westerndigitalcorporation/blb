// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package testblb

import (
	"fmt"

	log "github.com/golang/glog"

	"github.com/westerndigitalcorporation/blb/internal/cluster"
)

// TestCuratorPartition tests that during a network partition that a quorum of
// curators can't talk to each other we'll lose the availability of the curator
// because we opt for consistency instead of availability during network
// partitions. And when the partition gets healed(partially healed) that a
// quorum of curators of a curator group can talk the system will be available
// again eventually.
//
//	1. Create a blob and it should succeed.
//	2. Partition curator replicas of each curator gorup that no quorum can talk
//	   to each other.
//	3. Eventually the leaders of each curator group will step down.
//	4. Create will fail.
//	5. Heal the partition that a quorum of each curator group can talk with each
//	   other, a new leader of each group will be elected.
//	6. Create should succeed again.
//
func (tc *TestCase) TestCuratorPartition() error {
	//	1. Create a blob and it should succeed.

	// Create a blob and fill it with one tract of data.
	_, err := tc.c.Create()
	if err != nil {
		return err
	}

	//	2. Partition curator replicas of each curator group that no quorum can talk
	//	   to each other.

	captureLeaderStepdown := tc.captureLogs()

	log.Infof("Partition the each curator group...")
	var wanted []string
	for i, group := range tc.bc.Curators() {
		var replicas []cluster.ReplicaProc
		for _, curator := range group {
			replicas = append(replicas, curator)
		}
		wanted = append(wanted, fmt.Sprintf("c%d:@@@ Step down as the leader without quorum", i))
		partitionAllReplicas(replicas)
	}

	//	3. Eventually the leaders of each curator group will step down.

	log.Infof("Wait until current leaders of all curator groups step down.")
	if err := captureLeaderStepdown.WaitFor(wanted...); err != nil {
		return err
	}

	//	4. Create will fail.

	// Use non-retry client so it will not waste time on retry.
	if _, err := tc.noRetryC.Create(); err == nil {
		log.Errorf("Expected the create request to fail during curator partition.")
	}

	//	5. Heal the partition that a quorum of each curator group can talk with each
	//	   other, a new leader of each group will be elected.

	captureNewLeader := tc.captureLogs()
	log.Infof("Heal the network partition so a quorum of curators can talk.")

	wanted = nil
	for i, group := range tc.bc.Curators() {
		var replicas []cluster.ReplicaProc
		for _, curator := range group {
			replicas = append(replicas, curator)
		}
		wanted = append(wanted, fmt.Sprintf("c%d:@@@ leadership changed: true", i))
		quorum := len(group)/2 + 1
		connectAllReplicas(replicas[:quorum])
	}

	log.Infof("Wait until new leaders are elected.")
	if err := captureNewLeader.WaitFor(wanted...); err != nil {
		return err
	}

	//	6. Create should succeed again.

	if _, err := tc.c.Create(); err != nil {
		return err
	}

	return nil
}
