// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package testblb

import (
	log "github.com/golang/glog"

	"github.com/westerndigitalcorporation/blb/internal/cluster"
)

// TestMasterPartition tests that during a network partition that a quorum of
// masters can't talk to each other we'll lose the availability of the master
// because we opt for consistency instead of availability during network
// partitions. And when the partition gets healed(partially healed) that a
// quorum of masters can talk the system will be available again eventually.
//
//	1. Create a blob and it should succeed.
//	2. Partition master replicas that no quorum can talk to each other.
//	3. Eventually the leader of the master group will step down.
//	4. Create will fail.
//	5. Heal the partition that a quorum can talk with each other, a new
//	   leader will be elected.
//	6. Create should succeed again.
//
func (tc *TestCase) TestMasterPartition() error {

	//	1. Create a blob and it should succeed.

	// Create a blob and fill it with one tract of data.
	_, err := tc.c.Create()
	if err != nil {
		return err
	}

	//	2. Partition master replicas that no quorum can talk to each other.

	captureLeaderStepdown := tc.captureLogs()
	log.Infof("Partition the master group...")

	var replicas []cluster.ReplicaProc
	for _, replica := range tc.bc.Masters() {
		replicas = append(replicas, replica)
	}
	if err := partitionAllReplicas(replicas); err != nil {
		return err
	}

	//	3. Eventually the leader of the master group will step down.

	log.Infof("Wait until current leader steps down.")
	if err := captureLeaderStepdown.WaitFor("m:@@@ Step down as the leader without quorum contacts"); err != nil {
		return err
	}

	//	4. Create will fail.

	// Use non-retry client so it will not waste time on retry.
	if _, err := tc.noRetryC.Create(); err == nil {
		log.Errorf("Expected the create request to fail during master partition.")
	}

	//	5. Heal the partition so that a quorum can talk with each other, a new
	//	   leader will be elected.

	captureNewLeader := tc.captureLogs()
	log.Infof("Heal the network partition so a quorum of masters can talk.")

	quorum := len(tc.bc.Masters())/2 + 1
	if err := connectAllReplicas(replicas[:quorum]); err != nil {
		return err
	}

	log.Infof("Wait until a new leader is elected.")
	if err := captureNewLeader.WaitFor("m:@@@ became leader"); err != nil {
		return err
	}

	//	6. Create should succeed again.

	if _, err := tc.c.Create(); err != nil {
		return err
	}

	return nil
}
