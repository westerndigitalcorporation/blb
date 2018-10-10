// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package testblb

import (
	"github.com/westerndigitalcorporation/blb/internal/cluster"
)

// partitionAllReplicas makes all raft nodes given in 'replicas' can't talk to
// each other.
func partitionAllReplicas(replicas []cluster.ReplicaProc) error {
	for _, replica := range replicas {
		if err := setDropProb(1.0, replica, replicas...); err != nil {
			return err
		}
	}
	return nil
}

// connectAllReplicas connects(heals) all raft nodes given in 'replicas' to
// each other.
func connectAllReplicas(replicas []cluster.ReplicaProc) error {
	for _, replica := range replicas {
		if err := setDropProb(0.0, replica, replicas...); err != nil {
			return err
		}
	}
	return nil
}

// setDropProb sets the Raft message drop probabilities of a given Raft node 'dest' to
// other nodes specified in 'others' to probability 'prob'.
func setDropProb(prob float32, dest cluster.ReplicaProc, others ...cluster.ReplicaProc) error {
	dropConfig := make(map[string]float32)
	for _, replica := range others {
		if replica.RaftAddr() != dest.RaftAddr() {
			dropConfig[replica.RaftAddr()] = prob
		}
	}
	updates := map[string]interface{}{"msg_drop_prob": dropConfig}
	return dest.UpdateFailureConfig(updates)
}
