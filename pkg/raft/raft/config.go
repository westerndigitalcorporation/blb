// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package raft

import (
	"fmt"
	"time"
)

// Config stores the configurations for core layer.
type Config struct {
	// ID of the node. Users must assign a unique ID to each Raft node in a group.
	ID string

	// The ID of the cluster. For now we only use it as a metric label to make our
	// monitoring system easier to aggregate and analyze metrics from raft nodes
	// of a same cluster.
	// TODO: use it to guard against misconfiguration of a cluster?
	ClusterID string

	// CandidateTimeout is used with 'RandomElectionRange' by a candidate to start
	// an election of next term, in units of 'tick'. If a candidate can't be elected
	// during time "CandidateTimeout + [0, RandomElectionRange]" it will start a new
	// election for next term.
	CandidateTimeout uint32

	// FollowerTimeout is used with 'RandomElectionRange' by a follower to start an
	// election of next term, in units of 'tick'. If a follower can't hear anything
	// from its leader during time "FollowerTimeout + [0, RandomElectionRange]" it
	// will start a new election of next term.
	FollowerTimeout uint32

	// LeaderStepdownTimeout is used to control how long the leader can maintain
	// its leadership without being able to contact a quorum of nodes, in units of
	// 'tick'. Once a leader loses the contact from a quorum after this amount
	// of time it will step down as leader. 'LeaderStepdownTimeout' must be larger
	// than 'HeartbeatTimeout' if it's not 0, otherwise the leader might falsely
	// think it loses its leadership and step down, which will make the whole system
	// unavailable until a new leader is elected. This is a voluntary decision and
	// do not depend on this to guarantee there's only one leader exists in cluster.
	// This is only used to step down a leader node who might already lose its
	// leadership at some point, so there might be multiple leaders(though only
	// one is effective) exist at the same time.
	//
	// If 'LeaderStepdownTimeout' is 0 then leader will keep its leadership until
	// a higher term is detected.
	//
	// The motivation of using 'LeaderStepdownTimeout' is when a network partiton
	// happens a stale leader may stay in leader state indefinitely even there's
	// a newer leader gets elected on the other side. Without realizing it might
	// already lost its leadership the stale leader will keep returning timeout
	// error to clients who are connecting to it. But if it steps down from leader
	// state at some point then clients can get more useful feedbacks(errors about
	// not being leader anymore) and fail over to the real leader node sooner.
	LeaderStepdownTimeout uint32

	// RandomElectionRange is a range of the randomness we introduced to leader
	// election to avoid the problem of split votes. Before electing itself as
	// leader, nodes need to wait for an extra time in range [0, RandomElectionRange]
	// to avoid split votes.
	RandomElectionRange uint32

	// HeartbeatTimeout is the interval of AppEnts requests, in units of 'tick'.
	// Leader must send at least one AppEnts request every this interval to maintain
	// its leadership. HeartbeatTimeout must be smaller than FollowerTimeout,
	// otherwise an elected leader can not maintain its leadership for a long time.
	HeartbeatTimeout uint32

	// SnapshotTimeout specifies how long it's expected to transfer snapshot AND
	// recover state from the snapshot. Leader will try to send messages to
	// followers at some intervals to maintain its leadership or retransmit the
	// message that might be lost. Because snapshot messages might take a long
	// time to transfer and apply so we use a different timeout to avoid
	// retransmitting unnecessary snapshot messages. The timeout should be set to
	// be large enough to cover the time spent in transferring and recovery.
	//
	// TODO: It might be hard to guess a good value beforehand, we could adjust the
	// timeout value dynamically using:
	//  - based on size of snapshot
	//  - use exponential back-off
	// Some relevant discussions in ZooKeeper group(https://issues.apache.org/jira/browse/ZOOKEEPER-1977)
	// (I don't think this has been addressed by them)
	SnapshotTimeout uint32

	// Duration per tick. We separated the logic time("tick") and real time(duration
	// per tick) to make our implementation more testable.
	DurationPerTick time.Duration

	// GenSeed is the function that takes a Raft ID as parameter and generates a
	// seed number for random number generator of that node. If users don't
	// specify one, a default seed generator will be used.
	GenSeed func(string) int64

	// MaxNumEntsPerAppEnts specifies the maximum number of entries per AppEnts
	// message. We don't want to saturate disk/network IO when synchronizing a
	// straggler.
	MaxNumEntsPerAppEnts uint32

	// LogEntriesAfterSnapshot specifies the number of entries to leave in the
	// log after a snapshot is taken. Once a snapshot is taken we are OK to
	// trim all log entries which are included in snapshot, but doing that might
	// force a leader to ship its entire snapshot to a follower when a follower
	// is missing anything in the snapshot. By keeping some entries in the log
	// after a snapshot, we can send those entries to followers to synchronize
	// them, instead of having to send an entire snapshot.
	// LogEntriesAfterSnapshot is the maximum number of entries we can trim(included
	// in snapshot) but keep them untrimmed in the log.
	LogEntriesAfterSnapshot uint64

	// SnapshotThreshold specifies how many applied commands there must be since last
	// snapshhot before we perform a new snapshot. If it's 0 then no snapshot will
	// be performed. For example, if you set "SnapshotThreshold" to 1000, then after
	// every 1000 commands applied to state machine a new snapshot will be taken.
	// There's a trade-off on this number -- the larger it is, the less frequently
	// we'll take snapshot so that it would cost less IO, but it might take longer
	// time in in recovery to apply all log entries that are not included in snapshot.
	SnapshotThreshold uint64

	// MaximumPendingProposals specifies the maximum number of pending proposals
	// allowed. Further requests will be blocked once the size of pending proposals
	// reaches this limit. If it is set to 0, no limit will be enforced.
	MaximumPendingProposals uint32

	// MaximumProposalBatch is the maximum number of proposals leader will try to
	// process them at once from proposal channel. The larger it is, the larger
	// throughput you might get, but also larger latency per request you might
	// experience.
	MaximumProposalBatch uint32
}

// validateConfig validates a config object.
func validateConfig(config Config) error {
	if config.ID == "" {
		return fmt.Errorf("ID can't be empty")
	}
	if config.ClusterID == "" {
		return fmt.Errorf("Cluster ID can't be empty")
	}
	if config.CandidateTimeout == 0 {
		return fmt.Errorf("CandidateTimeout can't be 0")
	}
	if config.FollowerTimeout == 0 {
		return fmt.Errorf("FollowerTimeout can't be 0")
	}
	if config.RandomElectionRange == 0 {
		return fmt.Errorf("RandomElectionRange can't be 0")
	}
	if config.HeartbeatTimeout == 0 {
		return fmt.Errorf("HeartbeatTimeout can't be 0")
	}
	if config.DurationPerTick == 0 {
		return fmt.Errorf("DurationPerTick can't be 0")
	}
	if config.MaxNumEntsPerAppEnts == 0 {
		return fmt.Errorf("MaxNumEntsPerAppEnts can't be 0")
	}
	if config.HeartbeatTimeout >= config.FollowerTimeout {
		return fmt.Errorf("HeartbeatTimeout must be smaller than FollowerTimeout")
	}
	if config.HeartbeatTimeout >= config.CandidateTimeout {
		return fmt.Errorf("HeartbeatTimeout must be smaller than CandidateTimeout")
	}
	if config.LeaderStepdownTimeout != 0 &&
		config.LeaderStepdownTimeout <= config.HeartbeatTimeout {
		return fmt.Errorf("LeaderStepdownTimeout must be larger than HeartbeatTimeout")
	}
	if config.MaximumProposalBatch == 0 {
		return fmt.Errorf("MaximumProposalBatch can't be 0.")
	}
	return nil
}
