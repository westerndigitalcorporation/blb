// Copyright (c) 2016 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package raft

import (
	"fmt"
	"io"
	"reflect"
	"strconv"
	"testing"
	"time"

	log "github.com/golang/glog"
)

func newEntry(term, index uint64) Entry {
	return Entry{Type: EntryNormal, Term: term, Index: index, Cmd: nil}
}
func newConfEntry(term, index uint64, members []string, epoch uint64) Entry {
	m := Membership{Members: members, Epoch: epoch}
	return Entry{Type: EntryConf, Term: term, Index: index, Cmd: encodeMembership(m)}
}

func testEntriesEqual(ch1, ch2 chan Entry, num int) bool {
	var ents1 []Entry
	var ents2 []Entry
	for i := 0; i < num; i++ {
		ents1 = append(ents1, <-ch1)
		ents2 = append(ents2, <-ch2)
	}
	return reflect.DeepEqual(ents1, ents2)
}

// An FSM implementation for testing only.
type testFSM struct {
	ID         string
	appliedCh  chan Entry
	leaderCh   chan struct{}
	followerCh chan struct{}
}

func newTestFSM(ID string) *testFSM {
	return &testFSM{
		ID:         ID,
		appliedCh:  make(chan Entry, 1024),
		leaderCh:   make(chan struct{}, 1),
		followerCh: make(chan struct{}, 1),
	}
}

func (f *testFSM) Apply(ent Entry) interface{} {
	log.Infof("%q : apply is called on entry %q!", f.ID, ent)
	f.appliedCh <- ent
	return ent.Cmd
}

func (f *testFSM) OnLeadershipChange(b bool, term uint64, leader string) {
	if b {
		log.Infof("%q becomes a leader for term %d", f.ID, term)
		select {
		case f.leaderCh <- struct{}{}:
		default:
			// It's not guaranteed the channel will be consumed by someone. So if the
			// channel is full just discard the leadership change event to avoid
			// being blocked forever.
			// We might lose the latest update if the channel is full, but we can
			// fix it when it turns out to be a problem.
		}
	} else {
		log.Infof("%q now is in non-leader state with term %d, leader: %q", f.ID, term, leader)
		select {
		case f.followerCh <- struct{}{}:
		default:
			// Ditto.
		}
	}
}

func (f *testFSM) OnMembershipChange(membership Membership) {
	log.Infof("OnMembershipChange: %+v", membership)
}

func (f *testFSM) SnapshotSave(writer io.Writer) error {
	log.Fatalf("not implemented yet")
	return nil
}

func (f *testFSM) Snapshot() (Snapshoter, error) {
	log.Fatalf("not implemented yet")
	return nil, nil
}

func (f *testFSM) SnapshotRestore(reader io.Reader, lastIndex, lastTerm uint64) {
	log.Fatalf("not implemented yet")
}

func getTestConfig(ID, clusterID string) Config {
	return Config{
		ID:                   ID,
		ClusterID:            clusterID,
		FollowerTimeout:      10,
		CandidateTimeout:     10,
		HeartbeatTimeout:     3,
		RandomElectionRange:  6,
		DurationPerTick:      10 * time.Millisecond,
		MaxNumEntsPerAppEnts: 10,
		MaximumProposalBatch: 1,
	}
}

// creates a raft node for testing.
func testCreateRaftNode(cfg Config, storage *Storage) *Raft {
	transport := NewMemTransport(TransportConfig{Addr: cfg.ID, MsgChanCap: 1024})
	r := NewRaft(cfg, storage, transport)
	return r
}

// connect all raft nodes.
func connectAllNodes(nodes ...*Raft) {
	var transports []*memTransport
	for _, node := range nodes {
		transports = append(transports, node.transport.(*memTransport))
	}
	connectAll(transports)
}

// Test that a new leader should be elected among a Raft group.
func TestRaftNewLeader(t *testing.T) {
	ID1 := "1"
	ID2 := "2"
	clusterPrefix := "TestRaftNewLeader"

	// Create n1 node.
	storage := NewStorage(NewMemSnapshotMgr(), NewMemLog(), NewMemState(0))
	fsm1 := newTestFSM(ID1)
	// NOTE we use different cluster ID for nodes within same cluster to avoid
	// registering same metric path twice. This should never happen in real world
	// because we'll never run nodes of a same cluster within one process.
	n1 := testCreateRaftNode(getTestConfig(ID1, clusterPrefix+ID1), storage)
	// Create n2 node.
	storage = NewStorage(NewMemSnapshotMgr(), NewMemLog(), NewMemState(0))
	fsm2 := newTestFSM(ID2)
	n2 := testCreateRaftNode(getTestConfig(ID2, clusterPrefix+ID2), storage)
	connectAllNodes(n1, n2)
	n1.Start(fsm1)
	n2.Start(fsm2)
	n2.ProposeInitialMembership([]string{ID1, ID2})

	// Wait until a leader is elected.
	select {
	case <-fsm1.leaderCh:
	case <-fsm2.leaderCh:
	}
}

// Start two Raft nodes with different log states and verify they will converge to
// leader's state eventually.
func TestRaftSynchronization(t *testing.T) {
	ID1 := "1"
	ID2 := "2"
	// Configuration entry, used by the cluster to find its configuration.
	confEntry := newConfEntry(1, 1, []string{ID1, ID2}, 54321)

	tests := []struct {
		// Entries in n1's log.
		n1Log []Entry
		// Entries in n2's log.
		n2Log []Entry
		// term of n1
		n1term uint64
		// term of n2
		n2term uint64
		// number of entries will be applied
		nApplied int
	}{

		// NOTE: entry (1, 1) will be the configuration.

		// n1: (1, 1), (1, 2)
		// n2: (1, 1), (1, 2)
		// applied: (1, 2)
		{
			[]Entry{confEntry, newEntry(1, 2)},
			[]Entry{confEntry, newEntry(1, 2)},
			1,
			1,
			1,
		},

		// n1: (1, 1), (1, 2), (1, 3)
		// n2: (1, 1), (1, 2)
		// applied: (1, 2), (1, 3)
		{
			[]Entry{confEntry, newEntry(1, 2), newEntry(1, 3)},
			[]Entry{confEntry, newEntry(1, 2)},
			1,
			1,
			2,
		},

		// n1: (1, 1),
		// n2: (1, 1), (1, 2)
		// applied: (1, 2)
		{
			[]Entry{confEntry},
			[]Entry{confEntry, newEntry(1, 2)},
			1,
			1,
			1,
		},

		// n1: (1, 1), (1, 2), (1, 3)
		// n2: (1, 1), (2, 2)
		// applied: (2, 2)
		{
			[]Entry{confEntry, newEntry(1, 2), newEntry(1, 3)},
			[]Entry{confEntry, newEntry(2, 2)},
			1,
			2,
			1,
		},
	}

	for i, test := range tests {
		t.Log("running test:", test)
		clusterPrefix := fmt.Sprintf("TestRaftSynchronization_%d", i)

		// Create n1 node.
		storage := NewStorage(NewMemSnapshotMgr(), initLog(test.n1Log...), NewMemState(test.n1term))
		fsm1 := newTestFSM(ID1)
		n1 := testCreateRaftNode(getTestConfig(ID1, clusterPrefix+ID1), storage)
		// Create n2 node.
		storage = NewStorage(NewMemSnapshotMgr(), initLog(test.n2Log...), NewMemState(test.n2term))
		fsm2 := newTestFSM(ID2)
		n2 := testCreateRaftNode(getTestConfig(ID2, clusterPrefix+ID2), storage)
		connectAllNodes(n1, n2)
		n1.Start(fsm1)
		n2.Start(fsm2)

		// Two FSMs should have applied same sequence of commands.
		if !testEntriesEqual(fsm1.appliedCh, fsm2.appliedCh, test.nApplied) {
			t.Fatal("two FSMs in same group applied different sequence of commands.")
		}
	}
}

// Test proposing 100 commands on leader side and eventually these commands will
// be applied to the state machines of both leader and follower in same sequence.
func TestRaftProposeNewCommand(t *testing.T) {
	ID1 := "1"
	ID2 := "2"
	clusterPrefix := "TestRaftProposeNewCommands"

	// Create n1 node.
	fsm1 := newTestFSM(ID1)
	n1 := testCreateRaftNode(getTestConfig(ID1, clusterPrefix+ID1), newStorage())
	// Create n2 node.
	fsm2 := newTestFSM(ID2)
	n2 := testCreateRaftNode(getTestConfig(ID2, clusterPrefix+ID2), newStorage())
	connectAllNodes(n1, n2)
	n1.Start(fsm1)
	n2.Start(fsm2)
	n2.ProposeInitialMembership([]string{ID1, ID2})

	// Find out who leader is.
	var leader *Raft
	var follower *Raft
	select {
	case <-fsm1.leaderCh:
		leader = n1
		follower = n2
	case <-fsm2.leaderCh:
		leader = n2
		follower = n1
	}

	// Proposing on follower node should fail.
	pending := follower.Propose([]byte("data"))
	<-pending.Done
	if pending.Err != ErrNodeNotLeader {
		t.Fatalf("expected 'ErrNotNotLeader' error when propose on follower node.")
	}

	// Propose 100 commands.
	for i := 0; i < 100; i++ {
		leader.Propose([]byte("I'm data-" + strconv.Itoa(i)))
	}

	// Two FSMs should have applied same sequence of commands.
	if !testEntriesEqual(fsm1.appliedCh, fsm2.appliedCh, 100) {
		t.Fatal("two FSMs in same group applied different sequence of commands.")
	}
}

// Test returned pending from "Propose".
func TestRaftPending(t *testing.T) {
	ID1 := "1"
	ID2 := "2"
	clusterPrefix := "TestRaftPending"

	// Create n1 node.
	fsm1 := newTestFSM(ID1)
	n1 := testCreateRaftNode(getTestConfig(ID1, clusterPrefix+ID1), newStorage())
	// Create n2 node.
	fsm2 := newTestFSM(ID2)
	n2 := testCreateRaftNode(getTestConfig(ID2, clusterPrefix+ID2), newStorage())
	connectAllNodes(n1, n2)
	n1.Start(fsm1)
	n2.Start(fsm2)
	n2.ProposeInitialMembership([]string{ID1, ID2})

	// Find out who leader is.
	var leader *Raft
	var follower *Raft
	select {
	case <-fsm1.leaderCh:
		leader = n1
		follower = n2
	case <-fsm2.leaderCh:
		leader = n2
		follower = n1
	}

	// Prpose a command on leader.
	pending := leader.Propose([]byte("I'm data"))

	// Block until the command concludes.
	<-pending.Done

	// "Apply" should return the exact command back.
	if pending.Err != nil {
		t.Fatal("expected no error returned in pending")
	}
	if string(pending.Res.([]byte)) != "I'm data" {
		t.Fatal("expected exact command to be returned in pending.")
	}

	// Propose to non-leader node should result an error.
	pending = follower.Propose([]byte("I'm data too"))

	// Block until the command concludes.
	<-pending.Done

	// Should return an error "ErrNodeNotLeader" when propose command to non-leader node.
	if pending.Err != ErrNodeNotLeader {
		t.Fatalf("expected to get error %q when propose to non-leader node", ErrNodeNotLeader)
	}
}

// Test VerifyRead method of Raft.
func TestRaftVerifyRead(t *testing.T) {
	ID1 := "1"
	ID2 := "2"
	clusterPrefix := "TestRaftVerifyRead"

	// Create n1 node.
	fsm1 := newTestFSM(ID1)
	n1 := testCreateRaftNode(getTestConfig(ID1, clusterPrefix+ID1), newStorage())
	// Create n2 node.
	fsm2 := newTestFSM(ID2)
	n2 := testCreateRaftNode(getTestConfig(ID2, clusterPrefix+ID2), newStorage())
	connectAllNodes(n1, n2)
	n1.transport = NewMsgDropper(n1.transport, 193, 0)
	n2.transport = NewMsgDropper(n2.transport, 42, 0)
	n1.Start(fsm1)
	n2.Start(fsm2)
	n2.ProposeInitialMembership([]string{ID1, ID2})

	// Find out who leader is.
	var leader *Raft
	var follower *Raft
	select {
	case <-fsm1.leaderCh:
		leader = n1
		follower = n2
	case <-fsm2.leaderCh:
		leader = n2
		follower = n1
	}

	// Verification on leader side should succeed.
	pending1 := leader.VerifyRead()
	pending2 := leader.VerifyRead()
	<-pending1.Done
	<-pending2.Done
	if pending1.Err != nil || pending2.Err != nil {
		t.Fatalf("VerifyRead on leader should succeed")
	}

	// A new round.
	pending3 := leader.VerifyRead()
	<-pending3.Done
	if pending3.Err != nil {
		t.Fatalf("VerifyRead on leader should succeed")
	}

	// Verification on follower side should fail.
	pending4 := follower.VerifyRead()
	<-pending4.Done
	if pending4.Err == nil {
		t.Fatalf("VerifyRead on follower should fail")
	}

	// Create a network partition between "1" and "2"
	n1.transport.(*msgDropper).Set(ID2, 1)
	n2.transport.(*msgDropper).Set(ID1, 1)

	// Now there's a network partition between "1" and "2", verification should
	// either timeout or fail.
	pending1 = leader.VerifyRead()
	select {
	case <-pending1.Done:
		if pending1.Err == nil {
			log.Fatalf("expected the verification to be timeout or failed in the case of partition")
		}
	case <-time.After(100 * time.Millisecond):
	}
}

// Test that in 3 nodes cluster the leader is partitioned away from the other
// two nodes.
//
//   - Commands proposed on the partitioned leader should fail.
//   - A new leader should be elected among the other two nodes.
//   - Commands proposed on the new leader should succeed.
//   - After healing the partition the old leader should join the new quorum and
//     gets synced from the new leader.
//
func TestRaftNetworkPartition(t *testing.T) {
	ID1 := "1"
	ID2 := "2"
	ID3 := "3"
	clusterPrefix := "TestRaftNetworkPartition"

	// Create n1 node.
	fsm1 := newTestFSM(ID1)
	cfg := getTestConfig(ID1, clusterPrefix+ID1)
	cfg.LeaderStepdownTimeout = cfg.FollowerTimeout
	n1 := testCreateRaftNode(cfg, newStorage())

	// Create n2 node.
	fsm2 := newTestFSM(ID2)
	cfg = getTestConfig(ID2, clusterPrefix+ID2)
	cfg.LeaderStepdownTimeout = cfg.FollowerTimeout
	n2 := testCreateRaftNode(cfg, newStorage())

	// Create n3 node.
	fsm3 := newTestFSM(ID3)
	cfg = getTestConfig(ID3, clusterPrefix+ID3)
	cfg.LeaderStepdownTimeout = cfg.FollowerTimeout
	n3 := testCreateRaftNode(cfg, newStorage())

	connectAllNodes(n1, n2, n3)
	n1.transport = NewMsgDropper(n1.transport, 193, 0)
	n2.transport = NewMsgDropper(n2.transport, 42, 0)
	n3.transport = NewMsgDropper(n3.transport, 111, 0)

	n1.Start(fsm1)
	n2.Start(fsm2)
	n3.Start(fsm3)
	n3.ProposeInitialMembership([]string{ID1, ID2, ID3})

	// Find out who leader is.
	var leader *Raft
	var follower1, follower2 *Raft
	select {
	case <-fsm1.leaderCh:
		leader = n1
		follower1 = n2
		follower2 = n3
	case <-fsm2.leaderCh:
		leader = n2
		follower1 = n1
		follower2 = n3
	case <-fsm3.leaderCh:
		leader = n3
		follower1 = n1
		follower2 = n2
	}

	// Propose a command on the leader.
	pending := leader.Propose([]byte("I'm data1"))
	<-pending.Done
	if pending.Err != nil {
		t.Fatalf("Failed to propose command on leader side: %v", pending.Err)
	}

	// Isolate the leader with follower1.
	leader.transport.(*msgDropper).Set(follower1.config.ID, 1)
	follower1.transport.(*msgDropper).Set(leader.config.ID, 1)
	// Isolate the leader with follower2.
	leader.transport.(*msgDropper).Set(follower2.config.ID, 1)
	follower2.transport.(*msgDropper).Set(leader.config.ID, 1)

	// Propose a second command on the partitioned leader.
	pending = leader.Propose([]byte("I'm data2"))

	// Wait a new leader gets elected on the other side of the partition.
	var newLeader *Raft
	select {
	case <-follower1.fsm.(*testFSM).leaderCh:
		newLeader = follower1
	case <-follower2.fsm.(*testFSM).leaderCh:
		newLeader = follower2
	}

	// The partitioned leader should step down at some point and conclude the
	// command proposed after the network partition with 'ErrNotLeaderAnymore'.
	<-pending.Done
	if pending.Err != ErrNotLeaderAnymore {
		t.Fatalf("expected 'ErrNotLeaderAnymore' for the command proposed on partitioned leader")
	}

	// Propose a new command on the newly elected leader, it should succeed.
	pending = newLeader.Propose([]byte("I'm data3"))
	<-pending.Done
	if pending.Err != nil {
		t.Fatalf("Failed to propose on new leader side: %v", pending.Err)
	}

	// Reconnect old leader and previous follower 1.
	leader.transport.(*msgDropper).Set(follower1.config.ID, 0)
	follower1.transport.(*msgDropper).Set(leader.config.ID, 0)
	// Reconnect old leader and previous follower 2.
	leader.transport.(*msgDropper).Set(follower2.config.ID, 0)
	follower2.transport.(*msgDropper).Set(leader.config.ID, 0)

	// At some point the old leader should join the new quorum and gets synced
	// from the new leader.
	testEntriesEqual(
		leader.fsm.(*testFSM).appliedCh,
		newLeader.fsm.(*testFSM).appliedCh, 2,
	)
}

func TestRaftProposeWithTerm(t *testing.T) {
	ID1 := "1"
	ID2 := "2"
	clusterPrefix := "TestRaftProposeWithTerm"

	// Create n1 node.
	fsm1 := newTestFSM(ID1)
	n1 := testCreateRaftNode(getTestConfig(ID1, clusterPrefix+ID1), newStorage())
	// Create n2 node.
	fsm2 := newTestFSM(ID2)
	n2 := testCreateRaftNode(getTestConfig(ID2, clusterPrefix+ID2), newStorage())
	connectAllNodes(n1, n2)
	n1.Start(fsm1)
	n2.Start(fsm2)
	n2.ProposeInitialMembership([]string{ID1, ID2})

	// Find out who leader is.
	var leader *Raft
	select {
	case <-fsm1.leaderCh:
		leader = n1
	case <-fsm2.leaderCh:
		leader = n2
	}

	// Use a term number that current group is extremely unlikely to have.
	pending := leader.ProposeIfTerm([]byte("data"), 1000)
	<-pending.Done
	if pending.Err != ErrTermMismatch {
		t.Fatal("expected ErrTermMismatch if a command can't have a required term number")
	}
}

// Test Raft leader will lose its leadership after leader lease expires.
func TestRaftLeaderLeaseLost(t *testing.T) {
	ID1 := "1"
	ID2 := "2"
	clusterPrefix := "TestRaftLeaderLeaseLost"

	// Create n1 node.
	fsm1 := newTestFSM(ID1)
	cfg := getTestConfig(ID1, clusterPrefix+ID1)
	cfg.LeaderStepdownTimeout = cfg.FollowerTimeout
	n1 := testCreateRaftNode(cfg, newStorage())

	// Create n2 node.
	fsm2 := newTestFSM(ID2)
	cfg = getTestConfig(ID2, clusterPrefix+ID2)
	cfg.LeaderStepdownTimeout = cfg.FollowerTimeout
	n2 := testCreateRaftNode(cfg, newStorage())

	connectAllNodes(n1, n2)
	n1.transport = NewMsgDropper(n1.transport, 193, 0)
	n2.transport = NewMsgDropper(n2.transport, 42, 0)
	n1.Start(fsm1)
	n2.Start(fsm2)
	n2.ProposeInitialMembership([]string{ID1, ID2})

	// Find out who leader is.
	var leader *Raft
	select {
	case <-fsm1.leaderCh:
		leader = n1
	case <-fsm2.leaderCh:
		leader = n2
	}

	// Create a network partition between "1" and "2", so leader will lost its leader lease eventually.
	n1.transport.(*msgDropper).Set(ID2, 1)
	n2.transport.(*msgDropper).Set(ID1, 1)

	// The leader will lose its leadership eventually because it can't talk to
	// the other node.
	<-leader.fsm.(*testFSM).followerCh
}

// Test we can make progress with a one-node Raft cluster.
func TestRaftSingleNodeCommit(t *testing.T) {
	ID1 := "1"
	clusterPrefix := "TestRaftSingleNodeCommit"

	// Create the Raft node.
	fsm := newTestFSM(ID1)
	cfg := getTestConfig(ID1, clusterPrefix+ID1)
	cfg.LeaderStepdownTimeout = cfg.FollowerTimeout
	n := testCreateRaftNode(cfg, newStorage())
	n.Start(fsm)
	n.ProposeInitialMembership([]string{ID1})

	// Wait it becomes leader.
	<-fsm.leaderCh

	// Propose 10 commands.
	for i := 0; i < 10; i++ {
		n.Propose([]byte("I'm data-" + strconv.Itoa(i)))
	}

	// These 10 proposed entries should be applied eventually.
	for i := 0; i < 10; i++ {
		<-fsm.appliedCh
	}
}

// Test that "VeriyRead" still works under a single-node Raft cluster.
func TestRaftSingleNodeVerifyRead(t *testing.T) {
	ID1 := "1"
	clusterPrefix := "TestRaftSingleNodeVerifyRead"

	// Create the Raft node.
	fsm := newTestFSM(ID1)
	cfg := getTestConfig(ID1, clusterPrefix+ID1)
	cfg.LeaderStepdownTimeout = cfg.FollowerTimeout
	n := testCreateRaftNode(cfg, newStorage())
	n.Start(fsm)
	n.ProposeInitialMembership([]string{ID1})

	// Wait it becomes leader.
	<-fsm.leaderCh
	pending := n.VerifyRead()
	<-pending.Done
	if pending.Err != nil {
		log.Fatalf("Failed to do VerifyRead in single-node Raft cluster.")
	}
}

// Test adding a new node to a cluster.
func TestRaftAddOneNode(t *testing.T) {
	ID1 := "1"
	ID2 := "2"
	clusterPrefix := "TestRaftAddOneNode"

	// Create n1 node.
	fsm1 := newTestFSM(ID1)
	n1 := testCreateRaftNode(getTestConfig(ID1, clusterPrefix+ID1), newStorage())

	// Create n2 node.
	fsm2 := newTestFSM(ID2)
	n2 := testCreateRaftNode(getTestConfig(ID2, clusterPrefix+ID2), newStorage())
	connectAllNodes(n1, n2)

	// The cluster starts with only one node -- n1.
	n1.Start(fsm1)
	n1.ProposeInitialMembership([]string{ID1})

	// Wait it to be elected as leader.
	<-fsm1.leaderCh

	// Propose two commands to the cluster. Now the cluster only contains node n1.
	n1.Propose([]byte("data1"))
	pending := n1.Propose([]byte("data2"))
	<-pending.Done

	// Add node n2 to the cluster.
	pending = n1.AddNode(ID2)

	// The reconfiguration will be blocked until n2 starts. Because the
	// new configuration needs to be committed in new quorum
	select {
	case <-pending.Done:
		// The node might step down, in that case 'ErrNotLeaderAnymore' will be
		// returned.
		if pending.Err == nil {
			t.Fatalf("the proposed command should fail as the cluster doesn't have a quorum")
		}
	case <-time.After(10 * time.Millisecond):
	}

	// Start n2 as a joiner.
	n2.Start(fsm2)

	// Two FSMs should apply all 2 commands, eventually.
	if !testEntriesEqual(fsm1.appliedCh, fsm2.appliedCh, 2) {
		t.Fatal("two FSMs in same group applied different sequence of commands.")
	}
}

// Test removing an existing node from a cluster.
func TestRaftRemoveOneNode(t *testing.T) {
	ID1 := "1"
	ID2 := "2"
	clusterPrefix := "TestRaftRemoveOneNode"

	// Create n1 node.
	fsm1 := newTestFSM(ID1)
	n1 := testCreateRaftNode(getTestConfig(ID1, clusterPrefix+ID1), newStorage())

	// Create n2 node.
	fsm2 := newTestFSM(ID2)
	n2 := testCreateRaftNode(getTestConfig(ID2, clusterPrefix+ID2), newStorage())
	connectAllNodes(n1, n2)

	// Start the cluster with the node n1 and n2.
	n1.Start(fsm1)
	n2.Start(fsm2)
	n2.ProposeInitialMembership([]string{ID1, ID2})

	// Find out who leader is.
	var leader *Raft
	var follower *Raft
	select {
	case <-fsm1.leaderCh:
		leader = n1
		follower = n2
	case <-fsm2.leaderCh:
		leader = n2
		follower = n1
	}

	// Propose two commands to the cluster.
	leader.Propose([]byte("data1"))
	pending := leader.Propose([]byte("data2"))
	<-pending.Done
	// They should be applied in the same order.
	if !testEntriesEqual(fsm1.appliedCh, fsm2.appliedCh, 2) {
		t.Fatal("two FSMs in same group applied different sequence of commands.")
	}

	// Remove the follower from the cluster.
	pending = leader.RemoveNode(follower.config.ID)
	<-pending.Done
	if pending.Err != nil {
		t.Fatalf("fail to remove follower from the cluster: %v", pending.Err)
	}
}

// Test that once a leader is removed in a new configuration a new leader should
// be elected.
func TestRaftRemoveLeader(t *testing.T) {
	ID1 := "1"
	ID2 := "2"
	ID3 := "3"
	clusterPrefix := "TestRaftRemoveLeader"

	// Create n1 node.
	fsm1 := newTestFSM(ID1)
	n1 := testCreateRaftNode(getTestConfig(ID1, clusterPrefix+ID1), newStorage())
	// Create n2.
	fsm2 := newTestFSM(ID2)
	n2 := testCreateRaftNode(getTestConfig(ID2, clusterPrefix+ID2), newStorage())
	// Create n3.
	fsm3 := newTestFSM(ID3)
	n3 := testCreateRaftNode(getTestConfig(ID3, clusterPrefix+ID3), newStorage())

	connectAllNodes(n1, n2, n3)

	// The cluster starts with only one node -- n1.
	n1.Start(fsm1)
	n1.ProposeInitialMembership([]string{ID1})

	// Wait it becomes to leader.
	<-fsm1.leaderCh

	// Add n2 to the cluster.
	pending := n1.AddNode(ID2)
	n2.Start(fsm2)
	// Wait until n2 gets joined.
	<-pending.Done

	// Add n3 to the cluster.
	pending = n1.AddNode(ID3)
	n3.Start(fsm3)
	// Wait until n3 gets joined.
	<-pending.Done

	// Now we have a cluster with 3 nodes and n1 as the leader.

	// Remove the leader itself.
	n1.RemoveNode(ID1)

	// A new leader should be elected in new configuration(n2, n3).
	var newLeader *Raft
	select {
	case <-fsm2.leaderCh:
		newLeader = n2
	case <-fsm3.leaderCh:
		newLeader = n1
	}

	newLeader.Propose([]byte("data1"))
	newLeader.Propose([]byte("data2"))
	newLeader.Propose([]byte("data3"))

	// Now n2 and n3 should commit newly proposed entries eventually>
	if !testEntriesEqual(fsm2.appliedCh, fsm3.appliedCh, 3) {
		t.Fatal("two FSMs in same group applied different sequence of commands.")
	}
}
