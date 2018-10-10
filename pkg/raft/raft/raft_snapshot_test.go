// Copyright (c) 2016 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package raft

import (
	"bytes"
	"io"
	"io/ioutil"
	"math/rand"
	"sync"
	"testing"
	"time"

	log "github.com/golang/glog"
)

type testSnapshoter struct {
	state []byte
}

func (s *testSnapshoter) Save(writer io.Writer) error {
	log.Infof("Save is called")
	_, err := writer.Write(s.state)
	return err
}

func (s *testSnapshoter) Release() {
	log.Infof("Release is called")
}

func defaultSnapshoterFactory(state []byte) Snapshoter {
	// Make a copy of current state and the copy is the state that will be dumped
	// to the snapshot file.
	snapState := make([]byte, len(state))
	copy(snapState, state)
	return &testSnapshoter{state: snapState}
}

// testSnapshotFSM is a state machine implementation that is used to test
// snapshot implementation of Raft. Its state is simply a slice of bytes, and
// every time a new command is applied it will concatenate the newly applied
// byte slice to the existing one. When it's asked to perform snapshoting it
// will simply write its state(the byte slice) to the given writer and when
// it's asked to restore from a snasphot it will simply restore its state to
// the byte slice read from the given reader.
//
// Some random delays are injected on follower side so followers are likely
// to be lag far behind leader thus leader might need to use its snasphot
// to synchronize followers once a while.
type testSnapshotFSM struct {
	state             []byte
	leaderCh          chan struct{}
	isLeader          bool
	expectedLastIndex uint64
	rand              *rand.Rand

	lock      sync.Mutex // Protects 'lastIndex'.
	lastIndex uint64     // The last index that is included in the 'state'.

	snapshoterFactory func([]byte) Snapshoter
}

func newTestSnapshotFSM(snapshoterFactory func([]byte) Snapshoter) *testSnapshotFSM {
	return &testSnapshotFSM{
		leaderCh:          make(chan struct{}, 1),
		rand:              rand.New(rand.NewSource(time.Now().UnixNano())),
		state:             make([]byte, 0, 1000),
		snapshoterFactory: snapshoterFactory,
	}
}

func (f *testSnapshotFSM) Apply(ent Entry) interface{} {
	f.lock.Lock()
	f.state = append(f.state, ent.Cmd...)
	f.lastIndex = ent.Index
	f.lock.Unlock()

	if !f.isLeader {
		// Add some random delays to followers so they are likely to lag far
		// behind leader thus the leader needs to send snapshot to synchronize
		// the followers once a while.
		time.Sleep(time.Duration(rand.Int()%20) * time.Millisecond)
	}

	return ent.Index
}

func (f *testSnapshotFSM) OnLeadershipChange(b bool, term uint64, leader string) {
	if b {
		f.isLeader = true
		f.leaderCh <- struct{}{}
	}
}

func (f *testSnapshotFSM) OnMembershipChange(membership Membership) {}

func (f *testSnapshotFSM) Snapshot() (Snapshoter, error) {
	return f.snapshoterFactory(f.state), nil
}

func (f *testSnapshotFSM) SnapshotRestore(reader io.Reader, lastIndex, lastTerm uint64) {
	log.Infof("Restoring from snapshot...")
	f.lock.Lock()
	defer f.lock.Unlock()
	f.state, _ = ioutil.ReadAll(reader)
	f.lastIndex = lastIndex
}

// Block until the 'lastIndex' to be advanced at least to 'index'. Used to wait
// until a node has a sufficient up-to-date state.
func (f *testSnapshotFSM) waitLastAppliedIndex(index uint64) {
	for {
		f.lock.Lock()
		if f.lastIndex >= index {
			f.lock.Unlock()
			return
		}
		f.lock.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func getSnapTestConfig(ID, clusterID string) Config {
	return Config{
		ID:                      ID,
		ClusterID:               clusterID,
		FollowerTimeout:         10,
		CandidateTimeout:        10,
		HeartbeatTimeout:        3,
		RandomElectionRange:     6,
		DurationPerTick:         100 * time.Millisecond,
		MaxNumEntsPerAppEnts:    10,
		SnapshotThreshold:       20, // Make the threshold small for testing.
		LogEntriesAfterSnapshot: 5,  // Keep the last 5 entries of snapshot in log.
		MaximumProposalBatch:    1,
	}
}

// Start 3 nodes from scratch and proposing commands. You should observe that
// leader will synchronize followers once a while and each node will also take
// snapshot independently.
func TestRaftSnapshotPropose(t *testing.T) {
	ID1 := "1"
	ID2 := "2"
	ID3 := "3"
	clusterPrefix := "TestRaftSnapshotPropose"

	fsm1 := newTestSnapshotFSM(defaultSnapshoterFactory)
	n1 := testCreateRaftNode(getSnapTestConfig(ID1, clusterPrefix+ID1), newStorage())

	fsm2 := newTestSnapshotFSM(defaultSnapshoterFactory)
	n2 := testCreateRaftNode(getSnapTestConfig(ID2, clusterPrefix+ID2), newStorage())

	fsm3 := newTestSnapshotFSM(defaultSnapshoterFactory)
	n3 := testCreateRaftNode(getSnapTestConfig(ID3, clusterPrefix+ID3), newStorage())

	connectAllNodes(n1, n2, n3)
	n1.Start(fsm1)
	n2.Start(fsm2)
	n3.Start(fsm3)
	n3.ProposeInitialMembership([]string{ID1, ID2, ID3})

	var leader *Raft // The leader node
	var f1, f2 *Raft // Two follower nodes
	select {
	case <-fsm1.leaderCh:
		leader = n1
		f1 = n2
		f2 = n3
	case <-fsm2.leaderCh:
		leader = n2
		f1 = n1
		f2 = n3
	case <-fsm3.leaderCh:
		leader = n3
		f1 = n1
		f2 = n2
	}

	for i := 0; i < 500; i++ {
		leader.Propose([]byte{byte(i % 256), byte((i + 1) % 256), byte((i + 2) % 256)})
	}

	pending := leader.Propose(nil)
	<-pending.Done
	if pending.Err != nil {
		t.Fatalf("Propose on leader side should not fail: %v", pending.Err)
	}

	// Get the raft index of the last proposed command.
	lastIndex := pending.Res.(uint64)
	// And wait until the two follower nodes to apply the last command.
	f1.fsm.(*testSnapshotFSM).waitLastAppliedIndex(lastIndex)
	f2.fsm.(*testSnapshotFSM).waitLastAppliedIndex(lastIndex)

	// Verify consistency of the states.
	if bytes.Compare(leader.fsm.(*testSnapshotFSM).state, f1.fsm.(*testSnapshotFSM).state) != 0 ||
		bytes.Compare(f2.fsm.(*testSnapshotFSM).state, f1.fsm.(*testSnapshotFSM).state) != 0 {
		t.Fatalf("Inconsistent state detected in the end!")
	}
}

// Test that a Raft node detects a snapshot when it starts, it should recover
// its state from the snapshot.
func TestRaftSnapshotRecovery(t *testing.T) {
	ID1 := "1"
	clusterPrefix := "TestRaftSnapshotRecovery"

	snapMgr := NewMemSnapshotMgr()
	snapMgr.(*memSnapshotMgr).snapData = []byte{1, 2, 3}
	snapMgr.(*memSnapshotMgr).snapMeta = SnapshotMetadata{LastIndex: 10, LastTerm: 1,
		Membership: &Membership{Index: 1, Term: 1, Members: []string{"1"}}}

	storage := NewStorage(snapMgr, NewMemLog(), NewMemState(0))
	fsm := newTestSnapshotFSM(defaultSnapshoterFactory)
	n := testCreateRaftNode(getSnapTestConfig(ID1, clusterPrefix), storage)
	n.Start(fsm)

	fsm.waitLastAppliedIndex(10)
	if bytes.Compare(fsm.state, []byte{1, 2, 3}) != 0 {
		t.Fatalf("Failed to recover to expected state.")
	}
}

// Similar to testSnapshoter, but simulates a long-time taking snapshot by
// sleeping in "Save" callback for a long time.
type testSlowSnapshoter struct {
	state []byte
}

func (s *testSlowSnapshoter) Save(writer io.Writer) error {
	log.Infof("Save is called")
	// Simulate long-time snapshot by sleeping 20 seconds...
	time.Sleep(20 * time.Second)
	_, err := writer.Write(s.state)
	return err
}

func (s *testSlowSnapshoter) Release() {
	log.Infof("Release is called")
}

func slowSnapshoterFactory(state []byte) Snapshoter {
	// Make a copy of current state and the copy is the state that will be dumped
	// to the snapshot file.
	snapState := make([]byte, len(state))
	copy(snapState, state)
	return &testSlowSnapshoter{state: snapState}
}

// Tests that if snapshot process and commands applications happen concurrently
// then a long-time snapshoter should not block the commands applications.
func TestRaftSnapshotConcurrency(t *testing.T) {
	ID1 := "1"
	clusterPrefix := "TestRaftSnapshotConcurrency"

	fsm := newTestSnapshotFSM(slowSnapshoterFactory) // Use long-time snapshoter.
	n := testCreateRaftNode(getSnapTestConfig(ID1, clusterPrefix), newStorage())
	n.Start(fsm)
	n.ProposeInitialMembership([]string{"1"})

	<-fsm.leaderCh

	// Propsing "SnapshotThreshold" commands to trigger the long-time snapshot.
	var pending *Pending
	for i := 0; i < int(n.config.SnapshotThreshold); i++ {
		pending = n.Propose([]byte{byte(i % 256)})
	}

	<-pending.Done
	if pending.Err != nil {
		log.Fatalf("Proposing on leader node should not fail: %v", pending.Err)
	}

	// Propose one more, this command should be applied without being blocked
	// by the snapshoter because they happen concurrently.
	pending = n.Propose([]byte{0})
	select {
	case <-pending.Done:
		if pending.Err != nil {
			t.Fatalf("Proposing on leader node should not fail: %v", pending.Err)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("Command application should not be blocked by long-time snapshot.")
	}
}
