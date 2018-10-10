// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package raft

import (
	"bytes"
	"io/ioutil"
	"reflect"
	"testing"
)

func entryFromCmd(cmd []byte) Entry {
	return Entry{Type: EntryNormal, Cmd: cmd}
}

// Given the logs of leader and follower, returns true if it's a legal synced
// state, otherwise returns false.
// Follower might contain some extra entries since these will not be truncated
// immediately.
func isLegalSyncedState(leader, follower Log) bool {
	followerFi, _ := follower.FirstIndex()
	leaderFi, _ := leader.FirstIndex()
	if leaderFi != followerFi {
		return false
	}
	followerLi, _ := follower.LastIndex()
	leaderLi, _ := leader.LastIndex()
	if leaderLi > followerLi {
		return false
	}

	leaderEnts := leader.Entries(leaderFi, leaderLi+1)
	followerEnts := follower.Entries(followerFi, followerLi+1)
	return reflect.DeepEqual(leaderEnts, followerEnts[:len(leaderEnts)])
}

func getAllEntries(log Log) []Entry {
	fi, li, _ := log.GetBound()
	ents := log.Entries(fi, li+1)
	return ents
}

// Test Raft state transitions between follower, leader and candidate.
func TestCoreStateTransitions(t *testing.T) {
	cfg := Config{
		ID:                   "1",
		FollowerTimeout:      5,
		CandidateTimeout:     3,
		MaxNumEntsPerAppEnts: 1000,
	}

	r := newCore(cfg, newStorage())
	r.proposeInitialMembership(Membership{Members: []string{"1", "2", "3"}, Epoch: 54321})

	// It should start as a follower.
	if r.state.name() != stateFollower {
		t.Fatal("expected Raft to start as a follower")
	}

	// Tick 5 times so follower will timeout, trigger the transition to candidate.
	r.Tick()
	r.Tick()
	r.Tick()
	r.Tick()
	r.Tick()

	if r.state.name() != stateCandidate {
		t.Fatal("expected Raft to become as a candidate")
	}

	msgs := r.TakeAllMsgs()
	// Node 1 should broadcast 'VoteReq' to nodes "2" and "3".
	if len(msgs) != 2 {
		t.Fatal("expected to receive 2 'VoteReq' once Raft becomes a candidate")
	}
	if r.storage.GetCurrentTerm() != 2 {
		t.Fatal("expected candidate to bump its term after it votes for itself.")
	}
	msgs = r.TakeAllMsgs()
	if len(msgs) != 0 {
		t.Fatal("expected no messages to sent without any event")
	}

	// Tick 3 times to trigger the re-election of candidate.
	r.Tick()
	r.Tick()
	r.Tick()
	msgs = r.TakeAllMsgs()
	// Node 1 should broadcast 'VoteReq' to nodes "2" and "3" again with a higher term.
	if len(msgs) != 2 {
		t.Fatal("expected to receive 2 'VoteReq' when candidate re-campaigns for leader")
	}
	if r.storage.GetCurrentTerm() != 3 {
		t.Fatal("expected candidate to bump its term agian when it re-campaigns for leader.")
	}

	// Let node "2" grant a vote to canddiate "1", so node "1" will have votes from a
	// quorum.
	vr := &VoteResp{
		BaseMsg: BaseMsg{Term: 3, From: "2"},
		Granted: true,
	}
	r.HandleMsg(vr)
	msgs = r.TakeAllMsgs()
	// Now it should be elected as leader.
	if r.state.name() != stateLeader {
		t.Fatal("expected candidate to be elected as leader")
	}
	// Once it's elected as leader, it should broadcast 'AppEnts' messages as
	// probing and heartbeat message.
	if len(msgs) != 2 {
		t.Fatal("expected to receive 2 'AppEnts' from a leader once a it's elected.")
	}

	// When a node received a message with a higher term, it should transfer to
	// the follower state. Now the node has term 3, we'll send a message with term 4.
	msg := &AppEnts{
		BaseMsg: BaseMsg{Term: 4, From: "2", To: "1"},
	}
	r.HandleMsg(msg)

	if r.state.name() != stateFollower {
		t.Fatal("expected Raft to become as a follower")
	}
}

// Test whether follower can grant vote to a candidate or not based on its own
// state and candidate's state.
func TestCoreGrantVote(t *testing.T) {
	tests := []struct {
		// Voter's state.
		voterLogEnts []Entry
		voterTerm    uint64
		voterVoteFor string
		// Candidate's state
		candidateTerm     uint64
		candidateLogTerm  uint64
		candidateLogIndex uint64
		// Expected result.
		granted bool
	}{
		// Voter's State:     [log: ..., (1, 2)], [term: 1, voteFor: "1"]
		// Candidate's State: [log: ..., (1, 3)], [term: 2]
		//
		// Candidate's log history is "better" than voter's log history and the
		// candidate has a higher term, the vote should be granted.
		{
			// Voter's state
			voterLogEnts: []Entry{Entry{Term: 1, Index: 2}},
			voterTerm:    1,
			voterVoteFor: "1",
			// Candidate's state.
			candidateTerm:     2,
			candidateLogTerm:  1,
			candidateLogIndex: 3,
			// Expected result.
			granted: true,
		},

		// Voter's State:     [log: ..., (1, 2)], [term: 1, voteFor: "1"]
		// Candidate's State: [log: ..., (1, 1)], [term: 2]
		//
		// Candidate has a higher term but its  log history is not as "up-to-dated" as the
		// voter's, the vote should be rejected.
		{
			// Voter's state
			voterLogEnts: []Entry{Entry{Term: 1, Index: 2}},
			voterTerm:    1,
			voterVoteFor: "1",
			// Candidate's state.
			candidateTerm:     2,
			candidateLogTerm:  1,
			candidateLogIndex: 1,
			// Expected result.
			granted: false,
		},

		// Voter's State:     [log: ..., (1, 2)], [term: 1, voteFor: "1"]
		// Candidate's State: [log: ..., (1, 2)], [term: 2]
		//
		// Candidate has a higher term and its log history is as "up-to-dated" as the
		// voter's, the vote should be granted.
		{
			// Voter's state
			voterLogEnts: []Entry{Entry{Term: 1, Index: 2}},
			voterTerm:    1,
			voterVoteFor: "1",
			// Candidate's state.
			candidateTerm:     2,
			candidateLogTerm:  1,
			candidateLogIndex: 2,
			// Expected result.
			granted: true,
		},

		// Voter's State:     [log: ..., (1, 2)], [term: 2, voteFor: "1"]
		// Candidate's State: [log: ..., (1, 3)], [term: 2]
		//
		// Candidate's log history is "better" than voter's log history but they have
		// the same term numbers and the voter has granted the vote to someone else,
		// the vote should be rejected.
		{
			// Voter's state
			voterLogEnts: []Entry{Entry{Term: 1, Index: 2}},
			voterTerm:    2,
			voterVoteFor: "1",
			// Candidate's state.
			candidateTerm:     2,
			candidateLogTerm:  1,
			candidateLogIndex: 3,
			// Expected result.
			granted: false,
		},

		// Voter's State:     [log: ..., (1, 2)], [term: 2, voteFor: "2"]
		// Candidate's State: [log: ..., (1, 2)], [term: 2]
		//
		// Candidate's log history is as up-to-dated as voter's log history and they
		// have the same term numbers, but the voter has granted the vote to this
		// candidate before, the vote should still be granted.
		{
			// Voter's state
			voterLogEnts: []Entry{Entry{Term: 1, Index: 2}},
			voterTerm:    2,
			voterVoteFor: "2",
			// Candidate's state.
			candidateTerm:     2,
			candidateLogTerm:  1,
			candidateLogIndex: 3,
			// Expected result.
			granted: true,
		},
	}

	for _, test := range tests {
		t.Log("running test:", test)
		config := Config{
			ID:                   "1",
			MaxNumEntsPerAppEnts: 1000,
		}

		c := newCore(config, newStorage())
		c.proposeInitialMembership(Membership{Members: []string{"1", "2"}, Epoch: 54321})
		// Initialize on disk state.
		c.storage.log.Append(test.voterLogEnts...)
		c.storage.SaveState(test.voterVoteFor, test.voterTerm)

		// Send a 'VoteReq' message to voter.
		msg := &VoteReq{
			BaseMsg:      BaseMsg{To: "1", From: "2", Term: test.candidateTerm},
			LastLogIndex: test.candidateLogIndex,
			LastLogTerm:  test.candidateLogTerm,
		}
		c.HandleMsg(msg)

		msgs := c.TakeAllMsgs()
		if len(msgs) != 1 {
			t.Fatal("expected to get one 'VoteResp' message")
		}
		// Verify the result.
		vr := msgs[0].(*VoteResp)
		if vr.Granted != test.granted {
			t.Fatalf("expected to get vote reply %t, but got %t", test.granted, vr.Granted)
		}
	}
}

// Test whether candidate can be elected as a leader or not based on votes it
// received.
func TestCoreLeaderElection(t *testing.T) {
	tests := []struct {
		peers     []string
		voteResps []Msg
		result    bool
	}{

		// "1" : granted (candidate)
		// "2" : no response
		// "3" : no response
		//
		// result: no elected
		{
			peers:     []string{"1", "2", "3"},
			voteResps: []Msg{},
			result:    false,
		},

		// "1" : granted (candidate)
		// "2" : granted
		// "3" : no response
		//
		// result: elected
		{
			peers: []string{"1", "2", "3"},
			voteResps: []Msg{
				&VoteResp{BaseMsg{From: "2"}, true},
			},
			result: true,
		},

		// "1" : granted (candidate)
		// "2" : granted
		// "3" : rejected
		//
		// result: elected
		{
			peers: []string{"1", "2", "3"},
			voteResps: []Msg{
				&VoteResp{BaseMsg{From: "2"}, true},
				&VoteResp{BaseMsg{From: "3"}, false},
			},
			result: true,
		},

		// "1" : granted (candidate)
		// "2" : granted
		// "3" : rejected
		// "4" : no response
		// "5" : no response
		//
		// result: not elected
		{
			peers: []string{"1", "2", "3", "4", "5"},
			voteResps: []Msg{
				&VoteResp{BaseMsg{From: "2"}, true},
				&VoteResp{BaseMsg{From: "3"}, false},
			},
			result: false,
		},

		// "1" : granted (candidate)
		// "2" : granted
		// "3" : rejected
		// "4" : granted
		// "5" : rejected
		//
		// result: elected
		{
			peers: []string{"1", "2", "3", "4", "5"},
			voteResps: []Msg{
				&VoteResp{BaseMsg{From: "2"}, true},
				&VoteResp{BaseMsg{From: "3"}, false},
				&VoteResp{BaseMsg{From: "4"}, true},
				&VoteResp{BaseMsg{From: "5"}, false},
			},
			result: true,
		},

		// "1" : granted (candidate)
		// "2" : granted twice
		// "3" : no response
		// "4" : no response
		// "5" : no response
		//
		// result: not elected
		{
			peers: []string{"1", "2", "3", "4", "5"},
			voteResps: []Msg{
				&VoteResp{BaseMsg{From: "2"}, true},
				&VoteResp{BaseMsg{From: "2"}, true},
			},
			result: false,
		},
	}

	for _, test := range tests {
		cfg := Config{
			ID:                   test.peers[0],
			MaxNumEntsPerAppEnts: 1000,
		}
		c := newCore(cfg, newStorage())
		c.proposeInitialMembership(Membership{Members: test.peers, Epoch: 54321})
		// Force it change to the candidate state.
		c.changeState(c.candidate, "")

		for _, resp := range test.voteResps {
			resp.SetTerm(c.storage.GetCurrentTerm())
			resp.SetTo(c.ID)
			c.HandleMsg(resp)
		}

		if c.state.name() == stateLeader && !test.result {
			t.Fatal("the node shouldn't be elected as leader")
		}
		if c.state.name() != stateLeader && test.result {
			t.Fatal("the node should be elected as leader")
		}
	}
}

// Test whether followers behave correctly based on leader's probing messages.
// We'll simulate leader side and keep probing follower's log until find the
// last agreed index.
func TestCoreFollowerProbing(t *testing.T) {
	tests := []struct {
		// Entries in leader's log.
		leaderLog []Entry
		// Entries in follower's log.
		followerLog []Entry
		// The index of last agreed entry.
		lastAgreedIndex uint64
	}{

		// follower and leader have the same log history.
		//
		// L: (1, 1), (1, 2)
		// F: (1, 1), (1, 2)
		//               |
		//             agreed
		{
			[]Entry{Entry{Term: 1, Index: 1}, Entry{Term: 1, Index: 2}},
			[]Entry{Entry{Term: 1, Index: 1}, Entry{Term: 1, Index: 2}},
			2,
		},

		// follower's log history is a prefix of leader's.
		//
		// L: (1, 1), (1, 2), (1, 3)
		// F: (1, 1), (1, 2)
		//               |
		//             agreed
		{
			[]Entry{Entry{Term: 1, Index: 1}, Entry{Term: 1, Index: 2}, Entry{Term: 1, Index: 3}},
			[]Entry{Entry{Term: 1, Index: 1}, Entry{Term: 1, Index: 2}},
			2,
		},

		// leader's log history is a prefix of follower's.
		//
		// L: (1, 1),
		// F: (1, 1), (1, 2)
		//       |
		//     agreed
		{
			[]Entry{Entry{Term: 1, Index: 1}},
			[]Entry{Entry{Term: 1, Index: 1}, Entry{Term: 1, Index: 2}},
			1,
		},

		// The logs between leader and follower diverged at some point.
		//
		// L: (1, 1), (1, 2), (1, 3)
		// F: (1, 1), (2, 2)
		//      |
		//    agreed
		{
			[]Entry{Entry{Term: 1, Index: 1}, Entry{Term: 1, Index: 2}, Entry{Term: 1, Index: 3}},
			[]Entry{Entry{Term: 1, Index: 1}, Entry{Term: 2, Index: 2}},
			1,
		},

		// The logs between leader and follower are completely different.
		//
		// NOTE: entry (0, 0) doesn't exist in logs physically, it's an imaginary entry.
		//
		// L: [(0, 0)] (2, 1), (2, 2)
		// F: [(0, 0)] (1, 1), (1, 2)
		//       |
		//     agreed
		{
			[]Entry{Entry{Term: 0, Index: 0}, Entry{Term: 2, Index: 1}, Entry{Term: 2, Index: 2}},
			[]Entry{Entry{Term: 1, Index: 1}, Entry{Term: 1, Index: 2}},
			0,
		},
	}

	for _, test := range tests {
		t.Log("running test:", test)
		cfg := Config{
			ID:                   "1",
			MaxNumEntsPerAppEnts: 1000,
		}
		follower := newCore(cfg, newStorage())
		follower.storage.log.Append(test.followerLog...)

		for i := len(test.leaderLog) - 1; i >= 0; i-- {
			// From the last entry in leader's log, we'll keep probing until the specified
			// last agreed index.

			// Get the entry with index 'i'.
			ent := test.leaderLog[i]
			msg := &AppEnts{
				BaseMsg:      BaseMsg{From: "2", Term: follower.storage.GetCurrentTerm()},
				PrevLogTerm:  ent.Term,
				PrevLogIndex: ent.Index,
				Entries:      nil,
			}
			follower.HandleMsg(msg)
			resp := follower.TakeAllMsgs()[0].(*AppEntsResp)

			if ent.Index > test.lastAgreedIndex {
				// We haven't reached the last agreed index yet, this 'AppEnts' should be rejected.
				if resp.Success {
					t.Fatal("expected the AppEnts to be rejected by follower on index > last agreed index")
				}
				// The response should include the rejected index.
				if resp.Index != ent.Index {
					t.Fatal("expected the index of AppEntsResp to be the 'PrevLogIndex' of AppEnts")
				}
				continue
			}

			if ent.Index == test.lastAgreedIndex {
				// Now we have reached the last agreed index, the request should be accepted.
				if !resp.Success {
					t.Fatal("expected the AppEnts to be accepted by follower on last agreed index")
				}
				if resp.Index != test.lastAgreedIndex {
					t.Fatal("expected the received index of AppEntsResp to be the PrevLogIndex of AppEnts")
				}
				// Done with this test.
				break
			}
		}
	}
}

// Create a follower core and leader core, initialize them with different log
// state and let them talk with each other. Finally they should end up in the same
// state.
func TestCoreSynchronization(t *testing.T) {
	tests := []struct {
		// Entries in leader's log.
		leaderLog []Entry
		// Entries in follower's log.
		followerLog []Entry
	}{

		// NOTE entry (1, 1) will be the reconfiguration entry and it will be appended
		// automatically.

		// follower and leader have the same log history.
		//
		//
		// L: (1, 1), (1, 2)
		// F: (1, 1), (1, 2)
		//
		{
			[]Entry{Entry{Term: 1, Index: 2}},
			[]Entry{Entry{Term: 1, Index: 2}},
		},

		// follower's log history is a prefix of leader's.
		//
		// L: (1, 1), (1, 2), (1, 3)
		// F: (1, 1), (1, 2)
		//
		{
			[]Entry{Entry{Term: 1, Index: 2}, Entry{Term: 1, Index: 3}},
			[]Entry{Entry{Term: 1, Index: 2}},
		},

		// leader's log history is a prefix of follower's.
		//
		// L: (1, 1),
		// F: (1, 1), (1, 2)
		//
		{
			[]Entry{},
			[]Entry{Entry{Term: 1, Index: 2}},
		},

		// The logs between leader and follower diverged at some point.
		//
		// L: (1, 1), (1, 2), (1, 3)
		// F: (1, 1), (2, 2)
		//
		{
			[]Entry{Entry{Term: 1, Index: 2}, Entry{Term: 1, Index: 3}},
			[]Entry{Entry{Term: 2, Index: 2}},
		},
	}

	for _, test := range tests {
		t.Log("running test:", test)
		// Create leader core.
		cfg := Config{
			ID:                   "1",
			HeartbeatTimeout:     3,
			MaxNumEntsPerAppEnts: 1000,
		}
		leader := newCore(cfg, newStorage())
		leader.proposeInitialMembership(Membership{Members: []string{"1", "2"}, Epoch: 54321})
		leader.storage.log.Append(test.leaderLog...)

		// Create follower core.
		cfg = Config{ID: "2"}
		follower := newCore(cfg, newStorage())
		follower.proposeInitialMembership(Membership{Members: []string{"1", "2"}, Epoch: 54321})
		follower.storage.log.Append(test.followerLog...)

		// Force it become a leader.
		leader.changeState(leader.leader, leader.ID)
		fpeer := leader.state.(*coreLeader).peers["2"]

		// last index of leader.
		li := leader.storage.lastIndex()
		// Check whether we initialize 'matchIndex' and 'nextIndex' in a
		// correct way.
		if fpeer.matchIndex != 0 || fpeer.nextIndex != li+1 {
			t.Fatalf("incorrect initial state of peer")
		}

		// Let follower and leader talk with each other until the follower is fully
		// synced.
		for {
			// Leader should send a request.
			msgs := leader.TakeAllMsgs()
			if len(msgs) != 1 {
				t.Fatalf("expected leader to send a request.")
			}
			// Give it to follower.
			follower.HandleMsg(msgs[0])
			msgs = follower.TakeAllMsgs()
			if len(msgs) != 1 {
				t.Fatalf("expected follower to respond request sent from leader")
			}
			// Give response to leader.
			leader.HandleMsg(msgs[0])
			fpeer := leader.state.(*coreLeader).peers["2"]

			if fpeer.matchIndex == li {
				// Now the follower is fully synced.
				break
			}
		}

		if !isLegalSyncedState(leader.storage.log, follower.storage.log) {
			t.Fatal("The logs of leader and follower are not in a legal synced state.")
		}
	}
}

// Test whether follower handles AppEnts request in an expected way.
func TestCoreHandleAppEnts(t *testing.T) {
	// The first entry should be a configuration entry.
	confEntry := newConfEntry(1, 1, []string{"follower", "leader"}, 54321)
	tests := []struct {
		// Follower's log state.
		followerLog []Entry
		// The AppEnts request sent to follower.
		appEnts *AppEnts
		// Expected AppEntsResp response.
		expectedResp *AppEntsResp
		// Expected log state after processing the request.
		expectedLog []Entry
	}{

		// Simulate sending a normal replicating message, send entries (1, 3), (1, 4) with
		// (1, 2) as consistency check to the follower. This should be one of the most
		// frequent case in Raft.
		//
		//		F: (1, 1), (1, 2)
		//
		// after processing the request:
		//
		//		F: (1, 1), (1, 2), (1, 3), (1, 4)
		//
		{
			followerLog: []Entry{confEntry, Entry{Term: 1, Index: 2}},
			appEnts: &AppEnts{
				BaseMsg:      BaseMsg{Term: 1},
				PrevLogIndex: 2,
				PrevLogTerm:  1,
				Entries:      []Entry{Entry{Term: 1, Index: 3}, Entry{Term: 1, Index: 4}},
			},
			expectedResp: &AppEntsResp{
				BaseMsg: BaseMsg{Term: 1, From: "follower"},
				Success: true,
				Index:   4,
			},
			expectedLog: []Entry{confEntry, Entry{Term: 1, Index: 2}, Entry{Term: 1, Index: 3}, Entry{Term: 1, Index: 4}},
		},

		// Simulate sending a probing message, send probing message with (1, 1) as
		// consistency check. This request will be accepted.
		//
		// Follower can't truncate its log at this time!
		//
		//		F: (1, 1), (1, 2)
		//
		// after processing the request:
		//
		//		F: (1, 1), (1, 2),
		//
		{
			followerLog: []Entry{confEntry, Entry{Term: 1, Index: 2}},
			appEnts: &AppEnts{
				BaseMsg:      BaseMsg{Term: 1},
				PrevLogIndex: 1,
				PrevLogTerm:  1,
				Entries:      nil,
			},
			expectedResp: &AppEntsResp{
				BaseMsg: BaseMsg{Term: 1, From: "follower"},
				Success: true,
				Index:   1,
			},
			expectedLog: []Entry{confEntry, Entry{Term: 1, Index: 2}},
		},

		// Simulate sending a probing message, send probing message with (2, 1) as
		// consistency check. This request will be rejected.
		//
		//
		//		F: (1, 1), (2, 2)
		//
		// after processing the request:
		//
		//		F: (1, 1), (2, 2),
		//
		{
			followerLog: []Entry{confEntry, Entry{Term: 2, Index: 2}},
			appEnts: &AppEnts{
				BaseMsg:      BaseMsg{Term: 1},
				PrevLogIndex: 1,
				PrevLogTerm:  2,
				Entries:      nil,
			},
			expectedResp: &AppEntsResp{
				BaseMsg: BaseMsg{Term: 1, From: "follower"},
				Success: false,
				Index:   1,
			},
			expectedLog: []Entry{confEntry, Entry{Term: 2, Index: 2}},
		},

		// Simulate sending a duplicate replicating message (1, 1), (1, 2) with (0, 0)
		// as consistency check.
		//
		//		F: (1, 1), (1, 2)
		//
		// after processing the request:
		//
		//		F: (1, 1), (1, 2),
		//
		{
			followerLog: []Entry{confEntry, Entry{Term: 1, Index: 2}},
			appEnts: &AppEnts{
				BaseMsg:      BaseMsg{Term: 1},
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries:      []Entry{confEntry, Entry{Term: 1, Index: 2}},
			},
			expectedResp: &AppEntsResp{
				BaseMsg: BaseMsg{Term: 1, From: "follower"},
				Success: true,
				Index:   2,
			},
			expectedLog: []Entry{confEntry, Entry{Term: 1, Index: 2}},
		},

		// Simulate sending a stale replicating message (1, 1), (1, 2) with (0, 0)
		// as consistency check.
		//
		//		F: (1, 1), (1, 2), (1, 3)
		//
		// after processing the request:
		//
		//		F: (1, 1), (1, 2), (1, 3)
		//
		{
			followerLog: []Entry{confEntry, Entry{Term: 1, Index: 2}, Entry{Term: 1, Index: 3}},
			appEnts: &AppEnts{
				BaseMsg:      BaseMsg{Term: 1},
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries:      []Entry{confEntry, Entry{Term: 1, Index: 2}},
			},
			expectedResp: &AppEntsResp{
				BaseMsg: BaseMsg{Term: 1, From: "follower"},
				Success: true,
				Index:   2,
			},
			expectedLog: []Entry{confEntry, Entry{Term: 1, Index: 2}, Entry{Term: 1, Index: 3}},
		},

		// Simulate sending a replicating message to overwrite follower's whole log,
		// sending (2, 1), (2, 2) with (0, 0) as consistency check.
		//
		//		F: (1, 1), (1, 2), (1, 3)
		//
		// after procesing the request:
		//
		//		F: (2, 1), (2, 2)
		//
		{
			followerLog: []Entry{confEntry, Entry{Term: 1, Index: 2}, Entry{Term: 1, Index: 3}},
			appEnts: &AppEnts{
				BaseMsg:      BaseMsg{Term: 1},
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries:      []Entry{Entry{Term: 2, Index: 1}, Entry{Term: 2, Index: 2}},
			},
			expectedResp: &AppEntsResp{
				BaseMsg: BaseMsg{Term: 1, From: "follower"},
				Success: true,
				Index:   2,
			},
			expectedLog: []Entry{Entry{Term: 2, Index: 1}, Entry{Term: 2, Index: 2}},
		},

		// Simulate sending a replicating message to overwrite part of follower's log with
		// all entries sent in AppEnts,
		// sending (2, 3), (2, 4) with (1, 2) as consistency check.
		//
		//		F: (1, 1), (1, 2), (1, 3)
		//
		// after procesing the request:
		//
		//		F: (1, 1), (1, 2), (2, 3), (2, 4)
		//
		{
			followerLog: []Entry{confEntry, Entry{Term: 1, Index: 2}, Entry{Term: 1, Index: 3}},
			appEnts: &AppEnts{
				BaseMsg:      BaseMsg{Term: 1},
				PrevLogIndex: 2,
				PrevLogTerm:  1,
				Entries:      []Entry{Entry{Term: 2, Index: 3}, Entry{Term: 2, Index: 4}},
			},
			expectedResp: &AppEntsResp{
				BaseMsg: BaseMsg{Term: 1, From: "follower"},
				Success: true,
				Index:   4,
			},
			expectedLog: []Entry{confEntry, Entry{Term: 1, Index: 2}, Entry{Term: 2, Index: 3}, Entry{Term: 2, Index: 4}},
		},

		// Simulate sending a replicating message to overwrite part of follower's log with
		// part of entries sent in AppEnts,
		// sending (1, 2), (2, 3), (2, 4) with (1, 1) as consistency check.
		//
		//		F: (1, 1), (1, 2), (1, 3)
		//
		// after procesing the request:
		//
		//		F: (1, 1), (1, 2), (2, 3), (2, 4)
		//
		{
			followerLog: []Entry{confEntry, Entry{Term: 1, Index: 2}, Entry{Term: 1, Index: 3}},
			appEnts: &AppEnts{
				BaseMsg:      BaseMsg{Term: 1},
				PrevLogIndex: 1,
				PrevLogTerm:  1,
				Entries:      []Entry{Entry{Term: 1, Index: 2}, Entry{Term: 2, Index: 3}, Entry{Term: 2, Index: 4}},
			},
			expectedResp: &AppEntsResp{
				BaseMsg: BaseMsg{Term: 1, From: "follower"},
				Success: true,
				Index:   4,
			},
			expectedLog: []Entry{confEntry, Entry{Term: 1, Index: 2}, Entry{Term: 2, Index: 3}, Entry{Term: 2, Index: 4}},
		},
	}

	for _, test := range tests {
		// Create follower core.
		storage := NewStorage(NewMemSnapshotMgr(), initLog(test.followerLog...), NewMemState(1))
		cfg := Config{
			ID:                   "follower",
			MaxNumEntsPerAppEnts: 1000,
		}
		follower := newCore(cfg, storage)

		follower.HandleMsg(test.appEnts)
		msgs := follower.TakeAllMsgs()
		if len(msgs) != 1 {
			t.Fatalf("expected to get an AppEntsResp from follower.")
		}

		resp := msgs[0].(*AppEntsResp)
		// Verify whether we got expected response.
		if !reflect.DeepEqual(resp, test.expectedResp) {
			t.Fatalf("expected to get resp %v, but got: %v", test.expectedResp, resp)
		}

		ents := getAllEntries(follower.storage.log)
		// Verify whether follower's log state is expected after processing the AppEnts request.
		if !reflect.DeepEqual(ents, test.expectedLog) {
			t.Fatalf("the log state after processing AppEnts request is not expected")
		}
	}
}

// Test whether leader sends out heartbeat messages or retransmit lost messages
// after certain timeout.
func TestCoreHeartbeatOrRetransmission(t *testing.T) {
	cfg := Config{
		ID:                   "1",
		HeartbeatTimeout:     3, // Timeout is 3.
		MaxNumEntsPerAppEnts: 1000,
	}

	c := newCore(cfg, newStorage())
	c.proposeInitialMembership(Membership{Members: []string{"1", "2", "3"}, Epoch: 54321})
	c.changeState(c.leader, c.ID)

	msgs := c.TakeAllMsgs()
	if len(msgs) != 2 {
		// Leader should broadcast probing message to "1" and "2"
		t.Fatalf("expected leader to broadcast 2 probing messages once it's elected.")
	}

	// Tick once.
	c.Tick()
	msgs = c.TakeAllMsgs()
	if len(msgs) != 0 {
		t.Fatalf("leader shouldn't send AppEnts before timeout.")
	}
	// Tick twice.
	c.Tick()
	c.Tick()

	// Since heartbeat timeout is 3, leader should send AppEnts message again.
	msgs = c.TakeAllMsgs()
	if len(msgs) != 2 {
		// Leader should broadcast probing message to "1" and "2"
		t.Fatalf("expected leader to broadcast 2 probing messages again after timeout")
	}
}

// Test whether we introduced randomness into follower timeout.
func TestCoreElectionFollowerRandomness(t *testing.T) {
	cfg := Config{
		ID:                  "1",
		FollowerTimeout:     20,
		CandidateTimeout:    5,
		RandomElectionRange: 10,
	}

	// It starts in follower state.
	c := newCore(cfg, newStorage())
	c.proposeInitialMembership(Membership{Members: []string{"1"}, Epoch: 54321})

	// Tick until the follower becomes candidate.
	var nTicks uint32
	for {
		c.Tick()
		nTicks++
		if c.state.name() != stateFollower {
			t.Logf("Follower becomes to candidate at tick %d", nTicks)
			break
		}
	}

	// See if it's in correct range.
	if nTicks < cfg.FollowerTimeout ||
		nTicks > cfg.FollowerTimeout+cfg.RandomElectionRange {
		t.Fatalf("Follower takes %d to campaign for leader", nTicks)
	}
}

// Test whether we introduced randomness into candidate timeout.
func TestCoreElectionCandidateRandomness(t *testing.T) {
	cfg := Config{
		ID:                   "1",
		FollowerTimeout:      20,
		CandidateTimeout:     5,
		RandomElectionRange:  10,
		MaxNumEntsPerAppEnts: 1000,
	}

	// It starts in follower state.
	c := newCore(cfg, newStorage())
	c.proposeInitialMembership(Membership{Members: []string{"1", "2", "3"}, Epoch: 54321})
	// Force it to become to candidate.
	c.changeState(c.candidate, "")
	// Take out all messages it broadcast.
	_ = c.TakeAllMsgs()

	// Tick until the candidate becomes candidate again.
	var nTicks uint32
	for {
		c.Tick()
		nTicks++
		// Once it becomes to candidate again, it should broadcast vote requests.
		if len(c.TakeAllMsgs()) != 0 {
			t.Logf("Candidate becomes to candidate again at tick %d", nTicks)
			break
		}
	}

	// See if it's in correct range.
	if nTicks < cfg.CandidateTimeout ||
		nTicks > cfg.CandidateTimeout+cfg.RandomElectionRange {
		t.Fatalf("Follower takes %d to campaign for leader", nTicks)
	}
}

// Test whether leader behaves correctly when propose new commands to it.
func TestCoreLeaderProposeCommands(t *testing.T) {
	confEntry := newConfEntry(1, 1, []string{"1", "2"}, 54321)
	storage := NewStorage(NewMemSnapshotMgr(), initLog(confEntry), NewMemState(1))
	cfg := Config{
		ID:                   "1",
		HeartbeatTimeout:     5,
		MaxNumEntsPerAppEnts: 1000,
	}
	leader := newCore(cfg, storage)
	// Force it to become leader.
	leader.changeState(leader.leader, leader.ID)

	msgs := leader.TakeAllMsgs()
	if len(msgs) != 1 {
		t.Fatal("expected leader to broadcast one probing message")
	}

	// Now leader's log has one command and follower.matchIndex == 0. So if
	// we propose new commands to leader leader will not send these commands to
	// follower("2") right away.
	leader.Propose(entryFromCmd([]byte("data1")), entryFromCmd([]byte("data2")))
	// Leader should have appended one command and two new commands to its log.
	if leader.storage.lastIndex() != 3 {
		t.Fatal("expected leader to append new commands to its log.")
	}

	msgs = leader.TakeAllMsgs()
	if len(msgs) != 0 {
		t.Fatal("leader shouldn't send any AppEnts to follower given it's lag behind.")
	}

	// Now we pretent follower has acked all entries.
	leader.HandleMsg(&AppEntsResp{
		BaseMsg: BaseMsg{From: "2", Term: 1},
		Success: true,
		Index:   3, // Leader has three entries in its log.
	})
	// Take out commit message.
	msgs = leader.TakeAllMsgs()
	if len(msgs) != 1 {
		t.Fatal("expected to get one commit message")
	}

	// Now leader's log is like: "(1, 1), (1, 2), (1, 3)", and follower.matchIndex
	// is 3 so leader knows the follower's log is up-to-date. If we propose new
	// commands to leader it will send AppEnts to the follower right away.
	leader.Propose(entryFromCmd([]byte("data3")), entryFromCmd([]byte("data4")), entryFromCmd([]byte("data5")))
	// Leader should also append new commands to log.
	if leader.storage.lastIndex() != 6 {
		t.Fatal("expected leader to append new commands to its log")
	}

	msgs = leader.TakeAllMsgs()
	// Leader should send new proposed commands to the follower since it's up-to-date.
	if len(msgs) != 1 {
		t.Fatal("expected leader to send AppEnts to follower given it's up-to-date")
		appEnts := msgs[0].(*AppEnts)
		if len(appEnts.Entries) != 3 {
			t.Fatal("expected the length of entries in AppEnts to be 3")
		}
	}

	// now we propose two more commands to leader, because follower hasn't acked the
	// leader for the last 3 commands so it's lag behind. leader shouldn't send the
	// newly appended commands to the follower.
	leader.Propose(entryFromCmd([]byte("data6")), entryFromCmd([]byte("data7")))
	msgs = leader.TakeAllMsgs()
	// leader shouldn't send new commands to follower right away given it's not up-to-date.
	if len(msgs) != 0 {
		t.Fatal("leader shouldn't send newly appended commands to follower who's not synced.")
	}
	// leader should also append new commands to log.
	if leader.storage.lastIndex() != 8 {
		t.Fatal("expected leader to append new commands to its log")
	}
}

// TestCoreLeaderCommit tests leader whether can find out correct commit index.
func TestCoreLeaderCommit(t *testing.T) {
	tests := []struct {
		// Leader's log state.
		leaderLog []Entry
		// A list of AppEntsResp response to leader.
		ackResps []*AppEntsResp
		// Expected commit index.
		commitIndex uint64
	}{

		// Leader's log(leader is node "1"):
		//
		//   L: (1, 1), (1, 2)
		//
		// Followers responses:
		//
		//   "2": won't ack
		//   "3": won't ack
		//
		// Expected result:
		//
		//   Nothing can be committed.
		{
			leaderLog:   []Entry{newConfEntry(1, 1, []string{"1", "2", "3"}, 54321), Entry{Term: 1, Index: 2}},
			ackResps:    nil,
			commitIndex: 0,
		},

		// Leader's log:
		//
		//   L: (1, 1), (1, 2)
		//
		// Followers responses:
		//
		//   "2": won't ack
		//   "3": ack with index 1
		//
		// Expected result:
		//
		//   committedIndex will be 1.
		{
			leaderLog:   []Entry{newConfEntry(1, 1, []string{"1", "2", "3"}, 54321), Entry{Term: 1, Index: 2}},
			ackResps:    []*AppEntsResp{&AppEntsResp{BaseMsg{From: "3", Term: 1}, true, 1, 0}},
			commitIndex: 1,
		},

		// Leader's log:
		//
		//   L: (1, 1), (1, 2)
		//
		// Followers responses:
		//
		//   "2": won't ack
		//   "3": ack with index 2
		//
		// Expected result:
		//
		//   committedIndex will be 2.
		{
			leaderLog:   []Entry{newConfEntry(1, 1, []string{"1", "2", "3"}, 54321), Entry{Term: 1, Index: 2}},
			ackResps:    []*AppEntsResp{&AppEntsResp{BaseMsg{From: "3", Term: 1}, true, 2, 0}},
			commitIndex: 2,
		},

		// Leader's log:
		//
		//   L: (1, 1), (1, 2)
		//
		// Followers responses:
		//
		//   "2": won't ack
		//   "3": ack with index 2
		//   "4": ack with index 1
		//   "5": won't ack
		//
		// Expected result:
		//
		//   committedIndex will be 1.
		{
			leaderLog: []Entry{newConfEntry(1, 1, []string{"1", "2", "3", "4", "5"}, 54321), Entry{Term: 1, Index: 2}},
			ackResps: []*AppEntsResp{
				&AppEntsResp{BaseMsg{From: "3", Term: 1}, true, 2, 0},
				&AppEntsResp{BaseMsg{From: "4", Term: 1}, true, 1, 0},
			},
			commitIndex: 1,
		},
	}

	for _, test := range tests {
		storage := NewStorage(NewMemSnapshotMgr(), initLog(test.leaderLog...), NewMemState(1))
		cfg := Config{
			ID:                   "1",
			HeartbeatTimeout:     3,
			MaxNumEntsPerAppEnts: 1000,
		}
		c := newCore(cfg, storage)
		// Force it change to leader state.
		c.changeState(c.leader, c.ID)
		// Give AppEntsResp acknowlegements to leader.
		for _, resp := range test.ackResps {
			c.HandleMsg(resp)
		}
		// Check if leader find out correct commit index.
		if c.committedIndex != test.commitIndex {
			t.Fatalf("expected %d is commit index, but got %d", test.commitIndex, c.committedIndex)
		}
		// Leader should also have found out all committed entries.
		ents := c.TakeNewlyCommitted()
		if uint64(len(ents)) != test.commitIndex {
			t.Fatalf("expected to get %d committed entries", test.commitIndex)
		}
	}
}

// Test the subtleties about when to commit new commands mentioned in paper.
func TestCoreLeaderCommitSubtleties(t *testing.T) {
	leaderLog := []Entry{
		newConfEntry(1, 1, []string{"1", "2"}, 54321),
		Entry{Term: 1, Index: 2},
		Entry{Term: 2, Index: 3},
	}
	storage := NewStorage(NewMemSnapshotMgr(), initLog(leaderLog...), NewMemState(2))
	cfg := Config{
		ID:                   "1",
		HeartbeatTimeout:     3,
		MaxNumEntsPerAppEnts: 1000,
	}
	c := newCore(cfg, storage)
	// Force it change to leader state.
	c.changeState(c.leader, c.ID)
	// Take out probing messages.
	c.TakeAllMsgs()

	// There're two nodes in group and the leader's state is:
	//
	//   log: (1, 1), (1, 2), (2, 3)  current term: 2
	//

	// Follower ackes leader with index 2.
	c.HandleMsg(&AppEntsResp{
		BaseMsg: BaseMsg{From: "2", Term: 2},
		Success: true,
		Index:   2,
	})

	// Given leader's current term is 2 and the entry at quorum index has term 1,
	// leader shouldn't commit anything even the entry with index 2 is in the logs
	// of a quorum.
	if c.committedIndex != 0 {
		t.Fatal("leader shouldn't commit anything of previous term")
	}

	// Follower ackes leader with index 3.
	c.HandleMsg(&AppEntsResp{
		BaseMsg: BaseMsg{From: "2", Term: 2},
		Success: true,
		Index:   3,
	})
	// Now the entry with index 3 has been persisted by a quorum and has term 2,
	// all entries should be committed.
	if c.committedIndex != 3 {
		t.Fatal("leader should commit all entries")
	}
}

// Test whether follower can commit commands correctly based on LeaderCommit in
// AppEnts request and its 'matchIndex'.
func TestCoreFollowerCommit(t *testing.T) {
	confEntry := newConfEntry(1, 1, []string{"1", "2"}, 54321)
	tests := []struct {
		followerLog  []Entry
		prevLogIndex uint64 // log index for consistency check.
		leaderCommit uint64 // the LeaderCommit in AppEnts mesage.
		shouldCommit uint64 // desired commit index after processing the AppEnts.
	}{
		{
			followerLog:  []Entry{confEntry, Entry{Term: 1, Index: 2}},
			prevLogIndex: 1,
			leaderCommit: 3,
			shouldCommit: 1,
		},
		{
			followerLog:  []Entry{confEntry, Entry{Term: 1, Index: 2}},
			prevLogIndex: 2,
			leaderCommit: 2,
			shouldCommit: 2,
		},
		{
			followerLog:  []Entry{confEntry, Entry{Term: 1, Index: 2}},
			prevLogIndex: 0,
			leaderCommit: 2,
			shouldCommit: 0,
		},
		{
			followerLog:  []Entry{confEntry, Entry{Term: 1, Index: 2}},
			prevLogIndex: 0,
			leaderCommit: 2,
			shouldCommit: 0,
		},
	}

	for _, test := range tests {
		storage := NewStorage(NewMemSnapshotMgr(), initLog(test.followerLog...), NewMemState(1))
		cfg := Config{
			ID:                   "1",
			MaxNumEntsPerAppEnts: 1000,
		}
		// It starts as follower.
		c := newCore(cfg, storage)

		c.HandleMsg(&AppEnts{
			BaseMsg:      BaseMsg{Term: 1, From: "2"},
			PrevLogIndex: test.prevLogIndex,
			PrevLogTerm:  1,
			LeaderCommit: test.leaderCommit,
		})

		if c.committedIndex != test.shouldCommit {
			t.Fatalf("expected follower's committed index to be %d, but got %d", test.shouldCommit, c.committedIndex)
		}
		ents := c.TakeNewlyCommitted()
		if uint64(len(ents)) != test.shouldCommit {
			t.Fatalf("expected to get %d committed entries", test.shouldCommit)
		}
	}
}

// Test whether leader sends correct commit message to a correct peer at correct time.
func TestCoreLeaderSendCommit(t *testing.T) {
	leaderLog := []Entry{
		newConfEntry(1, 1, []string{"1", "2", "3", "4", "5"}, 54321),
		Entry{Term: 1, Index: 2},
		Entry{Term: 1, Index: 3},
	}
	storage := NewStorage(NewMemSnapshotMgr(), initLog(leaderLog...), NewMemState(1))
	cfg := Config{
		ID:                   "1",
		HeartbeatTimeout:     3,
		MaxNumEntsPerAppEnts: 1000,
	}
	c := newCore(cfg, storage)
	// Force it change to leader state.
	c.changeState(c.leader, c.ID)
	// Take out probing messages.
	c.TakeAllMsgs()

	// There're five nodes in group and the leader's state is:
	//
	//   leader log: (1, 1), (1, 2), (1, 3)
	//
	//  1. "2" acks index 2, nothing should happen.
	//
	//  2. "3" acks index 3, leader should send commitIndex 2 to "3". No other
	//     messages should be sent.
	//
	//  3. "4" acked index 3, leader should send commitIndex 3 to "3" and "4". No
	//     other messages should be sent. Leader sent commitIndex to node "3" at
	//     this time because it's up-to-date so leader sends to it directly instead
	//     of waiting until next timeout.

	// (1)
	// Follower "2" ackes leader with index 2.
	c.HandleMsg(&AppEntsResp{
		BaseMsg: BaseMsg{From: "2", Term: 1},
		Success: true,
		Index:   2,
	})
	// Take out follow-up AppEnts.
	msgs := c.TakeAllMsgs()
	commitIndex := msgs[0].(*AppEnts).LeaderCommit
	// Nothing should be committed.
	if commitIndex != 0 {
		t.Fatalf("expected nothing can be committed, but got commit index %d", commitIndex)
	}
	// Nothing should be committed.
	if c.committedIndex != 0 {
		t.Fatal("nothing can be committed")
	}

	// (2)
	// Follower "3" acks leader with index 3.
	c.HandleMsg(&AppEntsResp{
		BaseMsg: BaseMsg{From: "3", Term: 1},
		Success: true,
		Index:   3,
	})
	msgs = c.TakeAllMsgs()
	if len(msgs) != 1 || msgs[0].GetTo() != "3" {
		t.Fatal("expected leader to send commit message to node 3")
	}
	// Index 2 can be committed.
	ci := msgs[0].(*AppEnts).LeaderCommit
	if ci != 2 {
		t.Fatalf("expected to commit index to be 2, but got %d", ci)
	}
	// Index 2 can be committed.
	if c.committedIndex != 2 {
		t.Fatalf("commitIndex on leader side should be 2, but got %d", c.committedIndex)
	}

	// (3)
	// Follower "4" ackes leader with index 3.
	c.HandleMsg(&AppEntsResp{
		BaseMsg: BaseMsg{From: "4", Term: 1},
		Success: true,
		Index:   3,
	})
	msgs = c.TakeAllMsgs()
	if len(msgs) != 2 {
		t.Fatal("leader should send commit to node '3' and '4' respectly")
	}
	if msgs[0].(*AppEnts).LeaderCommit != 3 || msgs[1].(*AppEnts).LeaderCommit != 3 {
		t.Fatal("Leader should send commit index 3 to node '3' and '4'")
	}
	if c.committedIndex != 3 {
		t.Fatalf("commitIndex on leader side should be 3, but got %d", c.committedIndex)
	}
}

// Test whether leader sends AppEnts correctly based on MaxNumEntsPerAppEnts limit.
func TestCoreAppEntsLimit(t *testing.T) {
	// Leader has 7 commands in its log.
	leaderLog := []Entry{
		newConfEntry(1, 1, []string{"1", "2"}, 54321),
		Entry{Term: 1, Index: 2},
		Entry{Term: 1, Index: 3},
		Entry{Term: 1, Index: 4},
		Entry{Term: 1, Index: 5},
		Entry{Term: 1, Index: 6},
		Entry{Term: 1, Index: 7},
	}
	storage := NewStorage(NewMemSnapshotMgr(), initLog(leaderLog...), NewMemState(1))
	cfg := Config{
		ID:               "1",
		HeartbeatTimeout: 3,
		// Leader can send at most 4 entries per AppEnts.
		MaxNumEntsPerAppEnts: 4,
	}
	c := newCore(cfg, storage)
	// Force it change to leader state.
	c.changeState(c.leader, c.ID)
	// Take out probing messages.
	c.TakeAllMsgs()

	// We pretend to be follower "2" and ack leader with index 0.
	c.HandleMsg(&AppEntsResp{
		BaseMsg: BaseMsg{From: "2", Term: 1},
		Success: true,
		Index:   0,
	})

	// For first AppEnts message leader should send 4 entries to the follower given
	// it's the limit of the number of entries per AppEnts.
	msgs := c.TakeAllMsgs()
	if len(msgs) != 1 {
		t.Fatal("expected leader to send a follow-up AppEnts to node '2'")
	}
	if len(msgs[0].(*AppEnts).Entries) != 4 {
		t.Fatal("leader should send 4 entries in the first AppEnts")
	}

	// We pretend to be follower "2" and ack leader with index 4.
	c.HandleMsg(&AppEntsResp{
		BaseMsg: BaseMsg{From: "2", Term: 1},
		Success: true,
		Index:   4,
	})
	// For second AppEnts message leader should send 3 entries to the follower given
	// there are only 3 entries left.
	msgs = c.TakeAllMsgs()
	if len(msgs) != 1 {
		t.Fatal("expected leader to send a follow-up AppEnts to node '2'")
	}
	if len(msgs[0].(*AppEnts).Entries) != 3 {
		t.Fatal("leader should send 3 entries in the second AppEnts")
	}
}

// Same as "TestCoreHandleAppEnts" execept follower's state is log + snapshot
// instead of just log.
func TestCoreHandleAppEntsWithSnapshot(t *testing.T) {
	tests := []struct {
		// Follower's log state.
		followerLog []Entry

		// Snapshot state.
		snapMeta SnapshotMetadata

		// The AppEnts request sent to follower.
		appEnts *AppEnts

		// Expected AppEntsResp response.
		expectedResp *AppEntsResp

		// Expected log state after processing the request.
		expectedLog []Entry
	}{
		// Note "{ (term, index) }" means a snapshot file with command (term, index) as
		// the last applied command in the snapshot.

		// Send entries (1, 3), (1, 4) with (1, 2) as consistency check to the follower.
		//
		// F:
		//    snap: { (1, 2) }
		//    log : empty
		//
		// after procesing the request:
		//
		// F: { (1, 2) }, (1, 3), (1, 4)
		//
		{
			followerLog: []Entry{},
			snapMeta: SnapshotMetadata{
				LastIndex:  2,
				LastTerm:   1,
				Membership: &Membership{Index: 1, Term: 1, Members: []string{"leader", "follower"}}},
			appEnts: &AppEnts{
				BaseMsg:      BaseMsg{Term: 1},
				PrevLogIndex: 2,
				PrevLogTerm:  1,
				Entries:      []Entry{Entry{Term: 1, Index: 3}, Entry{Term: 1, Index: 4}},
			},
			expectedResp: &AppEntsResp{
				BaseMsg: BaseMsg{Term: 1, From: "follower"},
				Success: true,
				Index:   4,
			},
			expectedLog: []Entry{Entry{Term: 1, Index: 3}, Entry{Term: 1, Index: 4}},
		},

		// Send entries (1, 4) with (1, 3) as consistency check to the follower.
		//
		// F:
		//    snap: { (1, 2) }
		//    log : empty
		//
		// after procesing the request:
		//
		// F: { (1, 2) }
		//
		{
			followerLog: []Entry{},
			snapMeta: SnapshotMetadata{
				LastIndex:  2,
				LastTerm:   1,
				Membership: &Membership{Index: 1, Term: 1, Members: []string{"leader", "follower"}}},
			appEnts: &AppEnts{
				BaseMsg:      BaseMsg{Term: 1},
				PrevLogIndex: 3,
				PrevLogTerm:  1,
				Entries:      []Entry{Entry{Term: 1, Index: 4}},
			},
			expectedResp: &AppEntsResp{
				BaseMsg: BaseMsg{Term: 1, From: "follower"},
				Success: false,
				Index:   3,
				Hint:    3,
			},
			expectedLog: []Entry{},
		},

		// Send entries (1, 1), (1, 2) with (0, 0) as consistency check to the follower.
		//
		// F:
		//    snap: { (1, 2) }
		//    log : empty
		//
		// after procesing the request:
		//
		// F: { (1, 2) }
		//
		{
			followerLog: []Entry{},
			snapMeta: SnapshotMetadata{
				LastIndex:  2,
				LastTerm:   1,
				Membership: &Membership{Index: 1, Term: 1, Members: []string{"leader", "follower"}}},
			appEnts: &AppEnts{
				BaseMsg:      BaseMsg{Term: 1},
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries:      []Entry{Entry{Term: 1, Index: 1}, Entry{Term: 1, Index: 2}},
			},
			expectedResp: &AppEntsResp{
				BaseMsg: BaseMsg{Term: 1, From: "follower"},
				Success: true,
				Index:   2,
			},
			expectedLog: []Entry{},
		},

		// Send entries (1, 1), (1, 2), (1, 3) with (0, 0) as consistency check to the follower.
		//
		// F:
		//    snap: { (1, 2) }
		//    log : empty
		//
		// after procesing the request:
		//
		// F: { (1, 2) } (1, 3)
		//
		{
			followerLog: []Entry{},
			snapMeta: SnapshotMetadata{
				LastIndex:  2,
				LastTerm:   1,
				Membership: &Membership{Index: 1, Term: 1, Members: []string{"leader", "follower"}}},
			appEnts: &AppEnts{
				BaseMsg:      BaseMsg{Term: 1},
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries:      []Entry{Entry{Term: 1, Index: 1}, Entry{Term: 1, Index: 2}, Entry{Term: 1, Index: 3}},
			},
			expectedResp: &AppEntsResp{
				BaseMsg: BaseMsg{Term: 1, From: "follower"},
				Success: true,
				Index:   3,
			},
			expectedLog: []Entry{Entry{Term: 1, Index: 3}},
		},

		// Send entries (1, 1), (1, 2), (1, 3) with (0, 0) as consistency check to the follower.
		//
		// F:
		//    snap: { (1, 2) }, (2, 3)
		//    log : empty
		//
		// after procesing the request:
		//
		// F: { (1, 2) } (1, 3)
		//
		{
			followerLog: []Entry{Entry{Term: 2, Index: 3}},
			snapMeta: SnapshotMetadata{
				LastIndex:  2,
				LastTerm:   1,
				Membership: &Membership{Index: 1, Term: 1, Members: []string{"leader", "follower"}}},
			appEnts: &AppEnts{
				BaseMsg:      BaseMsg{Term: 1},
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries:      []Entry{Entry{Term: 1, Index: 1}, Entry{Term: 1, Index: 2}, Entry{Term: 1, Index: 3}},
			},
			expectedResp: &AppEntsResp{
				BaseMsg: BaseMsg{Term: 1, From: "follower"},
				Success: true,
				Index:   3,
			},
			expectedLog: []Entry{Entry{Term: 1, Index: 3}},
		},

		// Send entries (1, 2), (1, 3), (2, 4) with (1, 1) as consistency check to the follower.
		//
		// F:
		//    snap: { (1, 2) }, (1, 3), (1, 4)
		//    log : empty
		//
		// after procesing the request:
		//
		// F: { (1, 2) } (1, 3), (2, 4)
		//
		{
			followerLog: []Entry{Entry{Term: 1, Index: 3}, Entry{Term: 1, Index: 4}},
			snapMeta: SnapshotMetadata{
				LastIndex:  2,
				LastTerm:   1,
				Membership: &Membership{Index: 1, Term: 1, Members: []string{"leader", "follower"}}},
			appEnts: &AppEnts{
				BaseMsg:      BaseMsg{Term: 1},
				PrevLogIndex: 1,
				PrevLogTerm:  1,
				Entries:      []Entry{Entry{Term: 1, Index: 2}, Entry{Term: 1, Index: 3}, Entry{Term: 2, Index: 4}},
			},
			expectedResp: &AppEntsResp{
				BaseMsg: BaseMsg{Term: 1, From: "follower"},
				Success: true,
				Index:   4,
			},
			expectedLog: []Entry{Entry{Term: 1, Index: 3}, Entry{Term: 2, Index: 4}},
		},
	}

	for _, test := range tests {
		// Create follower core.
		cfg := Config{
			ID:                   "follower",
			MaxNumEntsPerAppEnts: 1000,
		}
		follower := newCore(cfg, setupStorage(test.snapMeta, test.followerLog, t))

		follower.HandleMsg(test.appEnts)
		msgs := follower.TakeAllMsgs()
		if len(msgs) != 1 {
			t.Fatalf("expected to get an AppEntsResp from follower.")
		}

		resp := msgs[0].(*AppEntsResp)
		// Verify whether we got expected response.
		if !reflect.DeepEqual(resp, test.expectedResp) {
			t.Fatalf("expected to get resp %v, but got: %v", test.expectedResp, resp)
		}

		ents := getAllEntries(follower.storage.log)
		// Verify whther follower's log state is expected after processing the AppEnts request.
		if !reflect.DeepEqual(ents, test.expectedLog) {
			t.Log(ents, len(ents))
			t.Log(test.expectedLog, len(test.expectedLog))
			t.Fatalf("the log state after processing AppEnts request is not expected")
		}
	}
}

// Test whether follower handles InstallSnapshot message correctly.
func TestCoreHandleSnapshot(t *testing.T) {
	members := []string{"follower", "leader"}
	confEntry := newConfEntry(1, 1, members, 54321)
	tests := []struct {
		// Follower's state before processing InstallSnaphot.
		followerLog []Entry
		snapMeta    SnapshotMetadata
		term        uint64

		// Snapshot message.
		snapMetaSent SnapshotMetadata

		// Expected AppEntsResp response.
		expectedResp *AppEntsResp

		// Expected state after processing InstallSnapshot.
		expectedLastLogIndex  uint64
		expectedLastSnapIndex uint64
	}{

		// "..." means zero or more entries.

		// F:
		//    snap: empty
		//    log : (1, 1), (1, 2), (1, 3)
		//
		// send snapshot { {1, 4} }
		//
		// expected result:
		//
		// F:
		//    snap: { (1, 4) }
		//    log : empty
		//
		{
			followerLog: []Entry{confEntry, Entry{Term: 1, Index: 2}, Entry{Term: 1, Index: 3}},
			snapMeta:    NilSnapshotMetadata,
			term:        1,
			snapMetaSent: SnapshotMetadata{
				LastIndex:  4,
				LastTerm:   1,
				Membership: &Membership{Index: 1, Term: 1, Members: members}},
			expectedResp: &AppEntsResp{
				BaseMsg: BaseMsg{Term: 1, From: "follower"},
				Success: true,
				Index:   4,
			},
			// The log should be truncated all.
			expectedLastLogIndex:  0,
			expectedLastSnapIndex: 4,
		},

		// F:
		//    snap: empty
		//    log : (1, 1), (1, 2), (1, 3), (2, 4)
		//
		// send snapshot { {1, 4} }
		//
		// expected result:
		//
		// F:
		//    snap: { (1, 4) }
		//    log : empty
		//
		{
			followerLog: []Entry{confEntry, Entry{Term: 1, Index: 2}, Entry{Term: 1, Index: 3}, Entry{Term: 2, Index: 4}},
			snapMeta:    NilSnapshotMetadata,
			term:        1,
			snapMetaSent: SnapshotMetadata{
				LastIndex:  4,
				LastTerm:   1,
				Membership: &Membership{Index: 1, Term: 1, Members: members}},
			expectedResp: &AppEntsResp{
				BaseMsg: BaseMsg{Term: 1, From: "follower"},
				Success: true,
				Index:   4,
			},
			// It should be truncated all.
			expectedLastLogIndex:  0,
			expectedLastSnapIndex: 4,
		},

		// F:
		//    snap: empty
		//    log : (1, 1), (1, 2), (1, 3)
		//
		// send snapshot { {1, 2} }
		//
		// expected result:
		//
		// F:
		//    snap: { (1, 2) }
		//    log :     ...   , (1, 3)
		//
		{
			followerLog: []Entry{confEntry, Entry{Term: 1, Index: 2}, Entry{Term: 1, Index: 3}},
			snapMeta:    NilSnapshotMetadata,
			term:        1,
			snapMetaSent: SnapshotMetadata{
				LastIndex:  2,
				LastTerm:   1,
				Membership: &Membership{Index: 1, Term: 1, Members: members}},
			expectedResp: &AppEntsResp{
				BaseMsg: BaseMsg{Term: 1, From: "follower"},
				Success: true,
				Index:   2,
			},
			// It should be truncated all.
			expectedLastLogIndex:  3,
			expectedLastSnapIndex: 2,
		},

		// F:
		//    snap: { (1, 1) }
		//    log :    (1, 1), (1, 2), (1, 3)
		//
		// send snapshot { {1, 2} }
		//
		// expected result:
		//
		// F:
		//    snap: { (1, 2) }
		//    log :     ...   , (1, 3)
		//
		{
			followerLog: []Entry{Entry{Term: 1, Index: 1}, Entry{Term: 1, Index: 2}, Entry{Term: 1, Index: 3}},
			snapMeta: SnapshotMetadata{
				LastIndex:  1,
				LastTerm:   1,
				Membership: &Membership{Index: 1, Term: 1, Members: members}},
			term: 1,
			snapMetaSent: SnapshotMetadata{
				LastIndex:  2,
				LastTerm:   1,
				Membership: &Membership{Index: 1, Term: 1, Members: members}},
			expectedResp: &AppEntsResp{
				BaseMsg: BaseMsg{Term: 1, From: "follower"},
				Success: true,
				Index:   2,
			},
			// It should be truncated all.
			expectedLastLogIndex:  3,
			expectedLastSnapIndex: 2,
		},

		// F:
		//    snap: { (1, 3) }
		//    log : empty
		//
		// send snapshot { {1, 2} }
		//
		// expected result:
		//
		// F:
		//    snap: { (1, 3) }
		//    log : empty
		//
		{
			followerLog: []Entry{},
			snapMeta: SnapshotMetadata{
				LastIndex:  3,
				LastTerm:   1,
				Membership: &Membership{Index: 1, Term: 1, Members: members}},
			term: 1,
			snapMetaSent: SnapshotMetadata{
				LastIndex:  2,
				LastTerm:   1,
				Membership: &Membership{Index: 1, Term: 1, Members: members}},
			expectedResp: &AppEntsResp{
				BaseMsg: BaseMsg{Term: 1, From: "follower"},
				Success: true,
				Index:   3,
			},
			// It should be truncated all.
			expectedLastLogIndex:  0,
			expectedLastSnapIndex: 3,
		},

		// F:
		//    snap: { (1, 3) }
		//    log : empty
		//
		// send snapshot { {1, 3} }
		//
		// expected result:
		//
		// F:
		//    snap: { (1, 3) }
		//    log : empty
		//
		{
			followerLog: []Entry{},
			snapMeta: SnapshotMetadata{
				LastIndex:  3,
				LastTerm:   1,
				Membership: &Membership{Index: 1, Term: 1, Members: members}},
			term: 1,
			snapMetaSent: SnapshotMetadata{
				LastIndex:  3,
				LastTerm:   1,
				Membership: &Membership{Index: 1, Term: 1, Members: members}},
			expectedResp: &AppEntsResp{
				BaseMsg: BaseMsg{Term: 1, From: "follower"},
				Success: true,
				Index:   3,
			},
			// It should be truncated all.
			expectedLastLogIndex:  0,
			expectedLastSnapIndex: 3,
		},
	}

	for _, test := range tests {
		// Create follower core.
		cfg := Config{
			ID:                   "follower",
			MaxNumEntsPerAppEnts: 1000,
		}
		follower := newCore(cfg, setupStorage(test.snapMeta, test.followerLog, t))

		follower.HandleMsg(&InstallSnapshot{
			BaseMsg:    BaseMsg{Term: test.term},
			LastIndex:  test.snapMetaSent.LastIndex,
			LastTerm:   test.snapMetaSent.LastTerm,
			Membership: *test.snapMetaSent.Membership,
			Body:       ioutil.NopCloser(bytes.NewReader(nil)),
		})

		// See if we get expected response.
		if test.expectedResp != nil {
			msgs := follower.TakeAllMsgs()
			if len(msgs) != 1 {
				t.Fatalf("expected to send AppEntsResp.")
			}
			if !reflect.DeepEqual(msgs[0], test.expectedResp) {
				t.Fatalf("failed to get expected response")
			}
		} else {
			if follower.TakeAllMsgs() != nil {
				t.Fatalf("expected no messages to be sent.")
			}
		}

		// See if follower is in an expected state.
		// Log state.
		li, _ := follower.storage.log.FirstIndex()
		if li != test.expectedLastLogIndex {
			t.Fatalf("unexpected last log index after snapshot")
		}
		meta, _ := follower.storage.GetSnapshotMetadata()
		if meta == NilSnapshotMetadata {
			t.Fatalf("can't find snapshot file after installing snapshot file.")
		}
		if meta.LastIndex != test.expectedLastSnapIndex {
			t.Fatalf("expected last snap index to be %d, but got %d", test.expectedLastSnapIndex, meta.LastIndex)
		}
	}
}

// Test whether we trim log correctly or not.
func TestCoreTrimmingLog(t *testing.T) {
	tests := []struct {
		ents             []Entry
		snapMeta         SnapshotMetadata
		logEntsAfterSnap uint64
		// expected entries in log after trimming it.
		entsAfterTrim []Entry
	}{
		// Trim entire log.
		{
			ents: []Entry{Entry{Term: 1, Index: 1}, Entry{Term: 1, Index: 2}},
			snapMeta: SnapshotMetadata{
				LastIndex:  2,
				LastTerm:   1,
				Membership: &Membership{Index: 1, Term: 1, Members: []string{"c", "d"}}},
			logEntsAfterSnap: 0,
			entsAfterTrim:    []Entry{},
		},

		// Trim partial log.
		{
			ents: []Entry{Entry{Term: 1, Index: 1}, Entry{Term: 1, Index: 2}},
			snapMeta: SnapshotMetadata{
				LastIndex:  1,
				LastTerm:   1,
				Membership: &Membership{Index: 1, Term: 1, Members: []string{"c", "d"}}},
			logEntsAfterSnap: 0,
			entsAfterTrim:    []Entry{Entry{Term: 1, Index: 2}},
		},

		// Trim partial log because we were told to keep some entries.
		{
			ents: []Entry{Entry{Term: 1, Index: 1}, Entry{Term: 1, Index: 2}},
			snapMeta: SnapshotMetadata{
				LastIndex:  2,
				LastTerm:   1,
				Membership: &Membership{Index: 1, Term: 1, Members: []string{"c", "d"}}},
			logEntsAfterSnap: 1,
			entsAfterTrim:    []Entry{Entry{Term: 1, Index: 2}},
		},

		// Trim partial log because we were told to keep some entries.
		{
			ents: []Entry{Entry{Term: 1, Index: 1}, Entry{Term: 1, Index: 2}},
			snapMeta: SnapshotMetadata{
				LastIndex:  2,
				LastTerm:   1,
				Membership: &Membership{Index: 1, Term: 1, Members: []string{"c", "d"}}},
			logEntsAfterSnap: 2,
			entsAfterTrim:    []Entry{Entry{Term: 1, Index: 1}, Entry{Term: 1, Index: 2}},
		},

		// Trim partial log because we were told to keep some entries.
		{
			ents: []Entry{Entry{Term: 1, Index: 1}, Entry{Term: 1, Index: 2}},
			snapMeta: SnapshotMetadata{
				LastIndex:  2,
				LastTerm:   1,
				Membership: &Membership{Index: 1, Term: 1, Members: []string{"c", "d"}}},
			logEntsAfterSnap: 100,
			entsAfterTrim:    []Entry{Entry{Term: 1, Index: 1}, Entry{Term: 1, Index: 2}},
		},
	}

	for _, test := range tests {
		t.Logf("running test %v", test)
		// Create follower core.
		cfg := Config{
			ID:                      "c",
			MaxNumEntsPerAppEnts:    1000,
			LogEntriesAfterSnapshot: test.logEntsAfterSnap,
		}
		c := newCore(cfg, setupStorage(test.snapMeta, test.ents, t))
		c.TrimLog(test.snapMeta.LastIndex)

		if !reflect.DeepEqual(getAllEntries(c.storage.log), test.entsAfterTrim) {
			t.Fatalf("unexpected log state after trimming the log")
		}
	}
}

// Test a leader loses its leadership without a quorum contacts for
// 'LeaderStepdownTimeout'.
func TestCoreLeaderLeaseLost(t *testing.T) {
	cfg := Config{
		ID:                    "1",
		FollowerTimeout:       50,
		CandidateTimeout:      30,
		HeartbeatTimeout:      10,
		LeaderStepdownTimeout: 20,
		MaxNumEntsPerAppEnts:  1000,
	}

	r := newCore(cfg, newStorage())
	r.proposeInitialMembership(Membership{Members: []string{"1", "2", "3"}, Epoch: 54321})
	// Force it becomes leader.
	r.changeState(r.leader, r.config.ID)
	if r.state.name() != stateLeader {
		t.Fatalf("the node should be in leader state.")
	}

	for i := 0; i < int(cfg.LeaderStepdownTimeout+1); i++ {
		r.Tick()
	}

	if r.state.name() != stateFollower {
		t.Fatalf("the node should step down without quorum contacts.")
	}
}

// Test a leader should not lose its leadership with a quorum of active members.
func TestCoreLeaderLeaseMaintain(t *testing.T) {
	cfg := Config{
		ID:                    "1",
		FollowerTimeout:       50,
		CandidateTimeout:      30,
		HeartbeatTimeout:      10,
		LeaderStepdownTimeout: 20,
		MaxNumEntsPerAppEnts:  1000,
	}

	r := newCore(cfg, newStorage())
	r.proposeInitialMembership(Membership{Members: []string{"1", "2", "3"}, Epoch: 54321})
	// Force it becomes leader.
	r.changeState(r.leader, r.config.ID)
	if r.state.name() != stateLeader {
		t.Fatalf("the node should be in leader state.")
	}

	for i := 0; i < int(cfg.LeaderStepdownTimeout+1); i++ {
		if i == int(cfg.LeaderStepdownTimeout/2) {
			// Let node "2" respond the leader at some point.
			msg := &AppEntsResp{
				BaseMsg: BaseMsg{Term: 1, From: "2", To: "1"},
				Index:   0,
				Success: true,
			}
			r.HandleMsg(msg)
		}
		r.Tick()
	}

	// With active members of itself and node "2", leader should maitain its
	// leadership.
	if r.state.name() == stateFollower {
		t.Fatalf("the node should not step down with quorum contacts.")
	}
}

// Test that a node can be elected as leader when cluster only has 1 node.
func TestCoreSingleNodeElect(t *testing.T) {
	cfg := Config{
		ID:                   "1",
		FollowerTimeout:      5,
		CandidateTimeout:     3,
		MaxNumEntsPerAppEnts: 1000,
	}

	r := newCore(cfg, newStorage())
	r.proposeInitialMembership(Membership{Members: []string{"1"}, Epoch: 54321})

	// It should start as a follower.
	if r.state.name() != stateFollower {
		t.Fatal("expected Raft to start as a follower")
	}

	// Tick 5 times so follower will timeout, trigger the transition to candidate.
	for i := 0; i < int(cfg.FollowerTimeout); i++ {
		r.Tick()
	}

	// Since it's the only node in the cluster, so it will be elected as leader directly.
	if r.state.name() != stateLeader {
		t.Fatalf("expected the node to be elected as leader")
	}
}

// Test that a node can commit new proposed commands when cluster only has 1 node.
func TestCoreSingleNodeCommitNew(t *testing.T) {
	cfg := Config{
		ID:                   "1",
		FollowerTimeout:      5,
		CandidateTimeout:     3,
		MaxNumEntsPerAppEnts: 1000,
	}

	r := newCore(cfg, newStorage())
	r.proposeInitialMembership(Membership{Members: []string{"1"}, Epoch: 54321})

	// It should start as a follower.
	if r.state.name() != stateFollower {
		t.Fatal("expected Raft to start as a follower")
	}

	// Tick 5 times so follower will timeout, trigger the transition to candidate.
	for i := 0; i < int(cfg.FollowerTimeout); i++ {
		r.Tick()
	}

	// Since it's the only node in the cluster, so it will be elected as leader directly.
	if r.state.name() != stateLeader {
		t.Fatalf("expected the node to be elected as leader")
	}

	if err := r.Propose(entryFromCmd([]byte("cmd1")), entryFromCmd([]byte("cmd2"))); err != nil {
		t.Fatalf("failed to propose commands: %v", err)
	}

	commits := r.TakeNewlyCommitted()
	if len(commits) != 3 { // There's one configuration entry in log.
		t.Fatalf("expected the 3 entries to be committed")
	}
}

// Test that a node can commit existing entries in log when cluster only has
// 1 node.
func TestCoreSingleNodeCommitExisting(t *testing.T) {
	storage := NewStorage(
		NewMemSnapshotMgr(),
		// Create a log with 2 entries exist in it.
		initLog(newConfEntry(1, 1, []string{"1"}, 54321), Entry{Term: 1, Index: 2}),
		NewMemState(0),
	)
	cfg := Config{
		ID:                   "1",
		FollowerTimeout:      5,
		CandidateTimeout:     3,
		MaxNumEntsPerAppEnts: 1000,
	}

	r := newCore(cfg, storage)

	// It should start as a follower.
	if r.state.name() != stateFollower {
		t.Fatal("expected Raft to start as a follower")
	}

	// Tick 5 times so follower will timeout, trigger the transition to candidate.
	for i := 0; i < int(cfg.FollowerTimeout); i++ {
		r.Tick()
	}

	// Since it's the only node in the cluster, so it will be elected as leader directly.
	if r.state.name() != stateLeader {
		t.Fatalf("expected the node to be elected as leader")
	}

	// The 2 existing entries should be committed.
	commits := r.TakeNewlyCommitted()
	if len(commits) != 2 {
		t.Fatalf("expected the 2 existing commands get committed")
	}

	// Append 1 new entry.
	if err := r.Propose(entryFromCmd([]byte("cmd1"))); err != nil {
		t.Fatalf("failed to propose commands: %v", err)
	}

	// And it should be committed as well.
	commits = r.TakeNewlyCommitted()
	if len(commits) != 1 {
		t.Fatalf("expected the 1 just proposed command get committed")
	}
}

// Test if core can find the latest configuration from the storage(log & snapshot).
func TestCoreFindLatestConfiguration(t *testing.T) {
	tests := []struct {
		snap     SnapshotMetadata // Snapshot
		entries  []Entry          // Entries in log
		expected *Membership      // The expected configuration
	}{

		// No snapshot and no log entries. No configuration can be found.
		{
			snap:     NilSnapshotMetadata,
			entries:  nil,
			expected: nil,
		},

		// No snapshot and there's a conf entry in log.
		{
			snap:     NilSnapshotMetadata,
			entries:  []Entry{newConfEntry(1, 1, []string{"1"}, 54321)},
			expected: &Membership{Term: 1, Index: 1, Members: []string{"1"}, Epoch: 54321},
		},

		// No snapshot and there're two conf entries in log, should use the latest
		// one.
		{
			snap: NilSnapshotMetadata,
			entries: []Entry{
				newConfEntry(1, 1, []string{"1"}, 54321),
				newConfEntry(1, 2, []string{"1", "2"}, 54321),
			},
			expected: &Membership{Term: 1, Index: 2, Members: []string{"1", "2"}, Epoch: 54321},
		},

		// There're a snapshot and a log without conf entries. So should use the
		// conf in snapshot.
		{
			snap: SnapshotMetadata{
				LastTerm:   1,
				LastIndex:  10,
				Membership: &Membership{Term: 1, Index: 1, Members: []string{"1"}},
			},
			entries:  nil,
			expected: &Membership{Term: 1, Index: 1, Members: []string{"1"}},
		},

		// There're a snapshot and a log with a more recent conf entry. So should
		// use the one in log.
		{
			snap: SnapshotMetadata{
				LastTerm:   1,
				LastIndex:  10,
				Membership: &Membership{Term: 1, Index: 1, Members: []string{"1"}},
			},
			entries: []Entry{
				Entry{Term: 1, Index: 11},
				newConfEntry(1, 12, []string{"1", "2"}, 54321), // The more recent one.
			},
			expected: &Membership{Term: 1, Index: 12, Members: []string{"1", "2"}, Epoch: 54321},
		},

		// There're a snapshot and a log with a stale conf entry. So should use the
		// one in snapshot(also in log).
		{
			snap: SnapshotMetadata{
				LastTerm:   1,
				LastIndex:  10,
				Membership: &Membership{Term: 1, Index: 10, Members: []string{"1", "2"}, Epoch: 54321},
			},
			entries: []Entry{
				newConfEntry(1, 9, []string{"1"}, 54321), // the stale one.
				newConfEntry(1, 10, []string{"1", "2"}, 54321),
				Entry{Term: 1, Index: 11},
			},
			expected: &Membership{Term: 1, Index: 10, Members: []string{"1", "2"}, Epoch: 54321},
		},
	}

	cfg := Config{ID: "1"}
	for _, test := range tests {
		storage := setupStorage(test.snap, test.entries, t)
		c := newCore(cfg, storage)
		if !reflect.DeepEqual(c.latestConf, test.expected) {
			t.Fatalf("expected the latest conf to be %+v, but got %+v", test.expected,
				c.latestConf)
		}
	}
}

// Tests that invalid reconfiguration request should be rejected.
func TestCoreInvalidReconfig(t *testing.T) {
	c := newCore(Config{ID: "1"}, newStorage())
	c.proposeInitialMembership(Membership{Members: []string{"1"}, Epoch: 54321})

	if err := c.AddNode("3"); err != ErrNodeNotLeader {
		t.Fatalf("expected 'ErrNodeNotLeader' when in non-leader state.")
	}
	if err := c.RemoveNode("1"); err != ErrNodeNotLeader {
		t.Fatalf("expected 'ErrNodeNotLeader' when in non-leader state.")
	}

	// Force it becomes to leader
	c.changeState(c.leader, c.ID)

	if err := c.RemoveNode("3"); err != ErrNodeNotExists {
		t.Fatalf("expected 'ErrNodeNotExists' when remove a non-existing node")
	}
	if err := c.AddNode("1"); err != ErrNodeExists {
		t.Fatalf("expected 'ErrNodeExists' when add an existing node")
	}
}

// Tests that new reconfiguration request will be rejected before committing the
// pending one.
func TestCoreRejectNewBeforeCommittingPending(t *testing.T) {
	c := newCore(Config{ID: "1"}, newStorage())
	c.proposeInitialMembership(Membership{Members: []string{"1"}, Epoch: 54321})

	// Force it becomes to leader.
	c.changeState(c.leader, c.ID)
	// Propose first reconfig.
	if err := c.AddNode("2"); err != nil {
		t.Fatalf("expected no error get returned: %v", err)
	}

	// The second should be rejected before the first gets committied.
	if err := c.AddNode("3"); err != ErrTooManyPendingReqs {
		t.Fatalf("expected errors when propose reconfig before committing the latest conf.%v", err)
	}

	// Now pretend that the follower acks the reconfig.
	c.HandleMsg(&AppEntsResp{
		BaseMsg: BaseMsg{From: "2", Term: 1},
		Success: true,
		Index:   2,
	})

	// The conf should be committed.
	if c.committedIndex != 2 {
		t.Fatalf("The conf entry should be committed after the ack.")
	}

	// Now the latest conf gets committed, we can propose new one.
	if err := c.AddNode("3"); err != nil {
		t.Fatalf("expected no error get returned: %v", err)
	}

	// '3' should be in the new configuration.
	if !containMember(c.latestConf.Members, "3") {
		t.Fatalf("The new configuration should contain node '3'")
	}
}

// Tests that leader should step down when the reconfiguration without itself
// is committed.
func TestCoreLeaderStepdownAfterRemoval(t *testing.T) {
	c := newCore(Config{ID: "1"}, newStorage())
	c.proposeInitialMembership(Membership{Members: []string{"1", "2"}, Epoch: 54321})
	// Force it becomes to leader and commit the latest configuration.
	c.changeState(c.leader, c.ID)
	c.HandleMsg(&AppEntsResp{
		BaseMsg: BaseMsg{From: "2", Term: 1},
		Success: true,
		Index:   1,
	})

	// Propose to remove the leader from the new config.
	if err := c.RemoveNode("1"); err != nil {
		t.Fatalf("expected no error get returned: %v", err)
	}

	// Should not step down before commiting the new conf.
	if c.state != c.leader {
		t.Fatalf("can not step down before committing the new conf")
	}

	// Should step down after committing the new conf.
	c.HandleMsg(&AppEntsResp{
		BaseMsg: BaseMsg{From: "2", Term: 1},
		Success: true,
		Index:   2,
	})
	if c.state != c.follower {
		t.Fatalf("should step down after committing the new conf")
	}
}

// Tests whether commit entries properly after removing a node.
func TestCoreCommitAfterRemoval(t *testing.T) {
	c := newCore(Config{ID: "1"}, newStorage())
	c.proposeInitialMembership(Membership{Members: []string{"1", "2", "3", "4"}, Epoch: 54321})
	c.changeState(c.leader, c.ID)
	c.Propose(entryFromCmd([]byte("1")), entryFromCmd([]byte("2")),
		entryFromCmd([]byte("3")), entryFromCmd([]byte("4")))

	// Now the leader's log history should be((1, 1) will be conf):
	//
	// And followers will ack entries below:
	//
	// 1: (1, 1), (1, 2), (1, 3), (1, 4), (1, 5)
	// 2:                          ack
	// 3:          ack
	// 4: ack
	//
	//

	// Simulates acks
	c.HandleMsg(&AppEntsResp{
		BaseMsg: BaseMsg{From: "2", Term: 1},
		Success: true,
		Index:   4,
	})
	c.HandleMsg(&AppEntsResp{
		BaseMsg: BaseMsg{From: "3", Term: 1},
		Success: true,
		Index:   2,
	})
	c.HandleMsg(&AppEntsResp{
		BaseMsg: BaseMsg{From: "4", Term: 1},
		Success: true,
		Index:   1,
	})

	// The committed index will 2 given the quorum(1, 2, 3)
	if c.committedIndex != 2 {
		t.Fatalf("expected the commit index will be 2")
	}

	// Remove node 4.
	if err := c.RemoveNode("4"); err != nil {
		t.Fatalf("expected no error get returned: %v", err)
	}

	// Now the quorum size is changed from 3 to 2, so the commit index should be
	// bumped to 4 given the new quorum(1, 2).
	if c.committedIndex != 4 {
		t.Fatalf("expected the commit index will be 4")
	}
}

// Tests that a follower's latest configuration gets truncated, it should
// fall back the configuration to previous one.
func TestCoreConfTruncated(t *testing.T) {
	// Create a follower with two configurations in log.
	log := initLog(
		newConfEntry(1, 1, []string{"1"}, 54321), // first conf
		Entry{Term: 1, Index: 2},
		newConfEntry(1, 3, []string{"1", "2"}, 54321), // the last conf
	)
	state := NewMemState(1)
	storage := NewStorage(&memSnapshotMgr{}, log, state)
	c := newCore(Config{ID: "1"}, storage)

	// Verify the latest configuration is initialized properly.
	if !reflect.DeepEqual(
		c.latestConf,
		&Membership{Term: 1, Index: 3, Members: []string{"1", "2"}, Epoch: 54321},
	) {
		t.Fatalf("incorrect initialized latest configuration")
	}

	// Truncate the latest one by sending an AppEnts message that is conflicted
	// with the latest configuration.
	c.HandleMsg(
		&AppEnts{
			BaseMsg:      BaseMsg{Term: 1, To: "1", From: "2"},
			PrevLogIndex: 2,
			PrevLogTerm:  1,
			// This one is conflicted with the latest configuration
			Entries: []Entry{Entry{Term: 2, Index: 3}},
		})

	// The latest configuration should be falled back to the previous one.
	if !reflect.DeepEqual(
		c.latestConf,
		&Membership{Term: 1, Index: 1, Members: []string{"1"}, Epoch: 54321},
	) {
		t.Fatalf("incorrect configuratin after the truncation")
	}
}

// Tests that a follower receives a snapshot with newer configuration, it
// should set latest configuration to the configuration included in the
// snapshot.
func TestCoreConfReceivedFromSnapshot(t *testing.T) {
	// Create a follower with one configuration in log.
	log := initLog(
		newConfEntry(1, 1, []string{"1"}, 54321), // first conf
		Entry{Term: 1, Index: 2},
	)
	state := NewMemState(1)
	storage := NewStorage(&memSnapshotMgr{}, log, state)
	c := newCore(Config{ID: "1"}, storage)

	// Verify the latest configuration is initialized properly.
	if !reflect.DeepEqual(
		c.latestConf,
		&Membership{Term: 1, Index: 1, Members: []string{"1"}, Epoch: 54321},
	) {
		t.Fatalf("incorrect initialized latest configuration")
	}

	// Received a snapshot with a newer configuration
	c.HandleMsg(
		&InstallSnapshot{
			BaseMsg:    BaseMsg{Term: 1, To: "1", From: "2"},
			LastIndex:  10,
			LastTerm:   1,
			Membership: Membership{Term: 1, Index: 5, Members: []string{"1", "2"}, Epoch: 54321},
			Body:       ioutil.NopCloser(bytes.NewReader(nil)),
		})

	// The latest configuration should be updated to the one in the snapshot.
	if !reflect.DeepEqual(
		c.latestConf,
		&Membership{Term: 1, Index: 5, Members: []string{"1", "2"}, Epoch: 54321},
	) {
		t.Fatalf("the latest configuration should be updated")
	}
}

// Tests that a follower receives a stale snapshot message with a staler
// configuration, the latest configuration should not be changed.
func TestCoreConfReceivedFromSnapshotStale(t *testing.T) {
	// Create a follower with two configurations in log.
	log := initLog(
		newConfEntry(1, 1, []string{"1"}, 54321), // first conf
		Entry{Term: 1, Index: 2},
		newConfEntry(1, 3, []string{"1", "2"}, 54321), // last conf
	)
	state := NewMemState(1)
	storage := NewStorage(&memSnapshotMgr{}, log, state)
	c := newCore(Config{ID: "1"}, storage)

	// Verify the latest configuration is initialized properly.
	if !reflect.DeepEqual(
		c.latestConf,
		&Membership{Term: 1, Index: 3, Members: []string{"1", "2"}, Epoch: 54321},
	) {
		t.Fatalf("incorrect initialized latest configuration")
	}

	// Received a stale snapshot message with a staler configuration
	c.HandleMsg(
		&InstallSnapshot{
			BaseMsg:    BaseMsg{Term: 1, To: "1", From: "2"},
			LastIndex:  2,
			LastTerm:   1,
			Membership: Membership{Term: 1, Index: 1, Members: []string{"1"}, Epoch: 54321},
			Body:       ioutil.NopCloser(bytes.NewReader(nil)),
		})

	// The latest configuration should not be changed.
	if !reflect.DeepEqual(
		c.latestConf,
		&Membership{Term: 1, Index: 3, Members: []string{"1", "2"}, Epoch: 54321},
	) {
		t.Fatalf("the latest configuration should not be changed")
	}
}

// Tests that a follower receives log entries with new configuration, the
// latest configuration should be updated.
func TestCoreConfReceivedFromLog(t *testing.T) {
	// Create a follower with two configurations in log.
	log := initLog(
		newConfEntry(1, 1, []string{"1"}, 54321), // first conf
		Entry{Term: 1, Index: 2},
		newConfEntry(1, 3, []string{"1", "2"}, 54321), // last conf
	)
	state := NewMemState(1)
	storage := NewStorage(&memSnapshotMgr{}, log, state)
	c := newCore(Config{ID: "1"}, storage)

	// Verify the latest configuration is initialized properly.
	if !reflect.DeepEqual(
		c.latestConf,
		&Membership{Term: 1, Index: 3, Members: []string{"1", "2"}, Epoch: 54321},
	) {
		t.Fatalf("incorrect initialized latest configuration")
	}

	c.HandleMsg(
		&AppEnts{
			BaseMsg:      BaseMsg{Term: 1, To: "1", From: "2"},
			PrevLogIndex: 3,
			PrevLogTerm:  1,
			// New configuration.
			Entries: []Entry{newConfEntry(1, 4, []string{"1", "2", "3"}, 54321)},
		})

	// The latest configuration should be updated.
	if !reflect.DeepEqual(
		c.latestConf,
		&Membership{Term: 1, Index: 4, Members: []string{"1", "2", "3"}, Epoch: 54321},
	) {
		t.Fatalf("should update the latest configuration to received one")
	}
}

// Tests that follower should send a 'Hint' when it misses some log entries
// from a leader and the leader should adjust the probing message accordingly.
func TestCoreHint(t *testing.T) {
	tests := []struct {
		// Initial leader's log
		leaderLog []Entry
		// Initial follower's log
		followerLog []Entry
		// The expected hint from follower after receiving the first probing message
		expectedHint uint64
		// The expected next probing index after receiving the hint.
		expectedProbIndex uint64
	}{
		// Follower should send a hint 2 because it misses some log entries.
		// Leader should probe entry 1 the next time.
		{
			leaderLog:         []Entry{newConfEntry(1, 1, []string{"1", "2"}, 54321), Entry{Term: 1, Index: 2}, Entry{Term: 1, Index: 3}, Entry{Term: 1, Index: 4}},
			followerLog:       []Entry{newConfEntry(1, 1, []string{"1", "2"}, 54321)},
			expectedHint:      2,
			expectedProbIndex: 1,
		},

		// Follower should not send a hint because of unmatched entries.
		{
			leaderLog:         []Entry{newConfEntry(1, 1, []string{"1", "2"}, 54321), Entry{Term: 1, Index: 2}},
			followerLog:       []Entry{newConfEntry(1, 1, []string{"1", "2"}, 54321), Entry{Term: 2, Index: 2}},
			expectedHint:      0,
			expectedProbIndex: 1,
		},
	}

	for _, test := range tests {
		leader := newCore(Config{ID: "1"}, setupStorage(NilSnapshotMetadata, test.leaderLog, t))
		follower := newCore(Config{ID: "2"}, setupStorage(NilSnapshotMetadata, test.followerLog, t))

		// Force to leader.
		leader.changeState(leader.leader, leader.ID)

		msgs := leader.TakeAllMsgs()
		if len(msgs) != 1 {
			t.Fatalf("expected one message is sent")
		}

		follower.HandleMsg(msgs[0])
		msgs = follower.TakeAllMsgs()
		if len(msgs) != 1 {
			t.Fatalf("expected one message is sent")
		}
		if msgs[0].(*AppEntsResp).Hint != test.expectedHint {
			t.Fatalf("unexpected hint")
		}

		leader.HandleMsg(msgs[0])
		msgs = leader.TakeAllMsgs()
		if len(msgs) != 1 {
			t.Fatalf("expected one message is sent")
		}
		if msgs[0].(*AppEnts).PrevLogIndex != test.expectedProbIndex {
			t.Fatalf("unexpected probing index")
		}
	}
}
