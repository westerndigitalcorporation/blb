// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package raft

import (
	log "github.com/golang/glog"
)

// coreCandidate represents the state of candidate.
type coreCandidate struct {
	c *core
	// Received votes for this candidate on current term.
	votes map[string]bool
	// Actual number of ticks that the candidate has to wait before it starts the
	// election of next term.
	timeoutTicks uint32
}

func newCandidateState(c *core) *coreCandidate {
	return &coreCandidate{c: c}
}

func (s *coreCandidate) enter() {
	if !s.c.inLatestConf() && s.c.isLatestConfCommitted() {
		// The node is not in the latest configuration and the configuration has been
		// committed. It means this node has either been removed successfully or not
		// synced properly. Avoids distrupting the cluster by going back to follower
		// state.
		log.Infof("The node is either removed or not synced, becomes to follower.")
		s.c.changeState(s.c.follower, "")
		return
	}

	s.votes = make(map[string]bool)

	s.c.storage.SaveState(s.c.ID, s.c.storage.GetCurrentTerm()+1)
	if s.c.inLatestConf() {
		// If the node is in the latest configuration, vote for itself.
		//
		// When a node recovers and finds itself is not in the latest configuration
		// it might be still needed to commit the configuration. However, it will not
		// count itself in quorum once it's not in the latest configuration.
		s.votes[s.c.ID] = true
	}

	// Next, we ask everyone else to vote for us.
	li := s.c.storage.lastIndex()
	lt, ok := s.c.storage.term(li)
	if !ok {
		// The term of last index should always be found in storage.
		log.Fatalf("bug: can't get the term of last index in storage.")
	}

	// Broadcast vote request.
	for _, memberID := range s.c.latestConf.Members {
		if memberID == s.c.ID {
			// Skip itself.
			continue
		}
		msg := &VoteReq{
			BaseMsg:      BaseMsg{To: memberID},
			LastLogIndex: li,
			LastLogTerm:  lt,
		}
		s.c.send(msg)
	}

	// We'll wait a little extra time([0, RandomElectionRange]) to avoid split votes.
	extraTicks := s.c.rand.Uint32() % (s.c.config.RandomElectionRange + 1)
	s.timeoutTicks = s.c.config.CandidateTimeout + extraTicks
	log.Infof("Node %q is trying to elect itself with term %d", s.c.ID, s.c.storage.GetCurrentTerm())

	// If the node is the only node in the cluster it should be elected directly.
	// If we didn't do this check here the node would block forever as there's no
	// other nodes send vote response to it.
	s.checkIfElected()
}

func (s *coreCandidate) tick() {
	if s.c.elapsed >= s.timeoutTicks {
		s.c.changeState(s.c.candidate, "")
	}
}

func (s *coreCandidate) handle(msg Msg) {
	switch m := msg.(type) {
	case *VoteResp:
		log.V(10).Infof("[candidate] received a VoteResp message %s", msg)
		s.handleVoteResp(m)

	case *VoteReq:
		log.V(10).Infof("[candidate] received a VoteReq message %s", msg)
		// Reject this vote since the candidate has already voted itself for
		// this term.
		msg := &VoteResp{
			BaseMsg: BaseMsg{To: msg.GetFrom()},
			Granted: false,
		}
		s.c.send(msg)

	case *AppEnts, *InstallSnapshot:
		log.V(10).Infof("[candidate] received message %s", msg)
		// The message with higher term or stale term is handled in "core.HandleMsg".
		// So AppEnts, InstallSnapshot come from a node who has same term and it means
		// that node has already been elected for the term, change to follower state
		// directly.
		s.c.changeState(s.c.follower, msg.GetFrom())

	case *AppEntsResp:
		// AppEntsResp is only to leader, candidate is not supposed to receive them.
		log.Fatalf("[candidate] bug: not supposed to receive %s in candidate state.", m)

	default:
		log.Infof("[candidate] ignored message: %v", m)
	}
}

func (s *coreCandidate) name() string {
	return stateCandidate
}

func (s *coreCandidate) handleVoteResp(resp *VoteResp) {
	if resp.Granted {
		log.V(10).Infof("The vote is granted by %q", resp.GetFrom())
		s.votes[resp.GetFrom()] = true
		s.checkIfElected()
	} else {
		log.V(10).Infof("the vote is rejected by %q", resp.GetFrom())
	}
}

func (s *coreCandidate) checkIfElected() {
	// TODO(PL-1162): Should we also count the number of rejections here?
	// For instance, if a node gets rejected from a quorum, it knows it will not
	// win the election for that term, it can restart an election for next term
	// directly instead of waiting for 'CandidateTimeout'.
	//
	// I also feel the reason of rejection matters. If a node is rejected by
	// a quorum just because of a votes split, then it makes sense to let it
	// re-campaign for the leader of next term. But if it's rejected because of its
	// stale log history, then for sure it will not be elected before its log gets
	// synchronized, so seems it should remain "silent" until some one gets
	// elected?
	//
	// It's hard to decide how hard we want to optimize it, probably we'll figure
	// it out when we have a working Raft implementation.
	if len(s.votes) >= s.c.latestConf.Quorum() {
		// Great, now the node gets votes from a quorum.
		s.c.changeState(s.c.leader, s.c.ID)
	}
}
