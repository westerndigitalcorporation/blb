// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package raft

import (
	"io"

	log "github.com/golang/glog"
)

// coreFollower represents the state of follower.
type coreFollower struct {
	c *core
	// The time in ticks (as measured by core.elapsed) when we last received an
	// AppEnts from the leader of current term
	lastContact uint32
	// Actual number of ticks that the follower has to wait before it starts the
	// election of next term.
	timeoutTicks uint32
}

func newFollowerState(c *core) *coreFollower {
	return &coreFollower{c: c}
}

func (s *coreFollower) enter() {
	// We'll wait a little extra time([0, RandomElectionRange]) to avoid split votes.
	extraTicks := s.c.rand.Uint32() % (s.c.config.RandomElectionRange + 1)
	s.timeoutTicks = s.c.config.FollowerTimeout + extraTicks
	// Initialze the last leader contact time to current tick, otherwise its
	// default value will be zero so the follower will claim leader unreachable
	// when next "tick" is called.
	s.lastContact = s.c.elapsed
}

func (s *coreFollower) tick() {
	if s.c.elapsed-s.lastContact >= s.timeoutTicks {
		log.Infof("Follower %q couldn't hear from leader, become candidate", s.c.ID)
		s.c.changeState(s.c.candidate, "")
	}
}

func (s *coreFollower) handle(msg Msg) {
	switch m := msg.(type) {
	case *AppEnts:
		log.V(10).Infof("[follower] received an AppEnts message %s", msg)
		// AppEnts message can only come from leader, so if the node has not
		// granted vote of current term to anyone yet we'll grant the vote to the sender
		// directly.
		// This is not required for correctness, it's only for optimization.
		if s.c.storage.GetVoteFor() == "" {
			s.c.storage.SetVoteFor(msg.GetFrom())
		}
		// Sanity check, leaderID records the leader of current term, it will be
		// an empty string if the leader of current term is unknown. a node is not
		// supposed to receive messages from multiple leaders during same term.
		if s.c.leaderID == "" {
			s.c.leaderID = m.GetFrom()
		} else if s.c.leaderID != msg.GetFrom() {
			log.Fatalf("Follower received %s not from its leader %s", m, s.c.leaderID)
		}
		s.handleAppEnts(m)

	case *VoteReq:
		log.V(10).Infof("[follower] received a VoteReq message %s", msg)
		granted := false
		if s.canGrantVote(m) {
			granted = true
			s.c.storage.SetVoteFor(msg.GetFrom())
		}
		msg := &VoteResp{
			BaseMsg: BaseMsg{To: msg.GetFrom()},
			Granted: granted,
		}
		s.c.send(msg)

	case *InstallSnapshot:
		log.V(10).Infof("[follower] received a InstallSnapshot message %s", msg)
		// InstallSnapshot message can only come from leader, so if the node has not
		// granted vote of current term to anyone yet we'll grant the vote to the sender
		// directly.
		// This is not required for correctness, it's only for optimization.
		if s.c.storage.GetVoteFor() == "" {
			s.c.storage.SetVoteFor(msg.GetFrom())
		}
		// Sanity check, leaderID records the leader of current term, it will be
		// an empty string if the leader of current term is unknown. a node is not
		// supposed to receive messages from multiple leaders during same term.
		if s.c.leaderID == "" {
			s.c.leaderID = m.GetFrom()
		} else if s.c.leaderID != msg.GetFrom() {
			log.Fatalf("Follower received %s not from its leader %s", m, s.c.leaderID)
		}
		s.handleSnapshot(m)

	case *AppEntsResp:
		// Stale response.

	default:
		log.Infof("[follower] ignored message: %v", m)
	}
}

func (s *coreFollower) name() string {
	return stateFollower
}

// canGrantVote returns true if the 'VoteReq' can be granted, otherwise returns
// false.
func (s *coreFollower) canGrantVote(vote *VoteReq) bool {
	// For reference, this is the receiver implementation of RequestVote RPC
	//
	//   1. Reply false if term < currentTerm
	//   2. If voteFor is null or candidateId, and candidate's log is at least as
	//      up-to-date as receiver's log , grant vote.

	// --------------  Step 1 ---------------
	// We have already done 1 at previous step(see core.HandleMsg)

	// --------------  Step 2 ---------------

	// Raft has two restrictions on leader election:
	// (1). At most one leader can be elected for one term
	// (2). Elected leader must have all committed commands.
	//
	// To satisfy (1), one node can only grant one vote to one candidate for one
	// term.
	// To satisfy (2), one node can only grant vote to a candidate who has at least
	// as "up-to-date"(see definition below) log history as the log history of
	// itself.
	//
	// "up-to-date" definition:
	// either 1) candidate's last log term > voter's last log term OR
	//        2) candidate's last log term == voter's last log term but voter's last log
	//           index >= voter's last log index.

	// check for (1)
	voteFor := s.c.storage.GetVoteFor()
	if voteFor != "" && voteFor != vote.GetFrom() {
		// Means the node has already granted a vote to some other candidate.
		return false
	}

	// check for (2)
	li := s.c.storage.lastIndex()
	lt, ok := s.c.storage.term(li)
	if !ok {
		// The term of last index should always be found in storage.
		log.Fatalf("bug: can't get the term of last index in storage.")
	}

	if vote.LastLogTerm > lt ||
		(vote.LastLogTerm == lt && vote.LastLogIndex >= li) {
		// Means the candidate's log history is at least as up-to-date as the node's.
		return true
	}
	return false
}

func (s *coreFollower) handleSnapshot(msg *InstallSnapshot) {
	// For reference, this is the receiver implementation of InstallSnapshot RPC.
	//
	//   1. Reply immediately if term < currentTerm.
	//   2. Ignore the message if the last index in received snapshot <= the current
	//      snapshot's. It means it's a duplicated or stale message so do not waste IO
	//      to persist it.
	//   3. Write to snapshot file and commit.
	//   4. If existing log entry has same index and term as lastIndex and lastTerm,
	//      discard log up through lastIndex (but retain any following entries) and
	//      reply.
	//   5. Discard the entire log.
	//   6. Reset state machine using snapshot contents (and load lastConfig as
	//      cluster configuration)

	// --------------  Step 1 ---------------
	// We have already done 1 at previous step(see core.HandleMsg)

	// Update the last contact time of leader.
	s.lastContact = s.c.elapsed

	// --------------  Step 2 ---------------

	meta, _ := s.c.storage.GetSnapshotMetadata()
	if meta != NilSnapshotMetadata && meta.LastIndex >= msg.LastIndex {
		// We already have a snapshot file that is at least as up-to-date
		// as the received one, so it should be a stale or duplicate message,
		// replying with current index of snapshot file directly.
		log.Infof("Already has snapshot(last index:%d), ignore the received one(last index:%d)",
			meta.LastIndex, msg.LastIndex)

		// Ack leader with the current last index of snapshot file.
		resp := &AppEntsResp{
			BaseMsg: BaseMsg{
				To: msg.GetFrom(),
			},
			Success: true,
			Index:   meta.LastIndex,
		}
		s.c.send(resp)
		return
	}

	// --------------  Step 3 ---------------

	// First create a snapshot file.
	writer, err := s.c.storage.BeginSnapshot(SnapshotMetadata{
		LastIndex:  msg.LastIndex,
		LastTerm:   msg.LastTerm,
		Membership: &msg.Membership,
	})

	if err != nil {
		// We just return directly if we failed to create a snapshot file, this will not affect
		// the correctness since the snapshot file will not be visible in future. From
		// the perspective of sender, this message is just lost.
		log.Errorf("Failed to create a snashot: %v", err)
		return
	}

	// Copy will use a 32KB internal buffer to do the data transferring so
	// the memory footprint will be bounded.
	_, err = io.Copy(writer, msg.Body)
	if err != nil {
		// We just return directly if we failed to write data to the snapshot file, this will
		// not affect the correctness since the snapshot will not be visibile. From the
		// perspective of sender, this message is just lost.
		log.Errorf("Failed to write received data to snapshot file: %v", err)
		writer.Abort()
		return
	}

	// If we are here we have successfully written a more "recent" snapshot file.

	// Make it "effective".
	if err := writer.Commit(); err != nil {
		log.Errorf("Failed to make the snapshot file visible and durable %v", err)
		return
	}

	// If we are here, we've made the snapshot file "effective".
	log.Infof("The snapshot file is committed to disk successfully with last index %d, term: %d.", msg.LastIndex, msg.LastTerm)

	if s.c.storage.inLog(msg.LastIndex, msg.LastTerm) {
		// --------------  Step 4 ---------------
		log.Infof("The log contains the last applied entry in snapshot, fast forward.")
		// Fast-forward.
		s.c.TrimLog(msg.LastIndex)
	} else {
		// --------------  Step 5 ---------------
		// Truncate the whole log.
		log.Infof("The log doesn't contain the last applied entry in snapshot, discard the entire log.")
		s.c.storage.log.Truncate(0)

		// Update the latest configuration from the received snapshot.
		s.c.latestConf = &msg.Membership
	}

	if msg.LastIndex > s.c.committedIndex {
		// --------------  Step 6 ---------------
		s.c.commitUpTo(msg.LastIndex)
	}

	// Ack the leader.
	resp := &AppEntsResp{
		BaseMsg: BaseMsg{
			To: msg.GetFrom(),
		},
		Success: true,
		// Attach last index to the response so leader can update its 'matchIndex'.
		Index: msg.LastIndex,
	}
	s.c.send(resp)
}

// handleAppEnts handles 'AppEnts' message.
func (s *coreFollower) handleAppEnts(msg *AppEnts) {
	// For reference, this is the receiver implementation of AppendEntries RPC
	//
	//   1. Reply false if term < currentTerm
	//   2. Reply false if log doesn't contain an entry at prevLogIndex whose
	//      term matches prevLogTerm.
	//   3. If an existing entry conflicts with a new one, delete the existing
	//      entry and all that follow it.
	//   4. Append any new entries not already in the log.
	//   5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit,
	//      index of last new entry)

	// --------------  Step 1 ---------------
	// We have already done 1 at previous step(see core.HandleMsg)

	// Update the last contact time of leader.
	s.lastContact = s.c.elapsed

	// First we need to have the consistency check -- see if an entry with index
	// 'msg.PrevLogIndex' and term 'msg.PrevLogTerm' exists in log or snapshot.
	if !s.c.storage.hasEntry(msg.PrevLogIndex, msg.PrevLogTerm) {
		//
		// --------------  Step 2 ---------------
		//
		// The entry with given index and term exists in neither log nor snapshot, the
		// request will be rejected.

		var hint uint64
		li := s.c.storage.lastIndex()
		if li < msg.PrevLogIndex {
			// If the follower misses some log entries it will give the leader a hint
			// to bypass the unncessary probing for these missing entries.
			//
			// NOTE: leader will set 'nextIndex' to the 'hint' so the next time
			// leader will probe entry of index 'li' (leader will always probe
			// 'nextIndex'-1)
			hint = li + 1
		}

		resp := &AppEntsResp{
			BaseMsg: BaseMsg{
				To: msg.GetFrom(),
			},
			Success: false,
			Index:   msg.PrevLogIndex,
			Hint:    hint,
		}
		s.c.send(resp)
		return
	}

	// If we're here, a command with the given msg.PrevLogIndex and msg.PrevLogTerm
	// exists in either log or storage, so we match the leader's log up to entry
	// msg.PrevLogIndex.

	// --------------  Step Not Specified In Paper ---------------

	// Before leader finds where it and the follower's log match, leader will send
	// AppEnts with no entries to probe the last agreed point. This is specified
	// in the dissertation of Raft but not in paper. Also this might be just a pure
	// heartbeat message.

	if msg.Entries == nil {
		// Means this is either a probing message or a pure heartbeat message.
		// NOTE: we will not truncate the log until we find the actual conflict entries.
		// This is a crucial requirement for handling stale or duplicate message.
		resp := &AppEntsResp{
			BaseMsg: BaseMsg{
				To: msg.GetFrom(),
			},
			Success: true,
			// Attach last index to the response so leader can update its 'matchIndex'.
			Index: msg.PrevLogIndex,
		}
		s.c.send(resp)

		// --------------  Step 5 ---------------
		// We haven't changed the log, but a new value of LeaderCommit might allow us
		// to commit log entries we already have.
		s.maybeCommit(msg.LeaderCommit, msg.PrevLogIndex)
		return
	}

	// This is not a probing message or pure heartbeat message, we'll append the
	// entries of this request to follower's log if they are not already there.

	ci, anyConflict := s.conflictIndex(msg.Entries)

	if anyConflict {
		//
		// --------------  Step 3 ---------------
		//
		// follower contains some entries which are conflict with the entries in
		// the AppEnts, gonna truncate them.
		//
		// 'ci' is the index of the first divergent entry, so remove all entries
		// after that(including it).
		s.c.storage.log.Truncate(ci - 1)

		if s.c.latestConf != nil && s.c.latestConf.Index >= ci {
			// The latest configuration has been truncated so "s.c.latestConf" is not
			// correct anymore. Re-initialize it from storage.
			//
			// TODO: this could be optimized but we assume the truncation of the
			// latest configuration should be very rare, so choose this simple
			// solution.
			s.c.initLatestConf()
		}
	}

	// --------------  Step 4 ---------------
	//
	// If we're here, it means either the follower doesn't have any conflict entries
	// with the entries in AppEnts OR the follower has already truncated all
	// conflicting entries. We will append any new entries not in its log.

	li := msg.Entries[len(msg.Entries)-1].Index
	if li <= s.c.storage.lastIndex() {
		// It means all the entries sent from leader are included in follower's snapshot
		// or log. It's probably a duplicate or stale message, we'll just ack the leader
		// directly.
		resp := &AppEntsResp{
			BaseMsg: BaseMsg{
				To: msg.GetFrom(),
			},
			Success: true,
			Index:   li,
		}
		s.c.send(resp)

		// --------------  Step 5 ---------------
		// If it's a stale request, the follower should have already handled this
		// LeaderCommit before, but it doesn't harm to call it again here.
		s.maybeCommit(msg.LeaderCommit, li)
		return
	}

	// If we're here, it means there're some new entries need to be appended to
	// follower's log.

	// The index of first entry.
	fi := msg.Entries[0].Index
	// Entries those need to be appended(exclude those are already in log)
	ents := msg.Entries[s.c.storage.lastIndex()-fi+1:]

	if ents[0].Index != s.c.storage.lastIndex()+1 {
		// TODO: Sanity check, remove it when Raft impl is more stable.
		log.Fatalf("bug: the index of first entry to append is not contiguous with the index of last entry in log.")
	}

	// If the entries to be appended contain any new configuration, update the
	// 'latestConf' accordingly.
	for _, ent := range ents {
		if ent.Type == EntryConf {
			s.c.latestConf = decodeConfEntry(ent)
		}
	}

	s.c.storage.log.Append(ents...)

	// Ack leader.
	resp := &AppEntsResp{
		BaseMsg: BaseMsg{
			To: msg.GetFrom(),
		},
		Success: true,
		// Attach last index to the response so leader can update its 'matchIndex'.
		Index: li,
	}
	s.c.send(resp)

	// --------------  Step 5 ---------------
	// Acknowledgements of AppEnts from followers(either others or itself) may
	// advance LeaderCommit, we still need to check LeaderCommit here and commit
	// new commands if there're any.
	s.maybeCommit(msg.LeaderCommit, li)
}

// Given a list of entries need to be appended, conflictIndex returns the index of
// the first entry that is conflict with the entries in follower's log. "confIdx"
// will be the index of first conflicting entry. If there's no conflicts, 'anyConf'
// will be returned with false.
func (s *coreFollower) conflictIndex(ents []Entry) (confIdx uint64, anyConf bool) {
	if ents[0].Index == s.c.storage.lastIndex()+1 {
		// The entries are contiguous with the existing entries in follower's storage.
		// So there's no conflicts.
		return 0, false
	}
	if ents[0].Index > s.c.storage.lastIndex()+1 {
		// There's a gap bewteen last entry in follower's storage and the first entry sent
		// from its leader. This is impossible.
		log.Fatalf("bug: there's gap between last entry in follower's storage and first entry sent from leader")
	}

	// If we are here, it means there're some overlaps between the entries in
	// follower's storage and the entries in AppEnts, let's find out if there're any
	// conflicting entries.

	// Conflict entries can only exist in follower's log. For entries that are
	// trimmed due to log compaction, they are included in the snapshot and must
	// already be committed, so they must have no conflicts with entries from
	// leader.

	// startIndex is the index of the first entry we will compare between entries
	// in log and AppEnts.
	// endIndex is the index of the last entry we will compare between entries in
	// log and AppEnts.
	var startIndex, endIndex uint64

	// See if there's a snapshot already.
	meta, _ := s.c.storage.GetSnapshotMetadata()
	if meta != NilSnapshotMetadata {
		if meta.LastIndex >= ents[len(ents)-1].Index {
			// All entries in AppEnts are included in snapshot. Conflicts are impossible.
			return 0, false
		}
		// Skip entries that are included in snapshot given they might be trimmed
		// from the log, if there's any.
		startIndex = max(ents[0].Index, meta.LastIndex+1)
	} else {
		// No snapshot detected, start from the index of first entry in the AppEnts.
		startIndex = ents[0].Index
	}

	lastLogIndex, empty := s.c.storage.log.LastIndex()
	if empty {
		// If the log is empty, it means either the entire logs have been compacted
		// or there's no history yet. Conflicts are impossible.
		return 0, false
	}

	// Find out the index of the last entry to compare.
	endIndex = min(lastLogIndex, ents[len(ents)-1].Index)
	// Get these entries from log.
	entsInLog := s.c.storage.log.Entries(startIndex, endIndex+1)
	offset := int(startIndex - ents[0].Index)

	// Find out if there's any conflicts.
	for idx := 0; idx < int(endIndex-startIndex)+1; idx++ {
		if ents[idx+offset].Term != entsInLog[idx].Term {
			return ents[idx+offset].Index, true
		}
	}
	return 0, false
}

// maybeCommit is called with the commit index sent from leader. "leaderCommit"
// is the commitIndex on leader side, "matchIndex" is the index of last entry
// known in leader's log from an AppEnts request.
func (s *coreFollower) maybeCommit(leaderCommit, matchIndex uint64) {
	if min(matchIndex, leaderCommit) > s.c.committedIndex {
		s.c.commitUpTo(min(matchIndex, leaderCommit))
	}
}

func max(v1, v2 uint64) uint64 {
	if v1 > v2 {
		return v1
	}
	return v2
}
