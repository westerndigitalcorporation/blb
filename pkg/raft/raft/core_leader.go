// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package raft

import (
	"sort"

	log "github.com/golang/glog"
)

// coreLeader represents the state of leader.
type coreLeader struct {
	c     *core
	peers map[string]*peer // peers in Raft group, exclude the leader itself.

	// Number of ticks elapsed since last time it checks quorum contacts.
	elapsedSinceLastLeaderCheck uint64
}

func newLeaderState(c *core) *coreLeader {
	return &coreLeader{c: c}
}

func (s *coreLeader) enter() {
	log.Infof("Node: %q is elected as the leader of term %d", s.c.ID, s.c.storage.GetCurrentTerm())
	li := s.c.storage.lastIndex()
	// Initialize peers map.
	s.peers = make(map[string]*peer)
	// Initialize peers' 'state', 'nextIndex' and 'matchIndex'.
	for _, memberID := range s.c.latestConf.Members {
		if memberID == s.c.ID {
			continue
		}
		p := &peer{ID: memberID}
		s.peers[memberID] = p
		// Follow what Raft paper says : initialize 'matchIndex' to 0, 'nextIndex'
		// to the index just after the last one in its log.
		p.matchIndex = 0
		p.nextIndex = li + 1
		p.sendingSnap = false

		// Send out first probing message(aka heartbeat message).
		s.sendAppEnts(p)
	}

	if s.isSingleNodeCluster() {
		s.maybeCommit()
	}
}

func (s *coreLeader) tick() {
	// See if we need to send any AppEnts message to followers.
	for _, p := range s.peers {
		if s.shouldSend(p) {
			// The leader has not sent any messages to this peer for a duration of
			// 'HeartbeatTimeout'.
			//
			// Leader needs to send either normal requests or heartbeat messages to
			// followers periodically to maintain its leadership.
			//
			// Send an AppEnts message here based on peer's state.
			//
			// The purpose of the AppEnts message can be one of the followings(or both):
			//
			//   1. Send as a heartbeat message to prevent follower from campaining for
			//      leadership.
			//
			//   2. Retransmit AppEnts that might be lost last time.
			//
			log.V(10).Infof("Timeout on peer %q, send AppEnts.", p.ID)
			s.sendAppEnts(p)
		}
	}

	s.elapsedSinceLastLeaderCheck++
	if s.c.config.LeaderStepdownTimeout != 0 &&
		s.elapsedSinceLastLeaderCheck > uint64(s.c.config.LeaderStepdownTimeout) {
		s.elapsedSinceLastLeaderCheck = 0
		if !s.checkQuorumActive() {
			// If it loses contact from a quorum, it'll step down as leader.
			log.Infof("@@@ Step down as the leader without quorum contacts for %d ticks", s.c.config.LeaderStepdownTimeout)
			s.c.changeState(s.c.follower, "")
		}
	}
}

// Checks if a peer is still connected to the leader.
func (s *coreLeader) isAlive(p *peer) bool {
	elapsed := s.c.elapsed - p.lastReceiveTime
	if p.sendingSnap {
		// Use a different timeout for snapshot.
		return elapsed < s.c.config.SnapshotTimeout
	}
	return elapsed < s.c.config.LeaderStepdownTimeout
}

// Check if the leader should send a message(AppEnts/Snapshot) to a peer.
func (s *coreLeader) shouldSend(p *peer) bool {
	elapsed := s.c.elapsed - p.lastContactTime
	if p.sendingSnap {
		// Use a different timeout for snapshot.
		return elapsed >= s.c.config.SnapshotTimeout
	}
	return elapsed >= s.c.config.HeartbeatTimeout
}

// Check if a quorum of nodes have responded leader recently.
func (s *coreLeader) checkQuorumActive() bool {
	active := 1 // The leader itself must be active.
	for _, p := range s.peers {
		if s.isAlive(p) {
			active++
		}
	}
	if active < s.c.latestConf.Quorum() {
		// If the number of active members is less than the quorum the leader should
		// step down.
		return false
	}
	return true
}

func (s *coreLeader) handle(msg Msg) {
	switch m := msg.(type) {
	case *VoteReq:
		log.V(10).Infof("[leader] received a VoteReq message %s", msg)
		// Reject this vote since the leader has already been elected for this term.
		msg := &VoteResp{
			BaseMsg: BaseMsg{To: msg.GetFrom()},
			Granted: false,
		}
		s.c.send(msg)

	case *AppEntsResp:
		log.V(10).Infof("[leader] received an AppEntsResp message %s", msg)
		s.handleAppEntsResp(m)

	case *AppEnts, *InstallSnapshot:
		// Sanity check, this should never happen!
		log.Fatalf("[leader] bug: received %s from a peer who is in same term.", m)

	default:
		log.Infof("[leader] ignored message: %v", m)
	}
}

func (s *coreLeader) name() string {
	return stateLeader
}

func (s *coreLeader) sendAppEnts(p *peer) {
	// Send the message.
	if msg, sendSnap := s.getAppEnts(p); sendSnap {
		// If the AppEnts message can't be sent because some entries have been
		// compacted, we gonna ship the snapshot to follower.
		snapReader := s.c.storage.GetSnapshot()
		if snapReader == nil {
			log.Fatalf("can't find snapshot file when we were told to ship the snapshot")
		}
		// Send the snapshot.
		meta := snapReader.GetMetadata()
		s.c.send(&InstallSnapshot{
			BaseMsg:    BaseMsg{To: p.ID},
			LastIndex:  meta.LastIndex,
			LastTerm:   meta.LastTerm,
			Membership: *meta.Membership,
			Body:       snapReader,
		})
		p.sendingSnap = true
	} else {
		//  We are OK to send the AppEnts message.
		s.c.send(msg)
	}

	// We also need to update peer's lastContactTime once leader sent an AppEnts
	// message. From follower's perspective, as long as it can receive AppEnts
	// messages from leader it thinks the leader is alive. So we don't need to
	// distinguish the types of AppEnts messages here.
	p.lastContactTime = s.c.elapsed
}

// getAppEnts returns an AppEnts request that should be sent to the peer
// based on its current state. If no AppEnts can be sent because of log
// compaction, "sendSnap" will be returned as true.
func (s *coreLeader) getAppEnts(p *peer) (msg *AppEnts, sendSnap bool) {
	// Whether leader knows the last agreed entry or not.
	if p.nextIndex != p.matchIndex+1 {
		// we don't know how much our log agrees with the follower's log so we must
		// find out. Don't waste time sending log entries over yet.
		prevLogIndex := p.nextIndex - 1
		prevLogTerm, ok := s.c.storage.term(prevLogIndex)

		if !ok {
			log.V(10).Infof("Failed to get the term for index %d, gonna send a snapshot.", prevLogIndex)
			sendSnap = true
			return
		}

		msg = &AppEnts{
			BaseMsg: BaseMsg{
				To: p.ID,
			},
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      nil,
			LeaderCommit: s.c.committedIndex,
		}
		return
	}

	// If we're here, we know where the last agreed upon entry is.

	// See whether the peer is up-to-date or not, if it's up-to-date then we have
	// no data to send, will send a heartbeat.
	if p.matchIndex == s.c.storage.lastIndex() {
		// The peer is up-to-date, just send an AppEnts message with no entries as
		// heartbeat message.
		prevLogIndex := p.matchIndex
		prevLogTerm, ok := s.c.storage.term(prevLogIndex)

		if !ok {
			// We should always be able to get the term number of the last command.
			log.Fatalf("Failed to get the term number of last index in storage.")
			return
		}

		msg = &AppEnts{
			BaseMsg: BaseMsg{
				To: p.ID,
			},
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      nil,
			LeaderCommit: s.c.committedIndex,
		}
		return
	}

	// If we're here, we know the peer is lag behind, send missing entries to bring
	// it up-to-date.
	//
	// Now we send all entries between :
	// (p.matchIndex, min(last index of leader, p.matchIndex + MaxNumEntsPerAppEnts]
	// to the peer.
	end := min(s.c.storage.lastIndex()+1, p.matchIndex+1+uint64(s.c.config.MaxNumEntsPerAppEnts))

	// Include the index and term of the entry that immediately precedes
	// the first entry that needs to be sent.
	prevLogTerm, ents, ok := s.c.storage.getLogEntries(p.matchIndex+1, end)

	if !ok {
		log.V(10).Infof("Failed to get entries in range [%d, %d), gonna send a snapshot.", p.matchIndex+1, end)
		sendSnap = true
		return
	}

	msg = &AppEnts{
		BaseMsg: BaseMsg{
			To: p.ID,
		},
		PrevLogIndex: p.matchIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      ents,
		LeaderCommit: s.c.committedIndex,
	}
	return
}

// findMajorityIndex returns the highest index of entry that has been acked by a
// majority of peers during its leadership. The entry of returned index is
// guaranteed to be persisted by a majority but it might not be enough to be
// committed given there're some subtleties about when to commit entries after
// leader changes.
func (s *coreLeader) findMajorityIndex() uint64 {
	// Sort 'matchIndex' in descending order and the value at the index of
	// 'quorumSize' - 1 will be the highest index of entry acked by a majority.
	var matchIndices uint64Slice = make([]uint64, 0, len(s.peers)+1)

	if s.c.inLatestConf() {
		// if the leader is in the latest configuration it should count its own
		// 'matchIndex' as well.
		matchIndices = append(matchIndices, s.c.storage.lastIndex())
	}

	for _, peer := range s.peers {
		// Insert peers' "matchIndex".
		matchIndices = append(matchIndices, peer.matchIndex)
	}
	// Sort matchIndices in descending order.
	sort.Sort(matchIndices)
	ci := matchIndices[s.c.latestConf.Quorum()-1]
	return ci
}

func (s *coreLeader) handleAppEntsResp(msg *AppEntsResp) {
	p, ok := s.peers[msg.GetFrom()]
	if !ok {
		// The sender of the message might just be removed from the cluster, ignore
		// the message.
		log.Infof("The responder %q is not in current cluster, might just be removed.", msg.GetFrom())
		return
	}

	// TODO(PL-1160): Leader might send a follow-up request based on follower's
	// response, which is unnecessary if the response is stale. For now we don't
	// try hard to detect the stale responses.
	// Though processing stale response doesn't affect the correctness, we might
	// try as much as we can to detect and ignore them, thus avoid unnecessary
	// network traffic.

	if msg.Index < p.matchIndex {
		// This must be a stale response.
		log.Infof("A stale response from %q", msg.GetFrom())
		return
	}

	p.lastReceiveTime = s.c.elapsed
	p.sendingSnap = false

	if !msg.Success {
		// If AppEnts fails because of log inconsistency: decrement nextIndex and retry.
		// Follower might supply a 'Hint' so instead of drecrementing probing index by
		// one everytime leader can safely decrement 'nextIndex' to 'Hint' to bypass
		// all unnecessary probing messages.
		//
		// 'msg.Index' stores the index that's used for consistency check so we know
		// that the follower and leader differ in that index. We'll decrement the
		// 'nextIdx' and retry.
		//
		// 'msg.Hint' stores the hint from follower if it's not 0.

		if msg.Hint != 0 {
			// Got hint from the follower.
			p.nextIndex = msg.Hint
		} else {
			p.nextIndex = msg.Index // PrevLogIndex of next probing message will be 'nextIndex' - 1.
		}

		// Since AppEnts request has been rejected by this peer, keep probing.
		s.sendAppEnts(p)
		return
	}

	// If we are here, AppEnts succeeded.

	// If AppEnts is successful, update nextIndex and matchIndex for follower(peer).

	// Update 'matchIndex' to 'msg.Index'. If the AppEnts succeeded, 'Index' will
	// be the last matched index identified by that request. See 'AppEnts.Index'
	// in commands.go.
	p.matchIndex = msg.Index
	// 'matchIndex' + 1 should be the first index of subsequent commands we will
	// send to this peer.
	p.nextIndex = p.matchIndex + 1

	if p.matchIndex > s.c.storage.lastIndex() {
		// This is impossible.
		log.Fatalf("bug: ackIdx > last index")
	}

	// See if we need to send a follow-up AppEnts to this peer.
	if p.matchIndex != s.c.storage.lastIndex() {
		// We only send follow-up AppEnts requests to peer who is still not up-to-date.
		log.V(10).Infof("AppEntsResp from %q who's not fully synced, send AppEnts.", p.ID)
		s.sendAppEnts(p)
	}

	// We only need to check if there're any new entries can be committed if
	// AppEnts succeeded.
	s.maybeCommit()
}

func (s *coreLeader) propose(entries ...Entry) {
	// Get the index of the last entry before appending to the log.
	li := s.c.storage.lastIndex()

	// Gives entries correct indices and terms.
	for i := range entries {
		entries[i].Index = li + uint64(i) + 1
		entries[i].Term = s.c.storage.GetCurrentTerm()
	}

	// Append to leader's log first.
	s.c.storage.log.Append(entries...)
	log.V(10).Infof("Leader appended new commands from %d to %d to log", li+1, li+uint64(len(entries)))

	// Send the newly appended commands to peers who were up-to-date before them.
	for _, p := range s.peers {
		if p.matchIndex+1 == p.nextIndex && li+1 == p.nextIndex {
			// We only send new proposed commands to follower who were up-to-date
			// before these newly appended commands.
			// It's not necessary to send these entries right now because we send AppEnts
			// periodically, but doing that might cause unnecessary delay so we still
			// send them right away if we can.
			// For each follower who is not up to date, we are currently waiting for an
			// AppEntsResp from that follower, and when we receive it we can send that
			log.V(10).Infof("New commands proposed and %q is fully synced, send AppEnts", p.ID)
			s.sendAppEnts(p)
		}
	}

	if s.isSingleNodeCluster() {
		s.maybeCommit()
	}
}

func (s *coreLeader) addNode(member string) error {
	// Verify the NOP command of current term has already been committed.
	s.verifyNopCommitted()

	conf := s.c.latestConf
	if containMember(conf.Members, member) {
		return ErrNodeExists
	}
	if !s.c.isLatestConfCommitted() {
		// Can't propose a new reconfiguration before committing the latest one.
		return ErrTooManyPendingReqs
	}

	// If we didn't have an epoch already, generate a new random one. This will
	// allow existing raft groups to get a non-zero epoch.
	if conf.Epoch == 0 {
		conf.Epoch = s.c.rand.Uint64()
	}

	// Update the latest seen configuration.
	newMembers := addMember(conf.Members, member)
	s.c.latestConf = &Membership{
		Index:   s.c.storage.lastIndex() + 1, // the index in log will be 'lastIndex+1'
		Term:    s.c.storage.GetCurrentTerm(),
		Members: newMembers,
		Epoch:   conf.Epoch,
	}
	// Add the newly added peer to the peers map.
	p := &peer{
		ID:              member,
		lastReceiveTime: s.c.elapsed,
		matchIndex:      0,
		nextIndex:       s.c.storage.lastIndex() + 1,
	}
	s.peers[member] = p

	// Propose the membership change.
	s.propose(Entry{Type: EntryConf, Cmd: encodeMembership(*s.c.latestConf)})
	return nil
}

func (s *coreLeader) removeNode(member string) error {
	// Verify the NOP command of current term has already been committed.
	s.verifyNopCommitted()

	conf := s.c.latestConf
	if !containMember(conf.Members, member) {
		return ErrNodeNotExists
	}
	if !s.c.isLatestConfCommitted() {
		// Can't propose a new reconfiguration before committing the latest one.
		return ErrTooManyPendingReqs
	}

	// Do not send anything to removed peer anymore.
	delete(s.peers, member)

	// Update the latest seen configuration.
	newMembers := removeMember(conf.Members, member)
	s.c.latestConf = &Membership{
		Index:   s.c.storage.lastIndex() + 1, // the index in log will be 'lastIndex+1'
		Term:    s.c.storage.GetCurrentTerm(),
		Members: newMembers,
		Epoch:   conf.Epoch,
	}

	// Propose the membership change.
	s.propose(Entry{Type: EntryConf, Cmd: encodeMembership(*s.c.latestConf)})

	// Now we have switched to a new configuration that might have a smaller
	// quorum size, see if any entries can be committed.
	s.maybeCommit()
	return nil
}

// Verify that a command of current term has already been committed.
func (s *coreLeader) verifyNopCommitted() {
	// There's a bug in the Raft dissertation about one node reconfiguration.
	// See: (https://groups.google.com/forum/#!topic/raft-dev/t4xj6dJTP6E)
	//
	// To generalize the problem here:
	//
	// The whole point of simplification that one node reconfiguration provides is
	// based on the fact that if the new configuration S' and the old configuration
	// S differ by only one node, then the quorums of the two have at least a node
	// in common. So even the nodes of the cluster can see S and S' at same time but
	// two quorums can not be formed because of the overlap.
	//
	// So the generalized case of the problem is:
	//
	// 1. The cluster is in configuration S.
	// 2. Now the leader proposes a new configuration S', which only differs one
	//    node from S.
	// 3. Now the leader partially replicates S' to a minority of S and a new leader
	//    without S' in its log gets elected in S.
	// 4. It proposes a new configuration S'', which differs one node from S. The
	//    S'' is partially replicated to a minority of S but given it's used as
	//    soon as it's in the log so new entries might be committed in S'' but not
	//    in S.
	// 5. A node with S' in its log is elected again given S'' and S' might differ more
	//    than one node so there might be two quorums formed.
	//
	// The fix:
	//
	//   "a leader may not append a new configuration entry until it has committed
	//    an entry from its current term"
	//
	// This works because in order to switch to S'' the leader must commit a command
	// with the new term in S so once S'' is proposed the nodes with S' in its log is
	// guaranteed not be able to be elected as leader because it doesn't have a
	// sufficient history in log(a newer term has been committed in S and S and S'
	// have overlap in quorums).

	// This fix works well with our current implementation because we'll propose a
	// NOP command as the first command of a term and before this command is
	// committed we'll not process any requests(including reconfiguration requests)
	// from users. So this is done at Raft layer thus it's impossible for core to receive
	// a reconfiguration request while the latest committed term != current term.

	// Sanity check.
	lt, ok := s.c.storage.term(s.c.committedIndex)
	if !ok {
		// The term of the latest committed index should always be found.
		log.Fatalf("bug: can't get the term of last index in storage.")
	}
	if s.c.storage.GetCurrentTerm() != lt {
		log.Fatalf("bug: not supposed to receive a reconfig request before committing an entry of current term")
	}
}

// Returns true if the cluster only has single node.
func (s *coreLeader) isSingleNodeCluster() bool {
	return len(s.peers) == 0
}

func (s *coreLeader) maybeCommit() {
	majorityIndex := s.findMajorityIndex()

	// There're some subtleties on when to commit new entries.
	// First, we find the N such that the majority of matchIndex[i] >= N. Then
	// we check to see that N > commitIndex and currentTerm == log[N].term
	if majorityIndex > s.c.committedIndex {
		term, ok := s.c.storage.term(majorityIndex)
		if !ok {
			// If "majorityIndex" > "committedIndex" then for sure the command at
			// index "majorityIndex" must be in log because a command can be compacted
			// only when it's applied to snapshot and all commands in snapshot must be
			// committed.
			log.Fatalf("bug: can't find the term number of the command at 'majorityIndex'")
		}

		if term != s.c.storage.GetCurrentTerm() {
			// The term number at 'majorityIndex' doesn't match current term number,
			// we are not able to commit it.
			return
		}

		// Great, some new commands will be committed, we'll:
		// 1. commit these commands on leader side.
		// 2. see if there're any peers that should be notified.

		// for (1)
		s.commitUpTo(majorityIndex)
		// for (2)
		for _, p := range s.peers {
			// We only send newly committed index to the peer who is up-to-date. The
			// rationale behind this is if a peer is not fully synced the new commit
			// index will be piggybacked to it in subsequent syncing requests. But if
			// a peer is fully synced then it might need to wait until next timeout to
			// receive the commit index piggybacked in its heartbeat messages, thus
			// causes some unnecessary delay.
			if p.matchIndex == s.c.storage.lastIndex() {
				log.V(10).Infof("New commands can be committed on %q, who's fully synced, send AppEnts.", p.ID)
				s.sendAppEnts(p)
			}
		}
	}
}

func (s *coreLeader) commitUpTo(index uint64) {
	wasCommitted := s.c.isLatestConfCommitted()
	s.c.commitUpTo(index)
	// See if the latest configuration was just committed.
	if !wasCommitted && s.c.isLatestConfCommitted() {
		// The new configuration was just committed.
		if !s.c.inLatestConf() {
			// The new configuration with the leader removed was just commited.
			// Stepping down so a new leader in the new configuration can be elected.
			log.Infof("The new configuration without the leader is committed, stepping down.")
			s.c.changeState(s.c.follower, "")
		}
	}
}

func min(v1, v2 uint64) uint64 {
	if v1 < v2 {
		return v1
	}
	return v2
}

// uint64Slice implements sort interface.
type uint64Slice []uint64

func (s uint64Slice) Len() int { return len(s) }

// We want it to be sorted in descending order.
func (s uint64Slice) Less(i, j int) bool { return s[i] > s[j] }
func (s uint64Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
