// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package raft

import (
	"hash/crc64"
	"math/rand"

	log "github.com/golang/glog"
)

const (
	stateLeader    = "StateLeader"
	stateFollower  = "StateFollower"
	stateCandidate = "StateCandidate"

	stateLeaderInt = iota
	stateFollowerInt
	stateCandidateInt
)

// state is the interface which should be implemented by follower, leader and
// candidate state.
type state interface {
	// The methods below should be common for all states.
	enter()
	tick()
	handle(Msg)
	name() string
}

// Default seed number generator based on ID of a node.
func genSeed(ID string) int64 {
	// Use its CRC64 checksum as seed number.
	seed := crc64.Checksum([]byte(ID), crc64.MakeTable(crc64.ISO))
	return int64(seed)
}

// core implements the core raft protocol. It's up to the caller to receive/send
// messages over the wire and tick the time. By decoupling the two major sources
// (network/time) of nondeterminism from raft implementation, raft becomes very
// testable.
//
// Core layer doesn't have any assumptions about transport layer, messages can be
// lost, out-of-order or delayed. However, a reliable transport is desired in order
// to get good performance.
//
// NOTE: core layer still needs to interact with disk, which still can cause
// nondeterministic behavior.
//
// Core layer is not thread-safe so it must be manipulated by a single goroutine.
type core struct {
	config        Config   // configurations of this Raft instance.
	storage       *Storage // Raft persistent storage.
	state         state    // current state
	leaderID      string   // current leader, or an empty string if leader is unknown.
	ID            string   // ID of this Raft instance.
	GUID          uint64   // GUID of this Raft instance.
	msgs          []Msg    // the messages that need to be sent.
	committedEnts []Entry  // the committed entries that need to be applied to applications.

	latestConf *Membership // The latest configuration in storage.

	// 'needRestoreSnap' will be set to true when core layer has detected a
	// new snapshot file that applications need to restore its state from.
	needRestoreSnap bool

	// Number of ticks since its last state change.
	elapsed uint32

	committedIndex uint64 // the index of the last committed command

	rand *rand.Rand // random number generator.

	follower  state // follower state implementation.
	leader    state // leader state implementation.
	candidate state // candidate state implementation.
}

// Creates a core object that runs Raft core logic.
func newCore(config Config, storage *Storage) *core {
	var seed int64
	if config.GenSeed != nil {
		// Use the seed generator given by users.
		seed = config.GenSeed(config.ID)
	} else {
		// Use the default one.
		seed = genSeed(config.ID)
	}

	c := &core{
		ID:      config.ID,
		GUID:    storage.GetMyGUID(),
		config:  config,
		storage: storage,
		rand:    rand.New(rand.NewSource(seed)),
	}

	// Initialize the configuration from storage.
	c.initLatestConf()

	// If there's a snapshot file exists at startup time, restore state
	// from it first.
	snapMeta, _ := c.storage.GetSnapshotMetadata()
	if snapMeta == NilSnapshotMetadata {
		log.Infof("No snapshot file detected at startup time.")
	} else {
		log.Infof("Snapshot file detected at startup time.")
		c.commitUpTo(snapMeta.LastIndex)
	}

	c.follower = newFollowerState(c)
	c.candidate = newCandidateState(c)
	c.leader = newLeaderState(c)

	log.Infof("Node %q starts with seed: %#x", config.ID, seed)
	// Start as follower.
	c.changeState(c.follower, "")
	return c
}

// proposeInitialMembership adds a log entry with an initial configuration (set
// of members). Raft must be in a clean state.
func (c *core) proposeInitialMembership(m Membership) error {
	if !c.storage.IsInCleanState() || c.state != c.follower {
		return ErrAlreadyConfigured
	}

	// Initialize the state and propose the static configuration as the first
	// entry in log.
	ent := Entry{
		Type:  EntryConf,
		Cmd:   encodeMembership(m),
		Index: 1,
		Term:  1,
	}
	c.storage.SaveState("", 1)
	c.storage.log.Append(ent)
	c.latestConf = decodeConfEntry(ent)
	log.Infof("Proposing initial membership %+v", c.latestConf)

	return nil
}

// Propose proposes one or multiple entries to Raft group. Only leader
// can propose entries so requests will fail if this node is non-leader.
// "entries" doesn't have to have correct indices and terms, Raft will
// overwrite the index and term of the entries to the correct ones.
func (c *core) Propose(entries ...Entry) error {
	if c.state != c.leader {
		return ErrNodeNotLeader
	}
	c.state.(*coreLeader).propose(entries...)
	return nil
}

// HandleMsg handles an incoming message.
func (c *core) HandleMsg(msg Msg) {
	if snapMsg, ok := msg.(*InstallSnapshot); ok {
		// Ensure 'Body' gets closed after processing it for every 'InstallSnapshot'
		// messgae.
		defer snapMsg.Body.Close()
	}

	if (msg.GetTo() != "" && msg.GetTo() != c.ID) || (msg.GetToGUID() != 0 && msg.GetToGUID() != c.GUID) {
		log.Errorf("Received a message for %s:%d, but my ID is %s:%d", msg.GetTo(), msg.GetToGUID(), c.ID, c.GUID)
		return
	}

	if expectedGUID := c.storage.GetGUIDFor(msg.GetFrom()); expectedGUID != 0 {
		if expectedGUID != msg.GetFromGUID() {
			log.Errorf("Received a message from %s:%d, but last GUID was %d", msg.GetFrom(), msg.GetFromGUID(), expectedGUID)
			return
		}
	} else {
		// Save this GUID so we can compare next time.
		log.Infof("recording that %s has guid %d", msg.GetFrom(), msg.GetFromGUID())
		c.storage.SetGUIDFor(msg.GetFrom(), msg.GetFromGUID())
	}

	if msgE, myE := msg.GetEpoch(), c.getEpoch(); msgE != 0 && myE != 0 && msgE != myE {
		log.Errorf("Received a message from %s:%d with mismatched epoch (%d != %d)",
			msg.GetFrom(), msg.GetFromGUID(), msgE, myE)
		return
	}

	if msg.GetTerm() < c.storage.GetCurrentTerm() {
		// It's always safe to ignore messages with stale terms.
		log.V(3).Infof("Received a message %s with a stale term, ignore it.", msg)
		return
	}

	// Once a node received a message with a higher term, it will change to
	// follower state.
	if msg.GetTerm() > c.storage.GetCurrentTerm() {
		var voteFor string
		var leaderID string
		switch msg.(type) {
		case *AppEnts, *InstallSnapshot:
			// Since AppEnts, InstallSnapshot can only come from elected leader, and
			// the leader has a higher term than the node itself. So we simply update
			// "voteFor" here to prevent this node from granting vote to other
			// candidates who campaign for the same term. This is not required for
			// correctness, it's just an optimization.
			voteFor = msg.GetFrom()
			// The sender must be the elected leader of the term.
			leaderID = msg.GetFrom()

		case *VoteReq:
			// We are not sure if we can grant vote to the sender or not, so we simply
			// set an emtpy string to "voteFor", we will update "currentTerm" first and
			// transition to follower state and handle VoteReq message there.
			voteFor = ""
			leaderID = ""

		default:
			// All other messages are just responses to sender(the node itself), so they are
			// not supposed to have a higher term.
			log.Fatalf("Not expected to receive message %s with a higher term", msg)
		}
		c.storage.SaveState(voteFor, msg.GetTerm())
		c.changeState(c.follower, leaderID)
	}
	// Handle this message based on which state it is in.
	c.state.handle(msg)
}

// Tick increases the logic time by one.
func (c *core) Tick() {
	c.elapsed++
	c.state.tick()
}

// TakeAllMsgs takes out all the messages that need to be sent. It's caller's
// responsibility to send these message over the wire.
func (c *core) TakeAllMsgs() (msgs []Msg) {
	msgs = c.msgs
	c.msgs = nil
	return
}

// TakeNewlyCommitted returns all newly committed entries since last time
// it was called. It's caller's responsibility to deliver these entries to
// applications.
func (c *core) TakeNewlyCommitted() (commits []Entry) {
	commits = c.committedEnts
	c.committedEnts = nil
	return
}

// TrimLog trims the log based on the index of last applied command to
// snapshot file.
func (c *core) TrimLog(lastSnapIdx uint64) {
	// log entries [:lastSnapIdx] are contained within the snapshot, so we can
	// remove them from the log without affecting the correctness.
	fi, li, empty := c.storage.log.GetBound()

	if empty || lastSnapIdx == fi-1 {
		// Nothing needs to be compacted.
		//
		// "lastSnapIdx == fi-1" can only happen if users take a snapshot with the
		// same state taken by last snapshot and the last snapshot has trimmed the
		// whole log. So nothing needs to be trimmed anymore.
		return
	}

	// sanity check.
	if lastSnapIdx < fi || lastSnapIdx > li {
		log.Fatalf("bug: not in a valid state. first index: %d, last index: %d, lastSnapIdx: %d", fi, li, lastSnapIdx)
	}

	if lastSnapIdx-fi < c.config.LogEntriesAfterSnapshot {
		// The number of entries we can trim is fewer than the number of entries we
		// are told to keep, so we don't remove any.
		return
	}

	// Keep the last "c.config.LogEntriesAfterSnapshot" after snapshot so we can use them to
	// sync slow followers.
	trimTo := lastSnapIdx - c.config.LogEntriesAfterSnapshot

	log.Infof("Trimming the log up to index %d", trimTo)
	c.storage.log.Trim(trimTo)
}

func (c *core) AddNode(member string) error {
	if c.state != c.leader {
		return ErrNodeNotLeader
	}
	return c.state.(*coreLeader).addNode(member)
}

func (c *core) RemoveNode(member string) error {
	if c.state != c.leader {
		return ErrNodeNotLeader
	}
	return c.state.(*coreLeader).removeNode(member)
}

func (c *core) changeState(state state, leaderID string) {
	if c.state == nil {
		// It means it's first time called 'changeState'.
		log.Infof("Node %q initializes state to %q.", c.ID, state.name())
	} else {
		log.Infof("Node %q changes state from %q to %q.", c.ID, c.state.name(), state.name())
	}
	c.leaderID = leaderID
	c.elapsed = 0
	c.state = state
	c.state.enter()
}

func (c *core) send(msg Msg) {
	msg.SetTerm(c.storage.GetCurrentTerm())
	msg.SetFrom(c.ID)
	c.msgs = append(c.msgs, msg)
}

// Initializes the latest configuration from storage(log & snapshot). This call
// might be expensive as it needs to go through log and snapshot.
func (c *core) initLatestConf() {
	// Everything in snapshot is committed, so start finding configuration from
	// snapshot first.
	snapMeta, _ := c.storage.GetSnapshotMetadata()
	latest := snapMeta.Membership

	// Extract the latest configuration from the log, if any.
	if _, empty := c.storage.log.LastIndex(); !empty {
		iter := c.storage.log.GetIterator(snapMeta.LastIndex + 1)
		defer iter.Close()

		var lastEnt Entry // the last configuration entry in log.
		for iter.Next() {
			if ent := iter.Entry(); ent.Type == EntryConf {
				lastEnt = ent
			}
		}
		if iter.Err() != nil {
			log.Fatalf("Failure during iterator: %v", iter.Err())
		}

		if lastEnt.Index != 0 {
			// Found the last configuration entry in log.
			latest = decodeConfEntry(lastEnt)
		}
	}

	c.latestConf = latest
	log.Infof("The latest configuration found is %+v", latest)
}

// Returns true if the node is in the latest configuration.
func (c *core) inLatestConf() bool {
	return c.latestConf != nil && containMember(c.latestConf.Members, c.ID)
}

// Is the latest configuration committed? If it returns false the configuration
// might still have been committed but the node doesn't know yet.
func (c *core) isLatestConfCommitted() bool {
	return c.latestConf == nil || c.latestConf.Index <= c.committedIndex
}

func (c *core) commitUpTo(index uint64) {
	if snapMeta, _ := c.storage.GetSnapshotMetadata(); snapMeta != NilSnapshotMetadata {
		// Once a snapshot is received we'll also call commitUpTo with the index
		// of snapshot file. So first we need to check if this is a commit for
		// snapshot file.
		if snapMeta.LastIndex > c.committedIndex {
			// Once we received a snapshot we'll bump the committedIndex to the index of
			// snapshot file. So if the committedIndex before the update is smaller than
			// the index of snapshot we know this must be a commit for snapshot file.

			// Sanity check the commit index must equal the snapshot index.
			if snapMeta.LastIndex != index {
				log.Fatalf("bug: commit index != snap index")
			}
			log.Infof("Committing snapshot %s", snapMeta)
			c.committedIndex = snapMeta.LastIndex
			// Set it to true so Raft will ask FSM to restore from it.
			c.needRestoreSnap = true
			return
		}
	}

	wasConfCommitted := c.isLatestConfCommitted()

	// Otherwise we committed some entries in log.
	ents := c.storage.log.Entries(c.committedIndex+1, index+1)
	c.committedEnts = append(c.committedEnts, ents...)
	c.committedIndex = index

	if !wasConfCommitted && c.isLatestConfCommitted() {
		// We just committed a conf change. We should forget the GUIDs of nodes that are
		// no longer members, so that we can later add them back with a new GUID.
		c.storage.FilterGUIDs(c.latestConf.Members)
	}
}

func (c *core) getEpoch() uint64 {
	if c.latestConf != nil {
		return c.latestConf.Epoch
	}
	return 0
}

func stateToInt(state string) int {
	switch state {
	case stateLeader:
		return stateLeaderInt
	case stateFollower:
		return stateFollowerInt
	case stateCandidate:
		return stateCandidateInt
	default:
		return -1
	}
}
