// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package raft

import (
	"fmt"
	"io"

	log "github.com/golang/glog"
)

// NilSnapshotMetadata is a special value that should be returned when there's no
// snapshot file found.
var NilSnapshotMetadata = SnapshotMetadata{0, 0, nil}

// Membership represents one proposed cluster membership.
// It is used in-memory, as well as encoded in the log and snapshots for
// configuration messages.
type Membership struct {
	// Members is the cluster members.
	Members []string

	// Epoch is an id for this instance of raft. It will never change after the
	// initial configuration. It is used to detect misconfigurations and prevent
	// corruption.
	Epoch uint64

	// Index is the index of the change in log.
	// (When serialized in logs/snapshots, Index is unused.)
	Index uint64

	// Term is the term of the change in log.
	// (When serialized in logs/snapshots, Term is unused.)
	Term uint64
}

// Quorum returns the quorum size of the configuration.
func (m *Membership) Quorum() int {
	return len(m.Members)/2 + 1
}

// State is the interface of storing/retrieving the state of Raft.
type State interface {
	// SaveState persists states 'voteFor' and 'currentTerm' to disk atomically.
	SaveState(voteFor string, term uint64)

	// GetState returns the persisted states "voteFor" and "currentTerm"
	GetState() (voteFor string, term uint64)

	// GetCurrentTerm returns state "currentTerm".
	GetCurrentTerm() uint64

	// SetCurrentTerm modifies state "currentTerm", state "voteFor" will not be
	// changed.
	SetCurrentTerm(uint64)

	// GetVoteFor returns state "voteFor".
	GetVoteFor() string

	// SetVoteFor modifies state "voteFor", state "currentTerm" will not be changed.
	SetVoteFor(string)

	// GetMyGUID gets this node's GUID.
	GetMyGUID() uint64

	// GetGUIDFor gets the known GUID for the given node, or an empty string if
	// no messages from that node have been received.
	GetGUIDFor(id string) uint64

	// SetGUIDFor sets the known GUID for the given node.
	SetGUIDFor(id string, guid uint64)

	// FilterGUIDs forgets about GUIDs of nodes that are not in the given set of ids.
	FilterGUIDs(ids []string)
}

// SnapshotManager is the interface of managing snapshot data. SnapshotManager
// must be implemented in a thread-safe way so that one writer and multiple
// readers can access 'SnapshotManager' concurrently.
type SnapshotManager interface {
	// BeginSnapshot returns a "SnapshotFileWriter" to which applications can dump
	// their state.
	BeginSnapshot(SnapshotMetadata) (SnapshotFileWriter, error)

	// GetSnapshot returns a "SnapshotFileReader" of the latest snapshot file from
	// which applications can restore their state. If reader is returned as nil it
	// means there's no snapshot file found.
	GetSnapshot() SnapshotFileReader

	// GetSnapshotMetadata returns the metadata of the latest file, and
	// (optionally) a path to a file on disk where the snapshot is stored. If
	// there's no snapshot file found 'NilSnapshotMetadata', "" will be
	// returned.
	// The file path is only indended for backups. Other packages shouldn't
	// assume anything about the snapshot file format, and not all storage
	// implementations may provide it.
	GetSnapshotMetadata() (SnapshotMetadata, string)
}

// SnapshotMetadata represents the metadata of a snapshot.
type SnapshotMetadata struct {
	// The last index of applied command to the snapshot.
	LastIndex uint64
	// The term of last index of applied command to the snapshot.
	LastTerm uint64
	// Membership information in snapshot.
	Membership *Membership
}

// String returns a human-readable string of SnapshotMetadata.
func (m SnapshotMetadata) String() string {
	return fmt.Sprintf("SnapshotMetadata{LastIndex:%d, LastTerm:%d, Membership: %+v}",
		m.LastIndex, m.LastTerm, m.Membership)
}

// SnapshotFileWriter represents a writer to a newly created snapshot file.
type SnapshotFileWriter interface {
	io.Writer

	// Commit should be called once we have written all state to the file.
	// If everything goes smoothly and "Commit" succeeds, the snapshot file will
	// become "effective"(durable and visible).
	Commit() error

	// Abort aborts a pending snapshot file.
	Abort() error

	// GetMetadata returns the metadata of the snapshot file.
	GetMetadata() SnapshotMetadata
}

// SnapshotFileReader represents a reader to an existing snapshot file.
type SnapshotFileReader interface {
	io.ReadCloser

	// GetMetadata returns the meatadata of this snapshot file.
	GetMetadata() SnapshotMetadata
}

// Log defines the interface of Raft log and this the only log interface that
// Raft implementation will interact with. It stores a sequence of entries and the
// indices of these entries must be contiguous in log.
//
// For now Raft doesn't have much to do when IO error occurs. Now the IO error
// handling is done inside log, once an IO error occurs, it'll simply panic.
//
// TODO(PL-1130): Log should be an actual implementation instead of an
// interface, and it should be built on top of an on-disk log implementation which
// is passed to it. Here we define it as an interface so we can have a mock
// implementation to test Raft first without worrying about on-disk interface
// definition and implementation.
type Log interface {
	// The index of first entry in log. If there's no entry in log,
	// empty will be returned as true.
	FirstIndex() (index uint64, empty bool)

	// The index of last entry in log. If there's no entry in log,
	// empty will be returned as true.
	LastIndex() (index uint64, empty bool)

	// GetBound returns the first index and last index of a log if it's not empty,
	// otherwise the third return value "empty" will be returned as true.
	GetBound() (firstIndex, lastIndex uint64, empty bool)

	// Append appends multiple entries to log. It's user's responsibility to
	// guarantee the appended entries have contiguous indices with existing
	// entries in log. If any errors occur, process will log an error and die.
	Append(...Entry)

	// Truncate truncates all log entries after(not include) the entry with the
	// given index. If any errors occur, process will log an error and die.
	Truncate(index uint64)

	// Trim deletes all entries up to(include) the given index.
	// This is used for log compaction. If any errors occur, process will log an
	// error and die.
	Trim(index uint64)

	// Term returns the term number of an entry at the given index. It's caller's
	// responsibility to guarantee an entry with the given index is in the log,
	// otherwise we'll log an error message and die.
	Term(index uint64) uint64

	// Entries returns a slice of entries in range [beg, end). If 'end' exceeds
	// the index of last entry in log then the log should return as many entries
	// as it can. But users need to guarantee the index 'beg' exists in log,
	// otherwise we will panic.
	Entries(beg, end uint64) []Entry

	// GetIterator returns an iterator that can be used to iterate the log,
	// starting at the entry with ID 'first' or the first entry after where
	// it would be if there is no entry with that ID.
	GetIterator(first uint64) LogIterator

	// Close releases any resources used by the log. The log must not
	// be used after calling Close.
	Close()
}

// LogIterator is used to iterate over entries in a log.
type LogIterator interface {
	// Next advances the iterator. It returns true if it was able to
	// advance to the next entry or false if there are no more entries
	// or an error occurred. Use Err() to tell the difference.
	Next() bool

	// Entry returns the current entry. Next must be called before every
	// call to Entry.
	Entry() Entry

	// Err returns any error that occurred during iteration or nil if no
	// error occurred.
	Err() error

	// Close releases any resources associated with the iterator and
	// returns an error, if any occurred during iteration. The iterator
	// must not be used after Close is called.
	Close() error
}

// Storage manages all persistent state(log, raft state, snapshot).
type Storage struct {
	State
	SnapshotManager
	log Log
}

// NewStorage creates a storage object.
func NewStorage(snapshot SnapshotManager, log Log, state State) *Storage {
	return &Storage{SnapshotManager: snapshot, State: state, log: log}
}

// IsInCleanState returns true if the state on disk is empty.
func (s *Storage) IsInCleanState() bool {
	if _, empty := s.log.LastIndex(); !empty {
		return false
	}
	meta, _ := s.GetSnapshotMetadata()
	if meta != NilSnapshotMetadata {
		return false
	}
	voteFor, term := s.GetState()
	if voteFor != "" || term != 0 {
		return false
	}
	return true
}

// getLogEntries is called by leader to return log entries in range [beg, end) that
// need to be sent to followers, also the term number of the entry immediately
// preceding "beg" will be returned for consistency check. If any entries can't be
// found from the log because of compaction, "ok" will be returned as false.
// It's caller's responsibility to guarantee that log entry at index "end" is
// included in either log or snapshot, otherwise we'll panic.
func (s *Storage) getLogEntries(beg, end uint64) (prevTerm uint64, ents []Entry, ok bool) {
	// We also need to send the term number of the entry immediately preceding "beg" for
	// consistency check.
	prevLogIndex := beg - 1
	prevTerm, ok = s.term(prevLogIndex)
	if !ok {
		return
	}

	fi, empty := s.log.FirstIndex()

	// See if we can retrive all entries in range [beg, end) from the log.

	if empty || beg < fi {
		// We can't, some of the entries must be compacted.
		ok = false
		return
	}

	li, _ := s.log.LastIndex()
	// Sanity check: the caller should be responsible for guranteeing "end" can't
	// go beyond the last one of log.
	if end > li+1 {
		log.Fatalf("'end'(%d) can't exceed the last index(%d) of the log more than 1", end, li)
	}

	// If we are here, all entries within range [beg, end) can be retrieved from
	// the log.
	return prevTerm, s.log.Entries(beg, end), ok
}

// inLog returns true if a command with the given index and term exists in
// the node's log.
func (s *Storage) inLog(index, term uint64) bool {
	fi, li, empty := s.log.GetBound()
	if empty {
		return false
	}
	if index >= fi && index <= li {
		// An entry with given index exists in the log, but let's find out
		// if it's the same entry.
		return term == s.log.Term(index)
	}
	// If we are here, no entry with the given index exists in log.
	return false
}

// hasEntry returns true if a command with given index and term exists in a
// follower's storage(either in log or snapshot), false otherwise. This will
// *only* be called for consistency check of AppEnts messages from a leader.
func (s *Storage) hasEntry(index, term uint64) bool {
	if index == 0 {
		// The consistency check should always succeed for the entry with term 0 and
		// index 0, though it's not in log physically.
		// The reason is leader will keep decrementing probing entry if the probing entry
		// does not exist in follower's storage. If the logs of leader and follower differ
		// from the first entry, then eventually leader will decrement probing entry to
		// (0, 0) and they must agree with this one even it's not in follower's storage
		// physically.
		return true
	}

	// Get the metadata of snapshot file, if there's any.
	meta, _ := s.GetSnapshotMetadata()

	if meta != NilSnapshotMetadata && index <= meta.LastIndex {
		// hasEntry is called(indirectly) by the leader, who is checking to see
		// if the follower's log diverges from the leader's log. If the follower
		// has a snapshot, it consists entirely of applied entries(thus committed).
		// If a member is a leader, it's guaranteed to have all committed entries
		// otherwise it can't be elected, so it must have at least as many committed
		// entries as the follower does. So if a follower has a snapshot, it agrees
		// with the leader all entries in the snapshot so we can return true here.
		return true
	}

	// If we are here, it means either we don't have a snapshot file or we have
	// a snapshot file but the command with the given index doesn't exist in the
	// file. See if we can find it from the log.
	return s.inLog(index, term)
}

// lastIndex returns the index of last command persisted by this node.
// This can be from either snapshot file or log file.
func (s *Storage) lastIndex() uint64 {
	// Get the last index of the log file.
	lastLogIndex, empty := s.log.LastIndex()

	if !empty {
		// If the log is not emtpy then for sure lastLogIndex > lastSnapIndex because
		// snapshot is always a prefix of the log.
		return lastLogIndex
	}

	// If we are here, the log is empty because either the entire state is empty or
	// we have truncated the entire log. Let's see if there's any snapshot files.

	// Get metadata of the latest snapshot file, if there's any.
	if meta, _ := s.GetSnapshotMetadata(); meta != NilSnapshotMetadata {
		// We have a snapshot file, return the last index of snapshot file.
		return meta.LastIndex
	}

	// If we are here, we have neither log nor snapshot so we are in a blank state,
	// return 0.
	return 0
}

// term returns the term of the 'index'-th log entry.
// The 'index'-th log entry must exist (either in a snapshot or the log proper)
// or this function will panic.
// If the term of the entry is found from storage proper(either in log or
// snapshot), returns 'true' as its second arguement.
// If the term of the entry can't be found from storage, returns 'false' as its
// second argument. In this case the term is not known.
func (s *Storage) term(index uint64) (term uint64, ok bool) {
	li := s.lastIndex()
	if index > li {
		log.Fatalf("bug: index %d is not allowed to exceed the last index %d.", index, li)
	}

	if index == 0 {
		// The term of index 0 is always 0.
		return 0, true
	}

	// If we are here, a command at the given index exists in node's storage.
	// It's probably in either snapshot or log, or both.
	fi, empty := s.log.FirstIndex()

	if !empty && index >= fi {
		// If the log is not empty and the command at the given index is not trimmed
		// from the log, we are supposed to find its term from the log.
		return s.log.Term(index), true
	}

	// If we are here, we can't find the term number of the given index from the
	// log, see if we can find out by checking the metadata of the snapshot file.

	// Get the metadata of snapshot file.
	meta, _ := s.GetSnapshotMetadata()

	if meta == NilSnapshotMetadata {
		// We couldn't find the command in log, so it must exists in snapshot.
		log.Fatalf("There must be a snapshot file exists.")
	}

	if meta.LastIndex == index {
		// The command is the last applied command to the snapshot file, we have
		// recorded it in snapshot files's metadata, return it directly.
		return meta.LastTerm, true
	}

	// It's been trimmed.
	return 0, false
}
