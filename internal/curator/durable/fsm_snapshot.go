// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package durable

import (
	"encoding/binary"
	"io"
	"os"

	log "github.com/golang/glog"
	"github.com/westerndigitalcorporation/blb/internal/curator/durable/state"
	"github.com/westerndigitalcorporation/blb/pkg/raft/raft"

	"github.com/golang/snappy"
)

// snapshotVersion defines the version of a snapshot file.
type snapshotVersion uint32

const (
	invalidVersion snapshotVersion = iota
	boltSnappyVer                  // BoltDB with snappy compression
)

const (
	// The magic number that validates if a snapshot file is valid.
	magic uint32 = 0xDEFB0B
)

type curatorSnapshoter struct {
	txn *ReadOnlyTxn
}

// This will be called when the snapshot is no longer needed.
func (s *curatorSnapshoter) Release() {
	s.txn.Commit()
}

// The snapshot file has the following format:
//
//      4 bytes        4 bytes          8 bytes                   the rest
// -------------------------------------------------------------------------------------
// | magic number | snap version |     raft index     | ...compressed snapshot data... |
// -------------------------------------------------------------------------------------
//
func (s *curatorSnapshoter) Save(writer io.Writer) error {
	// ------- Write headers of snapshot in uncompressed format -------

	// Write "magic number"
	if err := binary.Write(writer, binary.BigEndian, magic); err != nil {
		log.Errorf("Failed to write magic number: %v", err)
		return err
	}
	// Write "snapshot version"
	if err := binary.Write(writer, binary.BigEndian, boltSnappyVer); err != nil {
		log.Errorf("Failed to write snapshot version: %v", err)
		return err
	}
	// Write "raft index".
	// Persist the Raft index of last applied write transaction to beginning
	// of the file so that we can skip restoring from a stale snapshot without
	// reading the whole file.
	if err := binary.Write(writer, binary.BigEndian, s.txn.GetIndex()); err != nil {
		return err
	}

	// ------ Write actual snapshot data in compressed format ---------

	writer = snappy.NewBufferedWriter(writer)
	defer writer.(*snappy.Writer).Flush()

	if _, err := s.txn.Dump(writer); err != nil {
		log.Errorf("Failed to write to snapshot file: %v", err)
		return err
	}
	return nil
}

// Snapshot implements FSM.Snapshot.
func (h *StateHandler) Snapshot() (raft.Snapshoter, error) {
	return &curatorSnapshoter{txn: h.LocalReadOnlyTxn()}, nil
}

// SnapshotRestore implements raft.FSM.
// See comments of StateHandler.SnapshotSave for the format of snapshot file.
func (h *StateHandler) SnapshotRestore(reader io.Reader, lastIndex, lastTerm uint64) {
	// Headers.
	var m uint32
	var sversion snapshotVersion
	var raftIndex uint64

	// Read "magic number".
	if err := binary.Read(reader, binary.BigEndian, &m); err != nil {
		log.Fatalf("Failed to read magic number: %v", err)
	}
	if m != magic {
		log.Fatalf("Mismatch on magic number, probably not a valid snapshot file?")
	}
	// Read "snapshot version".
	if err := binary.Read(reader, binary.BigEndian, &sversion); err != nil {
		log.Fatalf("Failed to read snapshot version: %v", err)
	}
	if sversion != boltSnappyVer {
		log.Fatalf("Snapshot file with version %d can not be handled.", sversion)
	}
	// Read "raft index".
	if err := binary.Read(reader, binary.BigEndian, &raftIndex); err != nil {
		log.Fatalf("Failed to read raft index: %v", err)
	}

	rtxn := h.state.ReadOnlyTxn()
	index := rtxn.GetIndex()
	rtxn.Commit()

	if raftIndex <= index {
		// The snapshot doens't contain anything new, skip.
		return
	}

	// The snapshot should have been compressed by snappy.
	reader = snappy.NewReader(reader)
	file, err := os.Create(h.config.GetStateTempDBPath())
	if err != nil {
		log.Fatalf("Failed to create a temporary file: %v", err)
	}
	// Copy from snapshot reader to the temporary file.
	_, err = io.Copy(file, reader)
	if err != nil {
		log.Fatalf("Failed to write to the temporary DB file: %v", err)
	}

	if err := file.Close(); err != nil {
		log.Fatalf("Failed to close the temporary file: %v", err)
	}

	// Grab the write lock so all read accesses will be blocked during
	// closing current the current DB and reopening the new DB.
	h.dbLock.Lock()
	defer h.dbLock.Unlock()

	// By grabbing write lock we guarantee all transactions have been closed.
	// Closing current database and make the newly received DB file effective.
	h.state.Close()

	if err := os.Rename(h.config.GetStateTempDBPath(), h.config.GetStateDBPath()); err != nil {
		log.Fatalf("Failed to rename: %v", err)
	}

	// Reopen the database with newly created file.
	h.state = state.Open(h.config.GetStateDBPath())
}
