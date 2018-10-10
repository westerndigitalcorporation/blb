// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package raft

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"sync"

	"github.com/westerndigitalcorporation/blb/pkg/wal"
)

// NewMemLog creates an in-memory mock log.
func NewMemLog() Log {
	return newLog(wal.NewMemLog())
}

type memState struct {
	voteFor   string
	term      uint64
	myGUID    uint64
	seenGUIDs map[string]uint64
}

// NewMemState creates an in-memory implementation of State interface.
func NewMemState(term uint64) State {
	var buf [8]byte
	rand.Read(buf[:])
	return &memState{
		voteFor:   "",
		term:      term,
		myGUID:    binary.LittleEndian.Uint64(buf[:]),
		seenGUIDs: make(map[string]uint64),
	}
}

func (m *memState) SaveState(voteFor string, term uint64) {
	m.voteFor, m.term = voteFor, term
}

func (m *memState) GetState() (voteFor string, term uint64) {
	return m.voteFor, m.term
}

func (m *memState) GetCurrentTerm() uint64 {
	return m.term
}

func (m *memState) SetCurrentTerm(term uint64) {
	m.term = term
}

func (m *memState) GetVoteFor() string {
	return m.voteFor
}

func (m *memState) SetVoteFor(voteFor string) {
	m.voteFor = voteFor
}

func (m *memState) GetMyGUID() uint64 {
	return m.myGUID
}

func (m *memState) GetGUIDFor(id string) uint64 {
	return m.seenGUIDs[id]
}

func (m *memState) SetGUIDFor(id string, guid uint64) {
	m.seenGUIDs[id] = guid
}

func (m *memState) FilterGUIDs(ids []string) {
	contains := func(k string) bool {
		for _, id := range ids {
			if id == k {
				return true
			}
		}
		return false
	}

	for k := range m.seenGUIDs {
		if !contains(k) {
			delete(m.seenGUIDs, k)
		}
	}
}

type memSnapshotMgr struct {
	lock sync.Mutex // protects the fields below
	// Snapshot data
	snapData []byte
	snapMeta SnapshotMetadata
}

// NewMemSnapshotMgr creates an in-memory implementation of Snapshot interface.
func NewMemSnapshotMgr() SnapshotManager {
	return &memSnapshotMgr{
		snapData: nil,
		snapMeta: NilSnapshotMetadata,
	}
}

func (m *memSnapshotMgr) BeginSnapshot(meta SnapshotMetadata) (SnapshotFileWriter, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	return newMemSnapshotWriter(meta, m), nil
}

func (m *memSnapshotMgr) GetSnapshot() SnapshotFileReader {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.snapData == nil {
		return nil
	}
	return newMemSnapshotReader(m.snapData, m.snapMeta)
}

func (m *memSnapshotMgr) GetSnapshotMetadata() (SnapshotMetadata, string) {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.snapMeta, ""
}

type memSnapshotReader struct {
	reader *bytes.Reader
	meta   SnapshotMetadata
}

func newMemSnapshotReader(data []byte, meta SnapshotMetadata) *memSnapshotReader {
	return &memSnapshotReader{
		reader: bytes.NewReader(data),
		meta:   meta,
	}
}

func (ms *memSnapshotReader) Close() error {
	ms.reader = nil
	return nil
}

func (ms *memSnapshotReader) Read(p []byte) (n int, err error) {
	return ms.reader.Read(p)
}

func (ms *memSnapshotReader) GetMetadata() SnapshotMetadata {
	return ms.meta
}

type memSnapshotWriter struct {
	writer   *bytes.Buffer
	meta     SnapshotMetadata
	snapshot *memSnapshotMgr
}

func newMemSnapshotWriter(meta SnapshotMetadata, s *memSnapshotMgr) *memSnapshotWriter {
	return &memSnapshotWriter{
		writer:   bytes.NewBuffer([]byte{}),
		meta:     meta,
		snapshot: s,
	}
}

func (ms *memSnapshotWriter) Write(p []byte) (n int, err error) {
	return ms.writer.Write(p)
}

func (ms *memSnapshotWriter) Commit() error {
	ms.snapshot.lock.Lock()
	defer ms.snapshot.lock.Unlock()

	// Make snapshot data visible.
	ms.snapshot.snapData = ms.writer.Bytes()
	ms.snapshot.snapMeta = ms.meta
	ms.writer = nil
	return nil
}

func (ms *memSnapshotWriter) Abort() error {
	ms.writer = nil
	return nil
}

func (ms *memSnapshotWriter) GetMetadata() SnapshotMetadata {
	return ms.meta
}
