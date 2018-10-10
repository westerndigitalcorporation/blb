// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package raftfs

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	log "github.com/golang/glog"
	"github.com/westerndigitalcorporation/blb/pkg/disk"
	"github.com/westerndigitalcorporation/blb/pkg/raft/raft"
)

// raftState is the state that Raft needs to maintain during its operation.
type raftState struct {
	VoteFor string
	Term    uint64

	MyGUID    uint64
	SeenGUIDs map[string]uint64
}

// fsState is a durable file-based implementation of State. The state is
// always persisted onto disk after each mutation. A copy of the state is also
// cached in memory for performance.
//
// The fsState is not made thread-safe because its only user is the Raft
// core which is single-threaded.
//
// The cache can never be out of sync with the on-file version because: (1) The
// program is single-threaded and thus no one can read the state until the
// writing of the state is finished; (2) The cache is updated everytime when the
// state is written to disk. The program panics if the write ever fails.
type fsState struct {
	cache     raftState // cached state
	homeDir   string    // home directory on the local filesystem
	stateFile string    // 'homeDir/raft_state', state file
	temp      string    // 'homeDir/raft_state.tmp', temporary state file
}

// NewFSState creates a fsState backed by a state file under directory
// 'homeDir' in a filesystem. The constructor checks whether the state file
// already exists and loads state from it if so.
func NewFSState(homeDir string) (raft.State, error) {
	// Check if 'homeDir' exists. Panic if it doesn't.
	if !isExist(homeDir) {
		return nil, fmt.Errorf("homeDir %q doesn't exist", homeDir)
	}

	// Create a state object.
	f := &fsState{
		homeDir:   homeDir,
		stateFile: filepath.Join(homeDir, "raft_state"),
		temp:      filepath.Join(homeDir, "raft_state.tmp"),
	}

	// If the state file already exists, load and cache state.
	if state, err := f.stateFromFile(); nil == err {
		f.cache = state
		log.Infof("loaded state from state file %q", f.stateFile)
		if f.cache.MyGUID == 0 {
			// Migrating to version with GUIDs. Need to generate one.
			f.cache.MyGUID = generateGUID()
			log.Infof("generated guid %d", f.cache.MyGUID)
			f.writeState()
		}
		return f, nil
	} else if !os.IsNotExist(err) {
		// Fatal if it is not the file not exist error.
		//
		// When it is the first time to initialize the fsState, the
		// state file does not exist and we expect a "file-not-exist" in
		// that case. We assume the state file is corrupted in case of
		// other errors and panic right away.
		return nil, fmt.Errorf("state file %q exists but corrupted: %s", f.stateFile, err)
	}

	// The state file doesn't exist. Create one.
	f.cache = raftState{
		VoteFor: "",
		Term:    0,
		MyGUID:  generateGUID(),
	}
	log.Infof("generated guid %d", f.cache.MyGUID)
	f.writeState()

	log.Infof("initialized state file %q", f.stateFile)
	return f, nil
}

// SaveState implements State. The state file and homeDir are fsync'ed.
func (f *fsState) SaveState(voteFor string, term uint64) {
	f.cache.VoteFor = voteFor
	f.cache.Term = term
	f.writeState()
}

func (f *fsState) writeState() {
	if err := f.stateToFile(f.cache); nil != err {
		log.Fatalf("failed to save state: %s", err)
	}
	log.V(3).Infof("set state to %+v", f.cache)
}

// GetState implements State. We can always read from cache because it is
// initialized from existing state file (if exists) at the beginning and gets
// updated every time when the state is mutated and persisted.
func (f *fsState) GetState() (string, uint64) {
	return f.cache.VoteFor, f.cache.Term
}

// SetCurrentTerm implements State.
func (f *fsState) SetCurrentTerm(term uint64) {
	f.cache.Term = term
	f.writeState()
}

// GetCurrentTerm implements State. We can always read from cache because it is
// initialized from existing state file (if exists) at the beginning and gets
// updated every time when the state is mutated and persisted.
func (f *fsState) GetCurrentTerm() uint64 {
	return f.cache.Term
}

// SetVoteFor implements State.
func (f *fsState) SetVoteFor(voteFor string) {
	f.cache.VoteFor = voteFor
	f.writeState()
}

// GetVoteFor implements State. We can always read from cache because it is
// initialized from existing state file (if exists) at the beginning and gets
// updated every time when the state is mutated and persisted.
func (f *fsState) GetVoteFor() string {
	return f.cache.VoteFor
}

// GetMyGUID implements State.
func (f *fsState) GetMyGUID() uint64 {
	return f.cache.MyGUID
}

// GetGUIDFor implements State.
func (f *fsState) GetGUIDFor(id string) uint64 {
	return f.cache.SeenGUIDs[id]
}

// SetGUIDFor implements State.
func (f *fsState) SetGUIDFor(id string, guid uint64) {
	if f.cache.SeenGUIDs == nil {
		f.cache.SeenGUIDs = make(map[string]uint64)
	}
	f.cache.SeenGUIDs[id] = guid
	f.writeState()
}

// FilterGUIDs implements State.
func (f *fsState) FilterGUIDs(ids []string) {
	contains := func(k string) bool {
		for _, id := range ids {
			if id == k {
				return true
			}
		}
		return false
	}

	for k := range f.cache.SeenGUIDs {
		if !contains(k) {
			delete(f.cache.SeenGUIDs, k)
		}
	}
	f.writeState()
}

//----------------
// Helper methods
//----------------

// stateFromFile retrieves the state from the state file.
func (f *fsState) stateFromFile() (raftState, error) {
	file, err := disk.NewChecksumFile(f.stateFile, os.O_RDONLY)
	if nil != err {
		return raftState{}, err
	}

	// Decode the state.
	dec := json.NewDecoder(file)
	var state raftState
	if err = dec.Decode(&state); nil != err {
		log.Errorf("failed to decode state: %s", err)
		if cerr := file.Close(); nil != cerr {
			log.Errorf("failed to close file %q: %s", f.stateFile, cerr)
		}
		return raftState{}, err
	}

	// Close the file.
	if err = file.Close(); nil != err {
		log.Errorf("failed to close file %q: %s", f.stateFile, err)
		return raftState{}, err
	}

	return state, nil
}

// stateToFile saves the state to the state file. This is done by first writing
// state to a temporary file (so that partial write does not affect the previous
// persisted state), fsyncing the write, renaming the temporary file to replace
// the state file, and finally fsyncing the home directory.
func (f *fsState) stateToFile(state raftState) error {
	// Create a temporary file to write the state. Note that we can safely
	// truncate the file if it already exists.
	temp, err := disk.NewChecksumFile(f.temp, os.O_CREATE|os.O_TRUNC|os.O_RDWR)
	if nil != err {
		log.Errorf("failed to create file %q: %s", f.temp, err)
		return err
	}

	// Encode the state to the temporary file.
	enc := json.NewEncoder(temp)
	if err = enc.Encode(state); nil != err {
		log.Errorf("failed to encode state: %s", err)
		if cerr := temp.Close(); nil != cerr {
			log.Errorf("failed to close file %q: %s", f.temp, cerr)
		}
		return err
	}

	// Sync and close the temporary file.
	if err = temp.Close(); nil != err {
		log.Errorf("failed to close file: %s", err)
		return err
	}

	// Rename the temporary file to replace the original state file.
	if err := disk.Rename(f.temp, f.stateFile); nil != err {
		log.Errorf("failed to rename file: %s", err)
		return err
	}

	return nil
}

// isExist tests if a path exists or not.
func isExist(name string) bool {
	if _, err := os.Stat(name); nil == err || !os.IsNotExist(err) {
		return true
	}
	return false
}

// generateGUID creates a new random GUID.
func generateGUID() uint64 {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		log.Fatalf("Couldn't generate random bytes for guid")
	}
	return binary.LittleEndian.Uint64(buf[:])
}
