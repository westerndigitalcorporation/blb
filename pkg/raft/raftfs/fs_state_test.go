// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package raftfs

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	test "github.com/westerndigitalcorporation/blb/pkg/testutil"
)

// TestFSStateBadHomeDir tests initializating a fileStore with a non-existing
// homeDir, which should panic.
func TestFSStateBadHomeDir(t *testing.T) {
	homeDir := "<|&:\this-should-not-be-valid&/&:|>"

	f, err := NewFSState(homeDir)
	if nil != f || nil == err {
		t.Errorf("expected error but got success")
	}
}

// TestFSStateSaveAndGet saves the state, gets the state and verifies if the two
// match, mutate the state, and repeats.
func TestFSStateSaveAndGet(t *testing.T) {
	states := []raftState{
		raftState{"", 0, 999, nil},
		raftState{"test", 123456, 888, nil},
		raftState{"hello world", 23, 777, nil},
		raftState{"we like spicy food", 888888, 666, nil},
		raftState{"and smoky tea", 4114346, 555, nil},
	}

	// Create temporary homeDir.
	homeDir, err := ioutil.TempDir(test.TempDir(), "fsstate")
	if nil != err {
		t.Fatalf("failed to create temporary directory: %s", err)
	}

	// Create a fsState and verifies the default state.
	f, err := NewFSState(homeDir)
	if nil != err {
		t.Fatalf("failed to create fs state: %s", err)
	}
	voteFor, term := f.GetState()
	if "" != voteFor || term != 0 {
		t.Errorf("expected [%q, %d] and got [%q, %d]", "", 0, voteFor, term)
	}
	if f.GetMyGUID() == 0 {
		t.Errorf("expected non-zero guid")
	}

	// Start the set-get-and-verify process.
	str, num := "data", uint64(1)
	for _, state := range states {
		f.SaveState(state.VoteFor, state.Term)
		if voteFor, term := f.GetState(); state.VoteFor != voteFor || state.Term != term {
			t.Errorf("expected [%q, %d] and got [%q, %d]", state.VoteFor, state.Term, voteFor, term)
		}

		f.SetVoteFor(str)
		if voteFor := f.GetVoteFor(); str != voteFor {
			t.Errorf("expected %s and got %s", str, voteFor)
		}

		f.SetCurrentTerm(num)
		if term := f.GetCurrentTerm(); num != term {
			t.Errorf("expected %d and got %d", num, term)
		}

		if voteFor, term := f.GetState(); str != voteFor || num != term {
			t.Errorf("expected [%q, %d] and got [%q, %d]", str, num, voteFor, term)
		}
	}
}

// TestFSStateInitialization initializes a fsState with an exisiting state file
// and verify the state.
func TestFSStateInitialization(t *testing.T) {
	// Create temporary homeDir.
	homeDir, err := ioutil.TempDir(test.TempDir(), "fsstate")
	if nil != err {
		t.Fatalf("failed to create temporary directory: %s", err)
	}

	// Create a fsState and write a state.
	f, err := NewFSState(homeDir)
	if nil != err {
		t.Fatalf("failed to create fs state: %s", err)
	}
	guid1 := f.GetMyGUID()
	str, num := "alice", uint64(23)
	f.SaveState(str, num)
	f.SetGUIDFor("alice", 87654)

	// Create another fsState with the same homeDir so that it should be
	// initialized with the previous written state.
	other, err := NewFSState(homeDir)
	if nil != err {
		t.Fatalf("failed to create fs state: %s", err)
	}
	voteFor, term := other.GetState()
	if str != voteFor || num != term {
		t.Errorf("expected [%q, %d] and got [%q, %d]", str, num, voteFor, term)
	}
	if f.GetMyGUID() != guid1 {
		t.Errorf("GUID was not loaded")
	}
	if f.GetGUIDFor("alice") != 87654 {
		t.Errorf("seen guid map was not loaded")
	}
	if f.GetGUIDFor("bob") != 0 {
		t.Errorf("unexpected value from guid map")
	}
}

func TestFSStateFilterGUIDS(t *testing.T) {
	// Create temporary homeDir.
	homeDir, err := ioutil.TempDir(test.TempDir(), "fsstate")
	if nil != err {
		t.Fatalf("failed to create temporary directory: %s", err)
	}

	// Create a fsState and write a state.
	f, err := NewFSState(homeDir)
	if nil != err {
		t.Fatalf("failed to create fs state: %s", err)
	}
	f.SetGUIDFor("alice", 87654)
	f.SetGUIDFor("bob", 321)
	f.SetGUIDFor("chris", 4444)

	if f.GetGUIDFor("alice") != 87654 ||
		f.GetGUIDFor("bob") != 321 ||
		f.GetGUIDFor("chris") != 4444 {
		t.Errorf("did not save guids")
	}

	f.FilterGUIDs([]string{"alice", "chris"})
	if f.GetGUIDFor("alice") != 87654 ||
		f.GetGUIDFor("bob") != 0 ||
		f.GetGUIDFor("chris") != 4444 {
		t.Errorf("did not filter out guid")
	}
}

// TestFSStateBadStateFile tests initializes a fsState with a corrupted state
// file, which should panic.
func TestFSStateBadStateFile(t *testing.T) {
	// Create temporary homeDir.
	homeDir := test.TempDir()

	// Create a corrupted state file.
	name := filepath.Join(homeDir, "raft_state")
	file, err := os.Create(name)
	if nil != err {
		t.Fatalf("failed to create state file: %s", err)
	}
	defer file.Close()

	if _, err = file.Write([]byte{1, 1, 1, 1, 1, 1}); nil != err {
		t.Errorf("failed to write state file: %s", err)
	}

	f, err := NewFSState(homeDir)
	if nil != f || nil == err {
		t.Errorf("expected error but got success")
	}
}
