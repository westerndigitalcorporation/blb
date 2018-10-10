// Copyright (c) 2017 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package backupblb

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

// State keeps track of our state.
type State struct {
	// Groups has one entry for the master, and one for each curator group.
	// The key is the service name, e.g. "blb-curator-g2".
	Groups map[string]*GroupState
}

// GroupState holds the state for one raft group.
type GroupState struct {
	Addrs      []string // What addresses do we have for this group?
	LastAddr   string   // Which node did we last ask for a backup?
	LastEtag   string   // What was the etag of the last completed backup?
	LastCheck  int64    // When was the most recent response? (unix time)
	LastBackup int64    // When was the most recent actual backup? (unix time)
	LastSeq    uint64   // What's the next backup sequence number?
}

func loadState(path string) (*State, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	st := new(State)
	if err = json.Unmarshal(data, st); err != nil {
		return nil, err
	}
	return st, nil
}

func saveState(state *State, path string) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	tmpPath := path + ".tmp"
	defer os.Remove(tmpPath)
	if err = ioutil.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}
