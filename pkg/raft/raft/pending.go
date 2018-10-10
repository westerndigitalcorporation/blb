// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package raft

import (
	"time"
)

// An interface with the "conclude" method.
type concluder interface {
	conclude(res interface{}, err error)
}

// Pending represents a pending request of Raft.
type Pending struct {
	// Res is the result of executing a command. When a command is applied to
	// the state machine, FSM.Apply is called. The return value of that function
	// is stored here.
	Res interface{}

	//  Err will be set if any error occured.
	Err error

	// Done is the channel that will be signaled when the command concludes.
	Done chan struct{}

	// index is the ID of this command used by Raft to identify a commmand.
	// It's not exposed to users.
	index uint64

	// term is only used by ProposeIfTerm.
	term uint64

	// This is used to tracking the start timestamp of some requests, which is
	// helpful to find out the time of serving the requests.
	start time.Time

	// ctx can be used to associate some additional value with a pending object.
	ctx interface{}
}

// conclude concludes a pending command. This will be called by Raft once a
// command is applied to state machine or an error occurs.
func (f *Pending) conclude(res interface{}, err error) {
	f.Res = res
	f.Err = err
	f.Done <- struct{}{}
}

// pendingGroup is a group of pending objects. It implements "concluder"
// interface so we can use one committed command to conclude all pending
// verification requests.
type pendingGroup []*Pending

// To implement "concluder" interface.
func (g pendingGroup) conclude(res interface{}, err error) {
	for _, p := range g {
		p.conclude(res, err)
	}
}
