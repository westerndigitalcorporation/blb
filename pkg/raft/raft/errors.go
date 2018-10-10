// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package raft

import (
	"errors"
)

var (
	// ErrNodeNotLeader will be returned if propose commands to non-leader node.
	ErrNodeNotLeader = errors.New("The node is not leader.")

	// ErrNotLeaderAnymore will be returned once the leader who proposed the
	// command has stepped down before committing the command.
	ErrNotLeaderAnymore = errors.New("Not leader anymore")

	// ErrTooManyPendingReqs will be returned if number of pending requests exceeds
	// a certain threshold.
	ErrTooManyPendingReqs = errors.New("Too many pending requests.")

	// ErrTermMismatch will be returned if a command can't have the term number
	// specified by users.
	ErrTermMismatch = errors.New("Term numbers mismatch")

	// ErrNodeExists will be returned if users require to add some node that is
	// already in configuration.
	ErrNodeExists = errors.New("The node required to be added already exists")

	// ErrNodeNotExists will be returned if users required to remove some node
	// that doesn't exist in current configuration.
	ErrNodeNotExists = errors.New("The node required to be removed doesn't exist")

	// ErrAlreadyConfigured will be returned if raft is given an initial
	// configuration when it already has a configuration.
	ErrAlreadyConfigured = errors.New("Raft can't process an initial configuration after it's already initialized")
)
