// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package raft

import (
	"fmt"
)

// Type of Entry.
const (
	EntryNormal uint8 = iota // Normal command proposed by applications.
	EntryConf                // Membership change command proposed internally by Raft.
	EntryNOP                 // NOP command
)

// Entry represents one command in log.
type Entry struct {
	Term  uint64
	Index uint64
	Cmd   []byte
	Type  uint8
}

// String returns human-readable representation of the entry.
func (e Entry) String() string {
	return fmt.Sprintf("Term: %d, Index: %d, Cmd: %s", e.Term, e.Index, e.Cmd)
}
