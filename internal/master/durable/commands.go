// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package durable

import (
	"bytes"
	"encoding/gob"

	log "github.com/golang/glog"

	"github.com/westerndigitalcorporation/blb/internal/core"
)

// Command wraps pre-defined command types to be proposed to Raft.
type Command struct {
	Cmd interface{}
}

// RegisterCuratorCmd registers a new curator.
type RegisterCuratorCmd struct {
}

// RegisterTractserverCmd registers a new tractserver.
type RegisterTractserverCmd struct {
}

// NewPartitionCmd assigns a new partition to a curator.
type NewPartitionCmd struct {
	CuratorID core.CuratorID
}

// NewPartitionRes is the result of a committed NewPartitionCmd.
type NewPartitionRes struct {
	PartitionID core.PartitionID
	Err         core.Error
}

// ChecksumRequestCmd asks the master to compute a checksum of its state.
type ChecksumRequestCmd struct {
}

// ChecksumRes is the result of a ChecksumRequestCmd.
type ChecksumRes struct {
	Index    uint64 // Index of the ChecksumRequestCmd itself
	Checksum uint32 // Computed checksum
}

// ChecksumVerifyCmd asks the master to compare the checksum it saved with this
// checksum. The index is provided so that we can skip the check if the master
// didn't apply the corresponding ChecksumRequestCmd (e.g. if it restored from a
// snapshot right after that command).
type ChecksumVerifyCmd struct {
	Index    uint64 // Index of the corresponding ChecksumRequestCommand
	Checksum uint32 // Checksum to verify
}

// SetReadOnlyModeCmd changes the read-only mode of the master. Use this during
// upgrades to prevent introducing corruption from differing implementations of
// raft commands.
type SetReadOnlyModeCmd struct {
	ReadOnly bool
}

// cmdToBytes wraps 'cmd' in Command and serializes it into bytes. It dies if it
// fails.
func cmdToBytes(cmd interface{}) []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(Command{Cmd: cmd}); nil != err {
		log.Fatalf("failed to encode command: %s", err)
	}
	return buf.Bytes()
}

// bytesToCmd deserializes a Command from bytes and returns the wrapped
// Command.Cmd. It dies if it fails.
func bytesToCmd(b []byte) interface{} {
	r := bytes.NewReader(b)
	dec := gob.NewDecoder(r)
	var c Command
	if err := dec.Decode(&c); nil != err {
		log.Fatalf("failed to decode command: %s", err)
		return Command{}
	}
	return c.Cmd
}
