// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package store

import (
	"bytes"
	"encoding/gob"

	log "github.com/golang/glog"
)

// init registers the various command types with the gob system so it
// can automatically encode and decode them.
func init() {
	gob.Register(PutCommand{})
	gob.Register(DelCommand{})
	gob.Register(CASCommand{})
	gob.Register(ChecksumRequestCommand{})
	gob.Register(ChecksumVerifyCommand{})
}

// Command is a command that is serialized and handed to the Raft algorithm.
// Gob requires a known type passed as an argument to serialize/deserialize,
// which is why we can't just pass the command.
type Command struct {
	Op interface{}
}

// PutCommand puts a key-value pair into database.
type PutCommand struct {
	Key   []byte
	Value []byte
}

// DelCommand deletes a key-value pair from database.
type DelCommand struct {
	Key []byte
}

// CASCommand puts a key-value pair into database only if the old value of the
// key matches what user specified.
type CASCommand struct {
	Key      []byte
	OldValue []byte
	NewValue []byte
}

// ChecksumRequestCommand asks the store to compute a checksum over a range of
// keys and store it in volatile memory.
type ChecksumRequestCommand struct {
	StartKey []byte
	N        int
}

// ChecksumResult is the result of a ChecksumRequestCommand.
type ChecksumResult struct {
	NextKey  []byte // Key following the range that was checksummed, or nil if we hit the end
	Checksum uint64 // Computed checksum
	Index    uint64 // Index of the ChecksumRequestCommand itself
}

// ChecksumVerifyCommand asks the store to compare the checksum it saved in
// volatile memory with this checksum. The index is provided so that we can skip
// the check if the store didn't apply the corresponding ChecksumRequestCommand
// (e.g. if it restored from a snapshot right after that command).
type ChecksumVerifyCommand struct {
	Index    uint64 // Index of the corresponding ChecksumRequestCommand
	Checksum uint64 // Checksum to verify
}

func cmdToBytes(cmd Command) []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(cmd); err != nil {
		log.Fatalf("Failed to encode command")
	}
	return buf.Bytes()
}

func bytesToCmd(b []byte) Command {
	r := bytes.NewReader(b)
	var cmd Command
	dec := gob.NewDecoder(r)
	dec.Decode(&cmd)
	return cmd
}
