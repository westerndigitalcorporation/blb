// Copyright (c) 2016 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package raft

import (
	"bytes"
	"encoding/gob"

	log "github.com/golang/glog"
)

// encodeMembership encodes a Membership to bytes.
func encodeMembership(m Membership) []byte {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	m.Index = 0
	m.Term = 0
	if err := enc.Encode(m); err != nil {
		log.Fatalf("Failed to encode members: %v", err)
	}
	return buffer.Bytes()
}

func containMember(members []string, member string) bool {
	for _, m := range members {
		if m == member {
			return true
		}
	}
	return false
}

// Returns a new slice with the "member" removed. It will not affect the
// original one.
func removeMember(members []string, member string) []string {
	newMembers := make([]string, 0, len(members)-1)
	for _, m := range members {
		if m != member {
			newMembers = append(newMembers, m)
		}
	}
	return newMembers
}

// Returns a new slice with the "member" added. It will not affect the
// original one.
func addMember(members []string, member string) []string {
	newMembers := make([]string, 0, len(members)+1)
	newMembers = append(newMembers, members...)
	return append(newMembers, member)
}

func decodeConfEntry(ent Entry) *Membership {
	if ent.Type != EntryConf {
		return nil
	}
	var m Membership
	if !tryDecodeGob(ent.Cmd, &m) {
		log.Fatalf("Failed to decode membership entry")
	}
	return &Membership{
		Index:   ent.Index,
		Term:    ent.Term,
		Members: m.Members,
		Epoch:   m.Epoch,
	}
}

func tryDecodeGob(data []byte, val interface{}) bool {
	dec := gob.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(val); err != nil {
		return false
	}
	return true
}
