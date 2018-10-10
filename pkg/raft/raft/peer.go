// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package raft

// peer refers to one node in Raft consensus group.
type peer struct {
	ID         string // ID of peer.
	nextIndex  uint64 // index of the next entry to send to that server.
	matchIndex uint64 // index of highest log entry known to be replicated on server.

	// The time of last message sent to the peer, leader uses this value to decide
	// when it should send heartbeat messages or resend lost messages to the peer.
	lastContactTime uint32

	// The time of last message received from the peer.
	lastReceiveTime uint32

	sendingSnap bool // Is the peer being synced with a snapshot?
}
