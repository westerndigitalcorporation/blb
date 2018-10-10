// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package raft

import "testing"

// testMsgDuplicatorEnv calls 'testMemTransportEnv' and wraps the
// returned transports in msgDuplicator.
func testMsgDuplicatorEnv(peerCount, maxMsg, limit int, p float32, seed int64) (peers []*msgDuplicator) {
	lowers := testMemTransportEnv(peerCount, maxMsg)
	for _, l := range lowers {
		peers = append(peers, NewMsgDuplicator(l, limit, p, seed).(*msgDuplicator))
	}
	return peers
}

// collectMsgDuplicatorStats collects and aggregates stats from 'peers'.
func collectMsgDuplicatorStats(peers []*msgDuplicator) (stats msgDuplicatorStats) {
	for _, p := range peers {
		s := p.stats
		stats.MsgSent += s.MsgSent
		stats.msgDuplicated += s.msgDuplicated
	}
	return stats
}

// testMsgDuplicator tests msgDuplicator. 'exp' is the expected sent
// message count.
func testMsgDuplicator(peerCount, maxMsg, limit int, p float32, seed int64, exp int, t *testing.T) {
	peers := testMsgDuplicatorEnv(peerCount, maxMsg, limit, p, seed)

	// Peers can talk to each other.
	for _, from := range peers {
		for _, to := range peers {
			if from.Addr() == to.Addr() {
				continue
			}
			from.Send(&BaseMsg{From: from.Addr(), To: to.Addr()})
		}
	}

	// Verify sent message count.
	s := collectMsgDuplicatorStats(peers)
	if exp != s.MsgSent {
		t.Errorf("expected %d and got %d", exp, s.MsgSent)
	}
}

// TestMsgDuplicator tests msgDuplicator with extreme cases.
func TestMsgDuplicator(t *testing.T) {
	tests := []struct {
		peerCount int
		maxMsg    int
		limit     int
		p         float32
		seed      int64
		exp       int
	}{
		{30, 60, 10, 0.0, 0, 30 * 29},     // never duplicate
		{30, 60, 10, 1.0, 0, 2 * 30 * 29}, // always duplicate
	}
	for _, test := range tests {
		testMsgDuplicator(test.peerCount, test.maxMsg,
			test.limit, test.p, test.seed, test.exp, t)
	}
}

// testPartitionAndDup tests a scenario where the replication group is
// partitioned into two halves and messages are duplicated. From the
// lowest level up, there are three layers: base 'peers',
// msgDuplicator, and msgDropper.
func testPartitionAndDup(peers []Transport, t *testing.T) {
	peerCount := len(peers)

	// Compose base in msgDuplicator.
	var dups []*msgDuplicator
	for _, p := range peers {
		dups = append(dups, NewMsgDuplicator(p, peerCount, 1.0, 0).(*msgDuplicator))
	}

	// Compose msgDuplicator in msgDropper.
	var drops []*msgDropper
	for _, dup := range dups {
		drops = append(drops, NewMsgDropper(dup, 0, 0).(*msgDropper))
	}

	// Create two partitions.
	split := peerCount / 2
	partition(drops[:split], drops[split:])

	// Have peers send messages to each other.
	for _, from := range drops {
		for _, to := range drops {
			if from.Addr() == to.Addr() {
				continue
			}
			from.Send(&BaseMsg{From: from.Addr(), To: to.Addr()})
		}
	}

	// Verify counts in msgDroppers.
	dropStats := collectMsgDropperStats(drops)
	if sent := peerCount * (peerCount - 1); sent != dropStats.MsgSent {
		t.Errorf("expected %d sent and got %d", sent, dropStats.MsgSent)
	}
	if dropped := peerCount * peerCount / 2; dropped != dropStats.MsgDropped {
		t.Errorf("expected %d dropped and got %d", dropped, dropStats.MsgDropped)
	}

	// Verify counts in msgDuplicators.
	dupStats := collectMsgDuplicatorStats(dups)
	if sent := (dropStats.MsgSent - dropStats.MsgDropped) * 2; sent != dupStats.MsgSent {
		t.Errorf("expected %d sent and got %d", sent, dupStats.MsgSent)
	}
	if duplicated := dropStats.MsgSent - dropStats.MsgDropped; duplicated != dupStats.msgDuplicated {
		t.Errorf("expected %d sent and got %d", duplicated, dupStats.msgDuplicated)
	}
}

// TestPartitionAndDupMem runs testPartitionAndDup with MemTransport as
// the base.
func TestPartitionAndDupMem(t *testing.T) {
	base := testMemTransportEnv(10, 100)
	var peers []Transport
	for _, p := range base {
		peers = append(peers, Transport(p))
	}
	testPartitionAndDup(peers, t)
}
