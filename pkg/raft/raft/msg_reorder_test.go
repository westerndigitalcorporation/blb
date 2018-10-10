// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package raft

import (
	"testing"
	"time"
)

// testMsgReorderEnv calls 'testMemTransportEnv' and wraps the returned
// transports in msgReorder.
func testMsgReorderEnv(peerCount, maxMsg int, p float32, limit time.Duration, seed int64) (peers []*msgReorder) {
	lowers := testMemTransportEnv(peerCount, maxMsg)
	for _, l := range lowers {
		peers = append(peers, NewMsgReorder(l, p, limit, seed).(*msgReorder))
	}
	return peers
}

// collectMsgDelayerStats collects and aggregates stats from 'peers'.
func collectMsgDelayerStats(peers []*msgReorder) (stats msgReorderStats) {
	for _, p := range peers {
		s := p.stats
		stats.MsgSent += s.MsgSent
		stats.MsgDelayed += s.MsgDelayed
		stats.MsgPending += s.MsgPending
	}
	return stats
}

// testMsgReorder creates a set of peers and have them send messages to each
// other.
func testMsgReorder(peerCount, maxMsg int, p float32, limit time.Duration, seed int64, exp int, t *testing.T) {
	peers := testMsgReorderEnv(peerCount, maxMsg, p, limit, seed)

	// Peers can talk to each other.
	for _, from := range peers {
		for _, to := range peers {
			if from.Addr() == to.Addr() {
				continue
			}
			from.Send(&BaseMsg{From: from.Addr(), To: to.Addr()})
		}
	}

	// Wait until all messages get delivered.
	for _, p := range peers {
		p.Close()
	}

	// Verify sent message count.
	stats := collectMsgDelayerStats(peers)
	if exp != stats.MsgSent {
		t.Errorf("expected %d and got %d", exp, stats.MsgSent)
	}
}

// TestMsgReorder tests msgReorder with various scenarios.
func TestMsgReorder(t *testing.T) {
	tests := []struct {
		peerCount int
		maxMsg    int
		p         float32
		limit     time.Duration
		seed      int64
		exp       int
	}{
		{30, 60, 0.00, 100 * time.Millisecond, 1422, 30 * 29}, // delay none
		{30, 60, 0.25, 200 * time.Millisecond, 4543, 30 * 29}, // delay about a quarter
		{35, 70, 0.50, 300 * time.Millisecond, 5445, 35 * 34}, // delay about a hanlf
		{35, 70, 0.75, 200 * time.Millisecond, 3432, 35 * 34}, // delay about three quarters
		{40, 80, 1.00, 100 * time.Millisecond, 3210, 40 * 39}, // delay all
	}
	for _, test := range tests {
		testMsgReorder(test.peerCount, test.maxMsg,
			test.p, test.limit, test.seed, test.exp, t)
	}
}
