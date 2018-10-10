// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package raft

import (
	"testing"
)

// testMsgDropperEnv calls 'testMemTransportEnv' and wraps the
// returned transports in msgDropper.
func testMsgDropperEnv(peerCount, maxMsg int, seed int64) (peers []*msgDropper) {
	lowers := testMemTransportEnv(peerCount, maxMsg)
	for _, p := range lowers {
		peers = append(peers, NewMsgDropper(p, seed, 0).(*msgDropper))
	}
	return peers
}

// collectMsgDropperStats collects and aggregates stats from 'peers'.
func collectMsgDropperStats(peers []*msgDropper) (stats msgDropperStats) {
	for _, p := range peers {
		s := p.stats
		stats.MsgSent += s.MsgSent
		stats.MsgDropped += s.MsgDropped
	}
	return stats
}

// TestMsgDropperDrop tests unlinking and linking all connections.
func TestMsgDropperDrop(t *testing.T) {
	// Create a set of peers.
	peerCount := 30
	peers := testMsgDropperEnv(peerCount, peerCount, 0)

	// Peers can talk to each other.
	for _, from := range peers {
		for _, to := range peers {
			if from.Addr() == to.Addr() {
				continue
			}
			from.Send(&BaseMsg{From: from.Addr(), To: to.Addr()})
			if m := recvFromChan(to); m == nil || m.GetFrom() != from.Addr() {
				t.Errorf("failed to receive msg from %s", from.Addr())
			}
		}
	}

	sent := peerCount * (peerCount - 1)
	s := collectMsgDropperStats(peers)
	if sent != s.MsgSent {
		t.Errorf("expected %d sent msgs and got %d", sent, s.MsgSent)
	}
	if 0 != s.MsgDropped {
		t.Errorf("expected %d dropped msgs and got %d", 0, s.MsgDropped)
	}

	// Unlink all connections and peers cannot talk.
	setAll(peers, 1.0)
	for _, from := range peers {
		for _, to := range peers {
			if from.Addr() == to.Addr() {
				continue
			}
			from.Send(&BaseMsg{From: from.Addr(), To: to.Addr()})
			if m := recvFromChan(to); nil != m {
				t.Errorf("msg from %s should be dropped", from.Addr())
			}
		}
	}

	s = collectMsgDropperStats(peers)
	if 2*sent != s.MsgSent {
		t.Errorf("expected %d sent msgs and got %d", 2*sent, s.MsgSent)
	}
	if sent != s.MsgDropped {
		t.Errorf("expected %d dropped msgs and got %d", sent, s.MsgDropped)
	}

	// Link all connections and peers can talk again.
	setAll(peers, 0.0)
	for _, from := range peers {
		for _, to := range peers {
			if from.Addr() == to.Addr() {
				continue
			}
			from.Send(&BaseMsg{From: from.Addr(), To: to.Addr()})
			if m := recvFromChan(to); m == nil || m.GetFrom() != from.Addr() {
				t.Errorf("failed to receive msg from %s", from.Addr())
			}
		}
	}

	s = collectMsgDropperStats(peers)
	if 3*sent != s.MsgSent {
		t.Errorf("expected %d sent msgs and got %d", 3*sent, s.MsgSent)
	}
	if sent != s.MsgDropped {
		t.Errorf("expected %d dropped msgs and got %d", sent, s.MsgDropped)
	}
}

// testMsgDropperPartition tests partioning and healing the network.
// Kill the test if it hangs.
func testMsgDropperPartition(peers []*msgDropper, t *testing.T) {
	peerCount := len(peers)
	// Peers can talk to each other.
	for _, from := range peers {
		for _, to := range peers {
			if from.Addr() == to.Addr() {
				continue
			}
			from.Send(&AppEnts{BaseMsg: BaseMsg{From: from.Addr(), To: to.Addr()}})
			if m := <-to.Receive(); m == nil || m.GetFrom() != from.Addr() {
				t.Errorf("failed to receive msg from %s", from.Addr())
			}
		}
	}

	s := collectMsgDropperStats(peers)
	if peerCount*(peerCount-1) != s.MsgSent {
		t.Error("all msgs should be delivered")
	}

	// Create two partitions.
	split := peerCount / 2
	left, right := peers[:split], peers[split:]
	partition(left, right)

	// Peers within a partition can talk to each other.
	for _, from := range left {
		for _, to := range left {
			if from.Addr() == to.Addr() {
				continue
			}
			from.Send(&AppEnts{BaseMsg: BaseMsg{From: from.Addr(), To: to.Addr()}})

			if m := <-to.Receive(); m == nil || m.GetFrom() != from.Addr() {
				t.Errorf("failed to receive msg from %s", from.Addr())
			}
		}
	}

	for _, from := range right {
		for _, to := range right {
			if from.Addr() == to.Addr() {
				continue
			}
			from.Send(&AppEnts{BaseMsg: BaseMsg{From: from.Addr(), To: to.Addr()}})

			if m := <-to.Receive(); m == nil || m.GetFrom() != from.Addr() {
				t.Errorf("failed to receive msg from %s", from.Addr())
			}
		}
	}

	s = collectMsgDropperStats(peers)
	if 0 != s.MsgDropped {
		t.Error("all msgs should be delivered")
	}

	// Peers across partitions cannot talk to each other.
	for _, from := range left {
		for _, to := range right {
			from.Send(&AppEnts{BaseMsg: BaseMsg{From: from.Addr(), To: to.Addr()}})
			if m := recvFromChan(to); nil != m {
				t.Errorf("msg from %s should be dropped", from.Addr())
			}
		}
	}

	s = collectMsgDropperStats(peers)
	if dropped := len(left) * len(right); dropped != s.MsgDropped {
		t.Errorf("expected %d dropped msgs and got %d", dropped, s.MsgDropped)
	}

	// heal the partition and peers can talk to each other again.
	heal(left, right)
	for _, from := range peers {
		for _, to := range peers {
			if from.Addr() == to.Addr() {
				continue
			}
			from.Send(&AppEnts{BaseMsg: BaseMsg{From: from.Addr(), To: to.Addr()}})
			if m := <-to.Receive(); m == nil || m.GetFrom() != from.Addr() {
				t.Errorf("failed to receive msg from %s", from.Addr())
			}
		}
	}
}

// TestMsgDropperPartitionMem tests partioning and healing the network,
// based on MemTransport.
func TestMsgDropperPartitionMem(t *testing.T) {
	peers := testMsgDropperEnv(9, 9, 0)
	testMsgDropperPartition(peers, t)
}
