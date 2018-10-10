// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package raft

import (
	"strconv"
	"testing"
)

// testMemTransportEnv creates a set of connected 'peerCount'
// memTransports, each of which allows at most 'maxMsg' pending
// incoming msgs.
func testMemTransportEnv(peerCount, maxMsg int) (peers []*memTransport) {
	// Create a set of peers.
	for i := 0; i < peerCount; i++ {
		addr := strconv.Itoa(i)
		cfg := TransportConfig{Addr: addr, MsgChanCap: maxMsg}
		peers = append(peers, NewMemTransport(cfg).(*memTransport))
	}

	// Connect peers.
	connectAll(peers)
	return peers
}

// recvFromChan tries to receive a msg from the incoming msg
// channel of 't', and nil is returned if no msg is present. This
// method is handy in testing.
func recvFromChan(t Transport) Msg {
	select {
	case m := <-t.Receive():
		return m
	default:
		// Do nothing.
	}
	return nil
}

// memStats collects and aggregates stats from 'peers'.
func memStats(peers []*memTransport) (stats memTransportStats) {
	for _, p := range peers {
		s := p.stats
		stats.MsgSent += s.MsgSent
		stats.MsgDelivered += s.MsgDelivered
		stats.MsgRejected += s.MsgRejected
	}
	return stats
}

// TestMemTransportSendAndRecv tests sending and receiving msgs.
func TestMemTransportSendAndRecv(t *testing.T) {
	// Create a set of peers.
	peerCount, maxMsg := 30, 1
	peers := testMemTransportEnv(peerCount, maxMsg)

	// Peers send messages to each other.
	for _, from := range peers {
		for _, to := range peers {
			if from.addr == to.addr {
				continue
			}
			from.Send(&BaseMsg{From: from.addr, To: to.addr})
			if m := recvFromChan(to); m == nil || m.GetFrom() != from.addr {
				t.Errorf("failed to receive msg from %s", from.addr)
			}
		}
	}

	// Verify msg counts.
	s := memStats(peers)
	if sent := peerCount * (peerCount - 1); sent != s.MsgSent {
		t.Errorf("expected %d sent msgs and got %d", sent, s.MsgSent)
	} else if sent != s.MsgDelivered {
		t.Errorf("expected %d recieved msgs and got %d", sent, s.MsgDelivered)
	}
}

// TestMemTransportReject tests sending messages that exceed the
// receiver's channel capacity.
func TestMemTransportReject(t *testing.T) {
	// Create a group of peers.
	peerCount, maxMsg := 30, 15
	peers := testMemTransportEnv(peerCount, maxMsg)

	// Let each of the first peerCount-1 peers send a msg to
	// the last one. The first 'maxMsg' msgs should go
	// through and the rest should be rejected.
	target := peers[peerCount-1]
	to := target.addr
	for i := 0; i < peerCount-1; i++ {
		from := peers[i].addr
		peers[i].Send(&BaseMsg{From: from, To: to})
	}

	// Verify received msgs.
	for i := 0; i < maxMsg; i++ {
		from := peers[i].addr
		if m := recvFromChan(target); m == nil || m.GetFrom() != from {
			t.Errorf("failed to receive msg from %s", from)
		}
	}

	// Verify rejected msgs.
	for i := maxMsg; i < peerCount-1; i++ {
		from := peers[i].addr
		if m := recvFromChan(target); m != nil {
			t.Errorf("msg from %s should be rejected", from)
		}
	}

	// Verify msg counts.
	s := memStats(peers)
	if peerCount-1 != s.MsgSent {
		t.Errorf("expected %d sent msgs and got %d", peerCount-1, s.MsgSent)
	}
	if maxMsg != s.MsgDelivered {
		t.Errorf("expected %d recieved msgs and got %d", maxMsg, s.MsgDelivered)
	}
	if peerCount-1-maxMsg != s.MsgRejected {
		t.Errorf("expected %d rejected msgs and got %d", peerCount-1-maxMsg, s.MsgRejected)
	}
}
