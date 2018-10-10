// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package raft

import (
	"sync"

	log "github.com/golang/glog"
)

// memTransportStats defines the stats to collect for Transport.
type memTransportStats struct {
	MsgSent      int // msgs sent.
	MsgDelivered int // msgs delivered.
	MsgRejected  int // msgs rejected because the destination's channel is full.
}

// memTransport is an in-memory transport to facilitate testing the
// Raft implementation against various transport layer failures.
type memTransport struct {
	// Network address of the current node for receiving messages.
	addr string

	// Channel for incoming messages.
	c chan Msg

	// Set of peers in the replication group (excluding itself).
	peers map[string]*memTransport

	// Collected stats.
	stats memTransportStats

	// Lock for 'peers' and 'stats'.
	lock sync.Mutex
}

// NewMemTransport creates a new memTransport for the given 'addr'
// with 'maxMsg' allowable pending messages.
func NewMemTransport(config TransportConfig) Transport {
	return &memTransport{
		addr:  config.Addr,
		c:     make(chan Msg, config.MsgChanCap),
		peers: make(map[string]*memTransport),
	}
}

// connectAll connects each peer in 'peers' to the rest in the list
// and thus creates a fully-meshed network connectivity (aka, a clique
// in graph theory). This is the normal case for peers in a
// replication group.
func connectAll(peers []*memTransport) {
	for _, from := range peers {
		for _, to := range peers {
			// Skip itself.
			if from.addr == to.addr {
				continue
			}

			// Connect to others.
			from.lock.Lock()
			from.peers[to.addr] = to
			from.lock.Unlock()
			log.V(1).Infof("%s connected to %s", from.addr, to.addr)
		}
	}
}

//---------- Transport implementation ----------//

// Addr returns the local address of the transport.
func (t *memTransport) Addr() string {
	return t.addr
}

// Receive returns the incoming message channel.
func (t *memTransport) Receive() <-chan Msg {
	return t.c
}

// Send sends a message out without delay.
func (t *memTransport) Send(m Msg) {
	t.lock.Lock()
	defer t.lock.Unlock()

	// Check if the target exists.
	to := m.GetTo()
	target, ok := t.peers[to]
	if !ok {
		log.Errorf("%s doesn't exist", to)
		return
	}

	// Send the message over.
	t.stats.MsgSent++
	select {
	case target.c <- m:
		// Message is successfully delivered.
		t.stats.MsgDelivered++

	default:
		// Message is rejected because the channel is full. It
		// is the Raft core's responsibility to timeout and
		// resend the message.
		t.stats.MsgRejected++
	}
}

// Close does nothing.
func (t *memTransport) Close() error {
	return nil
}
