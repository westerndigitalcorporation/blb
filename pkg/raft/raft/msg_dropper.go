// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package raft

import (
	"encoding/json"
	"math/rand"
	"sync"

	log "github.com/golang/glog"
	"github.com/westerndigitalcorporation/blb/pkg/failures"
)

// msgDropperStats defines the stats to collect for Transport.
type msgDropperStats struct {
	MsgSent    int // msgs sent.
	MsgDropped int // msgs dropped by the network.
}

// msgDropper is a composable transport that wraps another transport
// and drops messages in a configurable way. If a message is
// determined to be dropped, the corresponding counters are
// incremented and the message is thrown away; otherwise, the message
// is forwarded to the lower layer. msgDropper is useful for
// simulating network failures such as link breakage and network
// partitioning.
type msgDropper struct {
	// The transport sits underneath, to which undropped messages
	// are forwarded.
	lower Transport

	// Probability for dropping a message on the wire.
	msgDropProb map[string]float32

	// Random number generator.
	rand *rand.Rand

	// Collected stats.
	stats msgDropperStats

	// Lock for 'rand' and 'stats'.
	lock sync.Mutex

	defaultProb float32
}

// NewMsgDropper creates a new msgDropper. No message is dropped by
// default.
func NewMsgDropper(lower Transport, seed int64, defaultProb float32) Transport {
	if defaultProb > 1.0 || defaultProb < 0.0 {
		log.Fatalf("p must be within range [0.0, 1.0]")
	}
	msgDropper := &msgDropper{
		lower:       lower,
		msgDropProb: make(map[string]float32),
		rand:        rand.New(rand.NewSource(seed)),
		defaultProb: defaultProb,
	}
	failures.Register("msg_drop_prob", msgDropper.dropHandler)
	return msgDropper
}

func (d *msgDropper) dropHandler(config json.RawMessage) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	log.Infof("Receive new config: %s", string(config))

	if config == nil {
		// Clear failures.
		d.msgDropProb = make(map[string]float32)
		return nil
	}

	var dropMap map[string]float32
	if err := json.Unmarshal(config, &dropMap); err != nil {
		return err
	}

	// Reset failures.
	d.msgDropProb = dropMap
	return nil
}

//---------- Transport implementation ----------//

// Addr delegates the call to the lower transport.
func (d *msgDropper) Addr() string {
	return d.lower.Addr()
}

// Receive delegates the call to the lower transport.
func (d *msgDropper) Receive() <-chan Msg {
	return d.lower.Receive()
}

// Send delegates the call to the lower transport with the given
// probability.
func (d *msgDropper) Send(m Msg) {
	d.lock.Lock()
	defer d.lock.Unlock()

	to := m.GetTo()
	d.stats.MsgSent++

	// Flip a coin and decide if the message should be dropped.
	prob, ok := d.msgDropProb[to]
	if !ok {
		prob = d.defaultProb
	}
	if d.rand.Float32() < prob {
		d.stats.MsgDropped++
		log.V(10).Infof("msg from %s to %s dropped", m.GetFrom(), to)
		return
	}
	d.lower.Send(m)
	log.V(10).Infof("msg from %s to %s delivered", m.GetFrom(), to)
}

// Close delegates the call to the lower transport.
func (d *msgDropper) Close() error {
	return d.lower.Close()
}

//---------- Topology manipulations ----------//

// Set sets the message drop probability for the link to 'to' to 'p'.
func (d *msgDropper) Set(to string, p float32) {
	d.lock.Lock()
	defer d.lock.Unlock()
	d.msgDropProb[to] = p
}

// Link wraps Set(to, 0.0) and establishes the link to 'to'.
func (d *msgDropper) Link(to string) {
	d.Set(to, 0.0)
}

// Unlink wraps Set(to, 1.0) and disconnect the link to 'to'.
func (d *msgDropper) Unlink(to string) {
	d.Set(to, 1.0)
}

// setAll sets the message drop probability for all 'peers'.
func setAll(peers []*msgDropper, p float32) {
	for _, from := range peers {
		for _, to := range peers {
			if from.Addr() == to.Addr() {
				continue
			}
			from.Set(to.Addr(), p)
		}
	}
}

// Partition partitions peers into two groups as specified by 'left'
// and 'right'. Peers in each group are fully connected, but no
// message can go through between partitions. It is assumed that
// 'left' and 'right' are originally in the same replication group.
func partition(left, right []*msgDropper) {
	for _, l := range left {
		for _, r := range right {
			if l.Addr() == r.Addr() {
				continue
			}
			l.Unlink(r.Addr())
			r.Unlink(l.Addr())
		}
	}
	log.V(1).Infof("partitions created: %s, %s", listPeers(left), listPeers(right))
}

// heal is the reverse op of 'Partition' that reconnects the two
// partitions 'left' and 'right' so that they together form a fully
// connected group. It is assumed that 'left' and 'right' are
// originally connected within their own partitions.
func heal(left, right []*msgDropper) {
	for _, l := range left {
		for _, r := range right {
			if l.Addr() == r.Addr() {
				continue
			}
			l.Link(r.Addr())
			r.Link(l.Addr())
		}
	}
	log.V(1).Infof("partitions healed: %s, %s", listPeers(left), listPeers(right))
}

// listPeers returns string representing a list of peers.
func listPeers(peers []*msgDropper) string {
	s := "["
	for i, p := range peers {
		s += p.Addr()
		if i != len(peers)-1 {
			s += ", "
		}
	}
	s += "]"
	return s
}
