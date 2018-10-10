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

// msgDuplicatorStats defines the stats to collect for Transport.
type msgDuplicatorStats struct {
	MsgSent       int // msgs sent.
	msgDuplicated int // msgs duplicated.
}

// msgDuplicator is a composable transport that wraps another
// transport and duplicates messages in a configurable way. It caches
// certain amount of sent messages in a message pool. When it decides
// to sent a duplicated message, it randomly selects a cached message
// from the pool and forward it to the lower transport.
type msgDuplicator struct {
	// The transport sits underneath, to which undropped messages
	// are forwarded.
	lower Transport

	// A pool of cached messages that duplicates are chosen from.
	msgPool *msgPool

	// Probability for sending a duplicated message each time when
	// a new message is sent.
	msgDupProb float32

	// Random number generator.
	rand *rand.Rand

	// Collected stats.
	stats msgDuplicatorStats

	// Lock for 'msgPool', 'rand' and 'stats'.
	lock sync.Mutex
}

// NewMsgDuplicator creates a msgDuplicator. 'lower' is the wrapped
// transport, 'limit' is the maximum number of cached messages, 'p' is
// the probability to send a duplicated message when Send is called.
func NewMsgDuplicator(lower Transport, limit int, p float32, seed int64) Transport {
	if p > 1.0 || p < 0.0 {
		log.Fatalf("p must be within range [0.0, 1.0]")
	}
	msgDuplicator := &msgDuplicator{
		lower:      lower,
		msgPool:    newMsgPool(limit),
		msgDupProb: p,
		rand:       rand.New(rand.NewSource(seed)),
	}
	failures.Register("msg_duplicator", msgDuplicator.duplicateHandler)
	return msgDuplicator
}

func (d *msgDuplicator) duplicateHandler(config json.RawMessage) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	log.Infof("Receive new config: %s", string(config))

	if config == nil {
		// Clear failures.
		d.msgDupProb = 0.0
		return nil
	}

	var prob float64
	if err := json.Unmarshal(config, &prob); err != nil {
		return err
	}

	d.msgDupProb = float32(prob)
	return nil
}

//---------- Transport implementation ----------//

// Addr delegates the call to the lower transport.
func (d *msgDuplicator) Addr() string {
	return d.lower.Addr()
}

// Receive delegates the call to the lower transport.
func (d *msgDuplicator) Receive() <-chan Msg {
	return d.lower.Receive()
}

// Send delegates the call to the lower transport. It also sends a
// duplicated message with the predefined probability.
func (d *msgDuplicator) Send(m Msg) {
	d.lock.Lock()
	defer d.lock.Unlock()

	// Send the message and add it to the cache.
	d.lower.Send(m)
	d.stats.MsgSent++
	d.msgPool.add(m)

	// Flip a coin and decide if a duplicated message should be
	// sent. If so, randomly select a cached message to send.
	if d.rand.Float32() < d.msgDupProb {
		i := d.rand.Intn(d.msgPool.size())
		d.lower.Send(d.msgPool.get(i))
		d.stats.MsgSent++
		d.stats.msgDuplicated++
		log.V(10).Infof("duplicated msg from %s to %s sent", m.GetFrom(), m.GetTo())
	}
}

// Close delegates the call to the lower transport.
func (d *msgDuplicator) Close() error {
	return d.lower.Close()
}

//---------- msgPool ----------//

// msgPool provides a simple way to store a set of messages with a
// predefined count limit. Adding new messages beyond the limit will
// remove old messages accordingly.
// TODO: To simplify the code (an excuse for being lazy), there is no
// intention to optimize and reuse allocated memory. One can possibly
// write something like a circular buffer to improve efficiency if it
// matters.
type msgPool struct {
	// A set of cached messages.
	pool []Msg

	// Limit of the pool size.
	limit int
}

// newMsgPool creates a message pool.
func newMsgPool(limit int) *msgPool {
	return &msgPool{limit: limit}
}

// size returns the size of the pool.
func (p *msgPool) size() int {
	return len(p.pool)
}

// add adds message 'm' to the pool. The oldest message is removed if
// the capacity has reached.
func (p *msgPool) add(m Msg) {
	p.pool = append(p.pool, m)
	if p.limit < len(p.pool) {
		// The maximum number of messages in pool can't exceed the limit.
		p.pool = p.pool[1:]
	}
}

// get returns messge indexed at 'i' in the pool.
func (p *msgPool) get(i int) Msg {
	return p.pool[i]
}
