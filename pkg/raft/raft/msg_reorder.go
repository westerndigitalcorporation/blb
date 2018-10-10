// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package raft

import (
	"encoding/json"
	"math/rand"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/westerndigitalcorporation/blb/pkg/failures"
)

// msgReorderStats defines the stats to collect for Transport.
type msgReorderStats struct {
	MsgSent    int // msgs sent.
	MsgDelayed int // msgs delayed.
	MsgPending int // msgs delayed and not sent yet.
}

// msgReorder is a composable transport that wraps another transport and
// reorders the delivery of messages in a configurable way. With a predefined
// probability, a message is delayed for a random amount of time before being
// forwarded to the lower transport.
type msgReorder struct {
	// The transport sits underneath, to which messages are forwarded.
	lower Transport

	// Probability for delaying a message.
	msgDelayProb float32

	// Maximum amount of time to delay a message.
	maxDelay time.Duration

	// Random number generator.
	rand *rand.Rand

	// Collected stats.
	stats msgReorderStats

	// Lock for 'rand' and 'stats'.
	lock sync.Mutex

	// Close waits for all pending messages to be sent.
	wg sync.WaitGroup
}

// NewMsgReorder creates a msgReorder. 'lower' is the wrapped transport, 'p' is
// the probability to delay a message, 'limit' is the maximum amount of time to
// delay a message.
func NewMsgReorder(lower Transport, p float32, limit time.Duration, seed int64) Transport {
	if p > 1.0 || p < 0.0 {
		log.Fatalf("p must be within range [0.0, 1.0]")
	}
	msgReorder := &msgReorder{
		lower:        lower,
		msgDelayProb: p,
		maxDelay:     limit,
		rand:         rand.New(rand.NewSource(seed)),
	}
	failures.Register("msg_reorder", msgReorder.reorderHandler)
	return msgReorder
}

func (d *msgReorder) reorderHandler(config json.RawMessage) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	log.Infof("Receive new config: %s", string(config))

	if config == nil {
		// Clear failures.
		d.msgDelayProb = 0.0
		d.maxDelay = time.Duration(0)
		return nil
	}

	var msg struct {
		DelayProb float64 `json:"prob"`
		MaxDelay  int64   `json:"delay"`
	}

	if err := json.Unmarshal(config, &msg); err != nil {
		return err
	}

	d.maxDelay = time.Duration(msg.MaxDelay) * time.Millisecond
	d.msgDelayProb = float32(msg.DelayProb)
	return nil
}

//---------- Transport implementation ----------//

// Addr delegates the call to the lower transport.
func (d *msgReorder) Addr() string {
	return d.lower.Addr()
}

// Receive delegates the call to the lower transport.
func (d *msgReorder) Receive() <-chan Msg {
	return d.lower.Receive()
}

// Send flips a coin to determine if the message should be delayed before
// sending it out. If so, the message is delayed for a random amount of time.
func (d *msgReorder) Send(m Msg) {
	d.lock.Lock()
	defer d.lock.Unlock()

	d.stats.MsgPending++

	// Flip a coin and determine if the message is to be delayed.
	if d.rand.Float32() >= d.msgDelayProb {
		d.lower.Send(m)
		d.stats.MsgSent++
		d.stats.MsgPending--
		return
	}

	d.stats.MsgDelayed++
	delay := d.rand.Int63n(int64(d.maxDelay))
	d.wg.Add(1)
	go d.sendWithDelay(m, time.Duration(delay))
}

// Close waits for all pending messages to be sent and then close the lower
// transport.
func (d *msgReorder) Close() error {
	d.wg.Wait()
	return d.lower.Close()
}

//---------- Helper methods ----------//

// sendWithDelay sends the message after delaying for the given time.
func (d *msgReorder) sendWithDelay(m Msg, delay time.Duration) {
	// Sleep for the given time.
	time.Sleep(delay)

	// Forward the message to the lower transport.
	d.lower.Send(m)
	log.V(10).Infof("delayed msg from %s to %s sent", m.GetFrom(), m.GetTo())

	// Update the stats.
	d.lock.Lock()
	d.stats.MsgSent++
	d.stats.MsgPending--
	d.lock.Unlock()

	// Signal the wait group.
	d.wg.Done()
}
