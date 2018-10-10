// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package tokenbucket

import (
	"sync"
	"time"
)

// TokenBucket implements the basic token bucket rate limiting algorithm.
// It is safe for use by multiple threads at once.
type TokenBucket struct {
	lock     sync.Mutex
	rate     float32
	capacity float32
	current  float32
	last     time.Time
}

// New returns a new token bucket that fills at the given rate
// (tokens per second) and has the given capacity (tokens).
func New(rate float32, capacity float32) *TokenBucket {
	return &TokenBucket{
		rate:     rate,
		capacity: capacity,
		current:  capacity,
		last:     time.Now(),
	}
}

// Take consumes n tokens from the bucket and sleeps until those tokens are replenished.
func (tb *TokenBucket) Take(n float32) {
	time.Sleep(tb.TakeAndUpdate(n, time.Now()))
}

// TakeAndUpdate updates the state of the bucket to a new time, consumes n tokens, leaving
// a negative balance if necessary, and returns how long the caller should sleep until
// there's a non-negative balance again (may be negative if there was enough capacity).
func (tb *TokenBucket) TakeAndUpdate(n float32, now time.Time) (sleepTime time.Duration) {
	tb.lock.Lock()

	// Add capacity based on elapsed time, capped at capacity.
	elapsed := now.Sub(tb.last)
	tb.last = now
	tb.current += tb.rate * float32(elapsed.Seconds())
	if tb.current > tb.capacity {
		tb.current = tb.capacity
	}
	tb.current -= n

	sleepTime = time.Duration(-tb.current / tb.rate * float32(time.Second))

	tb.lock.Unlock()
	return
}

// SetRate allows you to change the rate and capacity of this TokenBucket after it's created.
func (tb *TokenBucket) SetRate(rate, capacity float32) {
	tb.lock.Lock()
	tb.rate = rate
	tb.capacity = capacity
	tb.lock.Unlock()
}
