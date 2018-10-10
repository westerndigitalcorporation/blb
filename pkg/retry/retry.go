// Copyright (c) 2018 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package retry

import (
	"context"
	"math/rand"
	"time"
)

// Task to execute with retries in the Do method.
// On every execution, it receives the iteration number.
// It should return true if it completes successfully and false if it should be retried.
type Task func(int) (done bool)

type Retrier struct {
	// MinSleep is the shortest and initial sleep time to be
	// used during the retry loop.
	MinSleep time.Duration

	// MaxSleep is the longest sleep time to be used during
	// the retry loop.  It is ignored for the Constant Retrier.
	MaxSleep time.Duration

	// MaxRetry, if greater than zero, will be used to bound the
	// total time to execute the retry loop.
	MaxRetry time.Duration

	// MaxNumRetries, if greater than zero, will limit the number of retry attempts.
	MaxNumRetries int
}

// Do will execute the given Task, retrying when the task returns false.
// If task returns true, Do will return (true, false).
// If it hits the maximum retry count or time, it will return (false, false).
// If the context is cancelled, it will return (false, true).
func (r *Retrier) Do(ctx context.Context, task Task) (success, cancelled bool) {
	if r.MaxSleep < r.MinSleep {
		r.MaxSleep = r.MinSleep
	}
	backoff := r.MinSleep
	start := time.Now()
	for i := 0; ; i++ {
		if r.MaxNumRetries > 0 && i >= r.MaxNumRetries ||
			r.MaxRetry > 0 && time.Since(start)+backoff > r.MaxRetry {
			return false, false
		}
		if task(i) {
			return true, false
		}
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return false, true
		}
		backoff = time.Duration(float64(backoff) * (1.75 + 0.5*rand.Float64()))
		if backoff > r.MaxSleep {
			backoff = r.MaxSleep + time.Duration(float64(r.MinSleep)*rand.Float64())
		}
	}
}
