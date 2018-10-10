// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package server

// Semaphore implementation using Go channel.
type Semaphore chan struct{}

// NewSemaphore creates a new semaphore with 'max' number of permits.
func NewSemaphore(max int) Semaphore {
	return make(Semaphore, max)
}

// Acquire tries to acquire a permit from the semaphore, blocking
// until one becomes available.
func (s Semaphore) Acquire() {
	s <- struct{}{}
}

// Release releases a permit to the semaphore.
func (s Semaphore) Release() {
	<-s
}

// TryAcquire tries to acquire a permit from the semaphore. It
// succeeds if and only if one is available at the invocation time.
func (s Semaphore) TryAcquire() bool {
	select {
	case s <- struct{}{}:
		return true
	default:
		return false
	}
}
