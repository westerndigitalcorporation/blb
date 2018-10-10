// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package tractserver

import (
	"testing"
)

// Test basic operation registration and cancellation.
func TestOpTrack(t *testing.T) {
	ot := newOpTracker()

	// Create a new op.
	ctx := ot.start("foo")
	if ctx == nil {
		t.Fatalf("got nil context for new op")
	}

	// Shouldn't be able to create a new one w/the same ID.
	if ot.start("foo") != nil {
		t.Fatalf("could create op w/dup id")
	}

	// Shouldn't be able to receive from the done channel yet as it's still active.
	select {
	case <-ctx.Done():
		t.Fatalf("ctx is done but shouldn't be")
	default:
	}

	// Cancel the op.
	if !ot.cancel("foo") {
		t.Fatalf("could not cancel op?!")
	}

	// We expect to receive from ctx.Done indicating the op is dead.
	select {
	case <-ctx.Done():
	default:
		t.Fatalf("ctx should be done")
	}

	// Just for the sake of politeness, clean up.
	ot.end("foo")

	// This shouldn't crash, but it won't do anything.
	if ot.cancel("foo") {
		t.Fatalf("canceled op but op shouldn't exist")
	}
	ot.end("foo")
}
