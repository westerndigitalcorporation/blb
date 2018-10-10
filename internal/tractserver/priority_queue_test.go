// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT
//
// Tests for priority_queue.go
package tractserver

import (
	"sync"
	"testing"
)

// A test element to ensure the priority queue ranks things correctly.
type testElt struct {
	pri int
}

func (t testElt) Less(ele interface{}) bool {
	e := ele.(testElt)
	return t.pri > e.pri
}

// Test that highest priority is dequeued first.
func TestPriorityInOrder(t *testing.T) {
	size := 10
	pq := NewPriorityQueue(size)

	// Insert elements with priorities 0 through 9 inclusive.
	for i := 0; i < size; i++ {
		if nil != pq.TryPush(testElt{i}) {
			t.Fatal("couldn't push element ", i)
		}
		if i+1 != pq.Len() {
			t.Fatal("wrong size pq")
		}
	}

	// Pushing one more should fail.
	if nil == pq.TryPush(testElt{size}) {
		t.Fatal("tryPush should have failed.")
	}

	// Should get them in descending priority order.
	for i := 0; i < size; i++ {
		elt := pq.Pop().(testElt)
		if elt.pri != 9-i {
			t.Fatal("didn't get items back in order")
		}
	}
}

// Attempt to test that multiple waiters wait and wake up.
func TestParallelPushPop(t *testing.T) {
	size := 20
	pq := NewPriorityQueue(size)

	// We'll start this many pop-ers and push-ers.
	iter := 1000
	var wg sync.WaitGroup

	// Start routines to pull.
	for i := 0; i < iter; i++ {
		wg.Add(1)
		go func() {
			pq.Pop()
			wg.Done()
		}()
	}

	// Start routines to try to push until success.
	for i := 0; i < iter; i++ {
		wg.Add(1)
		go func(i int) {
			item := testElt{i}
			for nil != pq.TryPush(item) {
			}
			wg.Done()
		}(i)
	}

	wg.Wait()
}

// Test that max of 0 and max of -1 means no max.
func TestNoMax(t *testing.T) {
	pq := NewPriorityQueue(0)
	for i := 0; i < 100; i++ {
		if nil != pq.TryPush(testElt{i}) {
			t.Fatal("expect unlimited pushing")
		}
	}

	pq = NewPriorityQueue(-1)
	for i := 0; i < 100; i++ {
		if nil != pq.TryPush(testElt{i}) {
			t.Fatal("expect unlimited pushing")
		}
	}
}
