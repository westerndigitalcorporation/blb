// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package tractserver

import (
	"container/heap"
	"errors"
	"sync"
)

// ErrQueueFull is the error returned when a push fails due to a full queue.
var ErrQueueFull = errors.New("Queue is full, cannot add additional items")

// PQAble if two elements are comparable, the one that is smaller will be ordered
// before the other.
type PQAble interface {
	// Less returns true if the element should be ordered before its peer.
	Less(interface{}) bool
}

// PriorityQueue of Requests, using the "container/heap" interface.
// The element with the highest priority() will be Pop()'d first.
type PriorityQueue struct {
	// Mutex to protect state
	lock sync.Mutex

	// Pop is a blocking operation and sleeps on this if the queue is empty.
	notEmpty sync.Cond

	// The actual data is stored in a container/heap-compatible structure.
	data pqHeap

	// Heap does not enforce any limit, but PriorityQueue does.
	max int
}

// NewPriorityQueue creates a new PriorityQueue with max capacity 'max'.
// If max is less than or equal to zero, there is no limit.
func NewPriorityQueue(max int) *PriorityQueue {
	q := &PriorityQueue{max: max}
	q.notEmpty.L = &q.lock
	return q
}

// TryPush tries to push an element to the priority queue.  If there is no space
// in the queue, returns ErrQueueFull.  Otherwise, returns nil.
func (pq *PriorityQueue) TryPush(item PQAble) error {
	pq.lock.Lock()
	defer pq.lock.Unlock()

	// The limit is only enforced if max > 0.
	if (pq.max > 0) && pq.data.Len() >= pq.max {
		return ErrQueueFull
	}

	heap.Push(&pq.data, item)

	// If we went from empty to not-empty wake up all waiters.
	// If we don't wake everyone up, we can deadlock if the following sequence occurs:
	// 1. Pop until empty.
	// 2. N threads block on Pop
	// 3. TryPush grows the queue from 0 to 1 item.
	// 4. Wake up just one thread, but it doesn't run yet.
	// 5. TryPush grows the queue from 1 to 100 items.
	// 6. Only one thread gets woken up, and N-1 wait, even though there's data.
	if pq.data.Len() == 1 {
		pq.notEmpty.Broadcast()
	}

	return nil
}

// Len returns the number of items currently in the queue.
func (pq *PriorityQueue) Len() int {
	pq.lock.Lock()
	defer pq.lock.Unlock()
	return pq.data.Len()
}

// Pop removes an element from the priority queue.  Blocks until an element can be removed.
func (pq *PriorityQueue) Pop() PQAble {
	pq.lock.Lock()
	for 0 == pq.data.Len() {
		pq.notEmpty.Wait()
	}
	defer pq.lock.Unlock()
	return heap.Pop(&pq.data).(PQAble)
}

// Internal data storage for the priority queue with receiver methods allowing
// use of container/heap.
type pqHeap []PQAble

// The methods below are required by the heap interface.

func (q pqHeap) Len() int {
	return len(q)
}

func (q pqHeap) Less(i, j int) bool {
	return q[i].Less(q[j])
}

func (q pqHeap) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
}

func (q *pqHeap) Push(x interface{}) {
	*q = append(*q, x.(PQAble))
}

func (q *pqHeap) Pop() interface{} {
	old := *q
	n := len(old)
	item := old[n-1]
	*q = old[0 : n-1]
	return item
}
