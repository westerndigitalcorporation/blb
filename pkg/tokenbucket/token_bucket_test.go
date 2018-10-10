// Copyright (c) 2016 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package tokenbucket

import (
	"math"
	"math/rand"
	"testing"
	"time"
)

func TestBasics(t *testing.T) {
	tb := New(100, 500)
	start := tb.last

	// t=1, take 100. expect no sleep.
	if tb.TakeAndUpdate(100, start.Add(1000*time.Millisecond)) > 0 {
		t.Errorf("a")
	}
	// t=2, take another 100. no sleep.
	if tb.TakeAndUpdate(100, start.Add(2000*time.Millisecond)) > 0 {
		t.Errorf("b")
	}
	// t=3, take 500, no sleep.
	if tb.TakeAndUpdate(500, start.Add(3000*time.Millisecond)) > 0 {
		t.Errorf("c")
	}
	// t=3.1, take 100. only 10 are available, so we should have to wait 0.9.
	if s := tb.TakeAndUpdate(100, start.Add(3000*time.Millisecond)); s < 800*time.Millisecond || s > 1000*time.Millisecond {
		t.Errorf("d")
	}
	// t=4.0, nothing should be available.
	if tb.TakeAndUpdate(10, start.Add(4000*time.Millisecond)) < 1 {
		t.Errorf("e")
	}
	// t=5.0, 90 should be available. take 100, so we have to wait 0.1.
	if s := tb.TakeAndUpdate(100, start.Add(5000*time.Millisecond)); s < 50*time.Millisecond || s > 150*time.Millisecond {
		t.Errorf("f")
	}

	// t=100, taking 500 should always be possible with no waiting.
	if tb.TakeAndUpdate(500, start.Add(100*time.Second)) > 0 {
		t.Errorf("g")
	}
	// t=200, taking 501 should not be possible without waiting.
	if tb.TakeAndUpdate(501, start.Add(200*time.Second)) < 0 {
		t.Errorf("h")
	}
}

func TestOneThread(t *testing.T) {
	testOneThread(t, 100, 0, 1, 1000)
	testOneThread(t, 100, 0, 10, 1000)
	testOneThread(t, 100, 0, 100, 1000)
	testOneThread(t, 100, 0, 1000, 1000)

	testOneThread(t, 100, 100, 10, 1000)
	testOneThread(t, 100, 200, 10, 1000)
	testOneThread(t, 100, 1000, 10, 1000)
	testOneThread(t, 100, 2000, 10, 1000)
}

func testOneThread(t *testing.T, rate, cap, unit, max float32) {
	expected := float64((max - cap) / rate)

	tb := New(rate, cap)
	start := tb.last
	now := start

	for i := float32(0); i < max; i += unit {
		sleep := tb.TakeAndUpdate(unit, now)
		if sleep < 0 {
			sleep = 0
		}
		now = now.Add(sleep)
	}

	elapsed := now.Sub(start).Seconds()
	if (elapsed > 0.001 || expected > 0.001) && math.Abs((elapsed-expected)/expected) > 0.01 {
		t.Errorf("wrong %v != %v", elapsed, expected)
	}
}

func TestThreads(t *testing.T) {
	rand.Seed(5557)

	testThreads(t, 100, 0, 1, 1000, 1)
	testThreads(t, 100, 0, 1, 1000, 10)
	testThreads(t, 100, 0, 1, 1000, 100)

	testThreads(t, 100, 100, 1, 1000, 10)
	testThreads(t, 100, 500, 1, 1000, 10)
}

func testThreads(t *testing.T, rate, cap, unit, max float32, n int) {
	expected := float64((max-cap/float32(n))/rate) * float64(n)

	tb := New(rate, cap)
	start := tb.last
	now := start

	slept := make([]time.Duration, n)

	// manually schedule "threads" fairly
	for _, x := range rand.Perm(n * int(max/unit)) {
		i := x % n
		sleep := tb.TakeAndUpdate(unit, now)
		if sleep < 0 {
			sleep = 0
		}
		slept[i] += sleep
		// sleep the right amount on average
		now = now.Add(sleep / time.Duration(n))
	}

	for _, s := range slept {
		elapsed := s.Seconds()
		if (elapsed > 0.001 || expected > 0.001) && math.Abs((elapsed-expected)/expected) > 0.05 {
			t.Errorf("wrong %v != %v", elapsed, expected)
		}
	}
}
