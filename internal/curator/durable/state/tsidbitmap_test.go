// Copyright (c) 2017 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package state

import (
	"reflect"
	"testing"

	"github.com/westerndigitalcorporation/blb/internal/core"
)

func TestBitmap(t *testing.T) {
	var bm tsidbitmap

	// test setting a few small values
	bm = bm.Set(1).Set(2).Set(5)
	if !reflect.DeepEqual(bm.ToSlice(), []core.TractserverID{1, 2, 5}) {
		t.Errorf("mismatch")
	}

	// test larger values
	bm = bm.Set(123).Set(4567)
	if !reflect.DeepEqual(bm.ToSlice(), []core.TractserverID{1, 2, 5, 123, 4567}) {
		t.Errorf("mismatch")
	}

	// merge
	var dirty bool
	bm, dirty = bm.Merge(tsidbitmap{}.Set(2).Set(999).Set(5).Set(13))
	if !reflect.DeepEqual(bm.ToSlice(), []core.TractserverID{1, 2, 5, 13, 123, 999, 4567}) {
		t.Errorf("mismatch")
	}
	if dirty != true {
		t.Errorf("should be dirty")
	}

	// merge with nothing new
	bm, dirty = bm.Merge(tsidbitmap{}.Set(999).Set(5))
	if dirty != false {
		t.Errorf("should not be dirty")
	}

	// serializing
	bm = tsidbitmapFromBytes(bm.ToBytes())
	if !reflect.DeepEqual(bm.ToSlice(), []core.TractserverID{1, 2, 5, 13, 123, 999, 4567}) {
		t.Errorf("mismatch")
	}
}
