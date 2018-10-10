// Copyright (c) 2017 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package curator

import (
	"reflect"
	"testing"

	"github.com/westerndigitalcorporation/blb/internal/curator/durable/state"
)

func TestMergeUpdates(t *testing.T) {
	updates := []state.UpdateTime{
		{Blob: 10, MTime: 0, ATime: 100},
		{Blob: 20, MTime: 100, ATime: 0},
		{Blob: 10, MTime: 150, ATime: 0},
		{Blob: 20, MTime: 0, ATime: 120},
		{Blob: 10, MTime: 110, ATime: 0},
		{Blob: 30, MTime: 300, ATime: 300},
	}
	merged := mergeUpdates(updates)
	if !reflect.DeepEqual(merged,
		[]state.UpdateTime{
			{Blob: 10, MTime: 150, ATime: 100},
			{Blob: 20, MTime: 100, ATime: 120},
			{Blob: 30, MTime: 300, ATime: 300},
		}) {
		t.Errorf("wrong result: %v", merged)
	}
}
