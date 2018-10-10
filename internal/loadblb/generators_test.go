// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package loadblb

import "testing"

// Test baseGenerator behavior.
func TestBaseGenerator(t *testing.T) {
	start := int64(3)
	end := int64(10)
	var count int64
	variate := VariateConfig{
		Name:       "Constant",
		Parameters: []byte("23"),
	}
	g := newBaseGenerator(generatorConfig{
		Name:  "writer",
		Start: start,
		End:   end,
		Rate:  variate,
		Size:  variate,
	})
	for i := int64(0); i <= 2*end; i++ {
		if g.genSizes() != nil {
			count++
		}
	}
	if end-start != count {
		t.Fatalf("expected activation time %d and got %d", end-start, count)
	}
}
