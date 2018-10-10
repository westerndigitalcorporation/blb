// Copyright (c) 2017 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package durable

import (
	"testing"

	"github.com/westerndigitalcorporation/blb/internal/core"
)

// Test that checksums behave as expected.
func TestChecksum(t *testing.T) {
	s := newState()

	c1 := s.checksum()
	if c1 != s.checksum() {
		t.Errorf("checksum is not deterministic")
	}

	s.NextCuratorID++
	s.NextCuratorID++

	c2 := s.checksum()
	if c1 == c2 {
		t.Errorf("NextCuratorID not included in checksum")
	}

	s.NextTractserverID += 123
	c3 := s.checksum()
	if c1 == c3 || c2 == c3 {
		t.Errorf("NextTractserverID not included in checksum")
	}

	s.Partitions = append(s.Partitions, core.CuratorID(543), core.CuratorID(210))
	c4 := s.checksum()
	if c1 == c4 || c2 == c4 || c3 == c4 {
		t.Errorf("Partitions not included in checksum")
	}

	if s.checksum() != c4 {
		t.Errorf("checksum is not deterministic")
	}
}
