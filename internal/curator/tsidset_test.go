// Copyright (c) 2017 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package curator

import (
	"reflect"
	"runtime"
	"testing"
	"unsafe"

	"github.com/westerndigitalcorporation/blb/internal/core"
)

const (
	id1 core.TractserverID = 1
	id2 core.TractserverID = 42
	id3 core.TractserverID = 424242
	idB core.TractserverID = 42424242 // > 20 bits
)

func spilled(s TSIDSet) bool {
	return s.tag() == 0 && s.p != nil
}

func TestTSIDSetSmall(t *testing.T) {
	var s TSIDSet

	if s.Len() != 0 {
		t.Errorf("empty len should be 0")
	}
	if s.Contains(id1) || s.Contains(id2) || s.Contains(id3) || s.Contains(idB) {
		t.Errorf("wrong membership")
	}

	s = s.Add(id1)
	if s.Len() != 1 {
		t.Errorf("len should be 1")
	}
	if !s.Contains(id1) || s.Contains(id2) || s.Contains(id3) || s.Contains(idB) {
		t.Errorf("wrong membership")
	}

	s = s.Add(id3)
	if s.Len() != 2 {
		t.Errorf("len should be 2")
	}
	if !s.Contains(id1) || s.Contains(id2) || !s.Contains(id3) || s.Contains(idB) {
		t.Errorf("wrong membership")
	}

	s = s.Add(id2)
	if s.Len() != 3 {
		t.Errorf("len should be 3")
	}
	if !s.Contains(id1) || !s.Contains(id2) || !s.Contains(id3) || s.Contains(idB) {
		t.Errorf("wrong membership")
	}

	if spilled(s) {
		t.Errorf("shouldn't have spilled")
	}

	if s.Get(0) != id1 || s.Get(1) != id3 || s.Get(2) != id2 {
		t.Errorf("wrong index results")
	}

	if !reflect.DeepEqual(
		s.ToSlice(),
		[]core.TractserverID{id1, id3, id2},
	) {
		t.Errorf("wrong toslice results")
	}
}

func TestTSIDSetSpillCount(t *testing.T) {
	s := TSIDSet{}.Add(1).Add(2).Add(3).Add(5).Add(7)
	if s.Len() != 5 {
		t.Errorf("len should be 5")
	}
	if !spilled(s) {
		t.Errorf("should have spilled")
	}
	if !s.Contains(1) || !s.Contains(2) || !s.Contains(3) || !s.Contains(5) || !s.Contains(7) || s.Contains(42) {
		t.Errorf("wrong membership")
	}
	if s.Get(0) != 1 || s.Get(1) != 2 || s.Get(2) != 3 || s.Get(3) != 5 || s.Get(4) != 7 {
		t.Errorf("wrong index results")
	}
	if !reflect.DeepEqual(
		s.ToSlice(),
		[]core.TractserverID{1, 2, 3, 5, 7},
	) {
		t.Errorf("wrong toslice results")
	}
}

func TestTSIDSetSpillBig(t *testing.T) {
	s := TSIDSet{}.Add(idB) // spill from 0
	if s.Len() != 1 {
		t.Errorf("len should be 1")
	}
	if !spilled(s) {
		t.Errorf("should have spilled")
	}
	if s.Contains(id1) || s.Contains(id2) || s.Contains(id3) || !s.Contains(idB) {
		t.Errorf("wrong membership")
	}
	if !reflect.DeepEqual(
		s.ToSlice(),
		[]core.TractserverID{idB},
	) {
		t.Errorf("wrong toslice results")
	}

	s = TSIDSet{}.Add(id3).Add(idB) // spill from 1
	if s.Len() != 2 {
		t.Errorf("len should be 2")
	}
	if !spilled(s) {
		t.Errorf("should have spilled")
	}
	if s.Contains(id1) || s.Contains(id2) || !s.Contains(id3) || !s.Contains(idB) {
		t.Errorf("wrong membership")
	}
	if !reflect.DeepEqual(
		s.ToSlice(),
		[]core.TractserverID{id3, idB},
	) {
		t.Errorf("wrong toslice results")
	}

}

func TestTSIDSetMerge(t *testing.T) {
	s1 := TSIDSet{}.Add(22).Add(44)
	s2 := TSIDSet{}.Add(55).Add(22).Add(33)
	s3 := TSIDSet{}.Add(11)
	// no spilling
	if !reflect.DeepEqual(s1.Merge(s3).ToSlice(),
		[]core.TractserverID{22, 44, 11}) {
		t.Errorf("s1.merge(s3) wrong")
	}
	if !reflect.DeepEqual(s3.Merge(s1).ToSlice(),
		[]core.TractserverID{11, 22, 44}) {
		t.Errorf("s3.merge(s1) wrong")
	}
	// spilling
	if !reflect.DeepEqual(s1.Merge(s2).ToSlice(),
		[]core.TractserverID{22, 44, 55, 33}) {
		t.Errorf("s1.merge(s2) wrong")
	}
	if !reflect.DeepEqual(s2.Merge(s1).ToSlice(),
		[]core.TractserverID{55, 22, 33, 44}) {
		t.Errorf("s2.merge(s1) wrong")
	}
}

func TestTSIDSetGC(t *testing.T) {
	s := TSIDSet{}.Add(1).Add(2).Add(3)
	gc := func(x TSIDSet) TSIDSet {
		runtime.GC() // trigger a GC just to make sure it's ok with our tricks
		return x.Add(5).Add(7).Add(42)
	}
	if gc(s).Len() != 6 {
		t.Errorf("len")
	}
	if unsafe.Sizeof(s) != 8 {
		t.Errorf("wrong size")
	}
}
