// Copyright (c) 2017 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package curator

import (
	"unsafe"

	"github.com/westerndigitalcorporation/blb/internal/core"
)

// TSIDSet is a representation of a set of tractserver IDs. Up to three IDs can
// be packed in a pointer with no external storage, as long as they fit in 20
// bits. The zero value (a nil pointer) represents the empty set. We use bit
// ranges (from lsb):
//   0-1: tag bits: 0 is pointer, 1-3 is count of ids
//   2-21: id 1
//   22-41: id 2
//   42-61: id 3
//   62: unused
//   63: always set if tag != 0 (to make it not look like a pointer)
// If we have more than three IDs, or any ID can't fit in 20 bits, then we hold
// a pointer to a plain []core.TractserverID.
//
// We call this a "Set" because it's used like one, even though it does preserve
// order, and Add doesn't check for duplicates.
//
// This casting is technically not allowed by the unsafe rules. To make it a
// little safer, we set the highest bit so the stuffed values definitely don't
// look like a valid pointer (on x86-64). Nothing should ever look at these
// except the GC, and the GC will skip pointers that fall outside the malloc
// heap range (as of Go 1.7).
type TSIDSet struct {
	p tsidsetptr
}

type tsidsetptr *[]core.TractserverID

const maxSmallID = 1<<20 - 1

// Len returns the number of ids stored in this set.
func (s TSIDSet) Len() int {
	if t := s.tag(); t != 0 {
		return int(t)
	} else if s.p == nil {
		return 0
	}
	return len(*s.p)
}

// Get returns the id at the given index. It will panic if idx is out of bounds
// (just like slice indexing).
func (s TSIDSet) Get(idx int) core.TractserverID {
	if t := s.tag(); t == 0 && s.p != nil {
		return (*s.p)[idx]
	} else if idx < 0 || idx >= t {
		panic("index out of range")
	}
	v := uintptr(unsafe.Pointer(s.p))
	return core.TractserverID((v >> (uint(idx)*20 + 2)) & maxSmallID)
}

// Add returns a new TSIDSet with the new id added. The old value should be
// considered invalidated (just like append).
func (s TSIDSet) Add(id core.TractserverID) TSIDSet {
	if s.p == nil {
		if id <= maxSmallID {
			v := uintptr(id)<<2 | 1 | 1<<63
			return TSIDSet{tsidsetptr(unsafe.Pointer(v))}
		}
		return TSIDSet{&[]core.TractserverID{id}}
	}
	switch s.tag() {
	case 0:
		*s.p = append(*s.p, id)
		return s
	case 1:
		v := uintptr(unsafe.Pointer(s.p))
		one := (v >> 2) & maxSmallID
		if id <= maxSmallID {
			v = one<<2 | uintptr(id)<<22 | 2 | 1<<63
			return TSIDSet{tsidsetptr(unsafe.Pointer(v))}
		}
		return TSIDSet{&[]core.TractserverID{core.TractserverID(one), id}}
	case 2:
		v := uintptr(unsafe.Pointer(s.p))
		one := (v >> 2) & maxSmallID
		two := (v >> 22) & maxSmallID
		if id <= maxSmallID {
			v = one<<2 | two<<22 | uintptr(id)<<42 | 3 | 1<<63
			return TSIDSet{tsidsetptr(unsafe.Pointer(v))}
		}
		return TSIDSet{&[]core.TractserverID{core.TractserverID(one), core.TractserverID(two), id}}
	case 3:
		v := uintptr(unsafe.Pointer(s.p))
		one := (v >> 2) & maxSmallID
		two := (v >> 22) & maxSmallID
		thr := (v >> 42) & maxSmallID
		return TSIDSet{&[]core.TractserverID{core.TractserverID(one), core.TractserverID(two), core.TractserverID(thr), id}}
	}
	panic("unreachable")
}

// Contains returns true if the given id is in the set.
func (s TSIDSet) Contains(id core.TractserverID) bool {
	if s.p == nil {
		return false
	}
	switch s.tag() {
	case 0:
		return contains(*s.p, id)
	case 1:
		v := uintptr(unsafe.Pointer(s.p))
		one := core.TractserverID((v >> 2) & maxSmallID)
		return id == one
	case 2:
		v := uintptr(unsafe.Pointer(s.p))
		one := core.TractserverID((v >> 2) & maxSmallID)
		two := core.TractserverID((v >> 22) & maxSmallID)
		return id == one || id == two
	case 3:
		v := uintptr(unsafe.Pointer(s.p))
		one := core.TractserverID((v >> 2) & maxSmallID)
		two := core.TractserverID((v >> 22) & maxSmallID)
		thr := core.TractserverID((v >> 42) & maxSmallID)
		return id == one || id == two || id == thr
	}
	panic("unreachable")
}

// Merge returns a new set with all the ids in this set and another set.
func (s TSIDSet) Merge(t TSIDSet) TSIDSet {
	l := t.Len()
	for i := 0; i < l; i++ {
		n := t.Get(i)
		if !s.Contains(n) {
			s = s.Add(n)
		}
	}
	return s
}

// ToSlice returns a slice with the same ids as this set.
func (s TSIDSet) ToSlice() []core.TractserverID {
	if s.p == nil {
		return nil
	}
	switch s.tag() {
	case 0:
		return *s.p
	case 1:
		v := uintptr(unsafe.Pointer(s.p))
		one := core.TractserverID((v >> 2) & maxSmallID)
		return []core.TractserverID{one}
	case 2:
		v := uintptr(unsafe.Pointer(s.p))
		one := core.TractserverID((v >> 2) & maxSmallID)
		two := core.TractserverID((v >> 22) & maxSmallID)
		return []core.TractserverID{one, two}
	case 3:
		v := uintptr(unsafe.Pointer(s.p))
		one := core.TractserverID((v >> 2) & maxSmallID)
		two := core.TractserverID((v >> 22) & maxSmallID)
		thr := core.TractserverID((v >> 42) & maxSmallID)
		return []core.TractserverID{one, two, thr}
	}
	panic("unreachable")
}

func (s TSIDSet) tag() int {
	return int(uintptr(unsafe.Pointer(s.p)) & 3)
}
