// Copyright (c) 2017 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package storageclass

import (
	"fmt"
	"reflect"

	"github.com/westerndigitalcorporation/blb/internal/core"
	pb "github.com/westerndigitalcorporation/blb/internal/curator/durable/state/statepb"
)

// Class is a representation of a storage class that allows manipulation of the
// tract metadata for that class.
type Class interface {
	// ID returns the enum for this class.
	ID() core.StorageClass
	// Has returns true if the tract is stored as this class.
	Has(tract *pb.Tract) bool
	// Clear removes this class' data for the tract.
	Clear(tract *pb.Tract)

	// The following methods only work for RS classes:

	// Set sets the RS pointer for this storage class.
	Set(tract *pb.Tract, key []byte) core.Error
	// RSParams returns the RS parameters for this class.
	RSParams() (n int, m int)
	// GetRS gets the RS pointer for this storage class, or nil if not present.
	GetRS(tract *pb.Tract) []byte
}

// All returns a slice of all known storage classes, including REPLICATED.
var All = makeAll()

func makeAll() []Class {
	out := []Class{repl{}} // make sure REPLICATED goes first
	for name, id := range core.StorageClass_value {
		var n, m int
		if items, err := fmt.Sscanf(name, "RS_%d_%d", &n, &m); err == nil && items == 2 {
			out = append(out, makeRS(core.StorageClass(id), n, m))
		}
	}
	return out
}

// AllRS returns a slice of all RS storage classes.
var AllRS = All[1:]

// Get returns the Class with the given id.
func Get(id core.StorageClass) Class {
	for _, c := range All {
		if c.ID() == id {
			return c
		}
	}
	return nil
}

// implements Class
type repl struct{}

func (r repl) ID() core.StorageClass {
	return core.StorageClass_REPLICATED
}

func (r repl) Has(tract *pb.Tract) bool {
	return len(tract.Hosts) >= 1
}

func (r repl) Clear(tract *pb.Tract) {
	tract.Hosts = nil
}

func (r repl) Set(tract *pb.Tract, key []byte) core.Error {
	return core.ErrInvalidArgument
}

func (r repl) RSParams() (int, int) {
	return -1, -1
}

func (r repl) GetRS(tract *pb.Tract) []byte {
	return nil
}

// implements Class
type rs struct {
	id    core.StorageClass // the storage class ID
	n, m  int               // RS parameters
	field int               // index of RsNMChunk field in pb.Tract
}

func makeRS(id core.StorageClass, n, m int) rs {
	name := fmt.Sprintf("Rs%d%dChunk", n, m)
	sf, ok := reflect.TypeOf(pb.Tract{}).FieldByName(name)
	if !ok || len(sf.Index) != 1 {
		panic(fmt.Sprintf("missing %s field in Tract proto message, or len(Index) != 1", name))
	}
	return rs{id, n, m, sf.Index[0]}
}

func (r rs) ID() core.StorageClass {
	return r.id
}

func (r rs) val(tract *pb.Tract) reflect.Value {
	return reflect.ValueOf(tract).Elem().Field(r.field)
}

func (r rs) Has(tract *pb.Tract) bool {
	return !r.val(tract).IsNil()
}

func (r rs) Clear(tract *pb.Tract) {
	r.val(tract).SetBytes(nil)
}

func (r rs) Set(tract *pb.Tract, key []byte) core.Error {
	v := r.val(tract)
	if !v.IsNil() {
		return core.ErrConflictingState
	}
	v.SetBytes(key)
	return core.NoError
}

func (r rs) RSParams() (int, int) {
	return r.n, r.m
}

func (r rs) GetRS(tract *pb.Tract) []byte {
	return r.val(tract).Bytes()
}
