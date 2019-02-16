// Copyright (c) 2017 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package storageclass

import (
	"fmt"
	"reflect"

	"github.com/westerndigitalcorporation/blb/internal/core"
	"github.com/westerndigitalcorporation/blb/internal/curator/durable/state/fb"
)

// Class is a representation of a storage class that allows manipulation of the
// tract metadata for that class.
type Class interface {
	// ID returns the enum for this class.
	ID() core.StorageClass
	// Has returns true if the tract is stored as this class.
	Has(tract *fb.TractF) bool
	// Clear removes this class' data for the tract.
	Clear(tract *fb.Tract)

	// The following methods only work for RS classes:

	// Set sets the RS pointer for this storage class.
	Set(tract *fb.Tract, cid core.RSChunkID) core.Error
	// RSParams returns the RS parameters for this class.
	RSParams() (n int, m int)
	// GetRS gets the RS pointer for this storage class.
	GetRS(tract *fb.TractF) core.RSChunkID
}

// All returns a slice of all known storage classes, including REPLICATED.
var All = makeAll()

func makeAll() []Class {
	out := []Class{repl{}} // make sure REPLICATED goes first
	for id, name := range core.EnumNamesStorageClass {
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
	return core.StorageClassREPLICATED
}

func (r repl) Has(tract *fb.TractF) bool {
	return tract.HostsLength() >= 1
}

func (r repl) Clear(tract *fb.Tract) {
	tract.Hosts = nil
}

func (r repl) Set(tract *fb.Tract, cid core.RSChunkID) core.Error {
	return core.ErrInvalidArgument
}

func (r repl) RSParams() (int, int) {
	return -1, -1
}

func (r repl) GetRS(tract *fb.TractF) core.RSChunkID {
	return core.RSChunkID{}
}

// implements Class
type rs struct {
	id     core.StorageClass // the storage class ID
	n, m   int               // RS parameters
	field  int               // index of RsNMChunk field in fb.Tract
	method int               // index of RsNMChunk method in fb.TractF
}

func makeRS(id core.StorageClass, n, m int) rs {
	name := fmt.Sprintf("Rs%d%dChunk", n, m)
	sf, ok := reflect.TypeOf(fb.Tract{}).FieldByName(name)
	if !ok || len(sf.Index) != 1 {
		panic(fmt.Sprintf("missing %s field in Tract struct, or len(Index) != 1", name))
	}
	sff, ok := reflect.TypeOf(&fb.TractF{}).MethodByName(name)
	if !ok {
		panic(fmt.Sprintf("missing %s method in TractF struct", name))
	}
	return rs{id, n, m, sf.Index[0], sff.Index}
}

func (r rs) ID() core.StorageClass {
	return r.id
}

func (r rs) val(tract *fb.Tract) reflect.Value {
	return reflect.ValueOf(tract).Elem().Field(r.field)
}

func (r rs) valfb(tract *fb.TractF) reflect.Value {
	return reflect.ValueOf(tract).Method(r.method)
}

func (r rs) Has(tract *fb.TractF) bool {
	f := r.valfb(tract).Interface().(func(*fb.TractIDF) *fb.TractIDF)
	var t fb.TractIDF
	return f(&t) != nil
}

func (r rs) Clear(tract *fb.Tract) {
	var none *core.TractID
	r.val(tract).Set(reflect.ValueOf(none))
}

func (r rs) Set(tract *fb.Tract, cid core.RSChunkID) core.Error {
	v := r.val(tract)
	if !v.IsNil() {
		return core.ErrConflictingState
	}
	tid := cid.ToTractID()
	v.Set(reflect.ValueOf(&tid))
	return core.NoError
}

func (r rs) RSParams() (int, int) {
	return r.n, r.m
}

func (r rs) GetRS(tract *fb.TractF) core.RSChunkID {
	f := r.valfb(tract).Interface().(func(*fb.TractIDF) *fb.TractIDF)
	var t fb.TractIDF
	if cid := f(&t); cid != nil {
		return cid.RSChunkID()
	}
	return core.RSChunkID{}
}
