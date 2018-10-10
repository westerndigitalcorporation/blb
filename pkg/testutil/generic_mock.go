// Copyright (c) 2017 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package testutil

import (
	"reflect"
	"sync"
	"testing"
)

// GenericMock is a simple library to help write mock objects. It's intended to
// be embedded in another struct that will define type-safe wrappers.
type GenericMock struct {
	t     *testing.T
	lock  sync.Mutex
	calls []mockCall
}

// NewGenericMock creates a new GenericMock. Errors will be reported with the
// given testing.T.
func NewGenericMock(t *testing.T) *GenericMock {
	return &GenericMock{t: t}
}

// AddCall registers a single call to be mocked. Arguments must match exactly
// (according to reflect.DeepEqual).
func (m *GenericMock) AddCall(method string, result interface{}, args ...interface{}) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.calls = append(m.calls, mockCall{
		method: method,
		args:   args,
		result: result,
		used:   false,
	})
}

// GetResult looks up a call for a given method and arguments. It will return
// the first unused call that matches. If no registered call matches, it will
// Fatalf on the testing context.
func (m *GenericMock) GetResult(method string, args ...interface{}) interface{} {
	m.lock.Lock()
	defer m.lock.Unlock()
	for i, call := range m.calls {
		if !call.used && call.method == method && reflect.DeepEqual(call.args, args) {
			m.calls[i].used = true
			return call.result
		}
	}
	m.t.Fatalf("no calls for method %q args %#v", method, args)
	return nil
}

// GetError looks up a call for a given method and arguments. It will return
// the first unused call that matches. If no registered call matches, it will
// Fatalf on the testing context. This is specialized for functions returning a
// single error value, so that callers don't have to do the nil check
// themselves.
func (m *GenericMock) GetError(method string, args ...interface{}) error {
	return ToErr(m.GetResult(method, args...))
}

// NoMoreCalls checks that there are no unused registered calls. If there are,
// it will Fatalf on the testing context.
func (m *GenericMock) NoMoreCalls() {
	m.lock.Lock()
	defer m.lock.Unlock()
	for _, call := range m.calls {
		if !call.used {
			m.t.Fatalf("unused call: %#v", call)
		}
	}
}

type mockCall struct {
	method string
	args   []interface{}
	result interface{}
	used   bool
}

// ToErr properly converts an interface{} to an error.
func ToErr(v interface{}) error {
	if v == nil {
		return nil
	}
	return v.(error)
}
