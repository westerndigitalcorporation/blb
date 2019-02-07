// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package blb

import (
	"context"
	"sync"

	"github.com/westerndigitalcorporation/blb/internal/core"
)

// A tsTraceEntry contains the args for a read or write on a tractserver (but
// not the bulk data).
type tsTraceEntry struct {
	write   bool
	addr    string
	id      core.TractID
	version int
	length  int
	off     int64
}

// A tsTraceFunc is called for each Write or Read to a fake tractserver so they
// can be logged for testing purposes, and also to inject errors.
type tsTraceFunc func(tsTraceEntry) core.Error

// A memTractserver simulates a fake tractserver in memory.
type memTractserver struct {
	versions map[core.TractID]int    // Version data
	data     map[core.TractID][]byte // Bulk data
}

// A memTractserverTalker simulates a bunch of fake tractservers in memory.
type memTractserverTalker struct {
	lock         sync.Mutex
	tractservers map[string]*memTractserver // All my tractservers
	trace        tsTraceFunc                // Send traces here
}

// Create creates a new tract on the tractserver and does a write to the newly
// created tract.
func (tt *memTractserverTalker) Create(ctx context.Context, addr string, tsid core.TractserverID, id core.TractID, b []byte, off int64) core.Error {
	tt.lock.Lock()
	ts := tt.getTractserver(addr)
	_, ok := ts.versions[id]
	if ok {
		tt.lock.Unlock()
		return core.ErrAlreadyExists
	}
	ts.versions[id] = 1
	tt.lock.Unlock()
	return tt.Write(context.Background(), addr, id, 1, b, off)
}

// Write writes the given data to a tract.
func (tt *memTractserverTalker) Write(ctx context.Context, addr string, id core.TractID, version int, b []byte, off int64) core.Error {
	tt.lock.Lock()
	if version < 0 {
		return core.ErrBadVersion
	}

	defer tt.lock.Unlock()
	ts := tt.getTractserver(addr)

	if e := tt.trace(tsTraceEntry{true, addr, id, version, len(b), off}); e != core.NoError {
		return e
	}

	myVersion, ok := ts.versions[id]
	if !ok {
		return core.ErrNoSuchTract
	}
	if myVersion != version {
		return core.ErrVersionMismatch
	}

	// Grow data if necessary
	data := ts.data[id]
	if len(data) < int(off)+len(b) {
		data = append(data, make([]byte, int(off)+len(b)-len(data))...)
		ts.data[id] = data
	}
	copy(data[off:], b)

	return core.NoError
}

// Read reads from a given tract.
func (tt *memTractserverTalker) Read(ctx context.Context, addr string, id core.TractID, version int, length int, off int64) ([]byte, core.Error) {
	if version < 0 {
		return nil, core.ErrBadVersion
	}

	tt.lock.Lock()
	defer tt.lock.Unlock()
	ts := tt.getTractserver(addr)

	if e := tt.trace(tsTraceEntry{false, addr, id, version, length, off}); e != core.NoError {
		return nil, e
	}

	myVersion, ok := ts.versions[id]
	if !ok {
		return nil, core.ErrNoSuchTract
	}
	if myVersion != version {
		return nil, core.ErrVersionMismatch
	}
	data := ts.data[id]
	if int(off) > len(data) {
		return nil, core.ErrEOF
	}
	if int(off)+length > len(data) {
		return copySlice(data[off:]), core.ErrEOF
	}
	return copySlice(data[off : int(off)+length]), core.NoError
}

func copySlice(b []byte) []byte {
	r := make([]byte, len(b))
	copy(r, b)
	return r
}

// ReadInto reads from a given tract.
func (tt *memTractserverTalker) ReadInto(ctx context.Context, addr string, otherHosts []string, reqID string, id core.TractID, version int, b []byte, off int64) (int, core.Error) {
	r, err := tt.Read(ctx, addr, id, version, len(b), off)
	return copy(b, r), err
}

// StatTract returns the number of bytes in a tract.
func (tt *memTractserverTalker) StatTract(ctx context.Context, addr string, id core.TractID, version int) (int64, core.Error) {
	if version < 0 {
		return 0, core.ErrBadVersion
	}

	tt.lock.Lock()
	defer tt.lock.Unlock()
	ts := tt.getTractserver(addr)

	myVersion, ok := ts.versions[id]
	if !ok {
		return 0, core.ErrNoSuchTract
	}
	if myVersion != version {
		return 0, core.ErrVersionMismatch
	}

	return int64(len(ts.data[id])), core.NoError
}

func (tt *memTractserverTalker) GetDiskInfo(ctx context.Context, addr string) ([]core.FsStatus, core.Error) {
	return nil, core.ErrNotYetImplemented
}

func (tt *memTractserverTalker) SetControlFlags(ctx context.Context, addr string, root string, flags core.DiskControlFlags) core.Error {
	return core.ErrNotYetImplemented
}

// getTractserver creates a new fake tractserver with no data initially, that
// will send a trace of reads and writes to the given function.
func (tt *memTractserverTalker) getTractserver(addr string) *memTractserver {
	if ts, ok := tt.tractservers[addr]; ok {
		return ts
	}
	ts := &memTractserver{
		versions: make(map[core.TractID]int),
		data:     make(map[core.TractID][]byte),
	}
	tt.tractservers[addr] = ts
	return ts
}

// newMemTractserverTalker creates a TractserverTalker that simulates
// tractservers in memory.
// will send a trace of reads and writes to the given function.
func newMemTractserverTalker(trace tsTraceFunc) TractserverTalker {
	if trace == nil {
		trace = func(tsTraceEntry) core.Error { return core.NoError }
	}
	return &memTractserverTalker{
		tractservers: make(map[string]*memTractserver),
		trace:        trace,
	}
}
