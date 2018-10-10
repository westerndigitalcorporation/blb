// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package tractserver

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/klauspost/reedsolomon"

	"github.com/westerndigitalcorporation/blb/internal/core"
)

var BG = context.Background()

type memTractserverTalker struct {
	sync.Mutex

	ctlReadCalls    map[string][]core.ReadReq
	ctlReadReplies  map[string][]core.ReadReply
	ctlWriteCalls   map[string][]core.WriteReq
	ctlWriteReplies map[string][]core.Error
}

func newMemTractserverTalker() *memTractserverTalker {
	return &memTractserverTalker{
		ctlReadCalls:    make(map[string][]core.ReadReq),
		ctlReadReplies:  make(map[string][]core.ReadReply),
		ctlWriteCalls:   make(map[string][]core.WriteReq),
		ctlWriteReplies: make(map[string][]core.Error),
	}
}

func (m *memTractserverTalker) addCtlReadReply(addr string, origB []byte, err core.Error) {
	// copy b to avoid any confusion about ownership
	b := make([]byte, len(origB))
	copy(b, origB)

	m.Lock()
	defer m.Unlock()
	m.ctlReadReplies[addr] = append(m.ctlReadReplies[addr], core.ReadReply{B: b, Err: err})
}

func (m *memTractserverTalker) addCtlWriteReply(addr string, err core.Error) {
	m.Lock()
	defer m.Unlock()
	m.ctlWriteReplies[addr] = append(m.ctlWriteReplies[addr], err)
}

func (m *memTractserverTalker) CtlRead(ctx context.Context, addr string, id core.TractID, version, length int, off int64) ([]byte, core.Error) {
	m.Lock()
	defer m.Unlock()

	m.ctlReadCalls[addr] = append(m.ctlReadCalls[addr], core.ReadReq{ID: id, Version: version, Len: length, Off: off})

	if len(m.ctlReadReplies[addr]) == 0 {
		return nil, core.ErrRPC
	}
	reply := m.ctlReadReplies[addr][0]
	m.ctlReadReplies[addr] = m.ctlReadReplies[addr][1:]
	return reply.B, reply.Err
}

func (m *memTractserverTalker) CtlWrite(ctx context.Context, addr string, id core.TractID, v int, off int64, b []byte) core.Error {
	m.Lock()
	defer m.Unlock()

	m.ctlWriteCalls[addr] = append(m.ctlWriteCalls[addr], core.WriteReq{ID: id, Version: v, B: b, Off: off})

	if len(m.ctlWriteReplies[addr]) == 0 {
		return core.ErrRPC
	}
	reply := m.ctlWriteReplies[addr][0]
	m.ctlWriteReplies[addr] = m.ctlWriteReplies[addr][1:]
	return reply
}

// Create an in-memory store for testing.
func getTestStore(disks []Disk, talker TractserverTalker) *Store {
	s := NewStore(talker, NewMetadataStore(), &DefaultTestConfig)
	for _, d := range disks {
		s.AddDisk(d)
	}
	return s
}

func getTestStoreDefault(t *testing.T) *Store {
	return getTestStore([]Disk{NewMemDisk()}, newMemTractserverTalker())
}

// Test version setting.
func TestSetVersion(t *testing.T) {
	s := getTestStoreDefault(t)

	id := core.TractID{Blob: 123456, Index: 0}

	// First, create the tract.
	if err := s.Create(BG, id, nil, 0); err != core.NoError {
		t.Fatalf("failed to create the tract: %s", err)
	}

	// We can't set a negative version for the tract.
	if _, err := s.SetVersion(id, -1, 0); err == core.NoError {
		t.Fatal("didn't fail to set the version to something negative")
	}

	// Versions increase by 1, and start at 1.
	if _, err := s.SetVersion(id, 100, 0); err == core.NoError {
		t.Fatal("didn't fail to set the version from 0 to 100")
	}

	// This is a good version.
	if _, err := s.SetVersion(id, 2, 0); err != core.NoError {
		t.Fatalf("couldn't bump version to 2: %s", err)
	}
}

// Test conditional version setting.
func TestConditionalSetVersion(t *testing.T) {
	// TODO: write test here
}

// Test that writes are rejected when they should be.
func TestWriteRejected(t *testing.T) {
	s := getTestStoreDefault(t)

	id := core.TractID{Blob: 123456, Index: 0}
	data := []byte("Hello, world!")

	// Writes fail until the tract is created.
	if core.NoError == s.Write(BG, id, 1, data, 0) {
		t.Fatal("write should have failed: no version")
	}

	// Create the tract.  Versions start at 1.
	if s.Create(BG, id, nil, 0) != core.NoError {
		t.Fatal("failed to create the tract")
	}

	// Writes fail with the wrong version.
	if core.NoError == s.Write(BG, id, 2, data, 0) {
		t.Fatal("write should have failed: wrong version")
	}

	// Writes fail with the wrong version.
	if core.NoError == s.Write(BG, id, 0, data, 0) {
		t.Fatal("write should have failed: wrong version")
	}
}

// Do a successful write.
func TestWriteAccepted(t *testing.T) {
	s := getTestStoreDefault(t)

	id := core.TractID{Blob: 123456, Index: 0}
	data := []byte("Hello, world!")
	version := 1

	// Write shouldn't work until the tract is created.
	if s.Write(BG, id, version, data, 0) == core.NoError {
		t.Fatal("write should not have worked")
	}

	// Create the tract and write data.
	if s.Create(BG, id, data, 0) != core.NoError {
		t.Fatal("failed to create the tract")
	}

	// Write should work now.
	if s.Write(BG, id, version, data, 0) != core.NoError {
		t.Fatal("write should not have worked")
	}

	// Read data from MemDisk and expect it to be there.
	if out, e := s.Read(BG, id, version, len(data), 0); core.NoError != e || len(out) != len(data) {
		t.Fatal("couldn't read back data from MemDisk")
	} else if !bytes.Equal(out, data) {
		t.Fatal("write didn't actually happen")
	}
}

// Test that writing with an old version fails.
func TestWriteOldVersion(t *testing.T) {
	s := getTestStoreDefault(t)

	id := core.TractID{Blob: 123456, Index: 0}

	// Create the tract with initial version 1.
	if s.Create(BG, id, nil, 0) != core.NoError {
		t.Fatal("failed to create the tract")
	}

	// Set the tract version to 2.
	if _, err := s.SetVersion(id, 2, 0); err != core.NoError {
		t.Fatal("failed to set tract version")
	}

	// Write should not work as we're using an old version.
	if core.NoError == s.Write(BG, id, 1, []byte("i am a jelly donut"), 0) {
		t.Fatal("write should have failed due to bad disk")
	}
}

// Test mod stamp semantics.
func TestModStamp(t *testing.T) {
	s := getTestStoreDefault(t)
	id := core.TractID{Blob: 123456, Index: 0}
	data := []byte("Hello, world!")
	version := 1

	var err core.Error
	var stamp1, stamp2, stamp3, stamp4 uint64

	if s.Create(BG, id, data, 0) != core.NoError {
		t.Fatal("create failed")
	}
	if _, stamp1, err = s.Stat(BG, id, version); err != core.NoError {
		t.Fatal("stat failed")
	}
	if s.Write(BG, id, version, data, 100) != core.NoError {
		t.Fatal("write failed")
	}
	if _, stamp2, err = s.Stat(BG, id, version); err != core.NoError {
		t.Fatal("stat failed")
	}
	if s.Write(BG, id, version, data, 200) != core.NoError {
		t.Fatal("write failed")
	}
	if _, stamp3, err = s.Stat(BG, id, version); err != core.NoError {
		t.Fatal("stat failed")
	}
	if _, err = s.Read(BG, id, version, len(data), 0); err != core.NoError {
		t.Fatal("read failed")
	}
	if _, stamp4, err = s.Stat(BG, id, version); err != core.NoError {
		t.Fatal("stat failed")
	}

	// stamp1, stamp2, and stamp3 should all be different since we did writes
	if stamp1 == stamp2 || stamp2 == stamp3 || stamp1 == stamp3 {
		t.Fatal("write did not change stamp")
	}
	// stamp3 and stamp4 should be the same since we just did a read
	if stamp3 != stamp4 {
		t.Fatal("read did change stamp", stamp3, stamp4)
	}
}

// Test a basic pulltract success.
func TestPullTract(t *testing.T) {
	s := getTestStoreDefault(t)

	id := core.TractID{Blob: 123456, Index: 0}
	addr := []string{"somehost:someport"}

	// Create a good reply from the fake other tractserver with the data.
	data := []byte("dark chili oil tastes good")
	s.tt.(*memTractserverTalker).addCtlReadReply(addr[0], data, core.NoError)

	// PullTract will call to the fake other tractserver which will return
	// the reply we just created above.
	if err := s.PullTract(BG, addr, id, 1); err != core.NoError {
		t.Fatalf("failed to PullTract: %s", err)
	}

	// Verify that it worked by doing a read via the store.
	readData, err := s.Read(BG, id, 1, len(data), 0)
	if err != core.NoError {
		t.Fatalf("read failed: %s", err)
	}
	if !bytes.Equal(readData, data) {
		t.Fatalf("data in pulltract reply wasn't data on disk")
	}
}

// Test that pulling a tract will overwrite an existing one if we have old data.
func TestPullTractOverwrite(t *testing.T) {
	s := getTestStoreDefault(t)

	id := core.TractID{Blob: 123456, Index: 0}
	addr := []string{"somehost:someport"}

	// Create the file in the store, will have version 1.
	if err := s.Create(BG, id, nil, 0); err != core.NoError {
		t.Fatalf("couldn't create file in the store: %s", err)
	}

	// Create a good reply from the fake other tractserver with the data.
	data := []byte("dark chili oil tastes good")
	s.tt.(*memTractserverTalker).addCtlReadReply(addr[0], data, core.NoError)

	// PullTract will clobber tracts if the version numbers are the same.
	if err := s.PullTract(BG, addr, id, 1); err != core.NoError {
		t.Fatalf("PullTract should have clobbered data w/same version, %s", err)
	}
}

// Test that pulltract fails if the file exists already on disk with a newer
// version.
func TestPullTractFileExistOnDisk(t *testing.T) {
	tt := newMemTractserverTalker()
	disks := []Disk{NewMemDisk()}
	s := getTestStore(disks, tt)

	id := core.TractID{Blob: 123456, Index: 0}
	addr := []string{"somehost:someport"}

	// Create the file on disk. This'll cause the pull below to fail.
	if err := s.Create(BG, id, nil, 0); err != core.NoError {
		t.Fatalf("couldn't create file on disk: %s", err)
	}

	// Proceed as though the pull would succeed.

	// Create a good reply from the fake other tractserver with the data.
	data := []byte("dark chili oil tastes good")
	tt.addCtlReadReply(addr[0], data, core.NoError)

	// PullTract will fail if the version number is stale. We should not
	// pull from a stale tract.
	if err := s.PullTract(BG, addr, id, 0); err == core.NoError {
		t.Fatalf("PullTract should have failed but didn't")
	}
}

// Test that if the create fails on the underlying disk, the pulltract fails.
func TestPullTractCreateFail(t *testing.T) {
	tt := newMemTractserverTalker()
	bad := newSimpleDisk(core.ErrGeneralDisk, core.DiskStatus{Full: false, Healthy: false})
	s := getTestStore([]Disk{bad}, tt)
	addr := []string{"highSt:444"}

	// id doesn't matter here
	id := core.TractID{Blob: 123456, Index: 0}

	// The request if it is sent will succeed, but the disk is bad.
	tt.addCtlReadReply(addr[0], []byte("some data"), core.NoError)
	if err := s.PullTract(BG, addr, id, 1); err == core.NoError {
		t.Fatal("bad disk should have prevented rerepl")
	}
}

// Test that pulltract fails if there's an error from the other tractserver.
func TestPullTractReadErrorOtherSide(t *testing.T) {
	s := getTestStoreDefault(t)

	id := core.TractID{Blob: 123456, Index: 0}
	addr := []string{"somehost:someport"}

	// There's nothing to return in the test tractserver talker so there's an RPC error.
	if err := s.PullTract(BG, addr, id, 1); err == core.NoError {
		t.Fatalf("should have failed to PullTract")
	}
}

// Test that pulltract succeeds even if there's an EOF error from the other tractserver.
func TestPullTractReadEOF(t *testing.T) {
	s := getTestStoreDefault(t)

	id := core.TractID{Blob: 123456, Index: 0}
	addr := []string{"somehost:someport"}

	// Create an EOF response.
	s.tt.(*memTractserverTalker).addCtlReadReply(addr[0], []byte("some data"), core.ErrEOF)
	if err := s.PullTract(BG, addr, id, 1); err != core.NoError {
		t.Fatalf("should not have failed to PullTract: %s", err)
	}
}

// Test that pulltract fails if pulling from all source hosts fails.
func TestPullTractRetryFailure(t *testing.T) {
	s := getTestStoreDefault(t)

	id := core.TractID{Blob: 123456, Index: 0}
	addr := []string{"down-server", "bad-disk", "invalid-argument"}

	tt := s.tt.(*memTractserverTalker)
	tt.addCtlReadReply(addr[0], nil, core.ErrRPC)
	tt.addCtlReadReply(addr[1], nil, core.ErrIO)
	tt.addCtlReadReply(addr[2], nil, core.ErrInvalidArgument)

	if err := s.PullTract(BG, addr, id, 1); err == core.NoError {
		t.Fatalf("should have failed to PullTract")
	}
}

// Test that pulltract succeeds if pulling from any source succeeds.
func TestPullTractRetrySuccess(t *testing.T) {
	s := getTestStoreDefault(t)

	id := core.TractID{Blob: 123456, Index: 0}
	addr := []string{"down-server", "bad-disk", "good-guy"}

	tt := s.tt.(*memTractserverTalker)
	tt.addCtlReadReply(addr[0], nil, core.ErrRPC)
	tt.addCtlReadReply(addr[1], nil, core.ErrIO)
	tt.addCtlReadReply(addr[2], nil, core.NoError)

	if err := s.PullTract(BG, addr, id, 1); err != core.NoError {
		t.Fatalf("should have succeeded to PullTract: %s", err)
	}
}

// Check that GCTract works correctly.
func TestGCTract(t *testing.T) {
	s := getTestStoreDefault(t)

	id := core.TractID{Blob: 123456, Index: 0}
	ts := core.TractState{ID: id, Version: 1}

	// GC the tract that doesn't exist, shouldn't crash.
	s.maybeGCTract(ts)

	// Create the tract with initial version 1.
	if s.Create(BG, id, nil, 0) != core.NoError {
		t.Fatal("failed to create the tract")
	}

	// Set the tract version to 2.
	if _, err := s.SetVersion(id, 2, 0); err != core.NoError {
		t.Fatal("failed to set tract version")
	}

	// GC version 1.  Since we have v=2 we won't delete it.
	s.maybeGCTract(ts)
	if _, _, err := s.Stat(BG, id, 2); core.NoError != err {
		t.Fatal("we should still be able to get the size of the tract as gc should have failed")
	}

	// We're asked to GC the version we have, that's fine, it'll work.
	if s.maybeGCTract(core.TractState{ID: id, Version: 2}) != core.NoError {
		t.Fatal("gc should have worked but didn't")
	} else if _, _, err := s.Stat(BG, id, 2); err == core.NoError {
		t.Fatal("getting the size should fail as the tract was GC-ed")
	}

	// Make a new blob as we've just GC-ed id.
	id2 := core.TractID{Blob: 1, Index: 0}

	// Create the tract id2 with initial version 1.
	if s.Create(BG, id2, nil, 0) != core.NoError {
		t.Fatal("failed to create the tract")
	}

	// We're asked to GC a version that's newer than the one we have.
	// This will work as versions strictly increase.
	if s.maybeGCTract(core.TractState{ID: id2, Version: 3}) != core.NoError {
		t.Fatal("failed to GC tract even newer than the one we have")
	} else if _, _, err := s.Stat(BG, id2, 2); err == core.NoError {
		t.Fatal("getting the size should fail as the tract was GC-ed")
	}
}

// Test that create should succeed when there is at least one non-full disk and
// fail when all disks are full.
func TestNoSpace(t *testing.T) {
	// Make some disks, but only one is not full.
	disks := make([]Disk, 5)
	disks[0] = NewMemDisk()
	for i := 1; i < len(disks); i++ {
		disks[i] = newSimpleDisk(core.ErrNoSpace, core.DiskStatus{Full: true, Healthy: true})
	}

	// Create a store.
	s := getTestStore(disks, newMemTractserverTalker())

	// Create a bunch of tracts and they should succeed.
	numTracts := 20
	for i := 0; i < numTracts; i++ {
		id := core.TractID{Blob: 123456, Index: core.TractKey(i)}
		if err := s.Create(BG, id, nil, 0); core.NoError != err {
			t.Fatalf("failed to create tract %s: %s", id, err)
		}
	}
}

// Test that we pay attention to a disk's health.
func TestHealthChecked(t *testing.T) {
	// Make some Disks that always say "no error" but aren't healthy.
	disks := make([]Disk, 5)
	for i := 0; i < len(disks); i++ {
		disks[i] = newSimpleDisk(core.NoError, core.DiskStatus{Full: false, Healthy: false})
	}

	// Create a store.
	s := getTestStore(disks, newMemTractserverTalker())

	// Tract creation shouldn't work.
	id := core.TractID{Blob: 123456, Index: core.TractKey(0)}
	if err := s.Create(BG, id, nil, 0); err == core.NoError {
		t.Fatalf("should have failed to create tract %s: %s", id, err)
	}
}

// Test basic Check functionality.
func TestCheck(t *testing.T) {
	s := getTestStoreDefault(t)

	// Create two tracts, both will have v==1.
	id0 := core.TractID{Blob: 123456, Index: 1}
	id1 := core.TractID{Blob: 31337, Index: 21}
	if s.Create(BG, id0, nil, 0) != core.NoError {
		t.Fatalf("failed to create %s", id0)
	}
	if s.Create(BG, id1, nil, 0) != core.NoError {
		t.Fatalf("failed to create %s", id1)
	}

	// Check that the tracts exist.
	state := []core.TractState{{ID: id0, Version: 1}, {ID: id1, Version: 1}}
	if len(s.Check(state)) != 0 {
		t.Errorf("check should have passed")
	}

	// The tractserver can have a higher version than the version sent from the curator.
	// Bump the version on the tractserver and verify that check passes.
	if nv, e := s.SetVersion(id0, 2, 0); nv != 2 || e != core.NoError {
		t.Fatalf("error bumping version: %s", e)
	}
	if len(s.Check(state)) != 0 {
		t.Errorf("check should have passed")
	}

	// The tractserver cannot have a lower version than what the curator sends.
	state[0].Version = 31337
	missing := s.Check(state)
	if len(missing) != 1 {
		t.Fatalf("check didn't detect too-low version")
	}
	if missing[0].ID != id0 {
		t.Fatalf("wrong ID for check")
	}
	if missing[0].Version != 31337 {
		t.Fatalf("wrong version for check")
	}

	// There is no id2 on the TS.
	id2 := core.TractID{Blob: 100000, Index: 1}
	missing = s.Check([]core.TractState{{ID: id2, Version: 1}})
	if len(missing) != 1 {
		t.Fatalf("check didn't detect too-low version")
	}
	if missing[0].ID != id2 {
		t.Fatalf("wrong ID for check")
	}
}

// statusDisk returns a fixed DiskStatus.  Also it counts how many calls to Close there
// are as a proxy for file operations performed.
type statusDisk struct {
	simpleDisk
	numClose int
}

func (s *statusDisk) Close(f interface{}) core.Error {
	s.numClose++
	return s.simpleDisk.Close(f)
}

func newStatusDisk(s core.DiskStatus) *statusDisk {
	return &statusDisk{simpleDisk: simpleDisk{status: s}}
}

// Test that if we have two disks we choose the one w/the lower expected wait time.
func TestChooseQuickerDisk(t *testing.T) {
	// This fake disk takes half the time as the other fake disk.
	sd := newStatusDisk(core.DiskStatus{
		Root:      "fastdisk",
		Full:      false,
		Healthy:   true,
		QueueLen:  100,
		AvgWaitMs: 100,
	})
	ld := newStatusDisk(core.DiskStatus{
		Root:      "slowdisk",
		Full:      false,
		Healthy:   true,
		QueueLen:  100,
		AvgWaitMs: 200,
	})
	s := getTestStore([]Disk{sd, ld}, newMemTractserverTalker())

	sd.numClose = 0
	ld.numClose = 0

	id0 := core.TractID{Blob: 31337, Index: 21}
	if s.Create(BG, id0, nil, 0) != core.NoError {
		t.Fatalf("couldn't create")
	}

	if sd.numClose != 1 || ld.numClose != 0 {
		t.Fatalf("expected speedy disk to be used")
	}
}

// Test that if disks are set to StopAllocating or Drain, we don't pick them.
func TestAllocationByControlFlags(t *testing.T) {
	d1 := newStatusDisk(core.DiskStatus{
		Root:      "stopalloc",
		Full:      false,
		Healthy:   true,
		QueueLen:  100,
		AvgWaitMs: 100,
		Flags: core.DiskControlFlags{
			StopAllocating: true,
		},
	})
	d2 := newStatusDisk(core.DiskStatus{
		Root:      "draining",
		Full:      false,
		Healthy:   true,
		QueueLen:  100,
		AvgWaitMs: 100,
		Flags: core.DiskControlFlags{
			Drain: 8,
		},
	})
	d3 := newStatusDisk(core.DiskStatus{
		Root:      "healthy",
		Full:      false,
		Healthy:   true,
		QueueLen:  100,
		AvgWaitMs: 100,
	})
	s := getTestStore([]Disk{d1, d2, d3}, newMemTractserverTalker())

	d1.numClose = 0
	d2.numClose = 0
	d3.numClose = 0

	for i := core.TractKey(0); i < 10; i++ {
		if s.Create(BG, core.TractID{Blob: 54321, Index: i}, nil, 0) != core.NoError {
			t.Fatalf("couldn't create")
		}
	}

	if d3.numClose != 10 {
		t.Fatalf("expected healthy disk to be used")
	}
}

func TestPackTractsInvalid(t *testing.T) {
	s := getTestStoreDefault(t)
	cid := core.RSChunkID{Partition: 0x80000555, ID: 5555}
	addrs := []core.TSAddr{
		{Host: "a1", ID: 1},
		{Host: "a2", ID: 2},
		{Host: "a3", ID: 3},
	}
	tid := core.TractIDFromParts(core.BlobIDFromParts(1, 1), 0)

	check := func(length int, srcs []*core.PackTractSpec) {
		err := s.PackTracts(BG, length, srcs, cid)
		if err != core.ErrInvalidArgument {
			t.Errorf("PackTracts should have returned an error for %v, %v", length, srcs)
		}
	}
	check(-100, []*core.PackTractSpec{}) // negative length
	check(1000, []*core.PackTractSpec{   // invalid tract id
		{ID: core.TractID{}, From: addrs, Version: 1, Offset: 0, Length: 500},
	})
	check(1000, []*core.PackTractSpec{ // no from
		{ID: tid, Version: 1, Offset: 0, Length: 500},
	})
	check(1000, []*core.PackTractSpec{ // too long
		{ID: tid, From: addrs, Version: 1, Offset: 0, Length: 1500},
	})
	check(1000, []*core.PackTractSpec{ // starting too high
		{ID: tid, From: addrs, Version: 1, Offset: 1500, Length: 500},
	})
	check(1000, []*core.PackTractSpec{ // out of order
		{ID: tid, From: addrs, Version: 1, Offset: 700, Length: 200},
		{ID: tid, From: addrs, Version: 1, Offset: 100, Length: 200},
	})
}

func TestPackTractsRPCError(t *testing.T) {
	s := getTestStoreDefault(t)
	cid := core.RSChunkID{Partition: 0x80000555, ID: 5555}
	addrs := []core.TSAddr{
		{Host: "a1", ID: 1},
		{Host: "a2", ID: 2},
		{Host: "a3", ID: 3},
	}
	tid1 := core.TractIDFromParts(core.BlobIDFromParts(1, 2), 0)
	tid2 := core.TractIDFromParts(core.BlobIDFromParts(1, 7), 0)

	// no read replies, should fail
	err := s.PackTracts(BG, 1000, []*core.PackTractSpec{
		{ID: tid1, From: addrs, Version: 1, Offset: 100, Length: 200},
		{ID: tid2, From: addrs, Version: 1, Offset: 700, Length: 200},
	}, cid)
	if err != core.ErrRPC {
		t.Fatalf("error from PackTracts: %s", err)
	}
}

func TestPackTracts(t *testing.T) {
	s := getTestStoreDefault(t)
	cid := core.RSChunkID{Partition: 0x80000555, ID: 5555}
	addrs := []core.TSAddr{
		{Host: "a1", ID: 1},
		{Host: "a2", ID: 2},
		{Host: "a3", ID: 3},
	}
	tid1 := core.TractIDFromParts(core.BlobIDFromParts(1, 2), 0)
	tid2 := core.TractIDFromParts(core.BlobIDFromParts(1, 7), 0)

	data1 := []byte("this is some data")
	data2 := []byte("this is some more data")

	// tract 1
	// first one fails, should fall back to second
	s.tt.(*memTractserverTalker).addCtlReadReply(addrs[0].Host, []byte("oops"), core.ErrRPC)
	s.tt.(*memTractserverTalker).addCtlReadReply(addrs[1].Host, data1, core.ErrEOF)

	// tract 2
	// first one has wrong length
	s.tt.(*memTractserverTalker).addCtlReadReply(addrs[0].Host, []byte("wrong length"), core.ErrEOF)
	// second is corrupt
	s.tt.(*memTractserverTalker).addCtlReadReply(addrs[1].Host, nil, core.ErrCorruptData)
	// third is ok
	s.tt.(*memTractserverTalker).addCtlReadReply(addrs[2].Host, data2, core.ErrEOF)

	err := s.PackTracts(BG, len(data1)+len(data2)+10, []*core.PackTractSpec{
		{ID: tid1, From: addrs, Version: 1, Offset: 2, Length: len(data1)},
		{ID: tid2, From: addrs, Version: 1, Offset: len(data1) + 5, Length: len(data2)},
	}, cid)
	if err != core.NoError {
		t.Fatalf("error from PackTracts: %s", err)
	}

	// read it back
	b, err := s.Read(BG, cid.ToTractID(), core.RSChunkVersion, 1000, 0)
	if err != core.ErrEOF {
		t.Fatalf("error reading back packed tract: %s", err)
	}
	if !bytes.Equal(b, []byte(
		"\x00\x00this is some data\x00\x00\x00this is some more data\x00\x00\x00\x00\x00")) {
		t.Errorf("wrong data: %v", b)
	}
}

func TestRSEncode(t *testing.T) {
	N, M := 3, 2
	B := 12000

	cfg := DefaultTestConfig
	cfg.EncodeIncrementSize = 5000
	s := NewStore(newMemTractserverTalker(), NewMetadataStore(), &cfg)
	s.AddDisk(NewMemDisk())

	cid := core.RSChunkID{Partition: 0x80000005, ID: 5000}

	addrs := make([]core.TSAddr, N+M)
	for i := range addrs {
		addrs[i] = core.TSAddr{Host: fmt.Sprintf("addr%d", i), ID: core.TractserverID(i)}
	}

	data := make([][]byte, N+M)
	for i := range data[:N] {
		data[i] = make([]byte, B)
		rand.Read(data[i])
	}

	// 0..5000
	mtt := s.tt.(*memTractserverTalker)
	mtt.addCtlReadReply(addrs[0].Host, data[0][0:5000], core.ErrEOF)
	mtt.addCtlReadReply(addrs[1].Host, data[1][0:5000], core.ErrEOF)
	mtt.addCtlReadReply(addrs[2].Host, data[2][0:5000], core.ErrEOF)
	mtt.addCtlWriteReply(addrs[3].Host, core.NoError)
	mtt.addCtlWriteReply(addrs[4].Host, core.NoError)
	// 5000..10000
	mtt.addCtlReadReply(addrs[0].Host, data[0][5000:10000], core.ErrEOF)
	mtt.addCtlReadReply(addrs[1].Host, data[1][5000:10000], core.ErrEOF)
	mtt.addCtlReadReply(addrs[2].Host, data[2][5000:10000], core.ErrEOF)
	mtt.addCtlWriteReply(addrs[3].Host, core.NoError)
	mtt.addCtlWriteReply(addrs[4].Host, core.NoError)
	// 10000..12000
	mtt.addCtlReadReply(addrs[0].Host, data[0][10000:], core.ErrEOF)
	mtt.addCtlReadReply(addrs[1].Host, data[1][10000:], core.ErrEOF)
	mtt.addCtlReadReply(addrs[2].Host, data[2][10000:], core.ErrEOF)
	mtt.addCtlWriteReply(addrs[3].Host, core.NoError)
	mtt.addCtlWriteReply(addrs[4].Host, core.NoError)

	// do the call
	err := s.RSEncode(BG, cid, B, addrs[:N], addrs[N:], nil)
	if err != core.NoError {
		t.Fatalf("error from RSEncode: %s", err)
	}

	// verify
	for i := N; i < N+M; i++ {
		for j, reply := range mtt.ctlWriteCalls[addrs[i].Host] {
			if reply.ID != cid.Add(i).ToTractID() {
				t.Errorf("bad tract id for reply from %d from %d: %v", j, i, reply.ID)
			}
			if reply.Off != int64(len(data[i])) {
				t.Errorf("bad offset for reply from %d from %d: %d != %d", j, i, reply.Off, len(data[i]))
			}
			data[i] = append(data[i], reply.B...)
		}
	}

	enc, _ := reedsolomon.New(N, M)
	ok, e := enc.Verify(data)
	if e != nil || !ok {
		t.Fatalf("RS verify failed: %v, %v", e, ok)
	}
}

func TestRSReconstruct(t *testing.T) {
	N, M := 3, 2
	B := 20000

	s := getTestStoreDefault(t)

	cid := core.RSChunkID{Partition: 0x80000005, ID: 5000}

	addrs := make([]core.TSAddr, N+M)
	for i := range addrs {
		addrs[i] = core.TSAddr{Host: fmt.Sprintf("addr%d", i), ID: core.TractserverID(i)}
	}

	data := make([][]byte, N+M)
	for i := range data {
		data[i] = make([]byte, B)
		rand.Read(data[i])
	}

	// Encode
	enc, _ := reedsolomon.New(N, M)
	if enc.Encode(data) != nil {
		t.Fatalf("RS encode failed")
	}

	// We're missing 1 (data) and 3 (parity): 0_2_4
	data[1] = nil
	data[3] = nil

	mtt := s.tt.(*memTractserverTalker)
	mtt.addCtlReadReply(addrs[0].Host, data[0], core.ErrEOF)
	mtt.addCtlReadReply(addrs[2].Host, data[2], core.ErrEOF)
	mtt.addCtlReadReply(addrs[4].Host, data[4], core.ErrEOF)
	mtt.addCtlWriteReply(addrs[1].Host, core.NoError)
	mtt.addCtlWriteReply(addrs[3].Host, core.NoError)

	// do the call
	err := s.RSEncode(BG, cid, B,
		[]core.TSAddr{addrs[0], addrs[2], addrs[4]},
		[]core.TSAddr{addrs[1], addrs[3]},
		[]int{0, 2, 4, 1, 3})
	if err != core.NoError {
		t.Fatalf("error from RSEncode: %s", err)
	}

	// verify
	for _, i := range []int{1, 3} {
		for j, reply := range mtt.ctlWriteCalls[addrs[i].Host] {
			if reply.ID != cid.Add(i).ToTractID() {
				t.Errorf("bad tract id for reply from %d from %d: %v", j, i, reply.ID)
			}
			if reply.Off != int64(len(data[i])) {
				t.Errorf("bad offset for reply from %d from %d: %d != %d", j, i, reply.Off, len(data[i]))
			}
			data[i] = append(data[i], reply.B...)
		}
	}

	ok, e := enc.Verify(data)
	if e != nil || !ok {
		t.Fatalf("RS verify failed: %v, %v", e, ok)
	}
}
