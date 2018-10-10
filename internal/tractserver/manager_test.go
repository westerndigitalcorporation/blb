// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT
//
// Tests for manager.go and request.go

package tractserver

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/westerndigitalcorporation/blb/internal/core"
	test "github.com/westerndigitalcorporation/blb/pkg/testutil"
)

func newTestManager(root string) (*Manager, error) {
	return NewManager(root, &DefaultTestConfig)
}

// Test that trying to create a Manager with a bad root directory fails.
func TestNewManagerBadMountPoints(t *testing.T) {
	// Try to make one on a totally bogus path.
	bogusPath := "/if/this/breaks/because/this/exists/ill/be/surprised"

	if _, e := newTestManager(bogusPath); nil == e {
		t.Fatal("is " + bogusPath + " really a real mountpoint?")
	}

	// And try to make one with a non-directory mountpoint.
	var f *os.File
	var err error
	if f, err = ioutil.TempFile(test.TempDir(), "disk_manager_test"); nil != err {
		t.Fatal("couldn't make a tempfile as a bogus mountpoint")
	}

	if _, e := newTestManager(f.Name()); nil == e {
		t.Fatal("non-directory mountpoint accepted by NewManager: " + f.Name())
	}
}

// getTestManager returns a disk manager under a new directory.
// Kills the test if any error is encountered.
func getTestManager(t *testing.T) *Manager {
	// Each test
	var e error
	var dir string
	if dir, e = ioutil.TempDir(test.TempDir(), "disk_manager_test"); nil != e {
		t.Fatal("couldn't get a TempDir err=" + e.Error())
	}

	var m *Manager
	if m, e = newTestManager(dir); nil != e {
		t.Fatal("couldn't create a test disk mgr, err=" + e.Error())
	}
	return m
}

// Test a basic open and a delete.
func TestBasicCreateDelete(t *testing.T) {
	m := getTestManager(t)

	id := core.TractID{Blob: core.BlobID(123456789), Index: 0}
	ctx := BG

	// Open it, but it doesn't exist, so it won't work.
	if _, err := m.Open(ctx, id, os.O_RDONLY); err == core.NoError {
		t.Fatal("expected an error for opening a file that doesn't exist")
	}

	// Create it.
	f, err := m.Open(ctx, id, os.O_CREATE|os.O_EXCL)
	if err != core.NoError {
		t.Fatal("couldn't create a tract")
	}
	if m.Close(f) != core.NoError {
		t.Fatal("couldn't close the tract")
	}

	// Try to create again with EXCL, shouldn't work as it's there already.
	if _, err := m.Open(ctx, id, os.O_CREATE|os.O_EXCL); err == core.NoError {
		t.Fatal("create shouldn't have worked")
	}

	// GetTracts should return it.
	ts, err := getAllTracts(m)
	if err != core.NoError {
		t.Fatal("couldn't GetTracts")
	}
	if len(ts) != 1 || ts[0] != id {
		t.Fatal("GetTracts returned wrong tracts")
	}

	// Delete.
	if m.Delete(id) != core.NoError {
		t.Fatal("could not delete tract")
	}

	// Try to open it again, should fail.
	if _, err := m.Open(ctx, id, os.O_RDONLY); err == core.NoError {
		t.Fatal("expected an error for opening a file that doesn't exist")
	}

	// GetTracts should not return it.
	ts, err = getAllTracts(m)
	if err != core.NoError {
		t.Fatal("couldn't GetTracts")
	}
	if len(ts) != 0 {
		t.Fatal("GetTracts returned unexpected tracts")
	}

	// But we should find it renamed.
	names, e := ioutil.ReadDir(m.delRoot)
	if e != nil {
		t.Fatal("couldn't read dir")
	}
	if len(names) != 1 || !strings.HasPrefix(names[0].Name(), "DELETED") {
		t.Fatal("deleted did not rename:", names)
	}

	// Now we trigger a sweep a while in the future.
	m.doSweep(time.Now().Add(30 * 24 * time.Hour))

	// Now we should not find it in the directory.
	names, e = ioutil.ReadDir(m.delRoot)
	if len(names) != 0 {
		t.Fatal("found unexpected files:", names)
	}
}

// Create a file, write to it, read it back, make sure it worked.
func TestBasicReadWrite(t *testing.T) {
	m := getTestManager(t)
	id := core.TractID{Blob: core.BlobID(0), Index: 0}

	// We'll write to id but first we create it.
	f, err := m.Open(BG, id, os.O_CREATE|os.O_RDWR)
	if err != core.NoError {
		t.Fatal("couldn't create tract to write to")
	}

	// Write some data.
	data := []byte("i am a jelly donut")
	if n, e := m.Write(BG, f, data, 0); e != core.NoError || n != len(data) {
		t.Fatal("couldn't write data")
	}

	// This is where we read the data.
	fileData := make([]byte, len(data))
	if n, e := m.Read(BG, f, fileData, 0); e != core.NoError || n != len(data) {
		t.Fatal("couldn't read data")
	}

	// Verify the data.
	if !bytes.Equal(fileData, data) {
		t.Fatal("file data corrupt")
	}

	// Clean up after ourselves.
	if m.Close(f) != core.NoError {
		t.Fatal("couldn't close file")
	}
	if m.Delete(id) != core.NoError {
		t.Fatal("couldn't delete file")
	}
}

// Read from an existing file, but in a way where the underlying ChecksumFile
// returns an error (admittedly a benign one -- io.EOF).
func TestReadToEOF(t *testing.T) {
	m := getTestManager(t)

	id := core.TractID{Blob: core.BlobID(42), Index: 0}

	// Make a file.
	f, err := m.Open(BG, id, os.O_CREATE|os.O_RDWR)
	if err != core.NoError {
		t.Fatal("failed to create file")
	}

	// Write 100 bytes.
	if n, e := m.Write(BG, f, make([]byte, 100), 0); e != core.NoError || n != 100 {
		t.Fatal("error appending 100 bytes")
	}

	// Read 200 bytes.
	buf := make([]byte, 200)
	if n, e := m.Read(BG, f, buf, 0); e != core.ErrEOF || n != 100 {
		t.Fatalf("should have gotten core.ErrEOF, got %+v, wanted 100 bytes got %d", e, n)
	}

	if m.Close(f) != core.NoError {
		t.Fatal("couldn't close file after I/O")
	}
}

// Set an xattr, get it back and verify.
func TestManagerSetGetxattr(t *testing.T) {
	m := getTestManager(t)
	id := core.TractID{Blob: core.BlobID(0), Index: 123}

	// Create a new file.
	f, err := m.Open(BG, id, os.O_CREATE|os.O_EXCL)
	if err != core.NoError {
		t.Fatalf("couldn't create tract: %s", err)
	}
	defer m.Close(f)

	// Set xattr.
	name := "spicy"
	in := []byte("food")
	if err := m.Setxattr(f, name, in); core.NoError != err {
		t.Fatalf("failed to set xattr: %s", err)
	}

	// Get it back and verify.
	out, err := m.Getxattr(f, name)
	if core.NoError != err {
		t.Fatalf("failed to get xattr: %s", err)
	}
	if 0 != bytes.Compare(in, out) {
		t.Fatalf("value doesn't match")
	}
}

// Write some files, stat fs, and verify the result.
func TestManagerStatfs(t *testing.T) {
	m := getTestManager(t)

	// Create some files.
	num := 10
	for i := 1; i < num+1; i++ {
		id := core.TractID{Blob: core.BlobID(123456789), Index: core.TractKey(i)}
		f, err := m.Open(BG, id, os.O_CREATE|os.O_EXCL)
		if err != core.NoError {
			t.Fatalf("couldn't create tract: %s", err)
		}
		m.Close(f)
	}

	// Stat and verify
	status := m.Statfs()
	if num != status.NumTracts {
		t.Fatalf("wrong file count: expected %d and got %d", num, status.NumTracts)
	}
}

// Check that toBlbError is snooping on errors correctly.
func TestToBlbErrorSnoop(t *testing.T) {
	m := getTestManager(t)
	s := m.Status()
	if s.Full || !s.Healthy {
		t.Fatalf("wrong status")
	}

	if m.toBlbError(syscall.ENOSPC) != core.ErrNoSpace {
		t.Fatalf("wrong translation of ENOSPC")
	} else if !m.Status().Full {
		t.Fatalf("should be full")
	}

	if m.toBlbError(syscall.EIO) != core.ErrIO {
		t.Fatalf("wrong translation of EIO")
	} else if m.Status().Healthy {
		t.Fatalf("healthy but got an EIO")
	}

	if m.toBlbError(core.ErrFileNotFound.Error()) != core.ErrFileNotFound {
		t.Fatalf("wrong translation of blb error")
	}
}

// Test that high priority things are dequeued before medium which are dequeued before low.
func TestQueueOrdering(t *testing.T) {
	q := NewPriorityQueue(0)
	n := 100

	for i := 0; i < n; i++ {
		if err := q.TryPush(request{priority: LowPri}); err != nil {
			t.Fatalf("couldn't push a low priority thing")
		}
		if err := q.TryPush(request{priority: MedPri}); err != nil {
			t.Fatalf("couldn't push a med priority thing")
		}
		if err := q.TryPush(request{priority: HighPri}); err != nil {
			t.Fatalf("couldn't push a high priority thing")
		}
	}

	for i := 0; i < n; i++ {
		req := q.Pop().(request)
		if req.priority != HighPri {
			t.Fatalf("got wrong priority")
		}
	}

	for i := 0; i < n; i++ {
		req := q.Pop().(request)
		if req.priority != MedPri {
			t.Fatalf("got wrong priority")
		}
	}

	for i := 0; i < n; i++ {
		req := q.Pop().(request)
		if req.priority != LowPri {
			t.Fatalf("got wrong priority")
		}
	}
}

// Test that cancellation works.
func TestOpenCancellation(t *testing.T) {
	m := getTestManager(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	id := core.TractID{Blob: core.BlobID(1), Index: core.TractKey(1)}
	_, err := m.Open(ctx, id, os.O_CREATE|os.O_EXCL)
	if err != core.ErrCanceled {
		t.Fatalf("expected op to be canceled")
	}

}

// Tests Stop.
func TestStop(t *testing.T) {
	m := getTestManager(t)
	root := m.root

	// Create three tracts in different states.
	id1 := core.TractID{Blob: core.BlobID(123), Index: 0} // open, write, close
	id2 := core.TractID{Blob: core.BlobID(456), Index: 0} // open, write
	id3 := core.TractID{Blob: core.BlobID(789), Index: 0} // open

	var f1, f2, f3 interface{}
	var err core.Error
	var n int
	data := []byte("i am a jelly donut")
	buf := make([]byte, len(data))

	// 1
	f1, err = m.Open(BG, id1, os.O_CREATE|os.O_RDWR)
	if err != core.NoError {
		t.Fatal("open", err)
	}
	if n, err = m.Write(BG, f1, data, 0); err != core.NoError || n != len(data) {
		t.Fatal("write", err)
	}
	if err = m.Close(f1); err != core.NoError {
		t.Fatal("close", err)
	}

	// 2
	f2, err = m.Open(BG, id2, os.O_CREATE|os.O_RDWR)
	if err != core.NoError {
		t.Fatal("open", err)
	}
	if n, err = m.Write(BG, f2, data, 0); err != core.NoError || n != len(data) {
		t.Fatal("write", err)
	}

	// 3
	f3, err = m.Open(BG, id3, os.O_CREATE|os.O_RDWR)
	if err != core.NoError {
		t.Fatal("open", err)
	}

	// STOP
	m.Stop()

	// 1: opening again should fail
	if _, err = m.Open(BG, id1, os.O_RDONLY); err != core.ErrDiskRemoved {
		t.Fatal("open after stop 1", err)
	}

	// 2: close should succeed
	if err = m.Close(f2); err != core.NoError {
		t.Fatal("close after stop 2", err)
	}

	// 3: write should fail, close should succeed
	if _, err = m.Write(BG, f3, data, 0); err != core.ErrDiskRemoved {
		t.Fatal("write after stop 3", err)
	}
	if err = m.Close(f3); err != core.NoError {
		t.Fatal("close after stop 3", err)
	}

	// NEW MANAGER
	var e error
	if m, e = newTestManager(root); e != nil {
		t.Fatal("couldn't create a test disk mgr", e)
	}

	// 1: data should be there
	f1, err = m.Open(BG, id1, os.O_RDONLY)
	if err != core.NoError {
		t.Fatal("open new manager", err)
	}
	if n, err = m.Read(BG, f1, buf, 0); err != core.NoError || n != len(buf) {
		t.Fatal("read new manager")
	}
	m.Close(f1)

	// 2: data should be there
	f2, err = m.Open(BG, id2, os.O_RDONLY)
	if err != core.NoError {
		t.Fatal("open new manager", err)
	}
	if n, err = m.Read(BG, f2, buf, 0); err != core.NoError || n != len(buf) {
		t.Fatal("read new manager")
	}
	m.Close(f2)

	// 3: should be empty because write failed
	f3, err = m.Open(BG, id3, os.O_RDONLY)
	if err != core.NoError {
		t.Fatal("open new manager", err)
	}
	if n, err = m.Read(BG, f3, buf, 0); err != core.ErrEOF || n != 0 {
		t.Fatal("read new manager")
	}
	m.Close(f3)
}

func TestStopConcurrency(t *testing.T) {
	data := []byte("i am a jelly donut")

	m := getTestManager(t)

	// Start a bunch of goroutines doing operations in a loop.
	for i := 0; i < 100; i++ {
		go func(i int) {
			id := core.TractID{Blob: core.BlobID(123456789), Index: core.TractKey(i)}
			for {
				f, e := m.Open(BG, id, os.O_CREATE|os.O_RDWR)
				if e == core.ErrDiskRemoved {
					return // We're done, stop.
				} else if e != core.NoError {
					t.Fatal("got unexpected error from Open", e)
				}
				_, e = m.Write(BG, f, data, 0)
				if e != core.NoError && e != core.ErrDiskRemoved {
					// This might fail with ErrDiskRemoved.
					t.Fatal("got unexpected error from Write", e)
				}
				e = m.Close(f)
				if e != core.NoError {
					// This shouldn't fail.
					t.Fatal("got unexpected error from Close", e)
				}
			}
		}(i)
	}

	time.Sleep(500 * time.Millisecond)

	m.Stop()

	// Wait until the Stop process finishes.
	for atomic.LoadUint64(&m.workers) > 0 {
		time.Sleep(10 * time.Millisecond)
	}
}

func getAllTracts(disk Disk) ([]core.TractID, core.Error) {
	dir, err := disk.OpenDir()
	if err != core.NoError {
		return nil, err
	}
	var out []core.TractID
	for {
		tracts, err := disk.ReadDir(dir)
		if err != core.NoError {
			break
		}
		out = append(out, tracts...)
	}
	return out, disk.CloseDir(dir)
}
