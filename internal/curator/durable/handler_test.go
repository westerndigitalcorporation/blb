// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package durable

import (
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"github.com/westerndigitalcorporation/blb/internal/core"
	"github.com/westerndigitalcorporation/blb/pkg/raft/raft"
	test "github.com/westerndigitalcorporation/blb/pkg/testutil"
)

const defHint = core.StorageHint_DEFAULT

// Returns a StateHandler with default configuration running single-node raft.
func newTestHandler(t *testing.T) *StateHandler {
	// We use this channel to wait until the handler's raft impl. thinks it's a leader
	// before returning the handler.
	leader := make(chan bool)

	dir, err := ioutil.TempDir(test.TempDir(), "state_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	DefaultStateConfig.DBDir = dir

	handler := NewStateHandler(&DefaultStateConfig, raft.NewTestRaftNode("handler_test", fmt.Sprintf("%d", time.Now().UnixNano())))
	handler.SetLeadershipChange(func(arg bool) {
		if arg {
			leader <- arg
		}
	})
	handler.Start()
	handler.ProposeInitialMembership([]string{"handler_test"})
	<-leader
	return handler
}

// Test that examining tracts for possible gc-ing works correctly.
func TestGC(t *testing.T) {
	h := newTestHandler(t)
	_, err := h.Register(core.CuratorID(1))
	if err != core.NoError {
		t.Fatalf("Failed to register: %v", err)
	}
	err = h.AddPartition(core.PartitionID(1), h.GetTerm())
	if err != core.NoError {
		t.Fatalf("Failed to add partition: %v", err)
	}

	tsid := core.TractserverID(1)

	// No blobs exist, so this one is garbage.
	old, gone := h.CheckForGarbage(tsid, []core.TractID{core.TractID{Blob: 1, Index: 0}})
	if len(gone) != 1 {
		t.Errorf("no blobs exist but didn't detect garbage")
	}
	if len(old) != 0 {
		t.Errorf("misclassified missing blob as old")
	}

	// Create a blob.
	id, e := h.CreateBlob(1, 123456789, 0, defHint, h.GetTerm())
	if e != core.NoError {
		t.Fatalf("couldn't create a blob to test GC with")
	}

	tid := core.TractID{Blob: id, Index: 0}

	// The blob exists but this tract does not.  We don't GC the non-existant tracts though.
	// This is a descriptive test not a normative test -- we currently don't GC these
	// tracts to avoid having to synchronize tract create and GC activity, but we could
	// do that in the future.
	old, gone = h.CheckForGarbage(tsid, []core.TractID{tid})
	if len(old)+len(gone) != 0 {
		t.Errorf("told to GC a possibly-being-created tract")
	}

	// Add a tract with tsid as host.
	if _, e = h.ExtendBlob(id, 0, [][]core.TractserverID{{tsid}}); e != core.NoError {
		t.Fatalf("couldn't extend")
	}

	// Check that we don't GC the tract on tsid as it's hosting it.
	old, gone = h.CheckForGarbage(tsid, []core.TractID{tid})
	if len(old)+len(gone) != 0 {
		t.Errorf("told to GC a possibly-being-created tract")
	}

	// Check that we do GC a host that shouldn't be hosting it.
	old, _ = h.CheckForGarbage(core.TractserverID(2), []core.TractID{tid})
	if len(old) != 1 {
		t.Errorf("not told to GC a tract we don't have")
	} else if old[0].Version != 1 {
		t.Errorf("got wrong version back for GC")
	}

	// Delete the blob, remove metadata, should get GC-ed as the tract has no blob.
	if e = h.FinishDelete([]core.BlobID{id}); e != core.NoError {
		t.Fatalf("failed to delete blob")
	}

	old, gone = h.CheckForGarbage(tsid, []core.TractID{tid})
	if len(gone) != 1 {
		t.Fatalf("expected tract to be gone")
	} else if gone[0] != tid {
		t.Fatalf("didn't get right goner tract")
	} else if len(old) != 0 {
		t.Fatalf("shouldn't have gotten any old tracts")
	}
}

// Basic test for ListBlobs
func TestList(t *testing.T) {
	h := newTestHandler(t)
	h.Register(core.CuratorID(1))
	h.AddPartition(core.PartitionID(1), h.GetTerm())

	// create a few blobs. assumes keys are assigned in order.
	_, _ = h.CreateBlob(1, 123456789, 0, defHint, h.GetTerm())
	id2, _ := h.CreateBlob(1, 123456789, 0, defHint, h.GetTerm())
	id3, _ := h.CreateBlob(1, 123456789, 0, defHint, h.GetTerm())
	id4, _ := h.CreateBlob(1, 123456789, 0, defHint, h.GetTerm())

	// delete one
	h.DeleteBlob(id3, time.Now(), h.GetTerm())

	// list all >= 2
	keys, err := h.ListBlobs(core.PartitionID(1), id2.ID())
	if err != core.NoError {
		t.Fatalf("ListBlobs error: %s", err)
	}
	if len(keys) != 2 || keys[0] != id2.ID() || keys[1] != id4.ID() {
		t.Fatalf("ListBlobs wrong result: %v", keys)
	}
}

func TestReadOnly(t *testing.T) {
	h := newTestHandler(t)
	h.Register(core.CuratorID(1))
	h.AddPartition(core.PartitionID(1), h.GetTerm())

	// Set RO mode.
	err := h.SetReadOnlyMode(true)
	if err != core.NoError {
		t.Fatal(err.Error())
	}

	// Lookup is ok.
	id, parts, err := h.GetCuratorInfo()
	if err != core.NoError || id != 1 || parts[0] != 1 {
		t.Fatal("lookup failed")
	}

	// Registering another should fail.
	if h.AddPartition(2, h.GetTerm()) != core.ErrReadOnlyMode {
		t.Fatal("should have failed")
	}

	// Set it back.
	err = h.SetReadOnlyMode(false)
	if err != core.NoError {
		t.Fatal(err.Error())
	}

	if h.AddPartition(2, h.GetTerm()) != core.NoError {
		t.Fatal("add failed")
	}
}
