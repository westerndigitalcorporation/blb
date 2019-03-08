// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package curator

import (
	"fmt"
	"io/ioutil"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/westerndigitalcorporation/blb/internal/core"
	"github.com/westerndigitalcorporation/blb/internal/curator/durable"
	"github.com/westerndigitalcorporation/blb/internal/curator/durable/state"
	"github.com/westerndigitalcorporation/blb/internal/curator/durable/state/fb"
	"github.com/westerndigitalcorporation/blb/pkg/raft/raft"
	test "github.com/westerndigitalcorporation/blb/pkg/testutil"
)

const defHint = core.StorageHintDEFAULT

// For testing purposes, adds a tractserver with provided id & address.
func (c *Curator) addTS(id core.TractserverID, addr string) []core.PartitionID {
	if !id.IsValid() {
		panic("invalid tractserver id")
	}
	return c.tractserverHeartbeat(id, addr, nil, nil, core.TractserverLoad{AvailSpace: 1024 * 1024 * 1024 * 1024})
}

// newTestCurator returns a new Curator.
func newTestCurator(mc MasterConnection, tt TractserverTalker, cfg Config) *Curator {
	return newTestCuratorWithFailureDomain(mc, tt, cfg, &nilFailureDomainService{})
}

func newTestCuratorWithFailureDomain(mc MasterConnection, tt TractserverTalker, cfg Config, fds FailureDomainService) *Curator {
	dir, err := ioutil.TempDir(test.TempDir(), "curator_test")
	if err != nil {
		panic("Failed to create temp dir")
	}
	stateCfg := durable.DefaultStateConfig
	stateCfg.DBDir = dir
	curator := NewCurator(
		&cfg,
		mc,
		tt,
		stateCfg,
		// Generate a unique cluster name from reading current timestamp.
		raft.NewTestRaftNode("curator_test", fmt.Sprintf("%d", time.Now().UnixNano())),
		fds,
		time.Now,
		// Add unique string to avoid metric name collisions.
		fmt.Sprintf("%d", time.Now().UnixNano()),
	)
	curator.stateHandler.ProposeInitialMembership([]string{"curator_test"})
	// Wait until it becomes Raft leader.
	curator.blockIfNotLeader()
	return curator
}

// MasterConnection for testing.
type testMasterConnection struct {
	// A channel that the testMasterConnection sends on when it gets a heartbeat.
	heartbeatChan chan bool

	// Next partition id to return on NewPartition call.
	nextPartition core.PartitionID

	// Next curator id to return on RegisterCurator call.
	nextCuratorID core.CuratorID

	// Allocated partitions.
	partitions map[core.CuratorID][]core.PartitionID

	// Lock for 'nextPartition', 'nextCuratorID' and 'partitions'
	lock sync.Mutex
}

func newTestMasterConnection() *testMasterConnection {
	return &testMasterConnection{
		heartbeatChan: make(chan bool, 10),
		nextCuratorID: 1,
		nextPartition: 1,
		partitions:    make(map[core.CuratorID][]core.PartitionID),
	}
}

// Register always succeeds, returning the same partition/curator ID every time.
func (mc *testMasterConnection) RegisterCurator() (core.RegisterCuratorReply, core.Error) {
	mc.lock.Lock()
	defer mc.lock.Unlock()

	reply := core.RegisterCuratorReply{
		CuratorID: mc.nextCuratorID,
		Err:       core.NoError,
	}
	mc.nextCuratorID++
	return reply, core.NoError
}

// SendHeartbeat always succeeds as well, but sends on heartbeatChan to allow tests to wait until
// the curator is ready to serve before sending commands to it.
func (mc *testMasterConnection) CuratorHeartbeat(id core.CuratorID) (core.CuratorHeartbeatReply, core.Error) {
	// This allows the test to wait until the curator is ready to serve.
	mc.heartbeatChan <- true

	reply := core.CuratorHeartbeatReply{
		Err:        core.NoError,
		Partitions: mc.partitions[id],
	}
	return reply, core.NoError
}

// NewPartition always succeeds. This method increments the next partition id
// and appends the assigned partition id to the stored list.
func (mc *testMasterConnection) NewPartition(id core.CuratorID) (core.NewPartitionReply, core.Error) {
	mc.lock.Lock()
	defer mc.lock.Unlock()

	var reply core.NewPartitionReply
	reply.PartitionID = mc.nextPartition
	mc.partitions[id] = append(mc.partitions[id], reply.PartitionID)
	mc.nextPartition++
	return reply, core.NoError
}

// Test basic argument validation by trying to create some nonsense blobs.
func TestBadCreate(t *testing.T) {
	badRepl := []int{0, -1, 100000}

	// Wait for the curator to send its initial heartbeat, indicating it's ready to serve tracts.
	mc := newTestMasterConnection()
	tt := newTestTractserverTalker()
	c := newTestCurator(mc, tt, DefaultTestConfig)
	<-mc.heartbeatChan

	for _, repl := range badRepl {
		if _, err := c.create(repl, defHint, time.Time{}); core.NoError == err {
			t.Errorf("could create a blob with replication %d", repl)
		}
	}

	if _, err := c.create(3, 100, time.Time{}); core.NoError == err {
		t.Errorf("could create a blob with hint %d", 100)
	}
}

// Create a blob, delete a blob.
func TestBasicCreateDelete(t *testing.T) {
	mc := newTestMasterConnection()
	tt := newTestTractserverTalker()
	c := newTestCurator(mc, tt, DefaultTestConfig)
	<-mc.heartbeatChan

	// Create a blob with replication factor 2.
	id, err := c.create(2, defHint, time.Time{})
	if core.NoError != err {
		t.Error("could not create blob with reasonable replication")
	}

	// Delete the blob.
	if core.NoError != c.remove(id) {
		t.Error("could not delete a blob")
	}

	// Delete it again.  Should be an error.
	if core.NoError == c.remove(id) {
		t.Error("could delete the blob twice?")
	}
}

// Test that extending a blob when there are no hosts for the new tracts does not work, but
// adding a host allows extension.
func TestExtendNoHosts(t *testing.T) {
	mc := newTestMasterConnection()
	tt := newTestTractserverTalker()
	c := newTestCurator(mc, tt, DefaultTestConfig)
	<-mc.heartbeatChan

	// Create a blob with replication factor 1.
	id, err := c.create(1, defHint, time.Time{})
	if core.NoError != err {
		t.Error("could not create blob with reasonable replication")
	}

	// Extending shouldn't work as we have no tractservers.
	var newTracts []core.TractInfo
	if newTracts, err = c.extend(id, 1); core.NoError == err {
		t.Error("extending worked but should not have")
	}

	// Add a host, and extend.
	addr := "somehost:someport"
	c.addTS(33, addr)
	if newTracts, err = c.extend(id, 1); core.NoError != err {
		t.Errorf("extending should have worked but did not, got %s", err)
	}
	if size, err := c.ackExtend(id, newTracts); err != core.NoError {
		t.Errorf("ack extending failed: %s", err)
	} else if size != 1 {
		t.Error("added 1 tract but size is not 1")
	}

	// Extend to the same size again, should get no new tracts.
	if newTracts, err = c.extend(id, 1); core.NoError != err {
		t.Errorf("extending should have worked but did not, got %s", err)
	}
	if len(newTracts) != 0 {
		t.Error("should get no new tracts.")
	}
}

// Test that we can't extend a blob unless we have enough hosts to form a repl group for the new
// tract.
func TestExtendHostsButHighRepl(t *testing.T) {
	mc := newTestMasterConnection()
	tt := newTestTractserverTalker()
	c := newTestCurator(mc, tt, DefaultTestConfig)
	<-mc.heartbeatChan

	// Create a blob with high repl factor
	repl := 5
	id, err := c.create(repl, defHint, time.Time{})
	if core.NoError != err {
		t.Error("could not create blob with reasonable replication")
	}

	// Extending shouldn't work as we have no tractservers.
	var newTracts []core.TractInfo
	newTracts, err = c.extend(id, 1)
	if core.NoError == err {
		t.Error("extending worked but should not have")
	}

	// Add a host, and extend.  Won't work -- can't satisfy repl=5
	addr := "thisisnt:validated"
	c.addTS(55, addr)

	newTracts, err = c.extend(id, 1)
	if core.NoError == err {
		t.Error("extending should not have worked")
	}
	for i := 1; i < repl; i++ {
		addr := fmt.Sprintf("tsaddr:%d", i)
		c.addTS(core.TractserverID(i), addr)
	}
	newTracts, err = c.extend(id, 1)
	if core.NoError != err {
		t.Errorf("extending should have worked, got %s", err)
	}
	if 1 != len(newTracts) {
		t.Error("size is wrong")
	}
}

// Test that we cannot allocate too many tracts at a time.
func TestExtendTooLong(t *testing.T) {
	mc := newTestMasterConnection()
	tt := newTestTractserverTalker()
	cfg := DefaultTestConfig
	cfg.MaxTractsToExtend = 5
	c := newTestCurator(mc, tt, cfg)
	<-mc.heartbeatChan

	// Create a blob.
	id, err := c.create(1, defHint, time.Time{})
	if core.NoError != err {
		t.Error("could not create blob with reasonable replication")
	}

	// Add one host.
	addr := "thisisnt:validated"
	c.addTS(core.TractserverID(1), addr)

	// Extending with a too large number should fail.
	if _, err = c.extend(id, cfg.MaxTractsToExtend+1); core.NoError == err {
		t.Fatalf("extending should not have worked with such a large number")
	}

	// Extending with a reasonable number each time should succeed. The
	// total length could be larger than cfg.MaxTractsToExtend.
	for i := 0; i < cfg.MaxTractsToExtend; i++ {
		if newTracts, err := c.extend(id, i+1); core.NoError != err {
			t.Fatalf("extending should have worked with a reasonable number")
		} else {
			c.ackExtend(id, newTracts)
		}
	}

	// Verify total length.
	info, err := c.stat(id)
	if core.NoError != err {
		t.Fatalf("failed to stat blob")
	}
	if cfg.MaxTractsToExtend != info.NumTracts {
		t.Fatalf("failed to extend blob to desired length")
	}
}

// failTalker fails to CreateTract.  It also fails the other methods.
type failTalker struct {
	lock    sync.Mutex
	replies []core.Error
}

func (f *failTalker) CreateTract(addr string, tsid core.TractserverID, id core.TractID) core.Error {
	f.lock.Lock()
	defer f.lock.Unlock()
	ret := f.replies[0]
	f.replies = f.replies[1:]
	return ret
}

func (f *failTalker) SetVersion(addr string, tsid core.TractserverID, id core.TractID, newVersion int, conditionalStamp uint64) core.Error {
	return core.ErrNotYetImplemented
}

func (f *failTalker) PullTract(addr string, tsid core.TractserverID, from []string, id core.TractID, version int) core.Error {
	return core.ErrNotYetImplemented
}

func (f *failTalker) GCTract(addr string, tsid core.TractserverID, old []core.TractState, gone []core.TractID) core.Error {
	return core.ErrNotYetImplemented
}

func (f *failTalker) CheckTracts(addr string, tsid core.TractserverID, tracts []core.TractState) core.Error {
	return core.ErrNotYetImplemented
}

func (f *failTalker) CtlStatTract(addr string, tsid core.TractserverID, id core.TractID, version int) core.StatTractReply {
	return core.StatTractReply{Err: core.ErrNotYetImplemented}
}

func (f *failTalker) PackTracts(addr string, tsid core.TractserverID, length int, tracts []*core.PackTractSpec, id core.RSChunkID) core.Error {
	return core.ErrNotYetImplemented
}

func (f *failTalker) RSEncode(addr string, tsid core.TractserverID, id core.RSChunkID, length int, srcs, dests []core.TSAddr, im []int) core.Error {
	return core.ErrNotYetImplemented
}

// Test that extend without an ack is not visiable.
func TestExtendWithoutAck(t *testing.T) {
	mc := newTestMasterConnection()
	tt := &failTalker{}
	c := newTestCurator(mc, tt, DefaultTestConfig)
	<-mc.heartbeatChan

	// Create a blob.
	id, err := c.create(3, defHint, time.Time{})
	for i := 1; i < 4; i++ {
		c.addTS(core.TractserverID(i), fmt.Sprintf("addr%d", i))
	}
	if core.NoError != err {
		t.Error("could not create blob with reasonable replication")
	}

	// Add 2 tracts.
	var newTracts []core.TractInfo
	if newTracts, err = c.extend(id, 2); core.NoError != err {
		t.Errorf("failed to extend the blob: %s", err)
	}

	// The change should not be visiable before ack.
	if blob, _ := c.stat(id); blob.NumTracts != 0 {
		t.Errorf("pending extend should not be visiable")
	}

	// Ack the extend and check again.
	c.ackExtend(id, newTracts)
	if blob, _ := c.stat(id); blob.NumTracts != 2 {
		t.Errorf("ack'd extend should be visiable")
	}
}

// Test concurrent extend works correctly.
func TestConcurrentExtend(t *testing.T) {
	mc := newTestMasterConnection()
	tt := &failTalker{}
	c := newTestCurator(mc, tt, DefaultTestConfig)
	<-mc.heartbeatChan

	// Create a blob.
	id, err := c.create(3, defHint, time.Time{})
	for i := 1; i < 4; i++ {
		c.addTS(core.TractserverID(i), fmt.Sprintf("addr%d", i))
	}
	if core.NoError != err {
		t.Error("could not create blob with reasonable replication")
	}

	// Add 2 tracts w/o ack.
	var newTracts []core.TractInfo
	if newTracts, err = c.extend(id, 2); core.NoError != err {
		t.Errorf("failed to extend the blob: %s", err)
	}

	// Add 1 tract w/ ack.
	if tracts, err := c.extend(id, 1); core.NoError != err {
		t.Errorf("failed to extend the blob: %s", err)
	} else {
		if _, err := c.ackExtend(id, tracts); core.NoError != err {
			t.Errorf("failed to ack extending the blob: %s", err)
		}
	}

	// Only ack'd change is visiable.
	if blob, _ := c.stat(id); blob.NumTracts != 1 {
		t.Errorf("ack'd extend should be visiable")
	}

	// Acking the first extend should fail as the second one has already
	// made to the durable state.
	if _, err := c.ackExtend(id, newTracts); core.NoError == err {
		t.Errorf("conflicting ack extend should fail")
	}

	// Double check that only the first ack'd extend is visiable.
	if blob, _ := c.stat(id); blob.NumTracts != 1 {
		t.Errorf("ack'd extend should be visiable")
	}
}

// Test stat-ing blobs that don't exist.
func TestStatBlobNoSuchBlob(t *testing.T) {
	mc := newTestMasterConnection()
	tt := newTestTractserverTalker()
	c := newTestCurator(mc, tt, DefaultTestConfig)
	<-mc.heartbeatChan

	// Stat a blob that does not exist.
	if _, err := c.stat(core.BlobID(0)); err == core.NoError {
		t.Errorf("stat should have failed, got %s", err)
	}
}

// Stat a blob that does exist.
func TestStatBlobBasic(t *testing.T) {
	mc := newTestMasterConnection()
	tt := newTestTractserverTalker()
	c := newTestCurator(mc, tt, DefaultTestConfig)
	<-mc.heartbeatChan

	// Create a blob.
	addr := "adventure:time"
	c.addTS(2222, addr)

	// repl=1
	id, err := c.create(1, defHint, time.Time{})
	if core.NoError != err {
		t.Errorf("create should have worked, got %s", err)
	}

	// Stat-ing it should be fine.
	if info, err := c.stat(id); err != core.NoError {
		t.Errorf("stat should have worked, got %s", err)
	} else if info.Repl != 1 || info.NumTracts != 0 {
		t.Errorf("got weird data back from stat")
	}

	// Add a tract.
	var newTracts []core.TractInfo
	if newTracts, err = c.extend(id, 1); core.NoError != err {
		t.Error("extending should have worked")
	}
	if _, err = c.ackExtend(id, newTracts); err != core.NoError {
		t.Errorf("failed to ack extending the blob: %s", err)
	}

	// See the updated NumTracts.
	if info, err := c.stat(id); err != core.NoError {
		t.Errorf("stat should have worked, got %s", err)
	} else if info.Repl != 1 || info.NumTracts != 1 {
		t.Errorf("got weird data back from stat")
	}
}

// Test GetTracts behavior.
func TestGetTracts(t *testing.T) {
	mc := newTestMasterConnection()
	tt := newTestTractserverTalker()
	c := newTestCurator(mc, tt, DefaultTestConfig)
	<-mc.heartbeatChan

	// Call GetTracts without any partitions with a bogus blobid.  Should fail.
	if _, _, err := c.getTracts(core.BlobID(0), 0, 1); err == core.NoError {
		t.Errorf("didn't fail to get tracts")
	}

	// Create a blob.
	for i := 1; i <= 3; i++ {
		addr := fmt.Sprintf("tsaddr:%d", i)
		c.addTS(core.TractserverID(i), addr)
	}
	id, err := c.create(3, defHint, time.Time{})
	if err != core.NoError {
		t.Fatalf("couldn't create a blob with r=3, err=%s", err)
	}

	// Extend the blob.
	var newTracts []core.TractInfo
	if newTracts, err = c.extend(id, 1); err != core.NoError || len(newTracts) != 1 {
		t.Fatalf("couldn't extend the blob: %s", err)
	}
	if _, err = c.ackExtend(id, newTracts); core.NoError != err {
		t.Fatalf("failed to ack extending the blob: %s", err)
	}

	// Call GetTracts yet again on the tract we just created, ensure we get what we expect.
	if tracts, _, err := c.getTracts(id, 0, 1); err != core.NoError {
		t.Errorf("couldn't gettracts the new tract just added")
	} else if len(tracts) != 1 {
		t.Errorf("expected 1 tract didn't get it, got %d", len(tracts))
	}

	// This GetTracts will return no tracts as no such tracts exist.
	if _, _, err := c.getTracts(id, 100, 1000); err != core.ErrNoSuchTract {
		t.Errorf("we asked for tracts that don't exist")
	}

	// Repeat the valid GetTracts above but test edge cases by asking for [0,2)
	if tracts, _, err := c.getTracts(id, 0, 2); err != core.NoError {
		t.Errorf("couldn't gettracts the new tract just added")
	} else if len(tracts) != 1 {
		t.Errorf("expected 1 tract didn't get it")
	}
}

// Add a bunch of tracts, ask for a subset of them, get them.
func TestGetTractsOverlap(t *testing.T) {
	mc := newTestMasterConnection()
	tt := newTestTractserverTalker()
	c := newTestCurator(mc, tt, DefaultTestConfig)
	<-mc.heartbeatChan

	// We'll use repl=1 so this should be fine.
	addr := "localhost:2001"
	c.addTS(2001, addr)

	// Create a blob.
	id, err := c.create(1, defHint, time.Time{})
	if err != core.NoError {
		t.Fatalf("couldn't create a blob err=%s", err)
	}

	// Add 10 tracts.
	var newTracts []core.TractInfo
	if newTracts, err = c.extend(id, 10); len(newTracts) != 10 || err != core.NoError {
		t.Fatalf("couldn't extend the blob, err=%s", err)
	}
	if _, err = c.ackExtend(id, newTracts); err != core.NoError {
		t.Fatalf("failed to ack extending the blob: %s", err)
	}

	// Ask for [0, 6) and validate them.
	tracts, _, err := c.getTracts(id, 0, 6)
	if err != core.NoError {
		t.Fatalf("expected no error, got %s", err)
	}
	if len(tracts) != 6 {
		t.Fatalf("expected 6 tracts of info got %d", len(tracts))
	}
	for i := 0; i < 6; i++ {
		expectedID := core.TractID{Blob: id, Index: core.TractKey(i)}
		if expectedID != tracts[i].Tract {
			t.Errorf("tractid mismatch")
		}
		if tracts[i].Version != 1 {
			t.Errorf("non-one version")
		}
		if len(tracts[i].Hosts) != 1 {
			t.Errorf("wrong number of hosts")
		}
	}

	// Ask for [6, 7) and validate it.
	tracts, _, err = c.getTracts(id, 6, 7)
	if err != core.NoError {
		t.Fatalf("expected no error, got %s", err)
	}
	if len(tracts) != 1 {
		t.Fatalf("expected 6 tracts of info got %d", len(tracts))
	}
	expectedID := core.TractID{Blob: id, Index: core.TractKey(6)}
	if expectedID != tracts[0].Tract {
		t.Errorf("tractid mismatch")
	}

	// Ask for [7, 100) and validate them.
	tracts, _, err = c.getTracts(id, 7, 100)
	if err != core.NoError {
		t.Fatalf("expected no error, got %s", err)
	}
	if len(tracts) != 3 {
		t.Fatalf("expected 3 tracts of info got %d", len(tracts))
	}
	for i := range tracts {
		expectedID := core.TractID{Blob: id, Index: core.TractKey(7 + i)}
		if expectedID != tracts[i].Tract {
			t.Errorf("tractid mismatch")
		}
		if tracts[i].Version != 1 {
			t.Errorf("non-one version")
		}
		if len(tracts[i].Hosts) != 1 {
			t.Errorf("wrong number of hosts")
		}
	}
}

func TestGetTractsEC(t *testing.T) {
	mc := newTestMasterConnection()
	tt := newTestTractserverTalker()
	c := newTestCurator(mc, tt, DefaultTestConfig)
	<-mc.heartbeatChan

	// set up some tractservers
	for i := 1; i <= 10; i++ {
		c.addTS(core.TractserverID(i), fmt.Sprintf("tsaddr:%d", i))
	}

	// create the blob and 13 tracts
	id, err := c.create(1, defHint, time.Time{})
	if err != core.NoError {
		t.Fatalf("couldn't create a blob err=%s", err)
	}
	var tracts []core.TractInfo
	if tracts, err = c.extend(id, 13); err != core.NoError || len(tracts) != 13 {
		t.Fatalf("couldn't extend the blob: %s", err)
	}
	if _, err = c.ackExtend(id, tracts); core.NoError != err {
		t.Fatalf("failed to ack extending the blob: %s", err)
	}

	var cbase uint64 = 123
	ppar := id.Partition() | core.PartitionID(1<<31)
	chunk := core.RSChunkID{Partition: ppar, ID: cbase}
	hosts := []core.TractserverID{9, 8, 7, 6, 5, 4, 3, 2, 1}
	pts := func(i int) state.EncodedTract {
		return state.EncodedTract{ID: tracts[i].Tract, Offset: 100 * i, Length: 100}
	}
	data := [][]state.EncodedTract{
		{pts(0), pts(11)},         // 9
		{pts(2), pts(7)},          // 8
		{pts(3), pts(6), pts(12)}, // 7
		{pts(5), pts(4)},          // 6
		{pts(8), pts(10)},         // 5
		{pts(9), pts(1)},          // 4
	}
	if err := c.stateHandler.CommitRSChunk(chunk, core.StorageClassRS_6_3, hosts, data, 0); err != core.NoError {
		t.Fatalf("CommitRSChunk failed: %s", err)
	}
	if err := c.stateHandler.UpdateStorageClass(id, core.StorageClassRS_6_3, 0); err != core.NoError {
		t.Fatalf("UpdateStorageClass failed: %s", err)
	}

	// ask for a subset of tracts
	base := 5
	tracts, _, err = c.getTracts(id, base, 20)
	if err != core.NoError {
		t.Fatalf("expected no error, got %s", err)
	}
	if len(tracts) != 13-base {
		t.Fatalf("expected %d tracts of info got %d", 13-base, len(tracts))
	}

	check := func(i int, tsid core.TractserverID) {
		tr := tracts[i-base]
		if len(tr.Hosts) > 0 || len(tr.TSIDs) > 0 {
			t.Errorf("%d shouldn't have hosts", i)
		}
		if !tr.RS.Chunk.IsValid() {
			t.Errorf("%d should have valid pointer: %+v", i, tr.RS)
		}
		expChunk := core.RSChunkID{Partition: ppar, ID: cbase + uint64(9-tsid)}
		if tr.RS.Chunk != expChunk {
			t.Errorf("%d has wrong pointer : %+v != %+v", i, tr.RS.Chunk, expChunk)
		}
		if tr.RS.Offset != uint32(i*100) || tr.RS.Length != 100 {
			t.Errorf("%d has wrong offset/length: %+v", i, tr.RS)
		}
		if tr.RS.TSID != tsid {
			t.Errorf("%d has wrong tsid: %v != %v", i, tr.RS.TSID, tsid)
		}
		if tr.RS.Host != fmt.Sprintf("tsaddr:%d", tsid) {
			t.Errorf("%d has wrong host: %v", i, tr.RS.Host)
		}
	}
	check(5, 6)
	check(6, 7)
	check(7, 8)
	check(8, 5)
	check(9, 4)
	check(10, 5)
	check(11, 9)
	check(12, 7)
}

// Curator should get a new partition when it's running out of space.
func TestNewPartition(t *testing.T) {
	cfg := DefaultTestConfig
	cfg.PartitionMonInterval = 10 * time.Millisecond
	cfg.MinFreeBlobSlot = core.MaxBlobKey - 10 // Ask for a new partition when num of free blob slots drops below this.
	cfg.FreeMemLimit = 1024 * 1024
	mc := newTestMasterConnection()
	tt := newTestTractserverTalker()
	c := newTestCurator(mc, tt, cfg)
	<-mc.heartbeatChan

	// We'll use repl=1 so this should be fine.
	addr := "localhost:2001"
	c.addTS(2001, addr)

	// Create enough blobs so that we reach the point for a second partition.
	for i := 0; i < 10; i++ {
		if _, err := c.create(1, defHint, time.Time{}); core.NoError != err {
			t.Fatalf("couldn't create a blob err=%s", err)
		}
	}

	// Wait for a new partition to be allocated.
	for {
		c.lock.Lock()
		numPartitions := len(c.partitions)
		c.lock.Unlock()
		if 1 != numPartitions {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Verify we have two partitions.
	_, partitions, err := c.stateHandler.GetCuratorInfo()
	if core.NoError != err {
		t.Fatalf(err.String())
	}
	if 2 != len(partitions) {
		t.Fatalf("should have allocated another partition")
	}
}

// FixVersion fails with invalid arguments and succeeds otherwise.
func TestFixVersion(t *testing.T) {
	mc := newTestMasterConnection()
	tt := newTestTractserverTalker()
	c := newTestCurator(mc, tt, DefaultTestConfig)
	<-mc.heartbeatChan

	// Add two hosts.
	addr1, addr2 := "addr1", "addr2"
	c.addTS(core.TractserverID(1), addr1)
	c.addTS(core.TractserverID(2), addr2)

	// Create a blob.
	id, err := c.create(2, defHint, time.Time{})
	if core.NoError != err {
		t.Fatalf("failed to create blob: %s", err)
	}

	// Add one tract to the blob.
	var newTracts []core.TractInfo
	if newTracts, err = c.extend(id, 1); err != core.NoError || len(newTracts) != 1 {
		t.Fatalf("failed to extend the blob: %s", err)
	}
	if _, err = c.ackExtend(id, newTracts); core.NoError != err {
		t.Fatalf("failed to ack extending the blob: %s", err)
	}

	tracts, _, err := c.getTracts(id, 0, 1)
	if core.NoError != err {
		t.Fatalf("failed to get tract info")
	}
	tract := tracts[0].Tract

	// Assume the client complaints about mismatched versions. We want to
	// fix the version.
	tt.addSetVersionReply(addr1, core.SetVersionReply{Err: core.NoError})
	tt.addSetVersionReply(addr2, core.SetVersionReply{Err: core.NoError})

	// Wrong tract id should fail.
	if core.NoError == c.fixVersion(core.TractID{Blob: id, Index: 1234}, 1, addr2) {
		t.Fatalf("wrong tract should fail")
	}

	// Wrong host should fail.
	if core.NoError == c.fixVersion(tract, 0, "this.should.not.exist") {
		t.Fatalf("wrong host should fail")
	}

	// Wrong versions should fail.
	if core.NoError == c.fixVersion(tract, 0, addr2) {
		t.Fatalf("wrong version should fail")
	}
	if core.NoError == c.fixVersion(tract, 100, addr2) {
		t.Fatalf("wrong version should fail")
	}

	// OK, a valid complaint should work.
	if err := c.fixVersion(tract, 1, addr2); core.NoError != err {
		t.Fatalf("failed to fix version: %s", err)
	}

	// Verify the new version.
	tracts, _, err = c.getTracts(id, 0, 1)
	if core.NoError != err {
		t.Fatalf("failed to get tract info")
	}
	if 2 != tracts[0].Version {
		t.Fatalf("version should have been bumped")
	}
}

// ReplicateTract fails with invalid arguments and succeeds otherwise.
func TestReplicateTract(t *testing.T) {
	mc := newTestMasterConnection()
	tt := newTestTractserverTalker()
	c := newTestCurator(mc, tt, DefaultTestConfig)
	<-mc.heartbeatChan

	// Add two hosts.
	addr1, addr2 := "addr1", "addr2"
	c.addTS(core.TractserverID(1), addr1)
	c.addTS(core.TractserverID(2), addr2)

	// Create a blob.
	id, err := c.create(2, defHint, time.Time{})
	if core.NoError != err {
		t.Fatalf("failed to create blob: %s", err)
	}

	// Add one tract to the blob.
	var newTracts []core.TractInfo
	if newTracts, err = c.extend(id, 1); err != core.NoError || len(newTracts) != 1 {
		t.Fatalf("failed to extend the blob: %s", err)
	}
	if _, err = c.ackExtend(id, newTracts); core.NoError != err {
		t.Fatalf("failed to ack extending the blob: %s", err)
	}

	tracts, _, err := c.getTracts(id, 0, 1)
	if core.NoError != err {
		t.Fatalf("failed to get tract info")
	}
	tract := tracts[0].Tract

	// Assume some host(s) have failed. We want to rereplicate the tract.

	// Wrong tract id should fail.
	if core.NoError == c.replicateTract(core.TractID{Blob: id, Index: 1234}, []core.TractserverID{1}) {
		t.Fatalf("wrong tract should fail")
	}

	// Wrong host should fail.
	if core.NoError == c.replicateTract(tract, []core.TractserverID{100}) {
		t.Fatalf("wrong host should fail")
	}

	// We are in trouble if all hosts are gone.
	if core.NoError == c.replicateTract(tract, []core.TractserverID{1, 2}) {
		t.Fatalf("no good hosts should fail")
	}

	// We fail if there is no other host we can rereplicate the tract on.
	tt.addSetVersionReply(addr2, core.SetVersionReply{Err: core.NoError})
	if err := c.replicateTract(tract, []core.TractserverID{1}); core.NoError == err {
		t.Fatalf("no replacement host should fail")
	}

	// OK. Add a new host and a valid request should work.
	addr3 := "addr3"
	c.addTS(core.TractserverID(3), addr3)
	tt.addSetVersionReply(addr2, core.SetVersionReply{Err: core.NoError})
	tt.addPullTractReply(addr3, core.NoError)
	if err := c.replicateTract(tract, []core.TractserverID{1}); core.NoError != err {
		t.Fatalf("failed to rereplicate tract: %s", err)
	}

	// Verify the new version.
	tracts, _, err = c.getTracts(id, 0, 1)
	if core.NoError != err {
		t.Fatalf("failed to get tract info")
	}
	if 2 != tracts[0].Version {
		t.Fatalf("version should have been bumped")
	}
}

func TestReconstructRSChunk(t *testing.T) {
	mc := newTestMasterConnection()
	tt := newTestTractserverTalker()
	c := newTestCurator(mc, tt, DefaultTestConfig)
	<-mc.heartbeatChan

	for i := 1; i <= 11; i++ {
		c.addTS(core.TractserverID(i), fmt.Sprintf("tsaddr:%d", i))
	}

	// create the blob and 6 tracts
	id, err := c.create(1, defHint, time.Time{})
	if err != core.NoError {
		t.Fatalf("couldn't create a blob err=%s", err)
	}
	var tracts []core.TractInfo
	if tracts, err = c.extend(id, 6); err != core.NoError || len(tracts) != 6 {
		t.Fatalf("couldn't extend the blob: %s", err)
	}
	if _, err = c.ackExtend(id, tracts); core.NoError != err {
		t.Fatalf("failed to ack extending the blob: %s", err)
	}

	chunk := core.RSChunkID{Partition: id.Partition() | core.PartitionID(1<<31), ID: 123}
	hosts := []core.TractserverID{9, 8, 7, 6, 5, 4, 3, 2, 1}
	pts := func(i int) state.EncodedTract {
		return state.EncodedTract{ID: tracts[i].Tract, Offset: 100 * i, Length: 100}
	}
	data := [][]state.EncodedTract{{pts(4)}, {pts(2)}, {pts(3)}, {pts(1)}, {pts(0)}, {pts(5)}}
	if err := c.stateHandler.CommitRSChunk(chunk, core.StorageClassRS_6_3, hosts, data, 0); err != core.NoError {
		t.Fatalf("CommitRSChunk failed: %s", err)
	}
	if err := c.stateHandler.UpdateStorageClass(id, core.StorageClassRS_6_3, 0); err != core.NoError {
		t.Fatalf("UpdateStorageClass failed: %s", err)
	}

	// Let's say four TS's are down (too many).
	err = c.reconstructChunk(chunk, []core.TractserverID{7, 6, 2, 1})
	if err != core.ErrAllocHost {
		t.Fatalf("should have failed with ErrAllocHost")
	}

	// Let's say 7 and 3 are bad. They'll be replaced by 10 and 11 (in random
	// order, so this is a little messy).
	var destID, otherID core.TractserverID = 10, 11
	destAddr, otherAddr := "tsaddr:10", "tsaddr:11"
	tt.addRSEncodeReply(destAddr, core.NoError)
	tt.addRSEncodeReply(otherAddr, core.NoError)

	err = c.reconstructChunk(chunk, []core.TractserverID{7, 3})
	if err != core.NoError {
		t.Fatalf("should have succeeded")
	}

	// Check the call:
	if len(tt.rsEncodeCalls[otherAddr]) > 0 {
		destID, otherID = otherID, destID
		destAddr, otherAddr = otherAddr, destAddr
	}

	req := tt.rsEncodeCalls[destAddr][0]
	if req.ChunkID != chunk {
		t.Errorf("wrong id")
	}
	if !reflect.DeepEqual(req.Srcs, []core.TSAddr{
		{ID: 9, Host: "tsaddr:9"},
		{ID: 8, Host: "tsaddr:8"},
		{ID: 6, Host: "tsaddr:6"},
		{ID: 5, Host: "tsaddr:5"},
		{ID: 4, Host: "tsaddr:4"},
		{ID: 2, Host: "tsaddr:2"},
	}) {
		t.Errorf("wrong Srcs: %v", req.Srcs)
	}
	if !reflect.DeepEqual(req.Dests, []core.TSAddr{
		{ID: destID, Host: destAddr},
		{ID: otherID, Host: otherAddr},
		{},
	}) {
		t.Errorf("wrong Dests: %v", req.Dests)
	}
	if !reflect.DeepEqual(req.IndexMap, []int{0, 1, 3, 4, 5, 7, 2, 6, -1}) {
		t.Errorf("wrong IndexMap: %v", req.IndexMap)
	}

	// Check that durable state was updated.
	ch := c.stateHandler.GetRSChunk(chunk)
	if !reflect.DeepEqual(fb.HostsList(ch), []core.TractserverID{9, 8, destID, 6, 5, 4, otherID, 2, 1}) {
		t.Errorf("hosts were not updated: %v, %d, %d", fb.HostsList(ch), destID, otherID)
	}
}

// Test that tracts are GC-ed.
func TestGCTracts(t *testing.T) {
	mc := newTestMasterConnection()
	tt := newTestTractserverTalker()
	c := newTestCurator(mc, tt, DefaultTestConfig)
	<-mc.heartbeatChan

	tsid1 := core.TractserverID(1)
	addr1 := "addr1"
	c.addTS(tsid1, addr1)

	// Create a blob.
	id, err := c.create(1, defHint, time.Time{})
	if core.NoError != err {
		t.Fatalf("failed to create blob: %s", err)
	}

	// Add one tract to the blob.
	var newTracts []core.TractInfo
	if newTracts, err = c.extend(id, 1); err != core.NoError || len(newTracts) != 1 {
		t.Fatalf("failed to extend the blob: %s", err)
	}
	if _, err = c.ackExtend(id, newTracts); core.NoError != err {
		t.Fatalf("failed to ack extending the blob: %s", err)
	}

	// This tract doesn't exist.
	tid1 := core.TractID{Blob: core.BlobIDFromParts(2, 1), Index: 0}

	// We're reporting a tract that doesn't exist as present in the TS's state.
	// Should also be GC-ed.
	c.tractserverHeartbeat(tsid1, addr1, nil, []core.TractID{tid1}, core.TractserverLoad{})
	req := <-tt.gcTractCallChan
	if req.Gone[0] != tid1 {
		t.Fatalf("expected %v to be reported as gone", tid1)
	}
}

// Test that undeletion restores a blob and it can then be further manipulated.
func TestUndelete(t *testing.T) {
	mc := newTestMasterConnection()
	tt := newTestTractserverTalker()
	c := newTestCurator(mc, tt, DefaultTestConfig)
	<-mc.heartbeatChan

	tsid1 := core.TractserverID(1)
	addr1 := "addr1"
	c.addTS(tsid1, addr1)

	// Create a blob.
	id, err := c.create(1, defHint, time.Time{})
	if core.NoError != err {
		t.Fatalf("failed to create blob: %s", err)
	}

	// Add one tract to the blob.
	var newTracts []core.TractInfo
	if newTracts, err = c.extend(id, 1); err != core.NoError || len(newTracts) != 1 {
		t.Fatalf("failed to extend the blob: err %s || len(newTracts) %d != 1", err, len(newTracts))
	}
	if _, err = c.ackExtend(id, newTracts); core.NoError != err {
		t.Fatalf("failed to ack extending the blob: %s", err)
	}

	// Delete the blob.
	if c.remove(id) != core.NoError {
		t.Error("could not delete a blob")
	}

	// Add a tract to the blob, should fail.
	if newTracts, err = c.extend(id, 1); err == core.NoError {
		t.Fatalf("should have failed to extend the blob")
	}

	// Undelete the blob.
	if c.unremove(id) != core.NoError {
		t.Fatalf("couldn't unremove the blob")
	}

	// Add another tract to the blob.
	if newTracts, err = c.extend(id, 2); err != core.NoError || len(newTracts) != 1 {
		t.Fatalf("failed to extend the blob: %s", err)
	}
	if _, err = c.ackExtend(id, newTracts); core.NoError != err {
		t.Fatalf("failed to ack extending the blob: %s", err)
	}
}

// Test basic functionality of tract-checking mechanism.  Add tracts, force an iteration of tract-checking,
// ensure that a tract-checking message is sent to each TS that hosts a tract.
func TestCheckTracts(t *testing.T) {
	mc := newTestMasterConnection()
	tt := newTestTractserverTalker()
	c := newTestCurator(mc, tt, DefaultTestConfig)
	<-mc.heartbeatChan

	tsid1 := core.TractserverID(1)
	addr1 := "addr1"
	c.addTS(tsid1, addr1)

	tsid2 := core.TractserverID(2)
	addr2 := "addr2"
	c.addTS(tsid2, addr2)

	// Make a blob with r=2.  Each TS should get a replica of each tract.
	id, err := c.create(2, defHint, time.Time{})
	if core.NoError != err {
		t.Fatalf("failed to create blob: %s", err)
	}

	tractsToAdd := 20
	for i := 0; i < tractsToAdd; i++ {
		// Add a tract to the blob, should succeed.
		newTracts, err := c.extend(id, i+1)
		if err != core.NoError {
			t.Fatalf("couldn't extend the blob")
		}
		if _, err = c.ackExtend(id, newTracts); core.NoError != err {
			t.Fatalf("failed to ack extending the blob: %s", err)
		}
	}

	// Force a call to checkTracts.  The fake tractserver talker should receive a request to check
	// for tractsToAdd tracts on each TS.  Because there's a tract-checking loop, we also have various
	// random requests to check tracts, but we look for at least one to check all the tracts we've
	// created above.
	c.checkTracts()

	tt.lock.Lock()
	defer tt.lock.Unlock()

	addr1ok := false
	for _, call := range tt.checkTractCalls[addr1] {
		if len(call.Tracts) == tractsToAdd {
			addr1ok = true
			break
		}
	}

	addr2ok := false
	for _, call := range tt.checkTractCalls[addr2] {
		if len(call.Tracts) == tractsToAdd {
			addr2ok = true
			break
		}
	}

	if !addr1ok || !addr2ok {
		t.Fatalf("didn't find a check tracts call for all tracts on TS!")
	}
}
