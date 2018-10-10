// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package master

import (
	"math/rand"
	"sort"
	"sync"
	"time"

	log "github.com/golang/glog"

	"github.com/westerndigitalcorporation/blb/internal/core"
	"github.com/westerndigitalcorporation/blb/internal/master/durable"
	"github.com/westerndigitalcorporation/blb/pkg/raft/raft"
)

// PL-1100
const (
	// The upper limit for new partition quota. The quota for each curator
	// is refilled periodically and upper bounded by this value.
	newPartitionUpperLimit = 2

	// At what interval do we refill the new partition quota for each
	// curator. One slot is added at each tick. As each partition can
	// accommodate 2^32 blobs and thus this value allows each curator to
	// create at the rate of 2^33 blobs per day.
	newPartitionRefillInterval = 12 * time.Hour
)

// CuratorInfo describes a curator. This is not persisted in the durable state,
// but rather being kept in memory.
//
// Since we don't persist the address and last heartbeat time, when a master
// failover happens, the new leader cannot service requests until it hears
// heartbeat from the curators. See PL-1102.
//
// Since we don't persist the new partition quota in durable state, when a
// master failover happens, the new leader will reset the quota for each curator
// and thus it's possible that a curator can have be allocated more partitions
// in such a case. However, we made this decision because this this quota is
// kind of "soft" limit in the sense that we just want to prevent a buggy
// client/curator from grabbing too many partitions, but not necessarily enforce
// that each curator cannot get more than the limit. Assuming master failure is
// rare, the chance of exceeding the quota should not appear too frequently.
type CuratorInfo struct {
	// What curator ID is this?
	ID core.CuratorID

	// Address for RPCs and http status.
	// These addrs are for the latest (and possibly stale) primary.
	Addr string

	// System clock time of when we received the last heartbeat from the host representing
	// this curator ID.
	//
	// We can't really take action when a curator dies.  Another member of the curator's
	// replication group has to take over, or we perhaps page an SRE.
	LastHeartbeat time.Time

	// How many additional partitions the curator can ask for. When this
	// quota drops to 0, 'newPartition' will fail. The quota will be
	// refreshed regularly.
	NewPartitionQuota uint32
}

// TractserverInfo contains information about the last heartbeat received from a TS.
type TractserverInfo struct {
	core.TractserverInfo

	// Add local-only fields here.
}

// The Master assigns partitions of the BlobID space to curators, and
// forwards clients to the correct curator for their requests.
type Master struct {
	// Configuration
	cfg Config

	// The interface for talking to Raft that manages the durable state.
	// Does own synchronization.
	stateHandler *durable.StateHandler

	// Volatile curator information.  We keep this sorted by curators.ID.
	curators []CuratorInfo

	// Volatile tractserver information, sorted by tractservers.ID
	tractservers []TractserverInfo

	// Used to notify that the master elected as Raft leader. Used for testing only.
	leaderCh chan struct{}

	// Lock for 'curators' and 'tractservers'.
	lock sync.Mutex
}

// NewMaster creates and returns a new Master.
func NewMaster(cfg Config, stateCfg durable.StateConfig, r *raft.Raft) *Master {
	m := &Master{
		cfg:          cfg,
		stateHandler: durable.NewStateHandler(&stateCfg, r),
		leaderCh:     make(chan struct{}, 10),
	}
	m.stateHandler.SetOnLeader(m.onLeader)
	m.stateHandler.Start()
	go m.partitionQuotaLoop()
	return m
}

// partitionQuotaLoop runs forever to periodically refill the new partition
// quotas for all curators.
func (m *Master) partitionQuotaLoop() {
	ticker := time.NewTicker(newPartitionRefillInterval)

	for {
		<-ticker.C
		m.lock.Lock()
		for i := range m.curators {
			// Refill one slot at a time up to the predefined limit.
			m.curators[i].NewPartitionQuota = min(m.curators[i].NewPartitionQuota+1, newPartitionUpperLimit)
		}
		m.lock.Unlock()
	}
}

func min(a, b uint32) uint32 {
	if a <= b {
		return a
	}
	return b
}

// Finds the curator with id 'id', or the position in m.curators where a curator
// with that ID would be inserted to maintain a sorted order.
//
// Assumes lock is held.
func (m *Master) findCurator(id core.CuratorID) (int, bool) {
	f := func(i int) bool {
		return m.curators[i].ID >= id
	}

	i := sort.Search(len(m.curators), f)
	found := i < len(m.curators) && m.curators[i].ID == id
	return i, found
}

// Inserts a curator with id 'id' currently serving at address 'addr' at position 'i'.
// 'i' must be provided by findCurator, and there must be no mutation of m.curators
// between calling findCurator and addCurator.
//
// Assumes lock is held.
func (m *Master) addCurator(id core.CuratorID, addr string, i int) {
	m.curators = append(m.curators, CuratorInfo{})
	copy(m.curators[i+1:], m.curators[i:])
	m.curators[i] = CuratorInfo{
		ID:                id,
		Addr:              addr,
		LastHeartbeat:     time.Now(),
		NewPartitionQuota: newPartitionUpperLimit,
	}
}

// registerCurator registers a new curator with the Master. registerCurator
// should only be called once per Curator replication group, and only by the
// leader. The Master assigns the Curator replication group a unique ID that it
// must persist and use for all further communication with the master.
//
// NOTE: The registration response from the master to the curator replication
// group can get lost over the wire, and the curator leader can crash before
// receiving or committing the assignment. As a result, the curator replication
// group will retry registering with the master and the previous assigned
// curator id gets lost. Unfortunately, the possibility of losing curator id's
// is thus unavoidable.
func (m *Master) registerCurator(addr string) (core.CuratorID, core.Error) {
	// Reject if we are not the leader.
	if !m.stateHandler.IsLeader() {
		return 0, core.FromRaftError(raft.ErrNodeNotLeader)
	}

	// Generate and persist a curator ID. Don't do this while holding the lock.
	curatorID, err := m.stateHandler.RegisterCurator(m.stateHandler.GetTerm())
	if core.NoError != err {
		return curatorID, err
	}

	// Add the newly-registered curator information to the volatile state.
	m.lock.Lock()
	defer m.lock.Unlock()
	if i, ok := m.findCurator(curatorID); ok {
		// Sanity check -- we shouldn't ever get the same curatorID from two registrations.
		log.Fatalf("curator ID %d has already been registered", curatorID)
	} else {
		m.addCurator(curatorID, addr, i)
		log.Infof("new curator registered with ID %d, addr %s", curatorID, addr)
	}

	return curatorID, core.NoError
}

// curatorHeartbeat informs the Master that a curator repl group is still alive and serving data.
// The heartbeat might be sent from a different host (different primary, new servers, etc.),
// so we update the existing hosts from the heartbeat.
//
// The reply to a newPartition request can get lost over the wire, and the
// curator leader can crash before receiving or committing the assignment.
// Therefore, the assigned partitions are sent back in the heartbeat response.
func (m *Master) curatorHeartbeat(curatorID core.CuratorID, addr string) ([]core.PartitionID, core.Error) {
	// Reject if we are not the leader.
	if !m.stateHandler.IsLeader() {
		return nil, core.FromRaftError(raft.ErrNodeNotLeader)
	}

	// Validate the curatorID.
	if err := m.stateHandler.ValidateCuratorID(curatorID); core.NoError != err {
		log.Errorf("invalid heartbeat from curator with ID %d at address %s: %s", curatorID, addr, err)
		return nil, core.ErrBadCuratorID
	}

	log.Infof("received heartbeat from curator with ID %d at address %s", curatorID, addr)

	// Update hosts for this curator repl group. We are trusting the curator
	// here to provide the correct hosts.
	m.lock.Lock()
	if i, ok := m.findCurator(curatorID); ok {
		m.curators[i].LastHeartbeat = time.Now()
		m.curators[i].Addr = addr
	} else {
		m.addCurator(curatorID, addr, i)
	}
	m.lock.Unlock()

	// Retrieve and return all assigned partitions.
	return m.stateHandler.GetPartitions(curatorID)
}

// newPartition allocates a partition of the BlobID space to the curator with the provided
// ID. This allocation is persisted.
func (m *Master) newPartition(curatorID core.CuratorID) (core.PartitionID, core.Error) {
	// Reject if we are not the leader.
	if !m.stateHandler.IsLeader() {
		return 0, core.FromRaftError(raft.ErrNodeNotLeader)
	}

	// Check quota and reject if it's 0.
	m.lock.Lock()
	i, ok := m.findCurator(curatorID)
	if !ok {
		m.lock.Unlock()
		return 0, core.ErrBadCuratorID
	}
	if m.curators[i].NewPartitionQuota == 0 {
		m.lock.Unlock()
		return 0, core.ErrExceedNewPartitionQuota
	}
	m.curators[i].NewPartitionQuota--
	m.lock.Unlock()

	// Generate and persist a new partition ID.
	return m.stateHandler.NewPartition(curatorID, m.stateHandler.GetTerm())
}

// lookup determines which curator is responsible for the provided partition and
// returns all replicas for that curator.  If there isn't a curator responsible
// for the partition, returns nil. This lookup should go through the state handler
// for consistent read.
func (m *Master) lookup(id core.PartitionID) ([]string, core.Error) {
	// Reject if we are not the leader.
	if !m.stateHandler.IsLeader() {
		return nil, core.FromRaftError(raft.ErrNodeNotLeader)
	}

	// Look for the curator who owns this blob. Don't do this while holding
	// the lock.
	cid, err := m.stateHandler.Lookup(id)
	if core.NoError != err {
		return nil, err
	}

	// Return the curator addresses.
	m.lock.Lock()
	defer m.lock.Unlock()

	if i, ok := m.findCurator(cid); ok {
		return []string{m.curators[i].Addr}, core.NoError
	}
	return nil, core.ErrCantFindCuratorAddr
}

// newBlob picks a curator to store the metadata for a new blob. This operation
// does not go through the state handler.
func (m *Master) newBlob() ([]string, core.Error) {
	// Reject if we are not the leader.
	if !m.stateHandler.IsLeader() {
		return nil, core.FromRaftError(raft.ErrNodeNotLeader)
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	if len(m.curators) == 0 {
		return nil, core.ErrAllocHost
	}

	// Pick a curator randomly for the blob creation.  See PL-1101 for balancing.
	c := m.curators[rand.Intn(len(m.curators))]
	return []string{c.Addr}, core.NoError
}

// tractserverHeartbeat is called when a tractserver beats.
// Returns an error if not leader.
// Otherwise, we note the beat time and return the curators that the tractserver should beat to.
func (m *Master) tractserverHeartbeat(id core.TractserverID, addr string, disks []core.FsStatus) ([]string, core.Error) {
	// Reject if we are not the leader.
	if !m.stateHandler.IsLeader() {
		return nil, core.FromRaftError(raft.ErrNodeNotLeader)
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	// Maintain m.tractservers sorted by .ID.
	f := func(i int) bool {
		return m.tractservers[i].ID >= id
	}

	// Boilerplate for inserting in sorted order if it's not found.
	i := sort.Search(len(m.tractservers), f)
	if !(i < len(m.tractservers) && m.tractservers[i].ID == id) {
		m.tractservers = append(m.tractservers, TractserverInfo{})
		copy(m.tractservers[i+1:], m.tractservers[i:])
	}

	m.tractservers[i] = TractserverInfo{core.TractserverInfo{
		ID:            id,
		Addr:          addr,
		Disks:         disks,
		LastHeartbeat: time.Now(),
	}}

	// Compile a list of curators for the TS to beat to.
	var ret []string
	for _, curator := range m.curators {
		ret = append(ret, curator.Addr)
	}
	return ret, core.NoError
}

// getTractserverInfo returns a copy of all the volatile tractserver info for export.
func (m *Master) getTractserverInfo() []core.TractserverInfo {
	m.lock.Lock()
	defer m.lock.Unlock()
	out := make([]core.TractserverInfo, len(m.tractservers))
	for i, t := range m.tractservers {
		out[i] = t.TractserverInfo
	}
	return out
}

// onLeader is called when the master becomes the leader of the replication group.
func (m *Master) onLeader() {
	log.Infof("@@@ became leader")

	go m.checksumLoop()

	// Notify that the master is elected as the Raft leader.
	// Used for testing only.
	select {
	case m.leaderCh <- struct{}{}:
	default:
	}
}

// registerTractserver registers a new tractserver with the Master.
// registerTractserver should only be called once per Tractserver replication
// group, and only by the leader. The Master assigns the Tractserver replication
// group a unique ID that it must persist and use for all further communication
// with the master.
func (m *Master) registerTractserver(addr string) (core.TractserverID, core.Error) {
	// Reject if we are not the leader.
	if !m.stateHandler.IsLeader() {
		return 0, core.FromRaftError(raft.ErrNodeNotLeader)
	}

	// Generate and persist a tractserver ID.
	log.Infof("@@@ received registration request from tractserver at %s", addr)
	tractserverID, err := m.stateHandler.RegisterTractserver(m.stateHandler.GetTerm())
	if core.NoError == err {
		log.Infof("new tractserver at %s registered with ID %d", addr, tractserverID)
	} else {
		log.Errorf("failed to register tractserver at %s: %s", addr, err)
	}

	return tractserverID, err
}

// getHeartbeats returns the latest heartbeat info, presumably to be formatted into a status page.
func (m *Master) getHeartbeats() ([]CuratorInfo, []TractserverInfo) {
	m.lock.Lock()
	defer m.lock.Unlock()

	c := make([]CuratorInfo, len(m.curators))
	copy(c, m.curators)

	t := make([]TractserverInfo, len(m.tractservers))
	copy(t, m.tractservers)

	return c, t
}

func (m *Master) getPartitions(curator core.CuratorID) ([]core.PartitionID, core.Error) {
	return m.stateHandler.GetPartitions(curator)
}

// checksumLoop periodically asks the handler to perform a consistency check
// across the raft group. It will exit when this node loses leadership.
func (m *Master) checksumLoop() {
	h := m.stateHandler
	startTerm := h.GetTerm()
	for _ = range time.NewTicker(m.cfg.ConsistencyCheckInterval).C {
		if !h.IsLeader() || h.GetTerm() != startTerm {
			return
		}

		err := h.ConsistencyCheck()
		if err != core.NoError {
			log.Errorf("error performing consistency check: %s", err)
		}
	}
}
