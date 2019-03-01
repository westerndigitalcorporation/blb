// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package curator

import (
	"math/rand"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/westerndigitalcorporation/blb/internal/core"
	"github.com/westerndigitalcorporation/blb/internal/curator/durable"
	"github.com/westerndigitalcorporation/blb/internal/curator/durable/state"
	"github.com/westerndigitalcorporation/blb/internal/server"
	"github.com/westerndigitalcorporation/blb/pkg/raft/raft"
	"github.com/westerndigitalcorporation/blb/pkg/tokenbucket"
)

// Curator manages metadata for tractservers.
type Curator struct {
	// Configuration parameters. Read-only.
	config *Config

	// Monitors tractserver health.
	// Does its own locking.
	tsMon *tractserverMonitor

	// The interface for talking to Raft that manages the durable state.
	// Does its own locking.
	stateHandler *durable.StateHandler

	// Connection to the master.
	// Does its own locking.
	mc MasterConnection

	// Aggregation of connections to tractservers.
	// Does its own locking.
	tt TractserverTalker

	// Rather than serialize 'blocked' with a mutex, client complaints are sent over this.
	blockedChan chan clientComplaint

	// A goroutine monitors this channel for "I own these tracts" reports from
	// tractservers.  Used to centralize and limit the amount of GC-related activity we perform
	// in response to tractserver messages.
	tsGcChan chan tsTractReport

	// Provide exclusive access to all blobs and tracts.
	// Does its own locking.
	lockMgr server.LockManager

	// Lock synchronizes everything below.
	lock sync.Mutex

	// What tracts do we know to be corrupt or missing?
	//
	// When tractservers issue heartbeats, they include which tracts are bad.
	// We batch those in this map, and the re-replication scheduler occasionally
	// dumps all corrupt tracts into its own internal state and clears this.
	//
	// We also periodically verify that tractservers are hosting the tracts we think
	// they are. If a tractserver doesn't have a tract we think it does, it's the same
	// as the tract being corrupt.
	corruptBatch map[core.TractID]TSIDSet

	// What partitions do we oversee?  Could be slightly stale.
	partitions []core.PartitionID

	// Batched mtime/atime updates.
	timeUpdates []state.UpdateTime

	// The latest leadership change from 'leaderChan'. True if it's leader, false otherwise.
	// We use it to keep track of the latest leadership change notification from
	// 'leaderChan' so the curator node can act based on whether it's leader or not.
	iAmLeader     bool
	iAmLeaderCond sync.Cond

	// Bandwidth limiters.
	rsEncodeBwLim *tokenbucket.TokenBucket
	recoveryBwLim *tokenbucket.TokenBucket

	// Pending RS encode operations.
	pendingPieces map[core.TractID]struct{}

	// Metrics we collect.
	internalOpM *server.OpMetric // Stats about various internal ops.

	// Recovery manager.
	recovery *recovery
}

// A complaint from a client that we hand off via a channel to the re-replication loop.
type clientComplaint struct {
	id   core.TractID
	tsid core.TractserverID
}

// A report from a tractserver of a subset of tracts that it owns.
type tsTractReport struct {
	id   core.TractserverID
	addr string
	has  []core.TractID
}

// NewCurator creates and returns a new Curator.
func NewCurator(curatorCfg *Config,
	mc MasterConnection,
	tt TractserverTalker,
	stateCfg durable.StateConfig,
	r *raft.Raft,
	fds FailureDomainService,
	getTime func() time.Time,
	metricNameSuffix string,
) *Curator {
	prefix := "curator"
	if metricNameSuffix != "" {
		prefix = prefix + "_" + metricNameSuffix
	}
	c := &Curator{
		config:        curatorCfg,
		stateHandler:  durable.NewStateHandler(&stateCfg, r),
		tsMon:         newTractserverMonitor(curatorCfg, fds, getTime),
		iAmLeader:     false,
		mc:            mc,
		tt:            tt,
		blockedChan:   make(chan clientComplaint, 1000),
		tsGcChan:      make(chan tsTractReport, 100),
		lockMgr:       server.NewFineGrainedLock(),
		corruptBatch:  make(map[core.TractID]TSIDSet),
		rsEncodeBwLim: tokenbucket.New(1e9, 0), // overridden by dyconfig
		recoveryBwLim: tokenbucket.New(1e9, 0), // overridden by dyconfig
		pendingPieces: make(map[core.TractID]struct{}),
		internalOpM:   server.NewOpMetric(prefix+"_internal_ops", "op"),
	}
	c.iAmLeaderCond.L = &c.lock

	// Get dynamic config from zookeeper. We've initialized things with
	// reasonable values, so we don't need to block on this.
	go c.registerDyConfig()

	// The "pick random tractserver hosts" uses the default source of randomness in math/rand.
	rand.Seed(curatorCfg.CuratorSeed)

	// When we become leader we want to send heartbeats to the master, monitor tractservers, etc.
	c.stateHandler.SetLeadershipChange(c.LeadershipChange)

	// Begin the Raft algorithm.
	c.stateHandler.Start()

	// Send heartbeats to the master to let it know we're OK.
	go c.heartbeatLoop()

	// Update the tractserver monitor with what hosts are currently serving data.  This information
	// is in durable state and is occasionally refreshed.
	go c.updateTsmonLoop()

	// Recover unavailable data.
	c.recovery = newRecovery(c, prefix)

	// Ensure that the tractservers have the tracts we think they do.
	go c.checkTractsLoop()

	// Occasionally garbage collect deleted blobs from durable state.
	go c.gcMetadataLoop()

	// Process tractserver tract report messages and see if the tractserver could GC tracts.
	go c.gcTractserverContents()

	// Do periodic consistency check.
	go c.checksumLoop()

	// Batch time updates.
	go c.updateTimesLoop()

	// Migrate storage classes.
	go c.storageClassLoop()

	return c
}

// Periodically poll durable state for which tractservers we're monitoring, and tell the monitoring code
// to pay attention to them.  Tractservers can be assigned to host data by another leader but not heartbeat
// to us.  When we assume leadership we need to monitor those tractservers and ensure the safety of the
// data they host.
func (c *Curator) updateTsmonLoop() {
	scanTicker := time.NewTicker(10 * time.Second)

	for {
		c.blockIfNotLeader()
		ids := c.stateHandler.GetKnownTSIDs()
		c.tsMon.updateExpected(ids)
		log.Infof("@@@ tsmon: %s", c.tsMon)
		<-scanTicker.C
	}
}

// LeadershipChange is called from the replication level our leadership status
// changes.  We want to send heartbeats to the master, but only if we think we're leader.
func (c *Curator) LeadershipChange(iAmLeader bool) {
	log.Infof("@@@ leadership changed: %v", iAmLeader)

	// Wake up anyone waiting to become leader.
	c.lock.Lock()
	c.iAmLeader = iAmLeader
	c.iAmLeaderCond.Broadcast()
	c.lock.Unlock()

	if iAmLeader {
		log.Infof("LeadershipChange: is now leader, restarting tsmon")
		c.tsMon.Restart()
	} else {
		log.Infof("LeadershipChange: is not leader")
	}
}

// isLeader returns true if we are the current leader.
func (c *Curator) isLeader() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.iAmLeader
}

// blockIfNotLeader does exactly that: waits until it's leader, then returns.
// Note that returning only means the node transitioned from not-leader to leader, or was already leader;
// it may have lost leadership by the time the call returns.
func (c *Curator) blockIfNotLeader() {
	c.lock.Lock()
	for !c.iAmLeader {
		c.iAmLeaderCond.Wait()
	}
	c.lock.Unlock()
}

// tractserverHeartbeat records a heartbeat from the tractserver.
func (c *Curator) tractserverHeartbeat(id core.TractserverID, addr string, bad, has []core.TractID, load core.TractserverLoad) (ret []core.PartitionID) {
	log.Infof("heartbeat from (%d:%s) with %d/%d bad, %d total, %d/%d GB free",
		id, addr, len(bad), len(has), load.NumTracts, load.AvailSpace>>30, load.TotalSpace>>30)

	c.tsMon.recvHeartbeat(id, addr, load)

	// Protect c.partitions and c.corruptBatch.
	c.lock.Lock()

	// Return a copy of c.partitions so the tractserver knows what to heartbeat to us again.
	ret = make([]core.PartitionID, len(c.partitions))
	copy(ret, c.partitions)

	// Add every corrupt tract into the corrupt tract map.
	for _, t := range bad {
		if s, ok := c.corruptBatch[t]; !ok || !s.Contains(id) {
			c.corruptBatch[t] = s.Add(id)
		}
	}

	c.lock.Unlock()

	// Tell the TS GC thread to check the tracts received from this TS.
	if len(has) > 0 {
		select {
		case c.tsGcChan <- tsTractReport{id: id, addr: addr, has: has}:
		default:
			// No big deal.
		}
	}

	return
}

func (c *Curator) getCorruptBatch() map[core.TractID]TSIDSet {
	newMap := make(map[core.TractID]TSIDSet)
	c.lock.Lock()
	toMerge := c.corruptBatch
	c.corruptBatch = newMap
	c.lock.Unlock()
	return toMerge
}

// create creates a blob with replication factor 'repl' and storage hint 'hint'.
// Create does not create any tracts in the blob.
func (c *Curator) create(repl int, hint core.StorageHint, expires time.Time) (core.BlobID, core.Error) {
	if repl <= 0 || repl > c.config.MaxReplFactor {
		return core.BlobID(0), core.ErrInvalidArgument
	}
	if _, ok := core.EnumNamesStorageHint[hint]; !ok {
		return core.BlobID(0), core.ErrInvalidArgument
	}

	// Have Raft figure out the Blob's ID and commit the creation.
	var exp int64
	if !expires.IsZero() {
		exp = expires.UnixNano()
	}
	return c.stateHandler.CreateBlob(repl, time.Now().UnixNano(), exp, hint, c.stateHandler.GetTerm())
}

// extend allocates additional tracts to the blob. The allocated tractservers
// for hosting the new tracts are returned. The client is responsible for
// contacting those tractservers to create the tract.
//
// This is a metadata-only change. Mutating the data in these tracts requires
// coordinating with tractservers.
//
// Note that we could add an atomicExtend if we wanted to, like how Flat
// Datacenter Storage coordinates parallel writers who don't want to clobber
// each other.
func (c *Curator) extend(id core.BlobID, desiredSize int) ([]core.TractInfo, core.Error) {
	// Prevent parallel calls to extend.
	c.lockMgr.LockBlob(id)
	defer c.lockMgr.UnlockBlob(id)

	// Look up the replication factor and the current size.
	info, err := c.stateHandler.Stat(id)
	if err != core.NoError {
		log.Errorf("extend: %v couldn't Stat err=%s", id, err)
		return nil, err
	}

	// Figure out how many tracts we'd need to add to reach the target size.
	tractsToAdd := desiredSize - info.NumTracts
	if tractsToAdd <= 0 {
		// We've already allocated this many.
		log.Errorf("extend: %v current size %d satisfies desired size %d", id, info.NumTracts, desiredSize)
		return nil, core.NoError
	} else if tractsToAdd > c.config.MaxTractsToExtend {
		// Don't allocate too many tracts at once to guard against clients overwhelming
		// the curator with bad requests.
		log.Errorf("extend: %v client wants to add too many tracts: wants %d, tract has %d already, max extend is %d", id, desiredSize, info.NumTracts, c.config.MaxTractsToExtend)
		return nil, core.ErrTooBig
	}

	// Allocate tractservers to host replicas of these tracts.
	var tracts []core.TractInfo
	for i := 0; i < tractsToAdd; i++ {
		tsAddrs, tsIDs := c.allocateTS(info.Repl, nil, nil)
		if tsAddrs == nil {
			log.Errorf("extend: %v failed to pick %d hosts while extending the blob by %d tracts", id, info.Repl, tractsToAdd)
			return nil, core.ErrAllocHost
		}
		tracts = append(tracts, core.TractInfo{
			Tract:   core.TractID{Blob: id, Index: core.TractKey(info.NumTracts + i)},
			Version: 1,
			Hosts:   tsAddrs,
			TSIDs:   tsIDs,
		})
	}
	return tracts, core.NoError
}

// ackExtend acknowledges the success of a previous extend operation.
func (c *Curator) ackExtend(id core.BlobID, tracts []core.TractInfo) (int, core.Error) {
	if len(tracts) == 0 {
		return 0, core.NoError
	} else if len(tracts) > c.config.MaxTractsToExtend {
		return 0, core.ErrTooBig
	}

	c.lockMgr.LockBlob(id)
	defer c.lockMgr.UnlockBlob(id)

	// Compose tract host slices.
	hosts := make([][]core.TractserverID, 0, len(tracts))
	for _, tract := range tracts {
		hosts = append(hosts, tract.TSIDs)
	}

	return c.stateHandler.ExtendBlob(id, tracts[0].Tract.Index, hosts)
}

// remove removes a blob.
// We don't actually tell the tractserver to do anything immediately.  We wait
// for garbage collection to eventually happen.
func (c *Curator) remove(id core.BlobID) core.Error {
	return c.stateHandler.DeleteBlob(id, time.Now(), c.stateHandler.GetTerm())
}

// unremove un-removes a blob, if possible.
func (c *Curator) unremove(id core.BlobID) core.Error {
	return c.stateHandler.UndeleteBlob(id, c.stateHandler.GetTerm())
}

func (c *Curator) setMetadata(id core.BlobID, md core.BlobInfo) core.Error {
	return c.stateHandler.SetMetadata(id, md)
}

// Stat returns information about the blob.
func (c *Curator) stat(id core.BlobID) (core.BlobInfo, core.Error) {
	return c.stateHandler.Stat(id)
}

// getTracts returns the tract information for the tracts [start, end) in the blob
// with id 'id'.  If 'end' is actually beyond the end of the blob, only tracts
// that exist are returned.
//
// This will also involve translating tract server IDs in durable state to their
// corresponding addresses learned from their heartbeats by tract server monitor.
// These addresses are stored in-memory only and can be stale. If the translation
// fails, we'll return empty strings in place of unknown hosts. Clients should
// be able to handle these.
func (c *Curator) getTracts(id core.BlobID, start, end int) ([]core.TractInfo, core.StorageClass, core.Error) {
	// Sanity check.
	if start < 0 || end < 0 || end < start {
		log.Errorf("getTracts: %v invalid start and/or end: [%d,%d)", id, start, end)
		return nil, core.StorageClassREPLICATED, core.ErrInvalidArgument
	}

	// Retrieve tractserver IDs from the durable state.
	tracts, cls, err := c.stateHandler.GetTracts(id, start, end)
	if err != core.NoError {
		log.Errorf("getTracts: %v GetTracts from durable state failed, err = %s", id, err)
		return nil, cls, err
	}

	// Retrieve tractserver addresses from tractserver monitor.
	for i := range tracts {
		if len(tracts[i].TSIDs) > 0 {
			tracts[i].Hosts, _ = c.tsMon.getTractserverAddrs(tracts[i].TSIDs)
		}
		if tracts[i].RS.Present() {
			// Durable layer will always fill in OtherTSIDs here.
			tracts[i].RS.OtherHosts, _ = c.tsMon.getTractserverAddrs(tracts[i].RS.OtherTSIDs)
			// One of them will be the TSID that this data is on directly. Copy that to Host.
			for j, tsid := range tracts[i].RS.OtherTSIDs {
				if tsid == tracts[i].RS.TSID {
					tracts[i].RS.Host = tracts[i].RS.OtherHosts[j]
					break
				}
			}
		}
	}
	return tracts, cls, core.NoError
}

// forward a request for timely re-replication of a tract to the replication scheduler.
// Note that both 'id' and 'badAddr' could be totally bogus.
func (c *Curator) reportBadTS(id core.TractID, badAddr string) core.Error {
	badID, found := c.tsMon.getIDByAddr(badAddr)

	// The client is complaining about this host but we haven't gotten a heartbeat from it yet.
	if !found {
		log.Errorf("reportBadTS: %s tsMon didn't have id for addr %s", id, badAddr)
		return core.ErrHostNotExist
	}

	select {
	case c.blockedChan <- clientComplaint{id: id, tsid: badID}:
		log.Infof("reportBadTS: %s added to blocked list", id)
		return core.NoError
	default:
		log.Errorf("reportBadTS: %s blocked list too full, dropped", id)
		return core.ErrTooBusy
	}
}

// fixVersion is called when a client tries to interact with a tract but has a bad version.
// The expected error case is that the version was bumped on the tractserver during an aborted
// re-replication of a tract or EC coding.
func (c *Curator) fixVersion(id core.TractID, cliVersion int, badAddr string) core.Error {
	// We might mutate state so use the term number for a continuity check.
	term := c.stateHandler.GetTerm()

	// Translate from address to tsid.
	badID, found := c.tsMon.getIDByAddr(badAddr)
	if !found {
		log.Errorf("fixVersion: %s couldn't find tsid for host %s", id, badAddr)
		return core.ErrHostNotExist
	}

	// Concurrent changes are hard to reason about so prevent them.
	c.lockMgr.LockTract(id)
	defer c.lockMgr.UnlockTract(id)

	// Pull out current state for the tract the client is complaining about.
	durInfoArr, _, err := c.stateHandler.GetTracts(id.Blob, int(id.Index), 1+int(id.Index))
	if err != core.NoError {
		log.Errorf("fixVersion: %s failed to GetTracts for %d: %s", id, int(id.Index), err)
		return err
	}
	durInfo := durInfoArr[0]

	// If the tract they're complaining about is RS-coded in addition to being replicated,
	// that means we're in the middle of transitioning the blob to RS storage. We've
	// already made a copy of this tract and have bumped the TS versions so that writes
	// can't happen anymore. So we need to reject this here. (If we didn't do this check,
	// the next check would also catch this case, since durInfo.TSIDs would be empty. But
	// we do a separate check to provide a better log message and error code.)
	if durInfo.RS.Present() {
		log.Errorf("fixVersion: %s tract is RS-encoded already", id)
		return core.ErrReadOnlyStorageClass
	}

	// Verify that the tract has 'badID' as a host.
	if !contains(durInfo.TSIDs, badID) {
		log.Errorf("fixVersion: %s tract not hosted by complained-about host %d at %s", id, badID, badAddr)
		return core.ErrInvalidArgument
	}

	// The client shouldn't have any version < durInfo.version -- if so their information is outdated.
	// The client should only ever attempt writes with the durable version.  If they have a larger
	// version, they're doing something wrong.
	if cliVersion != durInfo.Version {
		log.Errorf("fixVersion: %s tract version %d doesn't match client version %d", id, durInfo.Version, cliVersion)
		return core.ErrInvalidArgument
	}

	// TODO: We could add a RPC to poll the TSs and ask them what their versions are before doing the bump
	// rather than taking the client's word for it.

	// Translate the durable tsid set to an active address set.
	addrs, missing := c.tsMon.getTractserverAddrs(durInfo.TSIDs)
	if missing > 0 {
		log.Errorf("fixVersion: %s couldn't look up address of current hosts %+v", id, durInfo.TSIDs)
		return core.ErrHostNotExist
	}

	// OK, we can bump on all versions to 1 + durable version.
	errorChan := make(chan core.Error, len(durInfo.TSIDs))
	for i := range durInfo.TSIDs {
		go func(addr string, tsId core.TractserverID) {
			err := c.tt.SetVersion(addr, tsId, id, durInfo.Version+1, 0)
			if err != core.NoError {
				log.Errorf("fixVersion: %s SetVersion to %d on tsid=%d host=%s failed: %v", id, durInfo.Version+1, tsId, addr, err)
			}
			errorChan <- err
		}(addrs[i], durInfo.TSIDs[i])
	}
	for range durInfo.TSIDs {
		if res := <-errorChan; res != core.NoError {
			return res
		}
	}

	// Mutate the tract in durable state.
	err = c.stateHandler.ChangeTract(id, durInfo.Version+1, durInfo.TSIDs, term)
	if err != core.NoError {
		log.Errorf("fixVersion: %s ChangeTract failed: %s", id, err)
	}
	return err
}

func (c *Curator) listBlobs(partition core.PartitionID, start core.BlobKey) ([]core.BlobKey, core.Error) {
	return c.stateHandler.ListBlobs(partition, start)
}

func (c *Curator) stats() map[string]interface{} {
	m := make(map[string]interface{})
	for _, op := range []string{
		"RereplicateTract",
		"CheckAllTracts",
		"RSEncode",
		"RSEncode/pack",
		"RSEncode/encode",
		"RSEncode/bump",
	} {
		m[op] = c.internalOpM.String(op)
	}
	c.recovery.stats(m)
	return m
}

func (c *Curator) markPendingPieces(base core.RSChunkID, n int) {
	c.lock.Lock()
	defer c.lock.Unlock()

	for i := 0; i < n; i++ {
		c.pendingPieces[base.Add(i).ToTractID()] = struct{}{}
	}
}

func (c *Curator) unmarkPendingPieces(base core.RSChunkID, n int) {
	c.lock.Lock()
	defer c.lock.Unlock()

	for i := 0; i < n; i++ {
		delete(c.pendingPieces, base.Add(i).ToTractID())
	}
}

// filterPendingPieces returns the slice ids after removing ids that correspond
// to RS chunk pieces that are currently being worked on. Mutates ids.
func (c *Curator) filterPendingPieces(ids []core.TractID) (out []core.TractID) {
	c.lock.Lock()
	defer c.lock.Unlock()

	out = ids[:0]
	for _, id := range ids {
		if _, ok := c.pendingPieces[id]; !ok {
			out = append(out, id)
		}
	}
	return
}
