// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package curator

import (
	"time"

	sigar "github.com/cloudfoundry/gosigar"

	log "github.com/golang/glog"
	"github.com/westerndigitalcorporation/blb/internal/core"
	"github.com/westerndigitalcorporation/blb/internal/curator/durable/state"
	"github.com/westerndigitalcorporation/blb/internal/curator/durable/state/fb"
)

const (
	// At what interval do we retry when failure happens?
	retryInterval = 5 * time.Second

	// Finish deleting this many blobs per command.
	maxDeleteSize = 1000
)

// heartbeatLoop loops while we're leader, sending heartbeat to the master.
func (c *Curator) heartbeatLoop() {
	var curatorID core.CuratorID
	// No lock here as we're the only one that works on 'c.partitions' now.
	curatorID, c.partitions = c.initialize()

	log.Infof("@@@ curator group %d initialized, partitions owned %v", curatorID, c.partitions)

	// Ensure we will ask for a new partition when existing ones are almost full.
	go c.partitionMonitorLoop(curatorID)

	masterTicker := time.NewTicker(c.config.MasterHeartbeatInterval)

	// Convert a slice of partitions to a map with partition ID as keys.
	toMap := func(s []core.PartitionID) map[core.PartitionID]bool {
		m := make(map[core.PartitionID]bool)
		for _, p := range s {
			m[p] = true
		}
		return m
	}

	for {
		c.blockIfNotLeader()

		if reply, err := c.mc.CuratorHeartbeat(curatorID); err != core.NoError {
			log.Errorf("[curator] error sending heartbeat, will retry, err=%s", err)
		} else {
			log.V(2).Infof("[curator] sent heartbeat to master successfully")

			replyView := toMap(reply.Partitions)
			c.lock.Lock()
			cachedView := toMap(c.partitions)
			c.lock.Unlock()

			// Sanity check:
			// A partition must be allocated on master side first then will be added to
			// curator side. So every partition owned by the curator should always be in
			// master's state.
			for k := range cachedView {
				if !replyView[k] {
					log.Fatalf("bug: Parition %v is owned by the curator but missing on master", k)
				}
			}

			if len(replyView) != len(cachedView) {
				// The allocated partitions on the master side doesn't match the allocated
				// partitions on the curator's side. This might happen because previous
				// partition assignment could be lost due to various failures (reply lost
				// over the wire, node crashes before receiving/committing the assignment,
				// etc.) and we don't want to lose any assigned partitions.
				log.Infof("Curator's partition is inconsistent with master's, syncing the master's partitions.")
				if err = c.stateHandler.SyncPartitions(reply.Partitions, c.stateHandler.GetTerm()); core.NoError != err {
					log.Errorf("[curator] failed to sync partitions: %s", err)
					time.Sleep(retryInterval)
					continue
				}
			}

			// Update the cached partitions.
			c.lock.Lock()
			c.partitions = reply.Partitions
			c.lock.Unlock()
		}

		<-masterTicker.C
	}
}

// Initializes curator's state. First it will see if it's registered with master,
// if not, it will try to register with master and commit the registered ID to
// Raft. It will also try to request a new partition ID from master if this is
// the first time it starts up.
func (c *Curator) initialize() (curatorID core.CuratorID, partitions []core.PartitionID) {
	var id core.CuratorID
	for {
		// PL-1110
		c.blockIfNotLeader()
		log.Infof("[curator] determining registration status...")

		var partitions []core.PartitionID
		var err core.Error
		id, partitions, err = c.stateHandler.GetCuratorInfo()
		if core.NoError != err {
			log.Errorf("[curator] error determining registration status, retrying shortly, err=%s", err)
			time.Sleep(retryInterval)
		} else if id.IsValid() && len(partitions) != 0 {
			// Already registered and got some partitions, nothing to do.
			log.Infof("[curator] already registered with ID %d", id)
			return id, partitions
		} else {
			// Either the curator has not registered or it's registered but has no partitions.
			// We'll handle it below.
			log.Infof("[curator] couldn't find registration status, will try to register one.")
			break
		}
	}

	// If we are here, it means either the curator has not registered yet or it's
	// registered but has no partitions.

	if !id.IsValid() {
		// Not registered yet.
		var regReply core.RegisterCuratorReply
		for {
			// PL-1110
			c.blockIfNotLeader()
			// Now we try to register.  First, get an ID from the master.
			log.Infof("[curator] getting registration information from master...")

			var err core.Error
			if regReply, err = c.mc.RegisterCurator(); core.NoError != err {
				log.Errorf("[curator] error registering with master, will sleep and retry, err=%s", err)
				time.Sleep(retryInterval)
			} else {
				log.Infof("[curator] registered with master")
				break
			}
		}

		// Next, try to commit it to the replicated state.

		// The registration process in replicated state returns the actual curator ID.
		// We could lose leadership and another node could register, and then we could regain leadership.
		// The only real serialization point of state is in the application of commands to the state machine,
		// so we use the result of that command to determine our actual curator ID.
		for {
			// PL-1110
			c.blockIfNotLeader()
			log.Infof("[curator] trying to commit registered ID %d...", regReply.CuratorID)

			// Commit the ID to our state.
			// This can fail because we're not leader, in which case we'll break out of this loop.
			// This can fail because there's some RPC transit error, in which case we'll retry.
			// This can succeed but the ID could be different.  We read the ID again before using it in heartbeats.
			var err core.Error
			if id, err = c.stateHandler.Register(regReply.CuratorID); core.NoError != err {
				log.Errorf("[curator] error committing registration information, err=%s", err)
				time.Sleep(retryInterval)
			} else {
				log.Infof("[curator] registration commit succeeded, durable curator ID is %d", id)
				break
			}
		}
	}

	// Next get a partition ID.  We just registered so we don't have one.
	var partReply core.NewPartitionReply

	for {
		// PL-1110
		c.blockIfNotLeader()
		log.Infof("[curator] getting new partition from master")

		var err core.Error
		if partReply, err = c.mc.NewPartition(id); err != core.NoError {
			log.Errorf("[curator] error getting new partition from master: %s", err)
			time.Sleep(retryInterval)
		} else {
			log.Infof("[curator] new partition: %d", partReply.PartitionID)
			break
		}
	}

	// And commit it to replicated state.
	for {
		// PL-1110
		c.blockIfNotLeader()
		log.Infof("[curator] trying to commit partition ID %d", partReply.PartitionID)

		if err := c.stateHandler.AddPartition(partReply.PartitionID, c.stateHandler.GetTerm()); core.NoError != err {
			log.Errorf("[curator] error committing initial partition, err=%s", err)
			time.Sleep(retryInterval)
		} else {
			log.Infof("[curator] registered and initial partition acquired, ready to create BlobIDs.")
			break
		}
	}

	return id, []core.PartitionID{partReply.PartitionID}
}

// Occasionally asks the durable state layer to compact itself, removing deleted blobs
// from metadata storage.
func (c *Curator) gcMetadataLoop() {
	gcTicker := time.NewTicker(c.config.MetadataGCInterval)

	for {
		c.blockIfNotLeader()
		<-gcTicker.C

		cutoff := time.Now().Add(-c.config.MetadataUndeleteTime).UnixNano()

		var toDelete []core.BlobID
		c.stateHandler.ForEachBlob(true, func(id core.BlobID, blob *fb.BlobF) {
			del := blob.Deleted()
			exp := blob.Expires()
			if (del != 0 && del < cutoff) || (exp != 0 && exp < cutoff) {
				// The blob can be GC-ed.
				toDelete = append(toDelete, id)

				// Make sure our raft command doesn't get too big.
				if len(toDelete) >= maxDeleteSize {
					go c.stateHandler.FinishDelete(toDelete)
					toDelete = nil
				}
			}
		}, func() bool {
			time.Sleep(time.Second)
			return c.stateHandler.IsLeader()
		})
		if len(toDelete) > 0 {
			go c.stateHandler.FinishDelete(toDelete)
		}
	}
}

// Loops forever, monitoring the fullness of the partitions, and begging for a
// new one if we're running them out.
func (c *Curator) partitionMonitorLoop(curatorID core.CuratorID) {
	mem := sigar.Mem{}
	partitionMonTicker := time.NewTicker(c.config.PartitionMonInterval)

	for {
		c.blockIfNotLeader()
		<-partitionMonTicker.C

		free, err := c.stateHandler.GetFreeSpace()
		if core.NoError != err {
			log.Errorf("[curator] failed to get free space: %s", err)
			continue
		}

		// Only ask for a new partition when we're running out of space.
		if free >= c.config.MinFreeBlobSlot {
			continue
		}

		// Enough free memory to host a new partition?
		if err := mem.Get(); nil != err {
			log.Errorf("[curator] failed to get memory info: %s", err)
			continue
		}
		if mem.ActualFree < c.config.FreeMemLimit {
			log.Errorf("[curator] out of memory for hosting new partition")
			// TODO: Inform the master that we cannot handle new
			// blobs any more. See PL-1101.
			continue
		}

		// Ask for a new partition from the master.
		reply, err := c.mc.NewPartition(curatorID)
		if core.NoError != err {
			log.Errorf("[curator] error getting new partition from master: %s", err)
			continue
		}
		log.Infof("[curator] got new partition: %d", reply.PartitionID)

		// Commit it to the durable state.
		log.Infof("[curator] trying to commit partition ID %d", reply.PartitionID)
		if err := c.stateHandler.AddPartition(reply.PartitionID, c.stateHandler.GetTerm()); core.NoError != err {
			log.Errorf("[curator] error committing new partition: %s", err)
			continue
		}
		log.Infof("[curator] committed new partition: %d", reply.PartitionID)

		// Update the volatile state.
		c.lock.Lock()
		c.partitions = append(c.partitions, reply.PartitionID)
		c.lock.Unlock()
	}
}

// gcTractserverContents checks the contents of tractservers to see if they can GC
// any tracts.  We do this in a dedicated goroutine to allow very easy throttling of
// this reasonably low-priority task.
func (c *Curator) gcTractserverContents() {
	for {
		// Only the leader should do this.
		c.blockIfNotLeader()

		// Pull out a tract set from a TS.
		msg := <-c.tsGcChan

		// Look at each tract and see if this TSID is supposed to be hosting it.
		old, gone := c.stateHandler.CheckForGarbage(msg.id, msg.has)
		// Filter out RS chunk pieces that we are currently working on.
		gone = c.filterPendingPieces(gone)

		if len(old) > 0 || len(gone) > 0 {
			log.Infof("GC: examined %d tracts from ts %d (@%s), %d old and %d gone", len(msg.has), msg.id, msg.addr, len(old), len(gone))
			c.tt.GCTract(msg.addr, msg.id, old, gone)
		}
	}
}

// checksumLoop periodically asks the handler to perform a consistency check
// across the raft group. Note that the current position in the state
// (pos) persists within this process, across leadership terms. That's
// deliberate, so that if leadership changes frequently, we are more likely to
// cover a wider range of the whole curator state.
func (c *Curator) checksumLoop() {
	var pos state.ChecksumPosition
	var err core.Error
	ticker := time.NewTicker(c.config.ConsistencyCheckInterval).C
	for {
		c.blockIfNotLeader()
		<-ticker

		pos, err = c.stateHandler.ConsistencyCheck(pos, c.config.ConsistencyCheckBatchSize)
		if err != core.NoError {
			log.Errorf("[curator] error performing consistency check: %s", err)
		}
	}
}
