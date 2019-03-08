// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package curator

import (
	log "github.com/golang/glog"

	"github.com/westerndigitalcorporation/blb/internal/core"
)

/*

Re-replication replaces a member of a tract replication group, copying data from an available member
to new host(s), and removing failed host(s).

Re-replication can be triggered by scenarios including the following:
* The curator is unable to reach a tractserver and must assume it's down.
* A client is unable to write to a tract and is blocked from making forward progress.
* Tract servers report to the curator that some tracts are mssing or corrupted.

Re-replication must change the replication group of a tract while keeping consistency guarantees:
If a single client has a successful write to a tract, that write is always reflected in all future reads
from any replica.

However, the client's view of what's failed may not match the curator's view of what's failed, and as such
must re-replicate carefully.  An example below:

* Client creates a blob with one tract and repl=3.
* There is a network partition.
* The client can see all 3 replicas.
* The curator can only see 2 replicas, and assumes there is a node failure.
* The curator re-replicates by copying data from one of the 2 replicas can see to a new replica.
* The client issues a write to all 3 replicas, and it succeeds.
* The curator replaces the replica it can't see with the new replica, which doesn't have the contents of the write.
  So, some replicas have the result of the successful write and some do not.
* Inconsistency! (See ** below)

We solve this problem by not allowing client writes to succeed during the re-replication process.

With a lease mechanism, the lease for a tract can be revoked or allowed to expire.  The curator knows that without any active
lease no client will be able to write to the tract.

With versioning, the curator can increase the version numbers of the tracts before starting re-replication.
A client writes to a (tract ID, version) pair, and if the version doesn't match, the write can't proceed.

We use versioning to cause writes to fail.

If the version change happens before the write, the write will be rejected immediately, as the versions don't match.

If the write happens before the version change, the write will succeed.  This is OK as re-replication hasn't happened yet,
and when it does happen, it'll re-replicate from a post-write version of the tract.

If the version change happens before the write on any node but not all, the write will fail, as it won't succeed on every
client.  The client's write won't succeed and the contents of the tract in the interval of the client's write is undefined,
which agrees with our consistency.

** Recall Blb's consistency guarantee:

Cross-tract writes are not coordinated in any way.  We only provide guarantees for writes in one tract.
Parallel single-tract writes are not coordinated in any way.  Clients must serialize all their own writes.
Any write, whether or not it succeeds, only affects the write's region.
If a write fails, reads of the affected region are undefined and may differ from server to server.
Parallel writes to the same region are undefined and may differ from server to server, even if all writes are successful.
If a write succeeds, all future reads of the affected region will return the contents from the write.

*/

// Returns true if 'haystack' contains 'needle', false otherwise.
func contains(haystack []core.TractserverID, needle core.TractserverID) bool {
	for _, elt := range haystack {
		if elt == needle {
			return true
		}
	}
	return false
}

// replicateTract is used to re-replicate data from the tract 'id' when the hosts 'badHosts' have failed.
// We assume a higher level checks to ensure that 'badHosts' have actually failed.
//
// Returns core.NoError if successful.
// Returns another core.Error otherwise.
func (c *Curator) replicateTract(id core.TractID, badIds []core.TractserverID) core.Error {
	// We use continuity check to guard against conflict operations among different
	// curators, but we also need to use lock to guard conflict operations among
	// different goroutines within same curator.
	c.lockMgr.LockTract(id)
	defer c.lockMgr.UnlockTract(id)

	// Get current term number and use it for the continuity check of ChangeTract operation.
	term := c.stateHandler.GetTerm()

	// Do some basic verification on the input.
	info, _, err := c.stateHandler.GetTracts(id.Blob, int(id.Index), 1+int(id.Index))
	if err != core.NoError {
		log.Errorf("%v couldn't GetTracts, err %s", id, err)
		return err
	}

	// If this tract is RS-encoded, we don't need to worry about maintaining the replicated copies
	// anymore. RS recovery will take care of failures. (We might get here if a tract is both
	// replicated and RS-encoded during the transition period.)
	if info[0].RS.Present() {
		log.Infof("%v is RS-encoded", id)
		return core.ErrReadOnlyStorageClass
	}

	// Figure out what TSIDs we're keeping in the repl set.  We'll bump the versions on these and tell new hosts to pull tracts from them.
	var okIds []core.TractserverID
	hosts := info[0].TSIDs
	for _, host := range hosts {
		if !contains(badIds, host) {
			okIds = append(okIds, host)
		}
	}

	// This should probably error more loudly.  The tract is lost or unavailable, hopefully the latter.
	if len(okIds) == 0 {
		log.Errorf("%v has no healthy hosts, current durable hosts are: %v", id, info[0].Hosts)
		return core.ErrAllocHost
	}

	// If there aren't really any bad hosts, don't re-replicate.  It would just bump the version number
	// and incur other overhead w/o actually doing anything.
	if len(okIds) == len(hosts) {
		log.Errorf("%v has reported bad host %v not part of repl set, ignoring request to rereplicate", id, badIds)
		return core.ErrInvalidArgument
	}

	// Bump the version on all hosts aside from the ones the caller thinks are bad.
	nextVersion := info[0].Version + 1

	// If we haven't heart a heartbeat from every okHost, we can't bump the versions, so bail out now.
	okHosts, missing := c.tsMon.getTractserverAddrs(okIds)
	if missing > 0 {
		log.Errorf("%v couldn't get addrs for healthy hosts %v", id, okHosts)
		return core.ErrHostNotExist
	}

	// Retrying is fine, so long as we check to see if we're leader and refresh the tract version,
	// which we'll do by calling reReplicateTract again.
	//
	// We buffer the channel so that we can return when we first hit an error, and the goroutines
	// we start that send over the channel won't block forever.  Using info[0].TSIDs means we
	// can send one result for each good tract and each bad tract over it without any goroutine
	// blocking on the send (though max(bad, good) would also suffice).
	errorChan := make(chan core.Error, len(hosts))
	for i := range okIds {
		go func(addr string, tsId core.TractserverID) {
			err := c.tt.SetVersion(addr, tsId, id, nextVersion, 0)
			if err != core.NoError {
				log.Errorf("%v couldn't SetVersion to %d on tsid %d at %s, err: %s", id, nextVersion, tsId, addr, err)
			}
			errorChan <- err
		}(okHosts[i], okIds[i])
	}
	for _ = range okIds {
		if res := <-errorChan; res != core.NoError {
			// The error will be logged in the goroutine launched above.
			return res
		}
	}

	newAddrs, newIds := c.allocateTS(len(badIds), okIds, badIds)
	if newAddrs == nil {
		log.Errorf("%v couldn't allocate TSs to replace bad hosts (%v)", id, badIds)
		return core.ErrAllocHost
	}

	// ...and have them pull the tract from an OK host.
	for i := range newAddrs {
		go func(dstAddr string, dstId core.TractserverID) {
			err := c.tt.PullTract(dstAddr, dstId, okHosts, id, nextVersion)
			if err != core.NoError {
				log.Errorf("%v pulltract to (%d at %s) from (%v) version %d failed: %s",
					id, dstId, dstAddr, okHosts, nextVersion, err)
			}
			errorChan <- err
		}(newAddrs[i], newIds[i])
	}
	for _ = range newAddrs {
		if res := <-errorChan; res != core.NoError {
			// The errors are logged in the goroutine started above.
			return res
		}
	}

	if err = c.stateHandler.ChangeTract(id, nextVersion, append(okIds, newIds...), term); err != core.NoError {
		log.Errorf("%v couldn't commit change to durable state: %s", id, err)
		return err
	}

	log.Infof("@@@ rerepl %v succeeded", id)
	return core.NoError
}
