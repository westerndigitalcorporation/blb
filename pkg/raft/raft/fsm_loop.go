// Copyright (c) 2016 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package raft

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	log "github.com/golang/glog"
)

// Update from the main loop about leadership changes.
type leadershipUpdate struct {
	isLeader bool   // If so, won or lose leadership?
	term     uint64 // New term.
	leader   string
}

type commitTuple struct {
	entry   Entry
	pending concluder
}

// Update from the main loop about newly committed entries.
type commitsUpdate struct {
	// What are committed entries?
	commits []commitTuple
}

// Request for asking the fsm loop to restore from current snapshot file.
type restoreUpdate struct {
	reader  SnapshotFileReader
	pending *Pending
}

// fsmLoop is the only goroutine that can talk to FSM. It constantly receives
// updates from the main loop('core') and calls the callbacks of the FSM to
// notify it about leadership changes, committed commands, restoring/taking
// snapshot, etc.
type fsmLoop struct {
	fsm               FSM
	fsmCh             chan interface{} // The channel used to receive updates from main loop.
	lastAppliedIndex  uint64
	lastAppliedTerm   uint64
	snapshotThreshold uint64
	lastSnapIndex     uint64
	snapReqCh         chan *snapshotReq

	lastAppliedMembership *Membership

	// The metrics are passed from Raft. See comments in raft.go.
	metricAppliedIndex        prometheus.Gauge
	metricSnapRestore         prometheus.Counter
	metricSnapRestoreDuration prometheus.Gauge
}

// run starts fsm loop in a blocking way.
func (f *fsmLoop) run() {
	for {
		update := <-f.fsmCh
		switch u := update.(type) {
		case leadershipUpdate:
			f.fsm.OnLeadershipChange(u.isLeader, u.term, u.leader)

		case commitsUpdate:
			f.handleCommits(u)

		case restoreUpdate:
			// Wrap the actual snapshot reader within a metricSnapshotFileReader so
			// we can update the metrics we are interested in when restoring from the
			// snapshot is done.
			snapReader := &metricSnapshotFileReader{
				SnapshotFileReader:        u.reader,
				start:                     time.Now(),
				metricSnapRestoreDuration: f.metricSnapRestoreDuration,
			}
			if snapReader.SnapshotFileReader == nil {
				log.Fatalf("Can't find snapshot file when we were told to restore state from it")
			}
			f.restoreFromSnapshot(snapReader)
			snapReader.Close()
			u.pending.conclude(nil, nil)

		default:
			log.Fatalf("unknown type of update: %v+", u)
		}
	}
}

func (f *fsmLoop) handleCommits(update commitsUpdate) {
	// Handle the newly committed entries, if there're any.
	for _, pendingEntry := range update.commits {
		entry := pendingEntry.entry
		var res interface{}
		switch entry.Type {
		case EntryNormal:
			// The committed entry was proposed by clients, so apply it to FSM.
			res = f.fsm.Apply(entry)

		case EntryNOP:
			// The committed entry was proposed by Raft itself to commit entries of
			// previous terms when a new leader is elected. Nothing needs to be done
			// for NOP command.

		case EntryConf:
			f.applyMembership(decodeConfEntry(entry))

		default:
			log.Fatalf("Unknown type of command")
		}

		// Keep track of the last applied index and term.
		f.lastAppliedIndex = entry.Index
		f.lastAppliedTerm = entry.Term

		// Conclude this entry if someone is waiting this entry to be applied.
		if pendingEntry.pending != nil {
			pendingEntry.pending.conclude(res, nil)
		}
	}
	// Update the index of the last applied entry.
	f.metricAppliedIndex.Set(float64(f.lastAppliedIndex))

	// See if we need to perform snapshot or not, we take snapshot only if(it's always
	// OK and correct to not take snapshot):
	//
	//	1. Users have told us to take snapshot periodically.
	//	2. The threshold has reached.
	//  3. There's no pending snapshot request to snapshotLoop.
	//
	if f.snapshotThreshold > 0 && // check for 1.
		f.lastAppliedIndex-f.lastSnapIndex >= f.snapshotThreshold && // check for 2.
		len(f.snapReqCh) == 0 { // check for 3.
		log.Infof("%d new entries have been applied to state, taking a snapshot.",
			f.lastAppliedIndex-f.lastSnapIndex)

		// First create a temporary file for snapshotting.
		log.Infof("Snapshotting with last index %d and term %d",
			f.lastAppliedIndex, f.lastAppliedTerm)

		snapshoter, err := f.fsm.Snapshot()
		if err != nil {
			log.Errorf("Skip snapshoting due to returned error: %v", err)
			return
		}

		f.snapReqCh <- &snapshotReq{
			snapshoter: snapshoter,
			meta: SnapshotMetadata{
				LastIndex: f.lastAppliedIndex,
				LastTerm:  f.lastAppliedTerm,
				// We have to persist the last applied membership in snapshot as well so
				// if the membership information gets compacted from the log we can find
				// them from snapshot.
				Membership: f.lastAppliedMembership,
			},
		}

		// It's not guaranteed the snapshot will be taken sucessfully for various
		// reasons. (e.g. failed to write to disk, the snapshotLoop decideds to
		// ignore the request because it's too busy, snapshot is not up-to-date at
		// the time of trying to commit it, etc). But we update the "lastSnapIndex"
		// anyway, it's fine to do so because if the snapshot failes to be commited
		// then we just skip a snapshot, which will not affect the correctness.
		f.lastSnapIndex = f.lastAppliedIndex
	}
}

func (f *fsmLoop) restoreFromSnapshot(reader SnapshotFileReader) {
	meta := reader.GetMetadata()

	// Apply the membership from the snapshot.
	f.applyMembership(meta.Membership)
	log.Infof("membership from snapshot: %+v", meta.Membership)

	log.Infof("Restoring from snapshot, the metadata of snapshot is %+v", meta)
	f.fsm.SnapshotRestore(reader, meta.LastIndex, meta.LastTerm)
	f.lastAppliedIndex = meta.LastIndex
	f.lastAppliedTerm = meta.LastTerm
	f.lastSnapIndex = f.lastAppliedIndex
	// Update the metric as well when restore from a snapshot.
	f.metricAppliedIndex.Set(float64(f.lastAppliedIndex))
	f.metricSnapRestore.Inc()
}

func (f *fsmLoop) applyMembership(membership *Membership) {
	f.lastAppliedMembership = membership
	f.fsm.OnMembershipChange(*membership)
}

// Wraps an existing SnapshotFileReader implementation and updates metrics we
// want when close the file.
type metricSnapshotFileReader struct {
	SnapshotFileReader
	start                     time.Time // When did it start?
	metricSnapRestoreDuration prometheus.Gauge
}

func (r *metricSnapshotFileReader) Close() error {
	err := r.SnapshotFileReader.Close()
	if err == nil {
		r.metricSnapRestoreDuration.Set(float64(time.Since(r.start)) / 1e9)
	}
	return err
}
