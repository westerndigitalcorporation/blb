// Copyright (c) 2016 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package raft

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	log "github.com/golang/glog"
)

type snapshotReq struct {
	snapshoter Snapshoter
	meta       SnapshotMetadata // The index and term of last applied command to state.
}

// snapshotLoop is the goroutine that is responsible for writing application's
// state to a snapshot file. It works with fsmLoop and coreLoop to take a
// snapshot. The process of taking a snapshot is as the follows:
//
// 1. fsmLoop decides when to take a snapshot and asks applications to prepare
//    for taking a snapshot by calling "FSM.Snapshot" to get a "Snapshoter".
//    Because the fsmLoop is also responsible for applying commands so it knows
//    the index and term of the last applied command at the time "Snapshoter" is
//    returned. The fsmLoop will passe these informations to the snapshotLoop.
//
// 2. snapshotLoop is responsible for calling "Snapshoter.Save" to dump state of
//    the application to a temporary file. To avoid the race, snapshotLoop will
//    ask coreLoop to commit the snapshot file.
//
// 3. Once coreLoop receives the snapshot commit request from snapshotLoop, it
//    will commit the temporary snapshot file to make it visible and trim the
//    log if necessary.
//
type snapshotLoop struct {
	snapReqCh      chan *snapshotReq
	snapshotDoneCh chan SnapshotFileWriter
	snapMgr        SnapshotManager

	// The metrics are passed from Raft. See comments in raft.go.
	metricSnapSave         prometheus.Counter
	metricSnapSaveDuration prometheus.Gauge
	metricSnapshotSize     prometheus.Gauge
}

func (s *snapshotLoop) run() {
	for {
		// See if fsmLoop decides to take a snapshot?
		req := <-s.snapReqCh
		s.processSnapReq(req)
	}
}

func (s *snapshotLoop) processSnapReq(req *snapshotReq) {

	// We have to release the snapshot after we are done(either succeed or fail) with it.
	defer req.snapshoter.Release()

	if len(s.snapshotDoneCh) != 0 {
		// It's always correct to do not take snapshot, so if there's an uncommitted
		// snapshot request accumulated in the channel we'll just skip the new one.
		log.Infof("There's an uncommitted snapshot here, skip...")
		return
	}

	log.Infof("Snapshotting with last index %d and term %d",
		req.meta.LastIndex, req.meta.LastTerm)

	// First create a temporary file for snapshotting.
	w, err := s.snapMgr.BeginSnapshot(req.meta)
	if err != nil {
		// Failed to create the snapshot file.
		log.Errorf("Failed to create a snapshot file: %v", err)
		return
	}

	w = &metricSnapshotFileWriter{
		SnapshotFileWriter:     w,
		start:                  time.Now(),
		metricSnapshotSize:     s.metricSnapshotSize,
		metricSnapSaveDuration: s.metricSnapSaveDuration,
	}

	// If we are here, we have created a temporary snapshot file for dumping
	// application's state.

	// Ask application to dump its state that was captured in the snapshoter.
	if err = req.snapshoter.Save(w); err != nil {
		// There're some errors while we were taking snapshot, conclude this
		// request with the returned error.
		log.Errorf("Failed to write state to snapshot file: %v", err)
		// Discard the snapshot file if we can.
		if err := w.Abort(); err != nil {
			log.Errorf("Failed to discard the snapshot file: %v", err)
		}
		return
	}

	// If we are here, we have successfully written application's state to a
	// temporary file. Please NOTE that we can not make the snapshot 'visible'
	// here as this will overwrite previous snapshot file and the other thread
	// (core loop) might be accessing that file concurrently. To avoid the race,
	// we'll hand the temporary snapshot file over to the core loop and let it
	// make the snapshot 'visible'(commit the file) and trim the log.
	//
	// Also NOTE that we have explicitly checked that the channel has room for
	// this request so the enqueue operation will not block, thus no deadlock
	// can happen.
	s.snapshotDoneCh <- w
	s.metricSnapSave.Inc()
}

// Wraps an existing SnapshotFileWriter implementation and updates metrics we
// want when commit the snapshot file.
type metricSnapshotFileWriter struct {
	SnapshotFileWriter
	start                  time.Time // When did snapshot start?
	size                   int64     // How many bytes has been written?
	metricSnapshotSize     prometheus.Gauge
	metricSnapSaveDuration prometheus.Gauge
}

func (w *metricSnapshotFileWriter) Write(p []byte) (n int, err error) {
	w.size += int64(len(p))
	return w.SnapshotFileWriter.Write(p)
}

func (w *metricSnapshotFileWriter) Commit() error {
	err := w.SnapshotFileWriter.Commit()
	if err == nil {
		// Update the metrics only after committing the snapshot successfully.
		w.metricSnapshotSize.Set(float64(w.size))
		w.metricSnapSaveDuration.Set(float64(time.Since(w.start)) / 1e9)
	}
	return err
}
