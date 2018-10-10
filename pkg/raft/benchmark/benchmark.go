// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"encoding/binary"
	"io"
	"io/ioutil"
	"time"

	"github.com/beorn7/perks/quantile"

	log "github.com/golang/glog"

	"github.com/westerndigitalcorporation/blb/pkg/raft/raft"
)

// If there's no proposal gets applied after 'waitTimeout', followers will assume
// benchmark is finished.
const waitTimeout = 10 * time.Second

type benchmark struct {
	r              *raft.Raft
	leaderCh       chan struct{}
	appliedCh      chan struct{}
	appliedCount   uint
	histogram      *quantile.Stream
	totalTxns      uint
	progress       int   // The amount of proposals we have already applied. In the unit of 10%.
	lastProgressTm int64 // The timestamp of last 10% proposals were applied.
}

func newBenchmark(r *raft.Raft, numTxns uint, members []string) *benchmark {
	objectives := map[float64]float64{0.1: 0.05, 0.5: 0.05, 0.9: 0.01, 0.99: 0.001, 0.9999: 0.000001}
	b := &benchmark{
		r:         r,
		leaderCh:  make(chan struct{}, 1),
		appliedCh: make(chan struct{}, 1000),
		histogram: quantile.NewTargeted(objectives),
		totalTxns: numTxns,
	}
	r.Start(b)
	r.ProposeInitialMembership(members)
	return b
}

func (b *benchmark) Apply(ent raft.Entry) interface{} {
	if b.lastProgressTm == 0 {
		// It's the first proposal gets applied.
		b.lastProgressTm = time.Now().UnixNano()
	}

	b.appliedCount++
	progress := int((float32(b.appliedCount) / float32(b.totalTxns)) * 10)
	if progress != b.progress {
		b.progress = progress
		now := time.Now().UnixNano()
		durationMs := (now - b.lastProgressTm) / int64(time.Millisecond/time.Nanosecond)
		b.lastProgressTm = now
		log.Infof("%d proposals has been applied, progress at %d%%, takes %dms", b.appliedCount, progress*10, durationMs)
	}

	cmd := deserialize(ent.Cmd)
	b.histogram.Insert(float64(time.Now().UnixNano()-cmd.timestampNs) / 1e9)

	select {
	case b.appliedCh <- struct{}{}:
		// Notify 'appliedCh' if a proposal gets applied.
	default:
	}
	return nil
}

func (b *benchmark) OnLeadershipChange(isLeader bool, term uint64, leader string) {
	if isLeader {
		select {
		case b.leaderCh <- struct{}{}:
		default:
		}
	}
	log.Infof("Onleadership change: %v %v", isLeader, term)
}

func (b *benchmark) OnMembershipChange(membership raft.Membership) {}

func (b *benchmark) Snapshot() (raft.Snapshoter, error) {
	log.Fatalf("Snapshot is not implemented yet.")
	return nil, nil
}

func (b *benchmark) SnapshotRestore(reader io.Reader, index uint64, term uint64) {
	log.Fatalf("SnapshotRestore is not implemented yet.")
	return
}

func (b *benchmark) start(cmdSize, numTxns uint) {
	var isLeader bool

	// Wait until a leader is elected.
	select {
	case <-b.leaderCh:
		isLeader = true
	case <-b.appliedCh:
		// If a proposal gets applied it means a leader must be elected, so this node
		// must be a follower.
	}

	if isLeader {
		log.Infof("It's the leader, start benchmarking...")
		st := time.Now()
		buffer := make([]byte, cmdSize)
		var pending *raft.Pending

		for i := 0; i < int(numTxns); i++ {
			// TODO: we get timestamp of the proposal outside of `Propose` call. Our
			// backpressure strategy will block the proposal until the size of pending
			// proposals is within a certain threshold.
			//
			// So the latency we calculated here is:
			//
			//   "block time before gets enqueued to Raft" + "time gets processed and committed by Raft"
			//
			// This might not be what we want because our benchmark will saturate Raft's
			// pending proposals queue so every proposal will be blocked for a while
			// before it can gets enqueued.
			//
			// What we are interested in is propbably:
			//
			//   "time gets processed and committed by Raft"
			//
			pending = b.r.Propose(newFakeCmd(buffer).serialize())
		}

		// Wait the last proposal to be committed.
		<-pending.Done

		duration := time.Since(st)
		throughput := uint(float32(numTxns) / float32(float32(duration)/float32(time.Second)))

		log.Infof("Benchmark is done, it takes %v", duration)
		log.Infof("Throughput is %d txns per second", throughput)

		log.Infof("Latency distribution(ms):")
		for _, quantile := range []float64{0.1, 0.5, 0.9, 0.99, 0.9999} {
			log.Infof("%gth=%.3fms", quantile*100, b.histogram.Query(quantile)*1000)
		}
	} else {
		log.Infof("It's the follower, wait until the benchmark is done.")
	followerLoop:
		for {
			select {
			case <-b.appliedCh:
			case <-time.After(waitTimeout):
				// If no proposal gets applied after certain duration we assume the benchmarking is done.
				log.Infof("No proposal gets applied after %v, assume benchmark is done", waitTimeout)
				break followerLoop
			}
		}
	}
}

type fakeCmd struct {
	data        []byte
	timestampNs int64
}

func newFakeCmd(data []byte) fakeCmd {
	return fakeCmd{data: data, timestampNs: time.Now().UnixNano()}
}

func (c fakeCmd) serialize() []byte {
	var buffer bytes.Buffer
	binary.Write(&buffer, binary.LittleEndian, c.timestampNs)
	buffer.Write(c.data)
	return buffer.Bytes()
}

func deserialize(b []byte) fakeCmd {
	buffer := bytes.NewBuffer(b)
	var timestampNs int64
	binary.Read(buffer, binary.LittleEndian, &timestampNs)
	data, _ := ioutil.ReadAll(buffer)
	return fakeCmd{data: data, timestampNs: timestampNs}
}
