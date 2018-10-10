// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package testblb

import (
	"math/rand"
	"sync"
	"time"

	log "github.com/golang/glog"

	"github.com/westerndigitalcorporation/blb/internal/cluster"
)

// TestWriteReadWithFailureLong is similar to WriteAndRead, with the distinction
// that some master/curator/tractserver instances are killed and restarted
// randomly from time to time.
func (tc *TestCase) TestWriteReadWithFailureLong() error {
	// Start killers.
	wg := &sync.WaitGroup{}
	wg.Add(3)

	var procs []cluster.Proc
	for _, m := range tc.bc.Masters() {
		procs = append(procs, m)
	}
	go (&singleKiller{tc, procs, wg}).Start()

	var curatorProcs [][]cluster.Proc
	for _, group := range tc.bc.Curators() {
		procs = nil
		for _, curator := range group {
			procs = append(procs, curator)
		}
		curatorProcs = append(curatorProcs, procs)
	}
	go (&groupKiller{tc, curatorProcs, wg}).Start()

	procs = nil
	for _, ts := range tc.bc.Tractservers() {
		procs = append(procs, ts)
	}
	go (&singleKiller{tc, procs, wg}).Start()

	// Do write and read loop as usual.
	return tc.TestWriteReadLong()
}

// Killers that simulate server crashes.

// killer provides mechanism to kill and resurrect processes.
type killer interface {
	Start()
}

// singleKiller accepts a group of processes and kills one at a time.
type singleKiller struct {
	t     *TestCase
	procs []cluster.Proc
	wg    *sync.WaitGroup
}

// Start implements killer.
func (s *singleKiller) Start() {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	time.Sleep(time.Duration(r.Intn(8)) * time.Second)

	var killed cluster.Proc
	ticker := time.NewTicker(s.t.testCfg.FailureInterval)
	defer ticker.Stop()
	defer s.wg.Done()

	for {

		<-ticker.C

		// Revive previously killed process.
		if nil != killed {
			killed.Start()
			log.Infof("process %s restarted", killed)
		}

		// Kill a random process.
		killed = s.procs[r.Intn(len(s.procs))]
		killed.Stop()
		log.Infof("process %s killed", killed.Name())
	}
}

// groupKiller accepts a list of groups of processes and kills one from each
// group at a time.
type groupKiller struct {
	t      *TestCase
	groups [][]cluster.Proc
	wg     *sync.WaitGroup
}

// Start implements killer.
func (g *groupKiller) Start() {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	time.Sleep(time.Duration(r.Intn(8)) * time.Second)

	var killed []cluster.Proc
	ticker := time.NewTicker(g.t.testCfg.FailureInterval)
	defer ticker.Stop()
	defer g.wg.Done()

	for {

		<-ticker.C

		// Revive previously killed curators.
		for _, c := range killed {
			c.Start()
			log.Infof("process %s restarted", c.Name())
		}

		// Kill a random process from each repl group.
		killed = make([]cluster.Proc, 0)
		for _, group := range g.groups {
			c := group[r.Intn(len(group))]
			killed = append(killed, c)
			c.Stop()
			log.Infof("process %s killed", c.Name())
		}
	}
}
