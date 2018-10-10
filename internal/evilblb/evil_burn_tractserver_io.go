// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package evilblb

import (
	"fmt"
	"math/rand"
	"time"

	log "github.com/golang/glog"
	"github.com/westerndigitalcorporation/blb/internal/evilblb/failimpl"
	"github.com/westerndigitalcorporation/blb/internal/evilblb/topology"
)

func init() {
	registerEvil(BurnTractserverIO{})
}

// BurnTractserverIO burns IO of all disks of a tract server.
type BurnTractserverIO struct {
	// Length defines how long burning task will last.
	Length Duration
}

// Duration implements EvilTask.
func (b BurnTractserverIO) Duration() time.Duration {
	return b.Length.Duration
}

// Validate implements EvilTask.
func (b BurnTractserverIO) Validate() error {
	if b.Length.Duration <= 0 {
		return fmt.Errorf("Length of evil must > 0")
	}
	return nil
}

// Apply implements EvilTask.
//
// Starting IO intensive tasks on all hard disks of a randomly picked
// tract server.
func (b BurnTractserverIO) Apply(topo *topology.Topology, failer *failimpl.Failer) func() error {
	numTs := len(topo.Tractservers)
	if numTs == 0 {
		return nil
	}

	// Pick a random tract server.
	ts := topo.Tractservers[rand.Intn(numTs)]
	if len(ts.Roots) == 0 {
		return nil
	}

	host, _ := splitHostPort(ts.Addr)

	log.Infof("Starting burning disk IO on tract server %s", host)

	if err := failer.BurnIO(host); err != nil {
		log.Fatalf("%v", err)
	}

	return func() error {
		log.Infof("Stop burning IO on tract server %s", host)
		return failer.Heal(host)
	}
}
