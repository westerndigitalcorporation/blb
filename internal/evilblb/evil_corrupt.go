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
	registerEvil(CorruptTracts{})
}

// CorruptTracts is the evil that corrupts some random tracts on a
// randomly picked tract server. The number of tracts to be corrupted
// will be in range [NumMin, NumMax].
type CorruptTracts struct {
	// NumMin is the beginning of the range.
	NumMin int
	// NumMax is the ending of the range.
	NumMax int
}

// Duration implements EvilTask.
func (c CorruptTracts) Duration() time.Duration {
	return time.Duration(0)
}

// Validate implements EvilTask.
func (c CorruptTracts) Validate() error {
	if c.NumMin > c.NumMax {
		return fmt.Errorf("NumMin must <= NumMax")
	}
	if c.NumMin < 0 {
		return fmt.Errorf("NumMin must >= 0")
	}
	return nil
}

// Apply implements EvilTask.
// Corrupts tracts on a randomly picked disk of a randomly picked tract server.
func (c CorruptTracts) Apply(topo *topology.Topology, failer *failimpl.Failer) func() error {
	numTs := len(topo.Tractservers)
	if numTs == 0 {
		return nil
	}

	// Pick a random tract server.
	ts := topo.Tractservers[rand.Intn(numTs)]
	if len(ts.Roots) == 0 {
		return nil
	}

	// Pick a random disk.
	root := ts.Roots[rand.Intn(len(ts.Roots))]

	// Pick the number of tracts to corrupt.
	numTracts := c.NumMin + rand.Intn(c.NumMax-c.NumMin)

	log.Infof("Corrupting %d tracts on disk %s of tract server %s", numTracts, root, ts.Addr)

	host, _ := splitHostPort(ts.Addr)
	if err := failer.CorruptRandomFiles(host, root, "[0-9,a-f]*[0-9,a-f]", numTracts); err != nil {
		log.Fatalf("%v", err)
	}

	// No revert action, let Blb deal with it.
	return nil
}
