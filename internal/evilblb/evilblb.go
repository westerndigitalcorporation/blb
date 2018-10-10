// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package evilblb

import (
	"math/rand"
	"time"

	log "github.com/golang/glog"
	"github.com/westerndigitalcorporation/blb/internal/evilblb/failimpl"
	"github.com/westerndigitalcorporation/blb/internal/evilblb/topology"
)

// A revert action that can revert a triggered evil.
type revertFun func() error

// EvilBlb can inject predefined evils to a Blb cluster according to a
// given configuration.
type EvilBlb struct {
	config     Config
	discoverer *topology.Discoverer
	failer     *failimpl.Failer
	closeC     chan struct{}
}

// New creates a new EvilBlb instance.
func New(config Config) *EvilBlb {
	if err := config.Validate(); err != nil {
		log.Fatalf("Failed to validate configuration: %v", err)
	}
	return &EvilBlb{
		config:     config,
		discoverer: topology.NewDiscoverer(config.Params.Masters),
		failer:     failimpl.NewFailer(config.Params.User),
		closeC:     make(chan struct{}),
	}
}

// Run starts evil injections.
func (e *EvilBlb) Run() {
	evils := e.config.Evils
	params := e.config.Params
	deadline := time.Now().Add(params.TotalLength.Duration)

	log.Infof("Start running EvilBlb, total %d evils detected, will stop running at %s",
		len(evils), deadline)

	for time.Now().Before(deadline) {
		evil := evils[rand.Intn(len(evils))]
		reverter := evil.Apply(e.discoverer.GetTopology(), e.failer)

		if reverter != nil {
			log.Infof("Will run the revert action for this evil after %s", evil.Duration())
		}

		select {
		case <-time.After(evil.Duration()):
		case <-e.closeC:
			// Start the revert action immediately if EvilBlb is asked to shut down.
		}

		if reverter != nil {
			// Revert the evil if a revert action is provided.
			log.Infof("Running revert action to bring cluster back to the sane state...")
			reverter()
		}

		diff := params.MaxGapBetweenEvils.Duration - params.MinGapBetweenEvils.Duration
		gapDuration := params.MinGapBetweenEvils.Duration + time.Duration(rand.Int63n(int64(diff)))
		log.Infof("Will wait for %s for next round of evil injection...", gapDuration)

		select {
		case <-time.After(gapDuration):
		case <-e.closeC:
			return
		}
	}
}

// Shutdown shuts down the instance. A pending evil will be guaranteed to
// be reverted before shutting down.
func (e *EvilBlb) Shutdown() {
	close(e.closeC)
}
