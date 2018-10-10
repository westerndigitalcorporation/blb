// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package evilblb

import (
	"time"

	log "github.com/golang/glog"
	"github.com/westerndigitalcorporation/blb/internal/evilblb/failimpl"
	"github.com/westerndigitalcorporation/blb/internal/evilblb/topology"
)

func init() {
	registerEvil(KillMasterLeader{})
}

// KillMasterLeader unsurprisingly kills the leader of the master's repl group.
type KillMasterLeader struct {
}

// Duration implements EvilTask.
func (c KillMasterLeader) Duration() time.Duration {
	return time.Duration(0)
}

// Validate implements EvilTask.
func (c KillMasterLeader) Validate() error {
	return nil
}

// Apply implements EvilTask.
func (c KillMasterLeader) Apply(topo *topology.Topology, failer *failimpl.Failer) func() error {
	host, _ := splitHostPort(topo.Masters.Leader)
	log.Infof("Trying to kill master on %s", host)
	if err := failer.KillTask(host, "master"); err != nil {
		log.Errorf("error killing master at %s: %s", host, err)
	}
	return nil
}
