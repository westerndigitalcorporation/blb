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
	registerEvil(KillCuratorLeader{})
}

// KillCuratorLeader kills the leader of every curator repl group.
type KillCuratorLeader struct {
}

// Duration implements EvilTask.
func (c KillCuratorLeader) Duration() time.Duration {
	return time.Duration(0)
}

// Validate implements EvilTask.
func (c KillCuratorLeader) Validate() error {
	return nil
}

// Apply implements EvilTask.
func (c KillCuratorLeader) Apply(topo *topology.Topology, failer *failimpl.Failer) func() error {
	for _, c := range topo.CuratorGroups {
		host, _ := splitHostPort(c.Addr)
		log.Infof("Trying to curator leader on %s", host)
		if err := failer.KillTask(host, "curator"); err != nil {
			log.Errorf("error killing curator at %s: %s", host, err)
		}
	}
	return nil
}
