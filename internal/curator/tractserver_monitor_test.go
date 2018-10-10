// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package curator

import (
	"testing"
	"time"

	"github.com/westerndigitalcorporation/blb/internal/core"
)

// Test that if a TS is healthy, we won't use it to place new tracts if it's full.
//
// XXX: This test is pretty ugly, refactor tsmon & check this in a better way.
func TestDontPlaceDataOnFullHealthy(t *testing.T) {
	tsmon := newTractserverMonitor(&DefaultTestConfig, nilFailureDomainService{}, time.Now)

	// This one has a lot of available space.
	tsmon.recvHeartbeat(core.TractserverID(1), "ts1", core.TractserverLoad{AvailSpace: 1024 * 1024 * 1024 * 1024})

	// This one has very little.
	tsmon.recvHeartbeat(core.TractserverID(2), "ts2", core.TractserverLoad{AvailSpace: 1024})

	domains := tsmon.getFailureDomainToFreeTS()
	if len(domains) != 1 || len(domains[0]) != 1 {
		t.Fatalf("should only have one host in the available to serve hosts: %+v", domains)
	}

	// This one also has a lot of available space.
	tsmon.recvHeartbeat(core.TractserverID(3), "ts3", core.TractserverLoad{AvailSpace: 1024 * 1024 * 1024 * 1024})

	domains = tsmon.getFailureDomainToFreeTS()
	if len(domains) != 1 || len(domains[0]) != 2 {
		t.Fatalf("should have two hosts available: %+v", tsmon.getFailureDomainToFreeTS())
	}
}
