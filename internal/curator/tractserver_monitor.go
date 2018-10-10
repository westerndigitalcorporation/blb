// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package curator

import (
	"fmt"
	"sort"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/westerndigitalcorporation/blb/internal/core"
)

const (
	// How often do we scan our state and refresh the total cluster state?
	statusRefreshInterval = 1 * time.Second

	// Have we received a heartbeat recently from the TS?
	statusHealthy = "healthy"

	// The TS shouldn't host new data but hasn't been gone long enough to recover all data on it.
	statusUnhealthy = "unhealthy"

	// Has it been long enough from the last heartbeat that we should assume the TS is dead?
	statusDown = "down"

	// The smallest amount of available space (in bytes) that a TS must have for the C to create
	// a new tract on it.
	minAvailSpace = 1024 * 1024 * 1024
)

// tractserverData is all the state we have for a possibly active tractserver.  It's used internally
// by the monitor and also by monitoring (HTML templating requires the fields be Exported).
type tractserverData struct {
	// What's the unique ID?
	ID core.TractserverID

	// The reported address of this tractserver when it last beat.
	Addr string

	// When did the tractserver last beat?  0 if never.
	LastBeat time.Time

	// Last received load report.
	Load core.TractserverLoad

	// Current status, may be stale.
	Status string
}

func (d *tractserverData) hasBeaten() bool {
	return !d.LastBeat.IsZero()
}

// tractserverMonitor stores and analyzes data about tractservers.
type tractserverMonitor struct {
	// Protect the data below.
	lock sync.Mutex

	// Every tractserver that has beaten to us or we expect to be hosting data.
	// A given address could beat with many IDs.  We only keep the latest ID.
	idToHost map[core.TractserverID]tractserverData

	// Map from an address to the last tractserver ID that heartbeated from that address.
	hostToID map[string]core.TractserverID

	// When did the monitor start?  We don't consider any host down unless it's had a chance
	// to contact the monitor.
	start time.Time

	// What's the status of the cluster?  This information relies on heartbeats and time and as
	// such is inherently stale.  We consult this data every time a placement decision is made,
	// but the information doesn't change that often, so we rebuild the full status
	// occasionally.
	//
	// We combine:
	// What tractservers should be hosting data, from replicated state.  The "source of truth"
	// for this is idToHost.
	// What tractservers have heartbeaten to us.
	status  tsStatus
	healthy []core.TSAddr

	// Failure domains of the healty tractservers. Whenever we refresh
	// tractserver status, we also query FailureDomainService to get the
	// failure domains of 'healthy' and build the reverse index from failure
	// domains to tractservers.
	//
	// 'domainToTS' is first indexed by the failure domain levels (0 for
	// hosts, 1 for racks, 2 for clusters, and so on). The map key
	// represents the name of a failure domain, and the value is the list of
	// hosts in that domain. See the comment for 'buildReverseIndex' method
	// for an example.
	domainToTS []map[string][]string
	fds        FailureDomainService

	// When did we last recalculate status and healthy?
	lastRefresh time.Time

	// Global configuration for curator.
	config *Config

	// A time-providing function, shim layer inserted for testing.
	getTime func() time.Time
}

type tsStatus struct {
	healthy   map[core.TractserverID]struct{}
	unhealthy map[core.TractserverID]struct{}
	down      map[core.TractserverID]struct{}
}

func newTsStatus() tsStatus {
	return tsStatus{
		healthy:   make(map[core.TractserverID]struct{}),
		unhealthy: make(map[core.TractserverID]struct{}),
		down:      make(map[core.TractserverID]struct{}),
	}
}

// newTractserverMonitor returns a new tractserver monitor.
func newTractserverMonitor(config *Config, fds FailureDomainService, getTime func() time.Time) *tractserverMonitor {
	now := getTime()

	return &tractserverMonitor{
		start:       now,
		lastRefresh: now,
		idToHost:    make(map[core.TractserverID]tractserverData),
		hostToID:    make(map[string]core.TractserverID),
		fds:         fds,
		status:      newTsStatus(),
		config:      config,
		getTime:     getTime,
	}
}

func (t *tractserverMonitor) isHealthy(last, now time.Time) bool {
	return now.Sub(last) < t.config.TsUnhealthy
}

func (t *tractserverMonitor) isUnhealthy(last, now time.Time) bool {
	return !t.isHealthy(last, now) && now.Sub(last) < t.config.TsDown
}

func (t *tractserverMonitor) isDown(last, now time.Time) bool {
	return now.Sub(last) >= t.config.TsDown
}

// Restart the grace period timer on this tractserver monitor.
func (t *tractserverMonitor) Restart() {
	t.lock.Lock()
	defer t.lock.Unlock()

	t.start = t.getTime()
}

// String returns a string summarizing the health of all tractservers known to this monitor.
func (t *tractserverMonitor) String() string {
	t.lock.Lock()
	defer t.lock.Unlock()

	t.maybeRefreshStatus()

	return fmt.Sprintf("%d hosts, %d healthy, %d unhealthy, %d down",
		len(t.idToHost), len(t.status.healthy), len(t.status.unhealthy), len(t.status.down))
}

// updateExpected updates the set of tractservers that we expect to receive heartbeats from.
func (t *tractserverMonitor) updateExpected(expected []core.TractserverID) {
	t.lock.Lock()
	defer t.lock.Unlock()

	// Add entries for every tractserver that hasn't beaten to us.
	for _, id := range expected {
		if _, ok := t.idToHost[id]; !ok {
			t.idToHost[id] = tractserverData{ID: id}
		}
	}
	t.refreshStatus()
}

// getStatus returns a summary of the cluster's status as of this moment.
// Clients must treat the returned object as read-only.
func (t *tractserverMonitor) getStatus() tsStatus {
	t.lock.Lock()
	defer t.lock.Unlock()

	t.maybeRefreshStatus()
	return t.status
}

// getHealthy returns a list of the tractservers that are up.
func (t *tractserverMonitor) getHealthy() []core.TSAddr {
	t.lock.Lock()
	defer t.lock.Unlock()

	t.maybeRefreshStatus()
	return t.healthy
}

// assumes lock held.
func (t *tractserverMonitor) maybeRefreshStatus() {
	now := t.getTime()
	if now.Sub(t.lastRefresh) < statusRefreshInterval {
		return
	}
	t.refreshStatus()
}

// assumes lock held.
func (t *tractserverMonitor) refreshStatus() {
	now := t.getTime()
	t.lastRefresh = now
	t.status = newTsStatus()

	// Assume that most hosts are healthy.
	t.healthy = nil

	// When we refresh the tsmon's status we recalculate the failure domain
	// information.  Failure domains are specified as a function of the hostnames.
	// So, we keep a list of the hostnames that could host new data.
	var canHostNewData []string

	for id, data := range t.idToHost {
		if now.Sub(t.start) < t.config.TsHeartbeatGracePeriod {
			// Nothing can be unhealthy or down until it's been long enough to get heartbeats.
			t.status.healthy[id] = struct{}{}
			data.Status = statusHealthy
			t.idToHost[id] = data
			t.healthy = append(t.healthy, core.TSAddr{Host: data.Addr, ID: id})
			if data.Load.AvailSpace > minAvailSpace {
				canHostNewData = append(canHostNewData, data.Addr)
			}
			continue
		}

		// The curator has not received any heartbeat from the tract server since
		// 'noHeartbeatSince'.
		var noHeartbeatSince time.Time

		if !data.hasBeaten() {
			// If we haven't heard any heartbeat from this tract server yet we'll
			// use 't.start' to decide given the curator has not received any
			// heartbeat from the tract server since it restarted.
			noHeartbeatSince = t.start
		} else {
			noHeartbeatSince = data.LastBeat
		}

		if t.isHealthy(noHeartbeatSince, now) {
			data.Status = statusHealthy
			t.status.healthy[id] = struct{}{}
			t.healthy = append(t.healthy, core.TSAddr{Host: data.Addr, ID: id})

			if data.Load.AvailSpace > minAvailSpace {
				canHostNewData = append(canHostNewData, data.Addr)
			} else {
				log.Infof("tractserver %d at %s healthy but full, excluding from new tracts", id, data.Addr)
			}
		} else if t.isUnhealthy(noHeartbeatSince, now) {
			data.Status = statusUnhealthy
			t.status.unhealthy[id] = struct{}{}
		} else if t.isDown(noHeartbeatSince, now) {
			data.Status = statusDown
			t.status.down[id] = struct{}{}
		} else {
			log.Fatalf("undefined tractserver status")
		}

		t.idToHost[id] = data
	}

	// Look up the failure domains for each TS that can host more data.
	domains := t.fds.GetFailureDomain(canHostNewData)

	// Build the reverse mapping from failure domains to tractservers.
	t.domainToTS = buildReverseIndex(domains)
}

// recvHeartbeat is called when a tractserver sends a heartbeat to this host.
func (t *tractserverMonitor) recvHeartbeat(tractserverID core.TractserverID, addr string, load core.TractserverLoad) {
	t.lock.Lock()
	defer t.lock.Unlock()

	t.idToHost[tractserverID] = tractserverData{
		ID:       tractserverID,
		Addr:     addr,
		LastBeat: t.getTime(),
		Load:     load,
	}
	// Refresh status given we just received a heartbeat.
	t.refreshStatus()

	// Update the inverse mapping as well.
	if oldID, ok := t.hostToID[addr]; !ok {
		log.Infof("first heartbeat from TSID %s from addr %s", tractserverID, addr)
		t.hostToID[addr] = tractserverID
	} else if oldID != tractserverID {
		log.Errorf("tractserver at %s used to have id %v, now has id %v!", addr, tractserverID, oldID)
	}
}

// getTractserverAddrs returns the last reported addresses of the given
// tractservers. It returns the number of addresses it failed to look up.
func (t *tractserverMonitor) getTractserverAddrs(ids []core.TractserverID) (addrs []string, missing int) {
	t.lock.Lock()
	defer t.lock.Unlock()

	addrs = make([]string, len(ids))
	for i, id := range ids {
		if data, ok := t.idToHost[id]; ok {
			addrs[i] = data.Addr
		} else {
			missing++
		}
	}
	return
}

// The inverse of getTractserverAddrs.
func (t *tractserverMonitor) getTractserverIDs(addrs []string) (ids []core.TractserverID, missing int) {
	t.lock.Lock()
	defer t.lock.Unlock()

	ids = make([]core.TractserverID, len(addrs))
	for i, addr := range addrs {
		if id, ok := t.hostToID[addr]; ok {
			ids[i] = id
		} else {
			missing++
		}
	}
	return
}

// Another variation of getTractserverAddrs, returning TSAddr structs instead of
// just names. Unknown ids are skipped instead of returning an error.
func (t *tractserverMonitor) makeTSAddrs(ids []core.TractserverID) []core.TSAddr {
	t.lock.Lock()
	defer t.lock.Unlock()

	out := make([]core.TSAddr, 0, len(ids))
	for _, tsid := range ids {
		if host, ok := t.idToHost[tsid]; ok {
			out = append(out, core.TSAddr{
				ID:   tsid,
				Host: host.Addr,
			})
		} else {
			log.Infof("getTSAddrs: unknown tsid %v", tsid)
		}
	}
	return out
}

// getIDByAddr returns the tractserver ID that beated most recently from 'addr'.
func (t *tractserverMonitor) getIDByAddr(addr string) (core.TractserverID, bool) {
	t.lock.Lock()
	defer t.lock.Unlock()
	id, ok := t.hostToID[addr]
	return id, ok
}

// getAddrByID returns the most recent address that a tractserver with the provided ID beat from.
func (t *tractserverMonitor) getAddrByID(id core.TractserverID) (string, bool) {
	t.lock.Lock()
	defer t.lock.Unlock()
	data, ok := t.idToHost[id]
	return data.Addr, ok
}

// getData returns all tractserver data, sorted by tractserver ID.
func (t *tractserverMonitor) getData() []tractserverData {
	t.lock.Lock()
	defer t.lock.Unlock()

	var ret byID
	for _, data := range t.idToHost {
		ret = append(ret, data)
	}
	sort.Sort(ret)
	return ret
}

// Need to declare a type in order to sort the data.
type byID []tractserverData

func (b byID) Len() int           { return len(b) }
func (b byID) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b byID) Less(i, j int) bool { return b[i].ID < b[j].ID }

// getFailureDomainToFreeTS returns the TSs that can be used to serve new data, organized
// by failure domain.
//
// The format of the map is described below in buildReverseIndex.
func (t *tractserverMonitor) getFailureDomainToFreeTS() []map[string][]string {
	t.lock.Lock()
	defer t.lock.Unlock()

	// Force refreshing status if the mapping is nil.
	if t.domainToTS == nil {
		t.refreshStatus()
	}
	return t.domainToTS
}

// Given the mapping from tractservers to their failure domains, build reverse
// mappings from failure domains to the included tractservers. The result is a
// slice of maps indexed by the failure domain levels from the lowest to the
// highest.
//
// For example, consider the following topology.
//
//           c0           --cluster
//          /  \
//         /    \
//        r0    r1        --rack
//      / |    / | \
//     /  |   /  |  \
//    h0 h1  h2  h3  h4   --host
//
// The failure service should give us domains as:
//   {
//     {"h0", "r0", "c0"},
//     {"h1", "r0", "c0"},
//     {"h2", "r1", "c0"},
//     {"h3", "r1", "c0"},
//     {"h4", "r1", "c0"},
//   }
// We build the reverse index as follows:
//   {
//     // Host level.
//     {
//       "h0": {"h0"},
//       "h1": {"h1"},
//       "h2": {"h2"},
//       "h3": {"h3"},
//       "h4": {"h4"},
//     },
//     // Rack level.
//     {
//       "r0": {"h0", "h1"},
//       "r1": {"h2", "h3", "h4"},
//     },
//     // Cluster level.
//     {
//       "c0": {"h0", "h1", "h2", "h3", "h4"},
//     },
//   }
//
// This reverse index is used to provide "weights" when we allocate hosts across
// failure domains. If we evenly allocate across failure domains at each level
// when we walk down the tree, we are balancing among failure domains but not
// hosts. If failure domains have different hosts, the domain with fewer hosts
// will, on average, allocate more on its hosts. See 'curator.allocateTS' for
// more details.
func buildReverseIndex(domains [][]string) (index []map[string][]string) {
	if len(domains) == 0 {
		return nil
	}

	for _, domain := range domains {
		// The output should include as many levels as the input.
		for len(index) < len(domain) {
			index = append(index, make(map[string][]string))
		}

		// Host is the lowest level in the failure domain.
		if len(domain) == 0 {
			log.Errorf("failure domain should have at least one level")
			continue
		}
		host := domain[0]

		// Go through the levels and build the reverse index for host.
		for i, lvl := range domain {
			index[i][lvl] = append(index[i][lvl], host)
		}
	}
	return index
}
