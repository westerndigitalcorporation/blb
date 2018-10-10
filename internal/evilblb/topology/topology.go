// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package topology

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"time"

	log "github.com/golang/glog"

	"github.com/westerndigitalcorporation/blb/internal/core"
	"github.com/westerndigitalcorporation/blb/pkg/retry"
)

// The interval for re-discovery.
const refreshInterval = 5 * time.Second

// MastersInfo contains the information of a master group.
type MastersInfo struct {
	// Leader is the address of leader of the master group.
	Leader string
	// Members are the members of the master group.
	Members []string
}

// CuratorGroupInfo contains the information of a curator group.
type CuratorGroupInfo struct {
	// ID is the ID of the group.
	ID core.CuratorID
	// Addr is the address of group leader.
	Addr string
	// Members are the members of this curator group.
	Members []string
}

// TractserverInfo contains the information of a tract server.
type TractserverInfo struct {
	// ID is the tract server's ID.
	ID core.TractserverID
	// Addr is the address of the server.
	Addr string
	// Roots are a list of directories that store tract data.
	Roots []string
}

// Topology is the topology of a Blb cluster.
type Topology struct {
	// Masters are the topology of masters.
	Masters MastersInfo
	// Groups are the curator groups.
	CuratorGroups []CuratorGroupInfo
	// Tractservers are tract server in cluster.
	Tractservers []TractserverInfo
}

// Discoverer discovers the topology of a Blb cluster. A topology of a
// cluster can be changed(e.g. new node joins, old node leaves, new leader
// is elected, etc) and Discoverer will re-discover the cluster topology
// periodically. Please note that the discovered topology might be stale or
// inaccurate during network partition or leadership changes.
//
// It's different from updiscoverer on following aspects:
//
// (1) updiscoverer only works if you use it on the same cluster on which
//     Blb is running.
//
// (2) The functionalities of updiscoverer are very limited, it can not
//     tell you who is the leader among a master/curator group, what is
//     the ID of a given tract server/curator, etc.
//
// (3) It's flexible as it's built for Blb only, it's easy to add more
//     information(e.g. unhealthy services, metrics, etc) to the topology.
//
type Discoverer struct {
	clt *http.Client

	lock     sync.Mutex // Protects the fields below
	topology *Topology  // The latest discovered topology.
}

// NewDiscoverer creates a new Discoverer. The discoverer will bootstrap
// itself from the given masters addresses.
func NewDiscoverer(masters []string) *Discoverer {
	cd := &Discoverer{clt: &http.Client{}}

	// Do the initial discovery. The initial discovery must succeed,
	// otherwise it will panic if we can't discover the topology after
	// retrying for 60 seconds.
	retrier := &retry.Retrier{
		MinSleep: 1 * time.Second,
		MaxSleep: 10 * time.Second,
		MaxRetry: 60 * time.Second,
	}
	success, _ := retrier.Do(context.Background(), func(seq int) bool {
		var err error
		cd.topology, err = cd.discover(masters)
		if err != nil {
			log.Infof("Error in discovery: %v", err)
			return false
		}
		return true
	})
	if !success {
		log.Fatalf("Failed to discover cluster topology initially")
	}

	go cd.refreshLoop()
	return cd
}

// GetTopology returns the latest discovered topology.
func (c *Discoverer) GetTopology() *Topology {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.topology
}

func (c *Discoverer) refreshLoop() {
	tick := time.Tick(refreshInterval)
	for {
		<-tick

		newTopology, err := c.discover(c.topology.Masters.Members)
		if err != nil {
			// Do not update latest topology for a failed discovery.
			continue
		}
		c.lock.Lock()
		c.topology = newTopology
		c.lock.Unlock()
	}
}

// discover will try to find out the latest topology of the cluster.
func (c *Discoverer) discover(masters []string) (*Topology, error) {
	// Guess who is the leader of the Blb master.
	leader, err := c.guessLeader(masters)
	if err != nil {
		return nil, err
	}

	status, err := c.getJSON(fmt.Sprintf("http://%s", leader))
	if err != nil {
		return nil, err
	}

	var master MastersInfo
	master.Leader = leader
	// Get members of the master group.
	err = json.Unmarshal(*status["Members"], &master.Members)
	if err != nil {
		return nil, err
	}

	// Get tract servers.
	var tsInfos []TractserverInfo
	err = json.Unmarshal(*status["Tractservers"], &tsInfos)
	if err != nil {
		return nil, err
	}

	// Get mounted disks on each tract server in parallel.
	// TODO: consider using semaphore.
	var wg sync.WaitGroup
	errs := make([]error, len(tsInfos))

	for i := range tsInfos {
		wg.Add(1)

		go func(idx int) {
			defer wg.Done()
			tsStatus, err := c.getJSON(fmt.Sprintf("http://%s", tsInfos[idx].Addr))
			if err != nil {
				errs[idx] = err
				return
			}
			var cfg map[string]*json.RawMessage
			err = json.Unmarshal(*tsStatus["Cfg"], &cfg)
			if err != nil {
				errs[idx] = err
				return
			}
			err = json.Unmarshal(*cfg["DiskRoots"], &tsInfos[idx].Roots)
			if err != nil {
				errs[idx] = err
				return
			}
		}(i)
	}

	// Wait all workers are done.
	wg.Wait()

	// See if there's any error.
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}

	// Get curator groups.
	var curatorGroups []CuratorGroupInfo
	err = json.Unmarshal(*status["Curators"], &curatorGroups)
	if err != nil {
		return nil, err
	}

	// Get members of each curator group.
	for i, group := range curatorGroups {
		cStatus, err := c.getJSON(fmt.Sprintf("http://%s", group.Addr))
		if err != nil {
			return nil, err
		}
		var members []string
		if err = json.Unmarshal(*cStatus["Members"], &members); err != nil {
			return nil, err
		}
		curatorGroups[i].Members = members
	}

	return &Topology{
		Masters:       master,
		CuratorGroups: curatorGroups,
		Tractservers:  tsInfos,
	}, nil
}

func (c *Discoverer) guessLeader(masters []string) (leader string, err error) {
	perm := rand.Perm(len(masters))
	for _, i := range perm {
		// Get master in random order.
		master := masters[i]

		status, err := c.getJSON(fmt.Sprintf("http://%s", master))
		if err != nil {
			// Try next one.
			continue
		}

		if err = json.Unmarshal(*status["LeaderAddr"], &leader); err != nil {
			continue
		}

		if leader != "" {
			return leader, nil
		}
	}

	return "", fmt.Errorf("Failed to guess the leader")
}

func (c *Discoverer) getJSON(url string) (map[string]*json.RawMessage, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header["Accept"] = []string{"application/json"}
	resp, err := c.clt.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var ret map[string]*json.RawMessage
	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(&ret); err != nil {
		return nil, err
	}
	return ret, nil
}
