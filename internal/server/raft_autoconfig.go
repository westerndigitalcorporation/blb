// Copyright (c) 2016 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package server

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	log "github.com/golang/glog"
	"github.com/westerndigitalcorporation/blb/pkg/slices"

	"github.com/westerndigitalcorporation/blb/pkg/raft/raft"
	"github.com/westerndigitalcorporation/blb/platform/discovery"
)

// AutoConfig controls the membership of a raft cluster based on commands
// delivered over http and service membership information observed from
// discovery.
//
// By default, an AutoConfig doesn't do anything. To install http handlers, use
// HTTPHandlers. To watch for changes from discovery, call WatchDiscovery.
type AutoConfig struct {
	name discovery.Name // service to watch
	n    int            // target number of replicas
	h    RaftReconfig   // handler to perform operations on
	ch   chan []string  // channel for updates from discovery
	gen  uint64         // generation number for update processing

	// The rest of the fields are hooks to override for testing:
	dc             discovery.Client  // discovery client
	isReachable    func(string) bool // reachability checker
	quiesceDelay   func()            // delay before processing update
	proposalDelay  func()            // delay before checking proposal result
	reachableDelay func()            // delay if not reachable
	done           func()            // hook for update processing completed
}

// RaftReconfig is the interface used by AutoConfig to reconfigure a cluster.
type RaftReconfig interface {
	// ID returns the Raft ID of the node.
	ID() string
	// LeaderID returns the Raft ID of the leader.
	LeaderID() string
	// AddNode addes a node to the cluster.
	AddNode(string) error
	// RemoveNode removes a node from the cluster.
	RemoveNode(string) error
	// GetMembership gets the current membership of the raft cluster.
	GetMembership() []string
	// ProposeInitialMembership proposes an initial configuration.
	ProposeInitialMembership([]string) error
}

// NewAutoConfig creates an AutoConfig. spec should be empty to disable
// discovery, or a string of the form "cluster/service/user=n", where n is the
// expected number of members.
func NewAutoConfig(spec string, handler RaftReconfig) *AutoConfig {
	ac := &AutoConfig{
		h:              handler,
		ch:             make(chan []string),
		dc:             discovery.DefaultClient,
		isReachable:    isReachable,
		quiesceDelay:   func() { time.Sleep(30 * time.Second) },
		proposalDelay:  func() { time.Sleep(20 * time.Second) },
		reachableDelay: func() { time.Sleep(5 * time.Second) },
		done:           func() {},
	}
	if spec != "" {
		ac.name, ac.n = nameAndN(spec)
	} else {
		log.Infof("no autoconfig spec provided, will not watch discovery")
	}
	// Run this goroutine even if we're not watching discovery, for /inject_update.
	go ac.processUpdates()
	return ac
}

// HTTPHandlers returns a handler for reconfig endpoints. The paths handled are:
//   /add?node=host:port            adds a new node
//   /remove?node=host:port         removes a node
//   /initial                       proposes initial configuration from discovery
//   /initial?members=a,b,c         proposes initial configuration with explicit hosts
//   /inject_update?members=a,b,c   inject fake discovery update (for testing)
func (ac *AutoConfig) HTTPHandlers() *http.ServeMux {
	m := http.NewServeMux()
	m.HandleFunc("/add", ac.addHandler)
	m.HandleFunc("/remove", ac.removeHandler)
	m.HandleFunc("/initial", ac.initialHandler)
	m.HandleFunc("/inject_update", ac.updateHandler)
	return m
}

// WatchDiscovery starts a goroutine to watch for changes to this service in
// discovery.
func (ac *AutoConfig) WatchDiscovery() error {
	if ac.name.Cluster == "" || ac.name.User == "" || ac.name.Service == "" {
		return fmt.Errorf("service name not configured")
	}
	discCh, err := ac.dc.Watch(context.Background(), ac.name)
	if err != nil {
		return err
	}
	go ac.translateUpdates(discCh)
	log.Infof("watching discovery for updates on %s", ac.name)
	return nil
}

func (ac *AutoConfig) translateUpdates(discCh <-chan discovery.Update) {
	for update := range discCh {
		if update.IsDelete { // This probably shouldn't happen.
			log.Infof("discovery update: is deletion: %+v", update)
			continue
		}
		ac.ch <- update.Addrs(discovery.Binary)
	}
	close(ac.ch)
}

func (ac *AutoConfig) processUpdates() {
	for newMembers := range ac.ch {
		// Any other pending updates should stop working.
		thisGen := atomic.AddUint64(&ac.gen, 1)
		go ac.processUpdate(thisGen, newMembers)
	}
}

func (ac *AutoConfig) processUpdate(thisGen uint64, newMembers []string) {
	defer ac.done()
	log.Infof("got discovery update [%d]: %v, sleeping", thisGen, newMembers)

	// First sleep for a while to let discovery quiesce and cover up transient blips.
	ac.quiesceDelay()

	// Now try to make our state match the update.
	//
	// If we're no longer the latest update, abort.
	//
	// Note that we do this even if we're a follower: we'll probably get errors at first,
	// but eventually the leader's proposals will take effect and we'll have nothing left
	// to do. If a reconfiguration ends up causing a reelection and we become the new
	// leader, then we can pick up from there.
	for atomic.LoadUint64(&ac.gen) == thisGen {
		curMembers := ac.h.GetMembership()
		if len(curMembers) == 0 {
			log.Infof("autoconfig [%d] raft is not configured yet, waiting to be added", thisGen)
			return
		}

		toAdd := setDiff(newMembers, curMembers)
		toRemove := setDiff(curMembers, newMembers)

		// Adding:
		if len(curMembers) == ac.n && len(toAdd) == 1 {
			if ac.isReachable(toAdd[0]) {
				err := ac.h.AddNode(toAdd[0])
				if err != nil {
					log.Errorf("autoconfig [%d] proposed adding %s: %s", thisGen, toAdd[0], err)
				} else {
					log.Infof("autoconfig [%d] proposed adding %s: no error", thisGen, toAdd[0])
				}
				// Sleep for a while to allow this to propagate (or not).
				ac.proposalDelay()
			} else {
				log.Infof("autoconfig [%d] want to add %s, not reachable yet", thisGen, toAdd[0])
				// Sleep until the new node is reachable.
				ac.reachableDelay()
			}
			continue
		}

		// Removing. Do this whether the node being removed is reachable or not,
		// so we get back to the target size more quickly.
		if len(curMembers) == ac.n+1 && len(toRemove) == 1 {
			err := ac.h.RemoveNode(toRemove[0])
			if err != nil {
				log.Errorf("autoconfig [%d] proposed removing %s: %s", thisGen, toRemove[0], err)
			} else {
				log.Infof("autoconfig [%d] proposed removing %s: no error", thisGen, toRemove[0])
			}
			ac.proposalDelay()
			continue
		}

		if len(toAdd)+len(toRemove) == 0 {
			// Nothing to do:
			log.Infof("discovery update [%d]: done processing", thisGen)
		} else {
			// Couldn't figure out what to do:
			log.Errorf("discovery update [%d]: too much changed: add %v, remove %v", thisGen, toAdd, toRemove)
			// TODO: mark this state on the status page, dashboards, and alerts
		}
		return
	}

	log.Infof("discovery update [%d] aborted because of new update", thisGen)
}

func (ac *AutoConfig) addHandler(w http.ResponseWriter, req *http.Request) {
	ac.doAddRemove(w, req, true)
}

func (ac *AutoConfig) removeHandler(w http.ResponseWriter, req *http.Request) {
	ac.doAddRemove(w, req, false)
}

// addRemoveHandler returns a HTTP handler that processes
// reconfiguration request. The handler will get the node that will
// be added/removed by parsing URL parameter "node", for example you
// can add/remove a node by issuing a request to a join/remove handler
// with URL "<end-point>?node=<nodeID>". If this node is not leader and
// knows leader address then a HTTP response of redirection to leader
// will be returned.
//
// 'RaftReconfig' is needed by the handler for doing reconfiguration. If
// 'joinHandler' is true a handler that processes join request will be
// returned, otherwise a handler that processes remove request will be
// returned.
func (ac *AutoConfig) doAddRemove(writer http.ResponseWriter, req *http.Request, joinHandler bool) {
	// See if URL contains "node" parameter.
	node := req.URL.Query().Get("node")
	if node == "" {
		replyError(writer, "node not specified", http.StatusBadRequest)
		return
	}

	leaderID := ac.h.LeaderID()

	if leaderID == "" {
		// The leader is unknown.
		replyError(writer, "Leader is unknown", http.StatusServiceUnavailable)
		return
	}

	if leaderID != ac.h.ID() {
		// The node is not the leader, redirect clients to leader node.
		log.Infof("Redirect clients to leader %q", leaderID)

		http.Redirect(writer, req, fmt.Sprintf("http://%s%s", leaderID, req.RequestURI),
			http.StatusTemporaryRedirect)
		return
	}

	var err error
	if joinHandler {
		err = ac.h.AddNode(node)
	} else {
		err = ac.h.RemoveNode(node)
	}

	// It might be that a join/remove request had succeeded so we treat
	// 'ErrNodeExists' and 'ErrNodeNotExists' as OK.
	if err != nil && err != raft.ErrNodeExists && err != raft.ErrNodeNotExists {
		replyError(writer, err.Error(), http.StatusServiceUnavailable)
		return
	}
	if joinHandler {
		log.Infof("successfully added the node %q", node)
	} else {
		log.Infof("successfully removed the node %q", node)
	}
}

// initialHandler returns a HTTP handler that processes initial configuration requests. It accepts
// one optional parameter, 'members', that should be a comma-separated list of addresses. If not
// present (the expected use case), this will look up the configured service with discovery.
func (ac *AutoConfig) initialHandler(w http.ResponseWriter, req *http.Request) {
	var members []string
	if s := req.URL.Query().Get("members"); s == "" {
		// Lookup current set from discovery.
		rec, err := ac.dc.Lookup(ac.name)
		if err != nil {
			replyError(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		log.Infof("discovery lookup: %+v: %+v", ac.name, rec)
		members = rec.Addrs(discovery.Binary)
		if len(members) != ac.n {
			msg := fmt.Sprintf("wrong number of members: wanted %d, got %v", ac.n, members)
			replyError(w, msg, http.StatusBadRequest)
			return
		}
	} else {
		members = strings.Split(s, ",")
	}

	myID := ac.h.ID()
	if !slices.ContainsString(members, myID) {
		msg := fmt.Sprintf("members doesn't contain our own ID: %s ∉ %v", myID, members)
		replyError(w, msg, http.StatusBadRequest)
		return
	}

	log.Infof("proposing initial raft membership: %s", members)
	err := ac.h.ProposeInitialMembership(members)
	if err != nil {
		replyError(w, err.Error(), http.StatusServiceUnavailable)
	}
}

func (ac *AutoConfig) updateHandler(w http.ResponseWriter, req *http.Request) {
	s := req.URL.Query().Get("members")
	if s == "" {
		replyError(w, "missing members", http.StatusBadRequest)
		return
	}
	members := strings.Split(s, ",")
	log.Infof("injecting discovery update: %v", members)
	ac.ch <- members
}

func replyError(w http.ResponseWriter, errorMsg string, code int) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(code)
	fmt.Fprintln(w, errorMsg)
}

// nameAndN parses a spec of the form "cluster/user/service=n" into a discovery.Name
// and an integer.
func nameAndN(spec string) (discovery.Name, int) {
	parts := strings.Split(spec, "=")
	if len(parts) != 2 {
		log.Errorf("bad autoconfig spec: %q", spec)
		return discovery.Name{}, 0
	}
	n, _ := strconv.Atoi(parts[1])
	if n < 3 || n > 9 {
		log.Errorf("unsupported replica count: %d", n)
		return discovery.Name{}, 0
	}
	parts = strings.Split(parts[0], "/")
	if len(parts) != 3 {
		log.Errorf("bad name in autoconfig spec: %q", spec)
		return discovery.Name{}, 0
	}
	return discovery.Name{Cluster: parts[0], User: parts[1], Service: parts[2]}, n
}

// setDiff returns a - b as a set.
func setDiff(a, b []string) (out []string) {
	for _, i := range a {
		if !slices.ContainsString(b, i) && !slices.ContainsString(out, i) {
			out = append(out, i)
		}
	}
	return
}

// isReachable does a simple reachability test. We assume all nodes serve a status page on /.
func isReachable(addr string) bool {
	cli := http.Client{Timeout: 2 * time.Second}
	url := "http://" + addr + "/"
	resp, err := cli.Get(url)
	return err == nil && resp.StatusCode == http.StatusOK
}
