// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package blbrpc

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/westerndigitalcorporation/blb/internal/core"
	"github.com/westerndigitalcorporation/blb/platform/discovery"
)

const (
	masterUserName    = "prod"
	masterServiceName = "blb-master"
	masterDefaultPort = 4000
)

// MasterFailoverConnection is like FailoverConnection but if the failover
// targets are not provided it will use master addresses found from the
// discovery service and use it to watch for membership change so it can
// adjust failover targets at runtime.
type MasterFailoverConnection struct {
	dialTimeout time.Duration
	rpcTimeout  time.Duration

	lock sync.Mutex // Protects fields below
	fc   *FailoverConnection
}

// NewMasterFailoverConnection creates a new MasterFailoverConnection.
// addrSpec describes which masters to connect to. There are a few ways to
// specify them:
//   bb                                  - cluster name (use discovery)
//   bb/prod/other-blb-master            - cluster/user/service name (use discovery)
//   bbaa08,bbaa19,bbaa20                - list of hostnames with commas
//   bbaa08:4000,bbaa19:4000,bbaa20:4000 - list of hostnames and ports with commas
//   localhost:4444,                     - single hostname (with comma to force not discovery)
// addrSpec may not be empty!
func NewMasterFailoverConnection(
	addrSpec string,
	dialTimeout,
	rpcTimeout time.Duration,
) *MasterFailoverConnection {
	addrs, addrChangeCh := getAddrs(addrSpec)
	d := &MasterFailoverConnection{
		fc:          NewFailoverConnection(addrs, dialTimeout, rpcTimeout),
		dialTimeout: dialTimeout,
		rpcTimeout:  rpcTimeout,
	}
	if addrChangeCh != nil {
		go d.addrChangeMonitorLoop(addrChangeCh)
	}
	return d
}

// FailoverRPC does same thing as 'FailoverConnection' does.
func (d *MasterFailoverConnection) FailoverRPC(ctx context.Context, method string, req, reply interface{}) (err core.Error, rpcErr error) {
	return d.getFailoverConnection().FailoverRPC(ctx, method, req, reply)
}

func (d *MasterFailoverConnection) getFailoverConnection() *FailoverConnection {
	d.lock.Lock()
	defer d.lock.Unlock()
	return d.fc
}

func (d *MasterFailoverConnection) setFailoverConnection(fc *FailoverConnection) {
	d.lock.Lock()
	defer d.lock.Unlock()
	if d.fc != nil {
		d.fc.Close()
	}
	d.fc = fc
}

func (d *MasterFailoverConnection) addrChangeMonitorLoop(addrChangeCh <-chan discovery.Update) {
	for update := range addrChangeCh {
		if update.IsDelete {
			continue
		}
		newAddrs := update.Addrs(discovery.Binary)
		log.Infof("Failover targets change detected, new targets: %v", newAddrs)
		d.setFailoverConnection(NewFailoverConnection(newAddrs, d.dialTimeout, d.rpcTimeout))
	}
}

func getAddrs(addrSpec string) ([]string, <-chan discovery.Update) {
	if addrSpec == "" {
		log.Fatalf("No master address provided.")
	}
	if strings.Contains(addrSpec, ",") {
		// If master addresses are provided then assume master membership is static.
		return parseAddrList(addrSpec), nil
	}

	// Otherwise use discovery to find them out.
	addrChangeCh, err := discovery.DefaultClient.Watch(nil, parseDiscoverName(addrSpec))
	if err != nil {
		log.Fatalf("Failed to create the discovery watcher: %s", err)
	}

	// Get first update.
	var update discovery.Update
	select {
	case update = <-addrChangeCh:
	case <-time.After(time.Minute):
		log.Fatalf("Failed to lookup master addresses after a minute")
	}

	if update.IsDelete || len(update.Tasks) == 0 {
		log.Fatalf("Failed to lookup master addresses after a minute")
	}

	initialAddrs := update.Addrs(discovery.Binary)
	log.Infof("Discovered initial master addresses: %v", initialAddrs)
	return initialAddrs, addrChangeCh
}

// parseAddrList parses a "host[:port],host[:port]" into a []string of host:port.
func parseAddrList(list string) (addrs []string) {
	parts := strings.Split(list, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if strings.Contains(part, ":") {
			addrs = append(addrs, part)
		} else {
			addrs = append(addrs, fmt.Sprintf("%s:%d", part, masterDefaultPort))
		}
	}
	return
}

// parseDiscoverName parses a "Cluster[/User[/Service]]" into a discovery.Name.
func parseDiscoverName(spec string) discovery.Name {
	parts := strings.Split(spec, "/")
	name := discovery.Name{
		Cluster: parts[0],
		User:    masterUserName,
		Service: masterServiceName,
	}
	if len(parts) >= 2 {
		name.User = parts[1]
	}
	if len(parts) >= 3 {
		name.Service = parts[2]
	}
	return name
}
