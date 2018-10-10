// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package discovery

// Discovery is an interface for service discovery.
// A service is fully described by a (Cluster, User, Name) tuple.  This tuple is the primary key
// for setting or getting service information.

import (
	"context"
	"sort"
)

const (
	// Binary is the standard port that clients interact with on services.
	Binary = "binary"

	// Wildcard may be used in the User and Service fields of Name to recieve updates
	// from multiple services.
	Wildcard = "*"
)

var DefaultClient Client = &dnsClient{}

type Name struct {
	Cluster string
	User    string
	Service string
}

type Port struct {
	Name string
	Addr string
}

type Task struct {
	Addrs []Port
}

type Record struct {
	Name  Name
	Tasks []Task
}

type Update struct {
	IsDelete bool
	Record
}

type Client interface {
	// Lookup looks up the Record for the given Name.
	Lookup(Name) (Record, error)

	// Watch monitors service records for changes.
	// Watch will send updates for all services which match the provided query.
	// Every service that exists that matches will send an update on the channel.
	// Cluster must be a valid cluster name.
	Watch(context.Context, Name) (<-chan Update, error)
}

// Addrs returns the addresses corresponding to the provided address name
// for each of the tasks in the service
func (r Record) Addrs(portname string) (addrs []string) {
	for _, t := range r.Tasks {
		for _, p := range t.Addrs {
			if p.Name == portname {
				addrs = append(addrs, p.Addr)
			}
		}
	}
	sort.Strings(addrs)
	return
}
