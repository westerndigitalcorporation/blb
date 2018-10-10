// Copyright (c) 2018 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package discovery

import (
	"context"
	"net"
	"strings"
	"time"
)

type dnsClient struct {
	r *net.Resolver
}

// Lookup does a single dns lookup. It uses only n.Service, other fields are ignored.
func (cli *dnsClient) Lookup(n Name) (Record, error) {
	return cli.lookup(context.Background(), n)
}

// Watch periodically re-resolves the given name. DNS doesn't have any facility
// for notification, so we just poll. Consider whether this is appropriate for
// your environment or if you should make a new discovery.Client implementation.
func (cli *dnsClient) Watch(ctx context.Context, n Name) (<-chan Update, error) {
	ch := make(chan Update)
	go func() {
		tick := time.NewTicker(65 * time.Second)
		defer tick.Stop()
		for range tick.C {
			select {
			case <-ctx.Done():
				return
			default:
			}

			rec, err := cli.lookup(ctx, n)
			if err != nil && strings.HasSuffix(err.Error(), "no such host") {
				ch <- Update{IsDelete: true}
			} else if err == nil {
				ch <- Update{Record: rec}
			}
		}
	}()
	return ch, nil
}

func (cli *dnsClient) lookup(ctx context.Context, n Name) (Record, error) {
	names, err := cli.r.LookupHost(ctx, n.Service)
	if err != nil {
		return Record{}, err
	}
	r := Record{Name: Name{Service: n.Service}, Tasks: make([]Task, len(names))}
	for i, name := range names {
		r.Tasks[i] = Task{Addrs: []Port{Port{Name: Binary, Addr: name}}}
	}
	return r, nil
}
