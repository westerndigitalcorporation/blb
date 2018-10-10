// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package curator

import (
	"math/rand"

	log "github.com/golang/glog"
	"github.com/westerndigitalcorporation/blb/pkg/slices"

	"github.com/westerndigitalcorporation/blb/internal/core"
)

// allocateTS picks 'num' tractservers from the healthy pool reported by
// 'c.tsMon'. The allocation tries to spread out replicas across failure domains.
//
// 'existing' are the hosts that already had the tract. We try not to pick hosts
// from their failure domains but may do so if no other options are available.
//
// 'down' are the the hosts that to be replaced. We should not pick them but
// it's okay to pick other hosts in their failure domains.
//
// Return nil if the allocation is not feasible. Otherwise, return the addresses
// and IDs of the picked tractservers.
//
// In greater detail, the algorithm spread out the replicas as far away
// from each other as possible. It works by walking down the failure domain
// hierarchy and picks hosts that have different domains at the current level.
// If there aren't enough on this level, go down one more level and repeat. When
// it eventually hits the tractserver level, if there aren't enough distinct
// tractservers as replacement, it returns with nil. Otherwise, it picks
// distinct tractservers for the remaining piece.
func (c *Curator) allocateTS(num int, existing []core.TractserverID, down []core.TractserverID) (addrs []string, ids []core.TractserverID) {
	existingAddrs, eMissing := c.tsMon.getTractserverAddrs(existing)
	downAddrs, dMissing := c.tsMon.getTractserverAddrs(down)
	if eMissing > 0 || dMissing > 0 {
		log.Errorf("failed to get addresses for TSs: %v, %v", existingAddrs, downAddrs)
		return nil, nil
	}

	// Walk down from the highest failure domain level and pick.
	reverseIndex := c.tsMon.getFailureDomainToFreeTS()
	for i := len(reverseIndex) - 1; i >= 0 && num > 0; i-- {
		chosen := pickNFromDomain(num, existingAddrs, downAddrs, reverseIndex[i])
		// Update return addresses and existing addresses.
		addrs = append(addrs, chosen...)
		existingAddrs = append(existingAddrs, chosen...)
		num -= len(chosen)
	}

	if num != 0 {
		log.Errorf("not enough healthy TSs")
		return nil, nil
	}

	// Get the IDs and return.
	var missing int
	if ids, missing = c.tsMon.getTractserverIDs(addrs); missing > 0 {
		log.Errorf("failed to get IDs for picked TSs: %v", addrs)
		return nil, nil
	}
	log.V(2).Infof("picked tractservers with id: %s, addr: %s", ids, addrs)
	return
}

// pickNFromDomain picks at most 'num' tractservers from 'domainToHosts' to
// store replicas of a tract. 'domainToHosts' is a mapping from domains to hosts
// belonging to them.
//
// 'existing' are those hosts that already had the tract or have been selected
// to host the tract. We avoid picking hosts from their domains.
//
// 'down' are those hosts to be replaced. We avoid picking them but it's okay to
// pick other hosts in their domains.
//
// The addresses of the picked tractservers are returned.
func pickNFromDomain(num int, existing []string, down []string, domainToHosts map[string][]string) (ret []string) {
	// Create a pool of candidate hosts from domains not including
	// 'existing'. Also 'down' are excluded from the pool.
	pool := make([][]string, 0, len(domainToHosts))
eachDomain:
	for _, hosts := range domainToHosts {
		// Skip this domain if it contains any one in 'exisiting'.
		for _, ex := range existing {
			if slices.ContainsString(hosts, ex) {
				continue eachDomain
			}
		}

		// Exclude 'down' from the candidates.
		candidates := make([]string, 0, len(hosts))
		for _, host := range hosts {
			if !slices.ContainsString(down, host) {
				candidates = append(candidates, host)
			}
		}

		// Put candidates into pool.
		if len(candidates) != 0 {
			pool = append(pool, candidates)
		}
	}

	// If the number of domains in 'pool' is no larger than 'num', simply
	// pick one random host from each domain.
	if len(pool) <= num {
		for _, hosts := range pool {
			ret = append(ret, hosts[rand.Intn(len(hosts))])
		}
		return
	}

	// If we have more domains than the required number, we have to make
	// some choices. Pick uniformly at random from the entire pool (not
	// uniformly across domains).
	return weightedRand(pool, num)
}

// Pick 'num' randomly from 'pool', which represents hosts in different domains.
// It is required that len(pool)>num.
//
// Each host in the entire pool has equal probability to be choosen. However, at
// most one host from each domain should be choosen.
func weightedRand(pool [][]string, num int) (ret []string) {
	// Get the total size of the pool.
	var total int
	for _, hosts := range pool {
		total += len(hosts)
	}

	for n := 0; n < num; n++ {
		// Pick an index in terms of the entire pool.
		index := rand.Intn(total)
		// Walk through the domains to find which bucket (domain) the
		// picked index falls into.
		for i, hosts := range pool {
			if index >= len(hosts) {
				// The index doesn't fall into this bucket.
				// Update it before moving to the next one.
				index -= len(hosts)
				continue
			}
			// Update the result.
			ret = append(ret, hosts[index])
			// Remove the domain from the pool so that we
			// won't pick a second host there.
			pool = append(pool[:i], pool[i+1:]...)
			// Update the size of the pool.
			total -= len(hosts)
			break
		}
	}
	return
}
