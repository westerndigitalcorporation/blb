// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package curator

import "strings"

// FailureDomainService defines the interface for the curator to learn about the
// failure domain hierarchy of the cluster.
//
// It is assumed that backup/redundancy links are ignored and thus the failure
// domain hierarchy for the entire cluster can be described as a tree or a
// forest consisting of trees of equal height. The leaves of the tree(s) are
// tractservers, and moving up, one might see racks, rows, clusters, etc.
// Each entity (host, rack, ...) should have a globally unique name.
//
// PL-1362.
type FailureDomainService interface {
	// FailureDomain retrieves the failure domain hierarchy for 'hosts'. The
	// returned failure domains for each host are organized from the lowest
	// level (host) to the highest (e.g., datacenter). The number of
	// levels should be the same across all hosts. Nil is returned if no
	// information is available.
	//
	// Use our current naming convention for example,
	//   hosts := []string{"bbaa01", "bbaa02", "ggaa23"}
	// can be mapped to the following result,
	//   {
	//     {"bbaa01", "bbaa", "bb"}, // {host, rack, cluster}
	//     {"bbaa02", "bbaa", "bb"},
	//     {"ggaa23", "ggaa", "gg"},
	//   }
	GetFailureDomain(hosts []string) [][]string
}

// nilFailureDomainService just includes each host as its only failure domain.
type nilFailureDomainService struct {
}

// GetFailureDomain implements FailureDomainService.
func (s nilFailureDomainService) GetFailureDomain(hosts []string) [][]string {
	ret := make([][]string, 0, len(hosts))
	for _, host := range hosts {
		ret = append(ret, []string{host})
	}
	return ret
}

// RackBasedFailureDomain is an implementation of FailureDomainService that
// parses cluster and rack names out of hostname. Following our current naming
// convention, a host is named as "XXYYDD", where "XX" stands for a cluster,
// "YY" stands for a rack, and "DD" stands for a machine. For example, "bbaa20"
// is machine "20" in rack "aa" of cluster "bb". As a result,
// RackBasedFailureDomain doesn't need to maintain any state.
//
// Each entity (host, rack, cluster) should have a globally unique name. As a
// result, "bbaa20" will be mapped to {"bbaa20", "bbaa", "bb"}, because there
// could be another rack "aa" under a different cluster and similarly another
// host "20" somewhere under a different rack.
type RackBasedFailureDomain struct {
}

// GetFailureDomain simply parses the strings.
func (RackBasedFailureDomain) GetFailureDomain(hosts []string) [][]string {
	ret := make([][]string, 0, len(hosts))
	for _, host := range hosts {
		// Trim all trailing digits.
		rack := strings.TrimRight(host, "0123456789")
		cluster := rack
		if len(cluster) > 2 {
			// Keep the first two letters for cluster name.
			cluster = cluster[:2]
		}
		domain := []string{host, rack, cluster}
		ret = append(ret, domain)
	}
	return ret
}
