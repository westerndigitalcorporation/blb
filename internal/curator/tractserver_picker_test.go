// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package curator

import (
	"fmt"
	"reflect"
	"sort"
	"sync"
	"testing"

	"github.com/westerndigitalcorporation/blb/pkg/slices"

	"github.com/westerndigitalcorporation/blb/internal/core"
)

// Test reverse index is correct.
func TestBuildReverseIndex(t *testing.T) {
	domains := [][]string{
		{"a0", "b0", "c0"},
		{"a1", "b0", "c0"},
		{"a2", "b1", "c0"},
		{"a3", "b1", "c0"},
		{"a4", "b1", "c0"},
		{"a5", "b2", "c0"},
	}
	exp := []map[string][]string{
		{"a0": {"a0"}, "a1": {"a1"}, "a2": {"a2"}, "a3": {"a3"}, "a4": {"a4"}, "a5": {"a5"}},
		{"b0": {"a0", "a1"}, "b1": {"a2", "a3", "a4"}, "b2": {"a5"}},
		{"c0": {"a0", "a1", "a2", "a3", "a4", "a5"}},
	}
	got := buildReverseIndex(domains)

	// Sort the inner slices so that we can use reflect.DeepEqual to make
	// the comparison below.
	for _, lvl := range got {
		for _, hosts := range lvl {
			sort.Strings(hosts)
		}
	}

	if !reflect.DeepEqual(exp, got) {
		t.Fatalf("reversed index is incorrect")
	}
}

// A failure domain service implementation that simply stores info in memory.
type testFailureDomainService struct {
	domains map[string][]string
	lock    sync.Mutex
}

func newTestFailureDomainService() *testFailureDomainService {
	return &testFailureDomainService{domains: make(map[string][]string)}
}

func (s *testFailureDomainService) put(host string, domains []string) {
	s.lock.Lock()
	s.domains[host] = domains
	s.lock.Unlock()
}

func (s *testFailureDomainService) GetFailureDomain(hosts []string) [][]string {
	ret := make([][]string, len(hosts))
	s.lock.Lock()
	defer s.lock.Unlock()
	for i, host := range hosts {
		ret[i] = s.domains[host]
	}
	return ret
}

// Allocation from scratch should work if there are enough hosts.
func TestTSAllocationForExtend(t *testing.T) {
	mc := newTestMasterConnection()
	tt := newTestTractserverTalker()
	c := newTestCurator(mc, tt, DefaultTestConfig)
	<-mc.heartbeatChan

	// Add two hosts.
	c.addTS(0, "host0")
	c.addTS(1, "host1")

	// Ask to allocate three. Should fail.
	addrs, ids := c.allocateTS(3, nil, nil)
	if addrs != nil {
		t.Fatalf("allocation should have failed as we don't have enough hosts")
	}

	// Add one more host and try again. Should succeed.
	c.addTS(2, "host2")
	addrs, ids = c.allocateTS(3, nil, nil)
	if len(addrs) != 3 || len(ids) != 3 {
		t.Fatalf("failed to allocate hosts")
	}
}

// Allocation for rerepl should work in various situations. The basic setup is
// that hosts 0-2 used to host replicas for a tract but host 2 went down. Host 3
// is available as a replacement but we change its failure domains to see if the
// algorithm can pick it up.
func TestTSAllocationForRerepl(t *testing.T) {
	h := "host3"
	domains := [][]string{
		{h, "domain3"}, // A distinct domain.
		{h, "domain2"}, // Same as the bad host.
		{h, "domain1"}, // Same as a good host.
	}

	for _, domain := range domains {
		mc := newTestMasterConnection()
		tt := newTestTractserverTalker()
		fds := newTestFailureDomainService()
		c := newTestCuratorWithFailureDomain(mc, tt, DefaultTestConfig, fds)
		<-mc.heartbeatChan

		// Add three hosts, each of which belongs to a distinct failure
		// domain. They are assumed to host replicas of a tract.
		for i := 0; i < 3; i++ {
			host := fmt.Sprintf("host%d", i)
			fds.put(host, []string{host, fmt.Sprintf("domain%d", i)})
			c.addTS(core.TractserverID(i), host)
		}

		// Add another host with the given 'domain'.
		fds.put(h, domain)
		c.addTS(3, h)

		// Assume host 2 went down. To replace it, the curator should
		// pick host 3.
		addrs, ids := c.allocateTS(1, []core.TractserverID{0, 1}, []core.TractserverID{2})
		if len(addrs) != 1 || len(ids) != 1 {
			t.Fatalf("failed to allocate hosts")
		}
		if ids[0] != 3 {
			t.Fatalf("should have picked host3 but picked host%d", ids[0])
		}
	}
}

// A more complex setup with multiple failure domain levels.
//
//           c0
//         /    \
//        /      \
//      b0        b1
//     /  \       |
//    /    \      |
//   a0     a1    a2
//   |      |     |
//   |      |     |
//  h0(X)   h1    h2    h3?
//
// Assume h0-h2 used to host replicas of a tract. Now h0 went down. h3 is
// available as a replacement but we change its failure domains to see if the
// algorithm can pick it up.
func TestTSAllocationMultipleLevel(t *testing.T) {
	// Those used to host the replicas.
	old := []struct {
		id      core.TractserverID
		addr    string
		domains []string
	}{
		{0, "h0", []string{"h0", "a0", "b0", "c0"}},
		{1, "h1", []string{"h1", "a1", "b0", "c0"}},
		{2, "h2", []string{"h2", "a2", "b1", "c0"}},
	}

	// Various possible failure domains for h3.
	domains := [][]string{
		{"h3", "a3", "b2", "c1"}, // A distinct domain on c level.
		{"h3", "a3", "b2", "c0"}, // A distinct domain on b level.
		{"h3", "a3", "b1", "c0"}, // A distinct domain on a level.
		{"h3", "a0", "b0", "c0"}, // Same as the bad host on a level.
		{"h3", "a1", "b0", "c0"}, // Same as a good host on a level.
		{"h3", "a2", "b1", "c0"}, // Same as another good host on a level.
	}

	for _, domain := range domains {
		mc := newTestMasterConnection()
		tt := newTestTractserverTalker()
		fds := newTestFailureDomainService()
		c := newTestCuratorWithFailureDomain(mc, tt, DefaultTestConfig, fds)
		<-mc.heartbeatChan

		// Add three original hosts for the replicas.
		for _, host := range old {
			fds.put(host.addr, host.domains)
			c.addTS(host.id, host.addr)
		}

		// Add h3.
		fds.put("h3", domain)
		c.addTS(3, "h3")

		// Assume host 2 went down. To replace it, the curator should
		// pick host 3.
		addrs, ids := c.allocateTS(1, []core.TractserverID{0, 1}, []core.TractserverID{2})
		if len(addrs) != 1 || len(ids) != 1 {
			t.Fatalf("failed to allocate hosts")
		}
		if ids[0] != 3 {
			t.Fatalf("should have picked host3 but picked host%d", ids[0])
		}
	}
}

// If we pick 3 hosts from the topology below, we should succeed and c0 should
// be chosen.
//                  f0
//                /    \
//               /      \
//            e0          e1-----------
//            |           |           |
//            |           |           |
//            d0          d1         d2
//           /  \        /  \         |
//          /    \      /    \        |
//        a0 ---- a9  b0 ---- b9     c0
func TestAllocationMaxSpread(t *testing.T) {
	mc := newTestMasterConnection()
	tt := newTestTractserverTalker()
	fds := newTestFailureDomainService()
	c := newTestCuratorWithFailureDomain(mc, tt, DefaultTestConfig, fds)
	<-mc.heartbeatChan

	for i := 0; i < 9; i++ {
		// a0-a9.
		addr := fmt.Sprintf("a%d", i)
		fds.put(addr, []string{addr, "d0", "e0", "f0"})
		c.addTS(core.TractserverID(i), addr)

		// b0-b9.
		addr = fmt.Sprintf("b%d", i)
		fds.put(addr, []string{addr, "d1", "e1", "f0"})
		c.addTS(core.TractserverID(i+10), addr)
	}

	// c0.
	fds.put("c0", []string{"c0", "d2", "e1", "f0"})
	c.addTS(core.TractserverID(100), "c0")

	addrs, ids := c.allocateTS(3, nil, nil)
	if len(addrs) != 3 || len(ids) != 3 {
		t.Fatalf("failed to allocate hosts")
	}
	if !slices.ContainsString(addrs, "c0") {
		t.Fatalf("should have picked host c0 but didn't, picked %s", addrs)
	}
}

// Test that tracts can be created and rereplicated with large amounts of the
// failure domains inaccessible.
//
//            ----------- e0-----------
//            |           |           |
//            |           |           |
//           d0(X)       d1(X)       d2
//           /  \        /  \       /  \
//          /    \      /    \     /    \
//        a0 ---- a9  b0 ---- b9 c0 ---- c9
//
// If we fail d0 & d1, d2 will be the only available failure domain on that
// level. Create and rerepl should still work.
func TestAllocationLimitedDomains(t *testing.T) {
	mc := newTestMasterConnection()
	tt := newTestTractserverTalker()

	fds := newTestFailureDomainService()
	c := newTestCuratorWithFailureDomain(mc, tt, DefaultTestConfig, fds)
	<-mc.heartbeatChan

	// Prefix 'a' through 'c'.
	for prefix := 0; prefix < 3; prefix++ {
		// Suffix '0' through '9'.
		for suffix := 0; suffix < 10; suffix++ {
			// The host address takes format as PrefixSuffix, where
			// prefix is a letter in [a, c], and suffix is a
			// digit in [0, 9].
			addr := fmt.Sprintf("%c%d", 'a'+prefix, suffix)
			// 'a', 'b', 'c' hosts are under failure domain 'd0',
			// 'd1' and 'd2', respectively.
			fds.put(addr, []string{addr, fmt.Sprintf("d%d", prefix), "e0"})
			// The hosts assigned IDs sequentially, and thus 'a'
			// hosts are in [0, 9], 'b' hosts are in [10, 19], and
			// 'c' hosts are in [20, 29].
			c.addTS(core.TractserverID(10*prefix+suffix), addr)
		}
	}

	// This function returns which failure domain (d0-d3) a host (0-29)
	// belongs to. The mapping described above is straightforward so that we
	// can just divide the ID by 10 to get the failure domain.
	whichD := func(id core.TractserverID) int {
		return int(id) / 10
	}

	// Allocation for create should work.
	addrs, ids := c.allocateTS(3, nil, nil)
	if len(addrs) != 3 || len(ids) != 3 {
		t.Fatalf("failed to allocate hosts for create")
	}

	// Allocation for rerepl should work.
	// Assume existing hosts are 5, 18 (belong to d0 and d1).
	existing := []core.TractserverID{5, 18}
	// Down host is in d2.
	down := []core.TractserverID{23}
	addrs, ids = c.allocateTS(1, existing, down)
	if len(addrs) != 1 || len(ids) != 1 {
		t.Fatalf("failed to allocate hosts for rerepl")
	}
	// The picked host should be under d2 as existing are under d0 and d1.
	if whichD(ids[0]) != 2 {
		t.Fatalf("should have choosen from d2")
	}

	// Failing d0 and d1 is equivalent to failing all a0-a9 b0-b9 hosts.
	down = make([]core.TractserverID, 0, 20)
	for i := 0; i < 20; i++ {
		down = append(down, core.TractserverID(i))
	}

	// Allocation for create should still work.
	addrs, ids = c.allocateTS(3, nil, down)
	if len(addrs) != 3 || len(ids) != 3 {
		t.Fatalf("failed to allocate hosts")
	}
	// The picked hosts should be under d2 -- the only available domain.
	for _, id := range ids {
		if whichD(id) != 2 {
			t.Fatalf("should have choosen from d2")
		}
	}

	// Allocation for rerepl should still work.
	existing = []core.TractserverID{25}
	addrs, ids = c.allocateTS(2, existing, down)
	if len(addrs) != 2 || len(ids) != 2 {
		t.Fatalf("failed to allocate hosts for rerepl")
	}
	// The picked hosts should be under d2 -- the only available domain.
	for _, id := range ids {
		if whichD(id) != 2 {
			t.Fatalf("should have choosen from d2")
		}
	}
}
