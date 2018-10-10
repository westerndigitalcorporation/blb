// Copyright (c) 2017 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package server

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/westerndigitalcorporation/blb/pkg/testutil"
	"github.com/westerndigitalcorporation/blb/platform/discovery"
)

func (ac *AutoConfig) setupForTesting() (chan<- bool, chan<- bool, <-chan bool) {
	ac.isReachable = func(string) bool { return true }
	qch := make(chan bool)
	pch := make(chan bool)
	dch := make(chan bool)
	ac.quiesceDelay = func() { <-qch }
	ac.proposalDelay = func() { <-pch }
	ac.reachableDelay = func() {}
	ac.done = func() { dch <- true }
	return qch, pch, dch
}

// mockReconfig implements RaftReconfig
type mockReconfig struct {
	*testutil.GenericMock
	id, leaderID string
}

func newMockReconfig(t *testing.T, id, leaderID string) *mockReconfig {
	return &mockReconfig{testutil.NewGenericMock(t), id, leaderID}
}

func (m *mockReconfig) ID() string {
	return m.id
}

func (m *mockReconfig) LeaderID() string {
	return m.leaderID
}

func (m *mockReconfig) addAddNode(n string, res error) {
	m.AddCall("AddNode", res, n)
}

func (m *mockReconfig) addRemoveNode(n string, res error) {
	m.AddCall("RemoveNode", res, n)
}

func (m *mockReconfig) addGetMembership(res []string) {
	m.AddCall("GetMembership", res)
}

func (m *mockReconfig) addProposeInitialMembership(ms []string, res error) {
	m.AddCall("ProposeInitialMembership", res, ms)
}

func (m *mockReconfig) AddNode(n string) error {
	return m.GetError("AddNode", n)
}

func (m *mockReconfig) RemoveNode(n string) error {
	return m.GetError("RemoveNode", n)
}

func (m *mockReconfig) GetMembership() []string {
	return m.GetResult("GetMembership").([]string)
}

func (m *mockReconfig) ProposeInitialMembership(ms []string) error {
	return m.GetError("ProposeInitialMembership", ms)
}

type mockDisco struct {
	rec     discovery.Record
	updates chan discovery.Update
}

func (m *mockDisco) Lookup(discovery.Name) (discovery.Record, error) {
	return m.rec, nil
}

func (m *mockDisco) Watch(context.Context, discovery.Name) (<-chan discovery.Update, error) {
	return m.updates, nil
}

func TestAutoConfigAdd(t *testing.T) {
	rc := newMockReconfig(t, "myID", "")
	ac := NewAutoConfig("", rc)

	w := httptest.NewRecorder()
	ac.addHandler(w, httptest.NewRequest("GET", "/add", nil))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("add didn't complain about missing node")
	}

	w = httptest.NewRecorder()
	ac.addHandler(w, httptest.NewRequest("GET", "/add?node=newNode", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("add didn't complain about unknown leader")
	}

	rc.leaderID = "anotherNode"
	w = httptest.NewRecorder()
	ac.addHandler(w, httptest.NewRequest("GET", "/add?node=newNode", nil))
	if w.Code != http.StatusTemporaryRedirect {
		t.Fatalf("add didn't redirect to leader")
	}

	rc.leaderID = rc.id
	w = httptest.NewRecorder()
	rc.addAddNode("newNode", fmt.Errorf("something bad happened"))
	ac.addHandler(w, httptest.NewRequest("GET", "/add?node=newNode", nil))
	rc.NoMoreCalls()
	if w.Code != http.StatusServiceUnavailable || !strings.Contains(w.Body.String(), "something bad") {
		t.Fatalf("add didn't fail")
	}

	w = httptest.NewRecorder()
	rc.addAddNode("newNode", nil)
	ac.addHandler(w, httptest.NewRequest("GET", "/add?node=newNode", nil))
	rc.NoMoreCalls()
	if w.Code != http.StatusOK {
		t.Fatalf("add didn't succeed")
	}
}

func TestAutoConfigRemove(t *testing.T) {
	rc := newMockReconfig(t, "myID", "")
	ac := NewAutoConfig("", rc)

	w := httptest.NewRecorder()
	ac.removeHandler(w, httptest.NewRequest("GET", "/remove", nil))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("remove didn't complain about missing node")
	}

	w = httptest.NewRecorder()
	ac.removeHandler(w, httptest.NewRequest("GET", "/remove?node=newNode", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("remove didn't complain about unknown leader")
	}

	rc.leaderID = "anotherNode"
	w = httptest.NewRecorder()
	ac.removeHandler(w, httptest.NewRequest("GET", "/remove?node=newNode", nil))
	if w.Code != http.StatusTemporaryRedirect {
		t.Fatalf("remove didn't redirect to leader")
	}

	rc.leaderID = rc.id
	w = httptest.NewRecorder()
	rc.addRemoveNode("newNode", fmt.Errorf("something bad happened"))
	ac.removeHandler(w, httptest.NewRequest("GET", "/remove?node=newNode", nil))
	rc.NoMoreCalls()
	if w.Code != http.StatusServiceUnavailable || !strings.Contains(w.Body.String(), "something bad") {
		t.Fatalf("remove didn't fail")
	}

	w = httptest.NewRecorder()
	rc.addRemoveNode("newNode", nil)
	ac.removeHandler(w, httptest.NewRequest("GET", "/remove?node=newNode", nil))
	rc.NoMoreCalls()
	if w.Code != http.StatusOK {
		t.Fatalf("remove didn't succeed")
	}
}

func TestAutoConfigInitialExplicit(t *testing.T) {
	rc := newMockReconfig(t, "myID", "")
	ac := NewAutoConfig("", rc)

	w := httptest.NewRecorder()
	ac.initialHandler(w, httptest.NewRequest("GET", "/initial?members=node1,node2,node3", nil))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("initial didn't complain that it wasn't a member")
	}

	w = httptest.NewRecorder()
	rc.addProposeInitialMembership([]string{"myID", "node2", "node3"}, fmt.Errorf("something bad happened"))
	ac.initialHandler(w, httptest.NewRequest("GET", "/initial?members=myID,node2,node3", nil))
	if w.Code != http.StatusServiceUnavailable || !strings.Contains(w.Body.String(), "something bad") {
		t.Fatalf("initial didn't return error")
	}

	w = httptest.NewRecorder()
	rc.addProposeInitialMembership([]string{"myID", "node2", "node3"}, nil)
	ac.initialHandler(w, httptest.NewRequest("GET", "/initial?members=myID,node2,node3", nil))
	rc.NoMoreCalls()
	if w.Code != http.StatusOK {
		t.Fatalf("initial failed")
	}
}

func TestAutoConfigInitialDiscoWrongNumber(t *testing.T) {
	rc := newMockReconfig(t, "myID", "")
	ac := NewAutoConfig("cl/user/svc=3", rc)
	ac.dc = &mockDisco{rec: makeRec("myID", "node2")}

	w := httptest.NewRecorder()
	ac.initialHandler(w, httptest.NewRequest("GET", "/initial", nil))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("initial didn't return error")
	}
}

func TestAutoConfigInitialDisco(t *testing.T) {
	rc := newMockReconfig(t, "myID", "")
	ac := NewAutoConfig("cl/user/svc=3", rc)
	ac.dc = &mockDisco{rec: makeRec("myID", "node2", "node3")}

	w := httptest.NewRecorder()
	rc.addProposeInitialMembership([]string{"myID", "node2", "node3"}, fmt.Errorf("something bad happened"))
	ac.initialHandler(w, httptest.NewRequest("GET", "/initial", nil))
	if w.Code != http.StatusServiceUnavailable || !strings.Contains(w.Body.String(), "something bad") {
		t.Fatalf("initial didn't return error")
	}

	w = httptest.NewRecorder()
	rc.addProposeInitialMembership([]string{"myID", "node2", "node3"}, nil)
	ac.initialHandler(w, httptest.NewRequest("GET", "/initial", nil))
	rc.NoMoreCalls()
	if w.Code != http.StatusOK {
		t.Fatalf("initial failed")
	}
}

func TestAutoConfigInject(t *testing.T) {
	rc := newMockReconfig(t, "myID", "")
	ac := NewAutoConfig("cl/user/svc=3", rc)
	qch, pch, dch := ac.setupForTesting()

	w := httptest.NewRecorder()
	ac.updateHandler(w, httptest.NewRequest("GET", "/inject_update", nil))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("inject_update didn't return error")
	}

	w = httptest.NewRecorder()
	ac.updateHandler(w, httptest.NewRequest("GET", "/inject_update?members=node1,node2,node4", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("inject_update failed")
	}

	// processUpdate should give up after quiesce delay and we should get no more calls
	rc.addGetMembership([]string{})
	qch <- true
	<-dch
	rc.NoMoreCalls()

	// multiple calls in a short time, only the last should do anything
	origGen := atomic.LoadUint64(&ac.gen)
	ac.updateHandler(httptest.NewRecorder(), httptest.NewRequest("GET", "/inject_update?members=nodex,nodey,nodez", nil))
	ac.updateHandler(httptest.NewRecorder(), httptest.NewRequest("GET", "/inject_update?members=nodei,nodej,nodek", nil))
	ac.updateHandler(httptest.NewRecorder(), httptest.NewRequest("GET", "/inject_update?members=node1,node2,node4", nil))

	// wait until all three have started
	for atomic.LoadUint64(&ac.gen) != origGen+3 {
		time.Sleep(time.Millisecond)
	}

	// one to remove and one to add
	rc.addGetMembership([]string{"node1", "node2", "node3"})
	rc.addAddNode("node4", nil)
	qch <- true
	qch <- true
	qch <- true

	rc.addGetMembership([]string{"node1", "node2", "node3", "node4"})
	rc.addRemoveNode("node3", nil)
	pch <- true

	rc.addGetMembership([]string{"node1", "node2", "node4"})
	pch <- true

	<-dch
	<-dch
	<-dch

	rc.NoMoreCalls()
}

func TestAutoConfigWatchBadName(t *testing.T) {
	rc := newMockReconfig(t, "myID", "")
	ac := NewAutoConfig("bad spec", rc)
	err := ac.WatchDiscovery()
	if err == nil {
		t.Fatalf("should have been error")
	}
}

func TestAutoConfigWatch(t *testing.T) {
	rc := newMockReconfig(t, "myID", "")
	ac := NewAutoConfig("cl/user/svc=3", rc)
	qch, pch, dch := ac.setupForTesting()

	uch := make(chan discovery.Update, 10)
	ac.dc = &mockDisco{updates: uch}

	err := ac.WatchDiscovery()
	if err != nil {
		t.Fatalf("error watching: %s", err)
	}

	uch <- discovery.Update{Record: makeRec("node1", "node2", "node4")}

	// processUpdate should give up and we should get no more calls
	rc.addGetMembership([]string{})
	qch <- true
	<-dch
	rc.NoMoreCalls()

	// multiple calls in a short time, only the last should do anything
	origGen := atomic.LoadUint64(&ac.gen)
	uch <- discovery.Update{Record: makeRec("nodex", "nodey", "nodez")}
	uch <- discovery.Update{Record: makeRec("nodei", "nodej", "nodek")}
	uch <- discovery.Update{Record: makeRec("node1", "node2", "node4")}

	// wait until all three have started
	for atomic.LoadUint64(&ac.gen) != origGen+3 {
		time.Sleep(time.Millisecond)
	}

	rc.addGetMembership([]string{"node1", "node2", "node3"})
	rc.addAddNode("node4", nil)
	qch <- true
	qch <- true
	qch <- true

	rc.addGetMembership([]string{"node1", "node2", "node3", "node4"})
	rc.addRemoveNode("node3", nil)
	pch <- true

	rc.addGetMembership([]string{"node1", "node2", "node4"})
	pch <- true

	<-dch
	<-dch
	<-dch

	rc.NoMoreCalls()
}

func TestAutoConfigReachable(t *testing.T) {
	rc := newMockReconfig(t, "myID", "")
	ac := NewAutoConfig("cl/user/svc=3", rc)
	qch, pch, dch := ac.setupForTesting()
	rch := make(chan bool, 1)
	ac.isReachable = func(string) bool { return <-rch }

	uch := make(chan discovery.Update, 10)
	ac.dc = &mockDisco{updates: uch}

	err := ac.WatchDiscovery()
	if err != nil {
		t.Fatalf("error watching: %s", err)
	}

	uch <- discovery.Update{Record: makeRec("node1", "node2", "node3", "node4")}

	rc.addGetMembership([]string{"node1", "node2", "node3"})
	qch <- true
	rch <- false
	rc.addGetMembership([]string{"node1", "node2", "node3"})
	rch <- false
	rc.addGetMembership([]string{"node1", "node2", "node3"})
	rc.addAddNode("node4", nil)
	rch <- true

	rc.addGetMembership([]string{"node1", "node2", "node3", "node4"})
	pch <- true

	<-dch

	rc.NoMoreCalls()
}

func makeRec(members ...string) discovery.Record {
	addrs := make([]discovery.Port, len(members))
	for i, m := range members {
		addrs[i] = discovery.Port{Name: discovery.Binary, Addr: m}
	}
	return discovery.Record{Tasks: []discovery.Task{{Addrs: addrs}}}
}
