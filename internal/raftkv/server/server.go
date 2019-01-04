// Copyright (c) 2016 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package server

import (
	"fmt"
	"net/http"
	"net/url"
	"time"

	log "github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/westerndigitalcorporation/blb/internal/raftkv/store"
	"github.com/westerndigitalcorporation/blb/pkg/raft/raft"
)

const (
	timeoutInSeconds = 10
)

// Server provides the RESTful service to our key-value database.
type Server struct {
	addr string

	// Raft-replicated store.
	store *store.Store

	// Metrics we collect.
	metricReqs *prometheus.CounterVec
}

// New creates a server on a given address.
func New(addr string, store *store.Store) *Server {
	s := &Server{
		addr:  addr,
		store: store,
		// Metrics we collect
		metricReqs: promauto.NewCounterVec(prometheus.CounterOpts{
			Subsystem: "raftkv",
			Name:      "req_count",
			Help:      "Number of requests",
		}, []string{"type"}),
	}

	http.DefaultServeMux.HandleFunc("/", s.ServeHTTP)
	http.DefaultServeMux.HandleFunc("/join", s.makeReconfigHandler(true))
	http.DefaultServeMux.HandleFunc("/remove", s.makeReconfigHandler(false))
	return s
}

// Serve starts serving requests.
func (s *Server) Serve() {
	log.Fatalf("Failed to listen: %v", http.ListenAndServe(s.addr, nil))
}

// ServeHTTP is the HTTP request handler.
func (s *Server) ServeHTTP(writer http.ResponseWriter, req *http.Request) {
	kvReq, err := buildRequest(req)
	if err != nil {
		replyError(
			writer,
			fmt.Sprintf("Bad request: %s", err.Error()),
			http.StatusBadRequest,
		)
		return
	}

	leaderAddr := s.store.LeaderAddr()

	if !kvReq.local {
		// Non-local operation needs to be initiated from leader.
		if leaderAddr == "" {
			// Leader is unknown.
			replyError(writer, "Leader is unknown", http.StatusServiceUnavailable)
			return
		} else if leaderAddr != s.addr {
			// Redirect to leader.
			http.Redirect(
				writer,
				req,
				fmt.Sprintf("http://%s%s", leaderAddr, req.RequestURI),
				http.StatusTemporaryRedirect,
			)
			return
		}
	}

	// If we are here, it means either the node thinks it's the leader
	// or this is a local read operation.

	switch kvReq.method {
	case "GET":
		if kvReq.list {
			s.doList(writer, kvReq)
		} else {
			s.doGet(writer, kvReq)
		}
	case "PUT":
		s.doPut(writer, kvReq)
	case "DELETE":
		s.doDelete(writer, kvReq)
	default:
		log.Errorf("Request type %q is not supported.", req.Method)
		replyError(writer, "Bad request type", http.StatusBadRequest)
	}
}

// Returns a HTTP handler for reconfiguration request. If "isJoinHandler' is
// true it returns a join request handler, otherwise returns a remove request
// handler.
func (s *Server) makeReconfigHandler(isJoinHandler bool) http.HandlerFunc {
	return func(writer http.ResponseWriter, req *http.Request) {
		var node string
		// See if URL contains "node" parameter.
		if m, err := url.ParseQuery(req.URL.RawQuery); err != nil {
			log.Errorf("Failed to parse the query: %v", err)
			replyError(writer, err.Error(), http.StatusBadRequest)
			return
		} else if _, ok := m["node"]; !ok {
			replyError(writer, "node not specified", http.StatusBadRequest)
			return
		} else {
			node = m["node"][0]
		}

		leaderAddr := s.store.LeaderAddr()

		if leaderAddr == "" {
			// The leader of the group is unknown.
			replyError(writer, "Leader is unknown", http.StatusServiceUnavailable)
			return
		}

		if leaderAddr != s.addr {
			// The node is not the leader, redirect clients to leader node.
			log.Infof("Redirect clients to leader %q", leaderAddr)
			http.Redirect(
				writer,
				req,
				fmt.Sprintf("http://%s%s", leaderAddr, req.RequestURI),
				http.StatusTemporaryRedirect,
			)
			return
		}

		var err error

		if isJoinHandler {
			err = s.store.AddNode(node)
		} else {
			err = s.store.RemoveNode(node)
		}

		// It might be that a join/remove request had succeeded so we treat
		// 'ErrNodeExists' and 'ErrNodeNotExists' as OK.
		if err != nil && err != raft.ErrNodeExists && err != raft.ErrNodeNotExists {
			replyError(writer, err.Error(), http.StatusServiceUnavailable)
			return
		}
		log.Infof("successfully reconfigured!")
	}
}

// =========== Store Handlers ============

// doGet returns a value of a specified key.
//
// Possible HTTP responses:
//
//  200 (OK) - Operation succeeded.
//
//  404 (Not Found) - If the key doesn't exist.
//
//  500 (Internal server error) - The node might not have an updated state at the
//                                point it receives the request.
//
func (s *Server) doGet(writer http.ResponseWriter, req *request) {
	s.metricReqs.WithLabelValues("get").Inc()

	v, err := s.store.Get(req.key, req.local, timeoutInSeconds*time.Second)
	if err != nil {
		log.Errorf("Failed to get key: %s", req.key)
		replyError(writer, err.Error(), http.StatusInternalServerError)
	}
	if v != nil {
		writer.Write([]byte(v))
	} else {
		// Key doesn't exist in DB.
		replyError(
			writer,
			fmt.Sprintf("Key %q doesn't exist", req.key),
			http.StatusNotFound,
		)
	}
}

// doList lists all the keys in the database, separated by newlines.
//
// Possible HTTP responses:
//
//  200 (OK) - Operation succeeded.
//
//  408 (Request Timeout) - The request timeouts.
//
//  504 (Service Unavailable) - The node might not have an updated state at the
//                              point it receives the request.
//
func (s *Server) doList(writer http.ResponseWriter, req *request) {
	keys, err := s.store.ListKeys("", req.local, timeoutInSeconds*time.Second)
	if err != nil {
		log.Errorf("Failed to list keys: %s", req.key)
		replyError(writer, err.Error(), http.StatusInternalServerError)
		return
	}
	for _, key := range keys {
		writer.Write(key)
		writer.Write([]byte{'\n'})
	}
}

// doPut puts a key-value pair into the database. If "cas" parameter is specified
// the key-value pair will be put into the database only if the current value
// of the key matches the value specified in cas parameter.
//
// Possible HTTP responses:
//
//  200 (OK) - Operation succeeded.
//
//  307 (Temporary Redirect) - Redirect to the node who might be the leader.
//
//  400 (Bad Request) - The request is invalid.
//
//  408 (Request Timeout) - The request timeouts.
//
//  409 (Conflict) - CAS operation failed.
//
//  504 (Service Unavailable) - The node is not the leader of group and it doesn't
//                              know who the real leader is.
//                            - It was the leader when receives the request, but steps
//                              down before applying the request.
//
func (s *Server) doPut(writer http.ResponseWriter, req *request) {
	s.metricReqs.WithLabelValues("put").Inc()

	var cmd store.Command
	if req.cas {
		// It's a CAS operation.
		cmd = store.Command{
			Op: store.CASCommand{
				Key: req.key, OldValue: req.oldValue, NewValue: req.value,
			},
		}
	} else {
		// Normal Put.
		cmd = store.Command{Op: store.PutCommand{Key: req.key, Value: req.value}}
	}

	res, err := s.store.Propose(cmd, timeoutInSeconds*time.Second)
	if err == raft.ErrNodeNotLeader {
		replyError(
			writer,
			raft.ErrNodeNotLeader.Error(),
			http.StatusServiceUnavailable,
		)
	} else if err == raft.ErrNotLeaderAnymore {
		// Return a timeout error back to clients, 'ErrNotLeaderAnymore'
		// indicates that this command might be committed eventually.
		replyError(
			writer,
			"Not leader anymore",
			http.StatusRequestTimeout,
		)
	} else if err != nil {
		replyError(writer, "Request timeout", http.StatusRequestTimeout)
	}

	if res != nil && res.(error) == store.ErrCAS {
		// It must be an error returned from a CAS operation.
		replyError(writer, err.Error(), http.StatusConflict)
	}
}

// doDelete deletes a key-value pair from the database. If the key is empty the
// entire database will be deleted, this is used for testing only.
//
// Possible HTTP responses:
//
//  200 (OK) - Operation succeeded.
//
//  408 (Request Timeout) - The request timeouts.
//
//  504 (Service Unavailable) - The node is not the leader of group and it doesn't
//                              know who the real leader is.
//                            - It was the leader when receives the request, but steps
//                              down before applying the request.
//
func (s *Server) doDelete(writer http.ResponseWriter, req *request) {
	s.metricReqs.WithLabelValues("del").Inc()

	cmd := store.Command{Op: store.DelCommand{Key: req.key}}
	_, err := s.store.Propose(cmd, timeoutInSeconds*time.Second)
	if err == raft.ErrNodeNotLeader {
		replyError(
			writer,
			raft.ErrNodeNotLeader.Error(),
			http.StatusServiceUnavailable,
		)
	} else if err == raft.ErrNotLeaderAnymore {
		// Return a timeout error back to clients, 'ErrNotLeaderAnymore'
		// indicates that this command might be committed eventually.
		replyError(
			writer,
			"Not leader anymore",
			http.StatusRequestTimeout,
		)
	} else if err != nil {
		replyError(writer, "Request timeout", http.StatusRequestTimeout)
	}
}

// ========== Helpers ==========

func replyError(w http.ResponseWriter, errorMsg string, code int) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(code)
	fmt.Fprintln(w, errorMsg)
}
