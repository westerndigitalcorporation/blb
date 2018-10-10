// Copyright (c) 2017 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package server

import (
	"fmt"
	log "github.com/golang/glog"
	"net/http"

	"github.com/westerndigitalcorporation/blb/internal/core"
)

// ROHandler is a subset of the master or curator state handler that's needed
// for controlling read-only mode.
type ROHandler interface {
	ID() string
	LeaderID() string
	ReadOnlyMode() (bool, core.Error)
	SetReadOnlyMode(bool) core.Error
}

// ReadOnlyHandler implements /readonly for the master and curator. GET requests
// return the current read-only state, POST requests like /readonly?mode=true or
// false change it. If this node is not the raft leader, it will return a
// redirect to what it thinks the current leader is.
func ReadOnlyHandler(w http.ResponseWriter, r *http.Request, h ROHandler) {
	const True, False = "true", "false"
	w.Header().Set("Content-Type", "text/plain")
	if r.Method == "GET" {
		mode, err := h.ReadOnlyMode()
		if err != core.NoError {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "Error getting mode: %s", err)
		}
		w.WriteHeader(http.StatusOK)
		if mode {
			w.Write([]byte(True))
		} else {
			w.Write([]byte(False))
		}
		return
	}
	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		fmt.Fprintln(w, "method must be POST")
		return
	}

	leaderID := h.LeaderID()
	if leaderID == "" {
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintln(w, "leader is unknown")
		return
	}
	if leaderID != h.ID() {
		// Redirect clients to the leader to be nice.
		http.Redirect(w, r, fmt.Sprintf("http://%s%s", leaderID, r.RequestURI),
			http.StatusTemporaryRedirect)
		return
	}

	mode := r.URL.Query().Get("mode")
	if mode != True && mode != False {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintln(w, "'mode' param must be 'true' or 'false'")
		return
	}
	err := h.SetReadOnlyMode(mode == True)
	if err != core.NoError {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "error setting read-only mode: %s", err)
		log.Errorf("error setting read-only mode: %s", err)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "set read-only mode to %s", mode)
	log.Infof("set read-only mode to %s", mode)
}
