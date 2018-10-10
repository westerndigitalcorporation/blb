// Copyright (c) 2017 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package server

import (
	"net/http"
	"path/filepath"

	"github.com/westerndigitalcorporation/blb/pkg/raft/raft"
)

type raftAdmin struct {
	r *raft.Raft
	s *raft.Storage
}

// RaftAdminHandler is an http.Handler for raft admin tasks. Currently it
// defines one sub-route:
//   /last_snapshot - serves the most recent snapshot directly from disk
func RaftAdminHandler(r *raft.Raft, s *raft.Storage) http.Handler {
	ra := &raftAdmin{r, s}
	mux := http.NewServeMux()
	mux.HandleFunc("/last_snapshot", ra.handleSnapshot)
	return mux
}

func (ra *raftAdmin) handleSnapshot(w http.ResponseWriter, req *http.Request) {
	meta, path := ra.s.GetSnapshotMetadata()
	if meta == raft.NilSnapshotMetadata || path == "" {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	// Any snapshot at a given index should be identical (at least semantically), so we
	// can use the filename (which contains the term and index) as an etag.
	name := filepath.Base(path)
	w.Header().Set("Etag", "W/\""+name+"\"")
	w.Header().Set("X-Blb-Snapshot-Filename", name)
	w.Header().Set("Content-Type", "application/octet-stream")

	http.ServeFile(w, req, path)
}
