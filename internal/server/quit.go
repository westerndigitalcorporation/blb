// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package server

import (
	"net/http"

	log "github.com/golang/glog"
)

// QuitHandler will shut down a process. Should be used for testing only.
func QuitHandler(w http.ResponseWriter, r *http.Request) {
	log.Fatalf("Received a quit request, kill the process.")
}
