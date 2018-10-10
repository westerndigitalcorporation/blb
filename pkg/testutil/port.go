// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package testutil

import (
	"net"
	"strconv"

	log "github.com/golang/glog"
)

// GetFreePort returns a port that nobody else is using.
func GetFreePort() int {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatalf("Failed to find a unused port: %v", err)
	}
	// Close the listener so we can reuse it in our services.
	defer l.Close()
	_, portStr, _ := net.SplitHostPort(l.Addr().String())
	port, err := strconv.Atoi(portStr)
	if err != nil {
		log.Fatalf("Failed to convert to port number: %v", err)
	}
	return port
}
