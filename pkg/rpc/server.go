// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package rpc

import (
	"io"
	"log"
	"net/http"
	"net/rpc"
	"sync"
)

const (
	bulkRPCPath     = "/_goRPC_bulk_crc_"
	connectedStatus = "200 Connected to Go RPC" // rpc.connected is not exported
)

var handleHTTPOnce sync.Once

// RegisterName wraps rpc.RegisterName, which uses the default RPC server.
func RegisterName(name string, rcvr interface{}) error {
	handleHTTPOnce.Do(func() {
		rpc.HandleHTTP()
		http.HandleFunc(bulkRPCPath, bulkServeHTTP)
	})
	return rpc.RegisterName(name, rcvr)
}

// StartStandaloneRPCServer starts the default RPC server.
func StartStandaloneRPCServer(addr string) {
	go http.ListenAndServe(addr, nil)
}

func bulkServeHTTP(w http.ResponseWriter, req *http.Request) {
	// Copied from go 1.7.5 net/rpc/server.go, replacing ServeConn with ServeCodec.
	if req.Method != "CONNECT" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		io.WriteString(w, "405 must CONNECT\n")
		return
	}
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		log.Print("rpc hijacking ", req.RemoteAddr, ": ", err.Error())
		return
	}
	io.WriteString(conn, "HTTP/1.0 "+connectedStatus+"\n\n")
	rpc.DefaultServer.ServeCodec(newBulkGobCodec(conn))
}
