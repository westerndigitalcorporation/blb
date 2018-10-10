// Copyright (c) 2017 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package rpc

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/rpc"
)

// dialHTTPContext is like rpc.DialHTTP but with a context and using the bulk codec.
// Copied and tweaked from Go 1.5.3 implementation in net/rpc/client.go.
func dialHTTPContext(ctx context.Context, network, address string) (*rpc.Client, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	io.WriteString(conn, "CONNECT "+bulkRPCPath+" HTTP/1.0\n\n")

	// Require successful HTTP response
	// before switching to RPC protocol.
	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "CONNECT"})

	if err == nil && resp.Status == connectedStatus {
		codec := newBulkGobCodec(conn)
		return rpc.NewClientWithCodec(codec), nil
	}
	if err == nil {
		err = errors.New("unexpected HTTP response: " + resp.Status)
	}
	conn.Close()
	return nil, &net.OpError{
		Op:   "dial-http",
		Net:  network + " " + address,
		Addr: nil,
		Err:  err,
	}
}
