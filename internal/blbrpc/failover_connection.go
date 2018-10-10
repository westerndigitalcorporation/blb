// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package blbrpc

import (
	"context"
	"reflect"
	"sync/atomic"
	"time"

	log "github.com/golang/glog"

	"github.com/westerndigitalcorporation/blb/internal/core"
	"github.com/westerndigitalcorporation/blb/pkg/rpc"
)

// FailoverConnection abstracts the rpc calls to a replication group, failing
// over to another member if it fails talking to the current one.
//
// FailoverConnection is thread-safe because internally it will fail over to
// another connection when current connection fails and we use an integer to
// point to the current connection and we use CAS operation to guarantee two
// concurrent updates on this index will not bump the index twice, only
// one will succeed and the other one will just use the updated index for
// next retry.
type FailoverConnection struct {
	addrs []string             // The members in the replication group.
	next  int32                // The member to try next time.
	cc    *rpc.ConnectionCache // The actual connections to members.
}

// NewFailoverConnection creates a new FailoverConnection.
func NewFailoverConnection(addrs []string, dialTimeout, rpcTimeout time.Duration) *FailoverConnection {
	return &FailoverConnection{addrs: addrs, cc: rpc.NewConnectionCache(dialTimeout, rpcTimeout, len(addrs))}
}

// Close closes the connections.
func (f *FailoverConnection) Close() {
	f.cc.CloseAll()
}

// FailoverRPC attempts to call the given RPC method on the current leader
// member. If a call fails with an RPC-level error, or with an application-level
// error that indicates that the member is not the raft leader, it will retry on
// other members.
//
// If a previous call was successful, this will try to use the same connection
// first, so that in the steady state, we're not creating a new connection for
// each call.
//
// This is not guaranteed to always find the current leader (e.g. if we try A,
// B, C in that order and leadership switches from C to A while we're trying B).
//
// reply must be of a type that is supported by extractError and clearStruct.
//
// Return value: two error values are returned, an core.Error in err and a go
// error in rpcErr. rpcErr != nil iff err == core.ErrRPC, and in that case rpcErr
// contains the detailed error from the RPC system.
func (f *FailoverConnection) FailoverRPC(ctx context.Context, method string, req, reply interface{}) (err core.Error, rpcErr error) {
	for _ = range f.addrs {
		rpcAddrIndex := atomic.LoadInt32(&f.next) // The index we'll use to locate server addresses for this RPC request.
		addr := f.addrs[rpcAddrIndex]

		// Clear any old Err value from this struct.
		f.clearStruct(reply)

		// Do the rpc.
		if rpcErr = f.cc.Send(ctx, addr, method, req, reply); rpcErr != nil {
			// ConnectionCache will close the connection for us in
			// case of any rpc errors, so we don't need to do it.

			err = core.ErrRPC

			// Try another member next time.
			// Only update "f.next" if there's no concurrent updates on it, if there's
			// an update already, we'll just use the updated index for next RPC call.
			atomic.CompareAndSwapInt32(&f.next, rpcAddrIndex, (rpcAddrIndex+1)%int32(len(f.addrs)))
			continue
		}

		err = f.extractError(reply)
		if err == core.ErrRaftNodeNotLeader || err == core.ErrRaftNotLeaderAnymore {
			// There is no rpc error but this member is not the
			// leader. We keep the connection in ConnectionCache so
			// that we may use it directly if we ever need to talk
			// to this member again.

			// Try another member next time.
			// Only update "f.next" if there's no concurrent updates on it, if there's
			// an update already, we'll just use the updated index for next RPC call.
			atomic.CompareAndSwapInt32(&f.next, rpcAddrIndex, (rpcAddrIndex+1)%int32(len(f.addrs)))
			continue
		}
		if err != core.NoError {
			// For other errors, assume this is the final result.
			// This member is the leader (since we didn't get a raft
			// not-leader error), so do not disconnect from it.
			break
		}
		return core.NoError, nil
	}
	// Return the last RPC or application-level error we got.
	if rpcErr != nil {
		log.Errorf("RPC-level error calling %s: %s", method, rpcErr)
	} else {
		log.Errorf("application-level error calling %s: %s", method, err)
	}
	return
}

// extractError extracts the Err value from a struct that we are using as an RPC
// reply. Note that v will be a pointer to a struct.
func (*FailoverConnection) extractError(v interface{}) core.Error {
	val := reflect.ValueOf(v)
	errField := reflect.Indirect(val).FieldByName("Err")
	if !errField.IsValid() {
		log.Fatalf("failed to extract error")
	}
	return core.Error(errField.Int())
}

// clearStruct clears a struct that we are using as an RPC v. It's critical to
// clear the Err field, so that an error from a previous call doesn't affect
// this call.
func (*FailoverConnection) clearStruct(v interface{}) {
	val := reflect.Indirect(reflect.ValueOf(v))
	val.Set(reflect.Zero(val.Type()))
}
