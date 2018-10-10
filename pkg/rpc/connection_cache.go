// Copyright (c) 2016 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package rpc

import (
	"context"
	"errors"
	"net/rpc"
	"sync"
	"time"

	"github.com/golang/groupcache/lru"

	log "github.com/golang/glog"
)

// ErrorRPCConnect is returned if we can't connect to the RPC server.
var ErrorRPCConnect = errors.New("RPC couldn't connect")

// ConnectionCache creates and caches RPC connections to addresses.
//
// ConnectionCache is thread-safe.
type ConnectionCache struct {
	// Protects conns.
	lock sync.Mutex

	// Holds open connections.
	conns *lru.Cache

	// What timeout to use for dialing.
	dialTimeout time.Duration

	// What timeout to use for calling RPCs.
	rpcTimeout time.Duration
}

// CancelAction describes the cancel RPC to send if the primary RPC is cancelled
// on the client side.
type CancelAction struct {
	Method string
	Req    interface{}
}

// NewConnectionCache makes a new ConnectionCache. dialTimeout is the timeout
// used for connecting. maxConns is the size of the cache. If we have more than
// that many connections, we may drop idle connections. If maxConns is zero,
// we never drop idle connections.
func NewConnectionCache(dialTimeout, rpcTimeout time.Duration, maxConns int) *ConnectionCache {
	if maxConns < 0 {
		log.Fatalf("max connections can not be negative")
	}
	conns := lru.New(maxConns)
	conns.OnEvicted = onConnEvicted
	return &ConnectionCache{
		conns:       conns,
		dialTimeout: dialTimeout,
		rpcTimeout:  rpcTimeout,
	}
}

// Get an RPC connection to the given address. If the connection could not
// be made, returns nil. Once the RPC has completed, the caller MUST call "done"
// to mark that the rpc.Client is no longer in use and can be closed if it's idle.
func (cc *ConnectionCache) get(ctx context.Context, addr string) *refCntClient {
	// See if a connection exists already.
	cc.lock.Lock()
	if v, ok := cc.conns.Get(addr); ok {
		rc := v.(*refCntClient)
		rc.count++
		cc.lock.Unlock()
		return rc
	}

	// If not, create it.  Drop the lock for this.
	cc.lock.Unlock()
	nctx, cancel := context.WithTimeout(ctx, cc.dialTimeout)
	defer cancel()
	rpcc, e := dialHTTPContext(nctx, "tcp", addr)
	if e != nil {
		log.Infof("error connecting to %s: %s", addr, e)
		return nil
	}

	// Add to map.
	cc.lock.Lock()
	// See if somebody else did this in parallel, if so just return the conn from there.
	if v, ok := cc.conns.Get(addr); ok {
		rc := v.(*refCntClient)
		rc.count++
		cc.lock.Unlock()
		rpcc.Close()
		log.Infof("established duplicate connection to %s, dropping", addr)
		return rc
	}

	log.Infof("established connection to %s", addr)

	// Initialize "count" to 2 because both the LRU cache and the caller of this
	// function have a reference to this client.
	rc := &refCntClient{count: 2, clt: rpcc}
	cc.conns.Add(addr, rc)
	cc.lock.Unlock()

	return rc
}

// Done marks that the rpc.Client is no longer in use. If the call resulted in
// an RPC-level error or the caller has some other reason to believe the
// connection is bad, the caller should pass true for `close`. In that case,
// ConnectionCache will close the connection and remove it from the cache.
func (cc *ConnectionCache) done(addr string, oldConn *refCntClient, err error) {
	cc.lock.Lock()
	defer cc.lock.Unlock()
	if oldConn.decAndMaybeClose() {
		// It means the connection has already been removed from the cache and
		// nobody is using it.
		return
	}

	if err == nil {
		return
	}

	// If we are here, it means the connection still exists in the LRU cache
	// and we should remove it from the cache.

	// If we got an error, we should try to close the client so that we
	// reconnect to it. If this call was done asynchronously, we might have
	// already closed and removed it due to a previous error, and even created a
	// new one. So we only want to remove the cached client if it's the same as
	// this one, and we want to make sure that we only close each client once.
	//
	// So, we check the map and do the close if and only if we are the call
	// that's removing this client from the map.
	//
	// (An alternative approach would be to check the return value of
	// rpc.Client.Close(), but this seems easier.)
	if newConn, ok := cc.conns.Get(addr); ok && newConn == oldConn {
		cc.conns.Remove(addr)
		log.Errorf("connection to %s lost (%s)", addr, err)
	} else {
		log.Errorf("connection to %s lost (%s) (not in cache)", addr, err)
	}
}

// Send wraps up the basic pattern of calling an RPC with a timeout.
func (cc *ConnectionCache) Send(ctx context.Context, addr, method string, req, reply interface{}) error {
	return cc.SendWithCancel(ctx, addr, method, req, reply, nil)
}

// SendWithCancel is like Send, but allows specifying an action to be taken if the RPC is cancelled
// on the client side. Currently the action must be another RPC, which will be done asynchronously,
// and its return value or error will be ignored.
func (cc *ConnectionCache) SendWithCancel(ctx context.Context, addr, method string, req, reply interface{}, can *CancelAction) error {
	rc := cc.get(ctx, addr)
	if rc == nil {
		return ErrorRPCConnect
	}

	nctx, cancel := context.WithTimeout(ctx, cc.rpcTimeout)
	defer cancel()
	call := rc.clt.Go(method, req, reply, make(chan *rpc.Call, 1))

	select {
	case <-call.Done:
		// Operation completes.
		cc.done(addr, rc, call.Error)

		// If we get ErrShutdown, the connection is shut down, because user has called Close or
		// server has told us to stop. The latter is caused by a TCP connection reset function,
		// which means that the server is alive but something is wrong with the TCP connection.
		// Let's re-establish the connection and try one more time in that case.
		// Note that we use nctx so that we don't extend the timeout again.
		if call.Error == rpc.ErrShutdown {
			return cc.SendWithCancel(nctx, addr, method, req, reply, can)
		}

		return call.Error

	case <-nctx.Done():
		err := nctx.Err()
		// Context cancelled or timed out. Run a cancel action if we have it.
		if can != nil {
			log.Errorf("rpc %q to %s: %s, doing cancel rpc", method, addr, err)
			go func() {
				rc.clt.Go(can.Method, can.Req, nil, make(chan *rpc.Call, 1))
				cc.done(addr, rc, nil)
			}()
		} else {
			log.Errorf("rpc %q to %s: %s", method, addr, err)
			cc.done(addr, rc, nil)
		}
		return err
	}
}

// Remove removes and closes a connection from the cache if a connection to
// "addr" exists.
func (cc *ConnectionCache) Remove(addr string) {
	cc.lock.Lock()
	cc.conns.Remove(addr)
	cc.lock.Unlock()
}

// CloseAll closes all connections in the cache. It returns any error that it
// got when closing connections, but even in the case of error, the cache will
// no longer hold references to the connections.
func (cc *ConnectionCache) CloseAll() (err error) {
	cc.lock.Lock()
	defer cc.lock.Unlock()

	// Remove all connections. They will be closed as soon as it's safe to
	// do so(when reference count becomes 0).
	for cc.conns.Len() > 0 {
		cc.conns.RemoveOldest()
	}
	return
}

func onConnEvicted(key lru.Key, val interface{}) {
	log.V(10).Infof("%s has been evicted from connection cache, closing the connection", key)

	rc := val.(*refCntClient)

	// It's called from the LRU and accessing to LRU is already
	// protected by lock so we don't have to lock here.
	rc.decAndMaybeClose()
}

// refCntClient wraps a RPC client with a reference count we know when to
// close the connection.
type refCntClient struct {
	// The count of using instances. The real client should be closed
	// once the count becomes 0. Accessing to it must be protected by
	// lock.
	count int

	clt *rpc.Client
}

// Decrements the "count" and close the connection if "count" becomes 0.
// This method MUST be called with lock.
func (c *refCntClient) decAndMaybeClose() (closed bool) {
	c.count--
	if c.count == 0 {
		c.clt.Close()
		return true
	}
	return false
}
