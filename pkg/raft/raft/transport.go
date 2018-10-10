// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package raft

// Transport defines the network transport layer used by raft to
// communicate between peers. For most of the messages transport
// only needs to encode the messages and send them over wire and
// decodes the messages it received. The only exception is that
// if the message has the type of 'InstallSnapshot' the transport
// not only needs to send the message itself, but also the data of
// the actual payload that is stored in 'InstallSnapshot.Body' and
// when transports receives a message with type of 'InstallSnapshot'
// it needs to set up 'InstallSnapshot.Body' so that receiver can
// access the actual payload of snapshot by reading the body.
type Transport interface {
	// Addr returns the local address of the transport.
	Addr() string

	// Receive returns a channel for receiving incoming messages
	// from the network.
	Receive() <-chan Msg

	// Send asynchronously sends message 'm' to the node referred
	// by 'm.GetTo()' through the transport.
	Send(m Msg)

	// Close closes all connections.
	Close() error
}

// TransportConfig stores the parameters for raft transport layer.
type TransportConfig struct {
	// Transport address that the raft instance listens on.
	Addr string

	// Capacity of the incoming message channel that is returned in
	// 'Transport.Receive()'.
	MsgChanCap int
}
