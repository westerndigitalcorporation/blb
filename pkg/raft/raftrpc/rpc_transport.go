// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package raftrpc

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	log "github.com/golang/glog"

	"github.com/westerndigitalcorporation/blb/pkg/raft/raft"
	"github.com/westerndigitalcorporation/blb/pkg/rpc"
)

const (
	raftRPCDialTimeout = 10 * time.Second
	defaultRPCName     = "MainRaft"
)

// Wraps an io.ReadCloser implementation and notifies 'closedCh' when the
// 'ReadCloser' is closed.
type closeNotifier struct {
	io.ReadCloser
	closedCh chan struct{}
}

func newCloseNotifier(reader io.ReadCloser) *closeNotifier {
	return &closeNotifier{
		ReadCloser: reader,
		closedCh:   make(chan struct{}, 1),
	}
}

func (c *closeNotifier) Close() error {
	c.closedCh <- struct{}{}
	return c.ReadCloser.Close()
}

// RPCTransportConfig specifies options for the RPC transport, in addition to
// those in the generic TransportConfig.
type RPCTransportConfig struct {
	// Name for this instance of raft, to allow mutiple raft instances can be
	// run in the same process. May be left blank, in which case a default will
	// be used. All peers of the same raft instance must use the same RPCName.
	// Only used by RPC transport.
	RPCName string

	// Duration before a send operation fails.
	SendTimeout time.Duration
}

// DefaultRPCTransportConfig include default values for the Raft RPC
// transport.
var DefaultRPCTransportConfig = RPCTransportConfig{
	// Name for a Raft instance, to differentiate among multiple instances
	// in one process.
	RPCName: defaultRPCName,

	// Duration before a send operation fails.
	SendTimeout: 100 * time.Millisecond,
}

// RPCCommand is a command that is serialized and used to communicate between
// Raft nodes.
// Gob requires a known type passed as an argument to serialize/deserialize,
// which is why we can't just pass the Msg.:
type RPCCommand struct {
	// Msg is actual message.
	Msg raft.Msg
}

func init() {
	gob.Register(&raft.BaseMsg{})
	gob.Register(&raft.AppEnts{})
	gob.Register(&raft.AppEntsResp{})
	gob.Register(&raft.VoteReq{})
	gob.Register(&raft.VoteResp{})
	gob.Register(&raft.InstallSnapshot{})
}

var errRPCTimeout = errors.New("RPC timeout")

// RPCTransport is an implementation of the Transport interface based
// on Go RPC.
type RPCTransport struct {
	// Transport configuration.
	cfg    raft.TransportConfig
	rpcCfg RPCTransportConfig

	// Connections to peers in the replication group (excluding itself).
	cc *rpc.ConnectionCache

	// RPC handler.
	handler RPCHandler
}

// NewRPCTransport creates a new RPCTransport.
func NewRPCTransport(cfg raft.TransportConfig, rpcCfg RPCTransportConfig) (*RPCTransport, error) {
	t := &RPCTransport{
		cfg:    cfg,
		rpcCfg: rpcCfg,
		// 0 means never drop idle connections
		cc:      rpc.NewConnectionCache(raftRPCDialTimeout, rpcCfg.SendTimeout, 0),
		handler: make(chan raft.Msg, cfg.MsgChanCap),
	}
	// Register with Go RPC service.
	if err := rpc.RegisterName(rpcCfg.RPCName, t.handler); nil != err {
		log.Fatalf("[raft-transport] failed to register the rpc handler")
		return nil, err
	}
	http.HandleFunc(t.snapshotEndpoint(), t.snapshotHandler)
	return t, nil
}

// StartStandaloneRPCServer starts the default RPC server. This should only be
// called by binaries that don't start an http server using the default mux.
func (t *RPCTransport) StartStandaloneRPCServer() {
	rpc.StartStandaloneRPCServer(t.cfg.Addr)
}

// Addr returns the local address of the transport.
func (t *RPCTransport) Addr() string {
	return t.cfg.Addr
}

// Receive returns a channel for receiving incoming message from the
// network.
func (t *RPCTransport) Receive() <-chan raft.Msg {
	return t.handler
}

// Send asynchronously sends message 'm' to the node referred by
// 'm.GetTo()' through the transport.
// TODO:
// (1) Should we send a notification when there is an error?
// (2) Group transmission?
func (t *RPCTransport) Send(m raft.Msg) {
	if snap, ok := m.(*raft.InstallSnapshot); ok {
		// We want to stream snapshot data instead of sending it at once.
		go t.sendSnapshot(snap)
	} else {
		// This should match the name of the method of RPCHandler below.
		method := t.rpcCfg.RPCName + ".HandleCommand"
		go t.cc.Send(context.Background(), m.GetTo(), method, RPCCommand{Msg: m}, nil)
	}
}

// sendSnapshot sends snapshot data in streaming fashion. Go RPC needs to provide
// messages you need to send upfront so we have to read all data out before
// sending out thus it will have large memory footprint. So we use HTTP protocol
// here to leverage the chunked tranfer encoding that HTTP offers.
func (t *RPCTransport) sendSnapshot(snapshot *raft.InstallSnapshot) {
	defer snapshot.Body.Close()
	stream, err := getSnapshotStream(*snapshot)
	if err != nil {
		log.Errorf("Failed to create the snapshot stream: %v", err)
		return
	}

	url := fmt.Sprintf("http://%s%s", snapshot.To, t.snapshotEndpoint())
	req, err := http.NewRequest("POST", url, stream)
	if err != nil {
		log.Errorf("Failed to create POST request for snapshot: %v", err)
		return
	}

	var resp *http.Response
	clt := &http.Client{}
	if resp, err = clt.Do(req); err != nil {
		log.Errorf("Failed to issue the snapshot request: %v", err)
		return
	}

	if resp.StatusCode != http.StatusOK {
		log.Errorf("Error %q returned for snapshot request", resp.Status)
	}
}

func (t *RPCTransport) snapshotHandler(writer http.ResponseWriter, req *http.Request) {
	// Decode the length of the header(InstallSnapshot).
	var headerLen uint32
	if err := binary.Read(req.Body, binary.LittleEndian, &headerLen); err != nil {
		log.Errorf("Failed to read header length: %v", err)
		writer.WriteHeader(http.StatusBadRequest)
		return
	}

	// Sanity check the length of the header, the header should be
	// reasonably small.
	if headerLen > 100*1024 {
		log.Errorf("The length(%d) of header is too large, ignore the bad request", headerLen)
		writer.WriteHeader(http.StatusBadRequest)
		return
	}

	// Decode the header with the io.LimitReader so that Gob will not read
	// past the header and consume the actual payload(snapshot data).
	reader := io.LimitReader(req.Body, int64(headerLen))
	var snapshot raft.InstallSnapshot
	dec := gob.NewDecoder(reader)
	if err := dec.Decode(&snapshot); err != nil {
		log.Errorf("Failed to decode header: %v", err)
		writer.WriteHeader(http.StatusBadRequest)
		return
	}

	// Wrap rest of the data into 'closerNotifier' so that the server can
	// block until the Raft is done with the message.
	snapshot.Body = newCloseNotifier(req.Body)
	t.handler <- &snapshot

	// Block until Raft is done with the message.
	<-snapshot.Body.(*closeNotifier).closedCh
}

// Close closes all connections.
func (t *RPCTransport) Close() error {
	if err := t.cc.CloseAll(); err != nil {
		return err
	}

	// We'd like to close the RPC server as well but there is no
	// good way to do it. Closing the listener will lead to a
	// call to log.Fatal in rpc.Server.Accept method, which is not
	// recoverable. There is a TODO for exit in Go's rpc.Server
	// implementation, which is not addressed yet.

	return nil
}

// Returns endpoint for snapshot request.
func (t *RPCTransport) snapshotEndpoint() string {
	return fmt.Sprintf("/_raft_internal/%s/send_snapshot", t.rpcCfg.RPCName)
}

//---------- RPC handler ----------//

// RPCHandler defines methods that conform to Go's RPC requirement.
type RPCHandler chan raft.Msg

// HandleCommand puts a Msg into incoming message queue.
func (h RPCHandler) HandleCommand(cmd RPCCommand, ok *bool) error {
	h <- cmd.Msg.(raft.Msg)
	*ok = true
	return nil
}

// Returns an io.Reader that encapsulates the data stream of a snapshot.
// The format of the data stream should be:
//
//  ------------------------------------------------------------
//  | header length | header(InstallSnapshot) | ... payload... |
//  ------------------------------------------------------------
//
func getSnapshotStream(snapshot raft.InstallSnapshot) (io.Reader, error) {
	// Set 'snapshot.Body' to nil because we don't want Gob to encode
	// it, store the real snapshot.Body to a local variable.
	body := snapshot.Body
	snapshot.Body = nil

	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	if err := enc.Encode(snapshot); err != nil {
		return nil, err
	}

	// First writes the length of encoded message 'InstallSnapshot' so the
	// receiver side can know how much data it needs to consume in order
	// to decode the message back.
	var bufLen bytes.Buffer
	if err := binary.Write(&bufLen, binary.LittleEndian, uint32(len(buffer.Bytes()))); err != nil {
		return nil, err
	}
	return io.MultiReader(&bufLen, &buffer, body), nil
}
