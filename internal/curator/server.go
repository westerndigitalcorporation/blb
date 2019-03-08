// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package curator

import (
	"errors"
	"net/http"
	"time"

	log "github.com/golang/glog"

	"github.com/westerndigitalcorporation/blb/internal/core"
	"github.com/westerndigitalcorporation/blb/internal/server"
	"github.com/westerndigitalcorporation/blb/pkg/raft/raft"
	"github.com/westerndigitalcorporation/blb/pkg/rpc"
)

// Server is the RPC server for a Curator.
type Server struct {
	// Curator that manages the cluster.
	curator *Curator

	// Configuration parameters.
	cfg *Config

	// Control handler.
	ctlHandler *CuratorCtlHandler

	// Service handler.
	srvHandler *CuratorSrvHandler

	// Raft and raft storage.
	raft        *raft.Raft
	raftStorage *raft.Storage
}

var (
	// errBusy is returned when the number of pending requests exceeds the limit.
	errBusy = errors.New("curator is too busy to serve the request")
)

// NewServer creates a new Server.  The server does not listen for or serve requests
// until Start() is called on it.
func NewServer(curator *Curator, cfg *Config, raft *raft.Raft, storage *raft.Storage) *Server {
	return &Server{curator: curator, cfg: cfg, raft: raft, raftStorage: storage}
}

// Start starts the RPC server.  Start is blocking and will run forever.
func (s *Server) Start() (err error) {
	// Set up status page.
	http.HandleFunc("/", s.statusHandler)
	//http.HandleFunc("/logs", health.HandleLogs)
	//http.HandleFunc("/loglevel", health.HandleLogsLevel)

	// Endpoint for shutting down the curator.
	http.HandleFunc("/_quit", server.QuitHandler)

	// Endpoints for membership reconfiguration.
	ac := server.NewAutoConfig(s.cfg.RaftACSpec, s.curator.stateHandler)
	http.Handle("/reconfig/", http.StripPrefix("/reconfig", ac.HTTPHandlers()))
	ac.WatchDiscovery()

	http.HandleFunc("/readonly", s.readOnlyHandler)

	// Expose administrative endpoints.
	http.Handle("/raft/", http.StripPrefix("/raft", server.RaftAdminHandler(s.raft, s.raftStorage)))

	opm := server.NewOpMetric("curator_rpc", "rpc")

	s.ctlHandler = &CuratorCtlHandler{
		curator: s.curator,
		opm:     opm,
	}
	if err = rpc.RegisterName("CuratorCtlHandler", s.ctlHandler); err != nil {
		return err
	}

	// Create a new server on the client service port.
	s.srvHandler = &CuratorSrvHandler{
		curator:    s.curator,
		pendingSem: server.NewSemaphore(s.cfg.RejectReqThreshold),
		opm:        opm,
	}
	if err = rpc.RegisterName("CuratorSrvHandler", s.srvHandler); err != nil {
		return err
	}

	log.Infof("listening on address %s", s.cfg.Addr)
	err = http.ListenAndServe(s.cfg.Addr, nil) // this blocks forever
	log.Fatalf("http listener returned error: %v", err)
	return
}

func (s *Server) readOnlyHandler(w http.ResponseWriter, r *http.Request) {
	server.ReadOnlyHandler(w, r, s.curator.stateHandler)
}

// CuratorSrvHandler defines methods that conform to the Go's RPC requirement.
type CuratorSrvHandler struct {
	// The curator we're handling service requests for.
	curator *Curator

	// Semaphore used to limit the number of pending requests from
	// the client.
	pendingSem server.Semaphore

	// Per-RPC info.
	opm *server.OpMetric
}

func (h *CuratorSrvHandler) rpcStats() map[string]string {
	return h.opm.Strings(
		"CreateBlob",
		"DeleteBlob",
		"UndeleteBlob",
		"SetMetadata",
		"ExtendBlob",
		"AckExtendBlob",
		"GetTracts",
		"StatBlob",
		"ReportBadTS",
		"FixVersion",
		"ListBlobs",
	)
}

// CreateBlob is the RPC callback for creating a blob.
func (h *CuratorSrvHandler) CreateBlob(req core.CreateBlobReq, reply *core.CreateBlobReply) error {
	op := h.opm.Start("CreateBlob")
	defer op.EndWithBlbError(&reply.Err)

	// Check pending request limit.
	if !h.pendingSem.TryAcquire() {
		op.TooBusy()
		return errBusy
	}
	defer h.pendingSem.Release()

	reply.ID, reply.Err = h.curator.create(req.Repl, req.Hint, req.Expires)

	log.Infof("CreateBlob: req %+v reply %+v", req, *reply)

	return nil
}

// ExtendBlob is the RPC callback for extending a blob.
func (h *CuratorSrvHandler) ExtendBlob(req core.ExtendBlobReq, reply *core.ExtendBlobReply) error {
	op := h.opm.Start("ExtendBlob")
	defer op.EndWithBlbError(&reply.Err)

	// Check pending request limit.
	if !h.pendingSem.TryAcquire() {
		op.TooBusy()
		return errBusy
	}
	defer h.pendingSem.Release()

	reply.NewTracts, reply.Err = h.curator.extend(req.Blob, req.NumTracts)

	log.Infof("ExtendBlob: req %+v reply %+v", req, *reply)

	return nil
}

// AckExtendBlob is the RPC callback for acknowledging the success of extending
// a blob.
func (h *CuratorSrvHandler) AckExtendBlob(req core.AckExtendBlobReq, reply *core.AckExtendBlobReply) error {
	op := h.opm.Start("AckExtendBlob")
	defer op.EndWithBlbError(&reply.Err)

	// This is really a confirmation rather than a request so we don't check
	// pending request limit to allow it to always go through.
	reply.NumTracts, reply.Err = h.curator.ackExtend(req.Blob, req.Tracts)

	log.Infof("AckExtendBlob: req %+v reply %+v", req, reply)

	return nil
}

// DeleteBlob is the RPC callback for deleting a blob.
func (h *CuratorSrvHandler) DeleteBlob(id core.BlobID, reply *core.Error) error {
	op := h.opm.Start("DeleteBlob")
	defer op.EndWithBlbError(reply)

	// Check pending request limit.
	if !h.pendingSem.TryAcquire() {
		op.TooBusy()
		return errBusy
	}
	defer h.pendingSem.Release()

	*reply = h.curator.remove(id)

	log.Infof("DeleteBlob: req %+v reply %+v", id, *reply)

	return nil
}

// UndeleteBlob is the RPC callback for undeleting a blob.
func (h *CuratorSrvHandler) UndeleteBlob(id core.BlobID, reply *core.Error) error {
	op := h.opm.Start("UndeleteBlob")
	defer op.EndWithBlbError(reply)

	if !h.pendingSem.TryAcquire() {
		op.TooBusy()
		return errBusy
	}
	defer h.pendingSem.Release()

	*reply = h.curator.unremove(id)

	log.Infof("UndeleteBlob: req %+v reply %+v", id, *reply)

	return nil
}

// SetMetadata is the RPC callback for changing blob metadata.
func (h *CuratorSrvHandler) SetMetadata(req core.SetMetadataReq, reply *core.Error) error {
	op := h.opm.Start("SetMetadata")
	defer op.EndWithBlbError(reply)

	if !h.pendingSem.TryAcquire() {
		op.TooBusy()
		return errBusy
	}
	defer h.pendingSem.Release()

	*reply = h.curator.setMetadata(req.Blob, req.Metadata)

	log.Infof("SetMetadata: req %+v reply %+v", req, *reply)

	return nil
}

// GetTracts is the RPC callback for getting tract location information for a read
// or a write.
func (h *CuratorSrvHandler) GetTracts(req core.GetTractsReq, reply *core.GetTractsReply) error {
	op := h.opm.Start("GetTracts")
	defer op.EndWithBlbError(&reply.Err)

	// Check pending request limit.
	if !h.pendingSem.TryAcquire() {
		op.TooBusy()
		return errBusy
	}
	defer h.pendingSem.Release()

	var cls core.StorageClass
	reply.Tracts, cls, reply.Err = h.curator.getTracts(req.Blob, req.Start, req.End)

	// Signal error if trying to write to EC tract here.
	if reply.Err == core.NoError && req.ForWrite && cls != core.StorageClassREPLICATED {
		reply.Tracts = nil
		reply.Err = core.ErrReadOnlyStorageClass
	}

	if req.ForRead || req.ForWrite {
		h.curator.touchBlob(req.Blob, time.Now().UnixNano(), req.ForRead, req.ForWrite)
	}

	log.Infof("GetTracts: req %+v reply %+v", req, *reply)

	return nil
}

// StatBlob is the RPC callback for getting information about a blob.
func (h *CuratorSrvHandler) StatBlob(id core.BlobID, reply *core.StatBlobReply) error {
	op := h.opm.Start("StatBlob")
	defer op.EndWithBlbError(&reply.Err)

	// Check pending request limit.
	if !h.pendingSem.TryAcquire() {
		op.TooBusy()
		return errBusy
	}
	defer h.pendingSem.Release()

	reply.Info, reply.Err = h.curator.stat(id)

	log.Infof("StatBlob: req %+v reply %+v", id, *reply)

	return nil
}

// ReportBadTS is sent by a client when they can't talk to the host in req.
// We can use this information to repair tracts more quickly.
func (h *CuratorSrvHandler) ReportBadTS(req core.ReportBadTSReq, reply *core.Error) error {
	op := h.opm.Start("ReportBadTS")
	defer op.EndWithBlbError(reply)

	// Check pending request limit.
	if !h.pendingSem.TryAcquire() {
		op.TooBusy()
		return errBusy
	}
	defer h.pendingSem.Release()

	// Ignore req.Operation and req.GotError for now.
	*reply = h.curator.reportBadTS(req.ID, req.Bad)

	log.Infof("ReportBadTS: req %+v reply %+v", req, *reply)

	return nil
}

// FixVersion is sent by a client when they're trying to interact with a tractserver but have
// invalid version information.
func (h *CuratorSrvHandler) FixVersion(req core.FixVersionReq, reply *core.Error) error {
	op := h.opm.Start("FixVersion")
	defer op.EndWithBlbError(reply)

	if !h.pendingSem.TryAcquire() {
		op.TooBusy()
		return errBusy
	}
	defer h.pendingSem.Release()

	*reply = h.curator.fixVersion(req.Info.Tract, req.Info.Version, req.Bad)

	log.Infof("FixVersion: req %+v reply %+v", req, *reply)

	return nil
}

// ListBlobs returns a range of blob keys in a partition.
func (h *CuratorSrvHandler) ListBlobs(req core.ListBlobsReq, reply *core.ListBlobsReply) error {
	op := h.opm.Start("ListBlobs")
	defer op.EndWithBlbError(&reply.Err)

	if !h.pendingSem.TryAcquire() {
		op.TooBusy()
		return errBusy
	}
	defer h.pendingSem.Release()

	reply.Keys, reply.Err = h.curator.listBlobs(req.Partition, req.Start)

	log.Infof("ListBlobs: req %+v reply %d keys", req, len(reply.Keys))

	return nil
}

// CuratorCtlHandler handles heartbeat message and other non-client-generated RPCs.
type CuratorCtlHandler struct {
	curator *Curator

	opm *server.OpMetric
}

// CuratorTractserverHeartbeat is called by a tractserver to notify the curator that it is still alive.
func (h *CuratorCtlHandler) CuratorTractserverHeartbeat(msg core.CuratorTractserverHeartbeatReq, reply *core.CuratorTractserverHeartbeatReply) error {
	op := h.opm.Start("CuratorTractserverHeartbeat")
	defer op.End()

	// If we're not the leader, we shouldn't let the tractserver think that this succeeded.
	if !h.curator.isLeader() {
		reply.Err = core.ErrRaftNodeNotLeader
		return nil
	}

	reply.Partitions = h.curator.tractserverHeartbeat(msg.TSID, msg.Addr, msg.Corrupt, msg.Has, msg.Load)
	return nil
}

func (h *CuratorCtlHandler) rpcStats() map[string]string {
	return h.opm.Strings(
		"CuratorTractserverHeartbeat",
	)
}
