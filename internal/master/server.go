// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package master

import (
	"errors"
	"net/http"

	log "github.com/golang/glog"

	"github.com/westerndigitalcorporation/blb/internal/core"
	"github.com/westerndigitalcorporation/blb/internal/server"
	"github.com/westerndigitalcorporation/blb/pkg/raft/raft"
	"github.com/westerndigitalcorporation/blb/pkg/rpc"
)

// Server is the RPC server implementation for the Master.
type Server struct {
	// Master that manages cluster-wide metadata.
	master *Master

	// Configuration parameters.
	cfg Config

	// Control handler.
	ctlHandler *MasterCtlHandler

	// Service handler.
	srvHandler *MasterSrvHandler

	// Raft and raft storage.
	raft        *raft.Raft
	raftStorage *raft.Storage
}

// NewServer creates a new Master Server.
func NewServer(master *Master, cfg Config, raft *raft.Raft, storage *raft.Storage) *Server {
	return &Server{master: master, cfg: cfg, raft: raft, raftStorage: storage}
}

// Start starts the RPC server.
func (s *Server) Start() (err error) {
	// Set up status page.
	http.HandleFunc("/", s.statusHandler)
	//http.HandleFunc("/logs", health.HandleLogs)
	//http.HandleFunc("/loglevel", health.HandleLogsLevel)

	// Endpoint for shutting down the master.
	http.HandleFunc("/_quit", server.QuitHandler)

	// Endpoints for Raft membership reconfiguration.
	ac := server.NewAutoConfig(s.cfg.RaftACSpec, s.master.stateHandler)
	http.Handle("/reconfig/", http.StripPrefix("/reconfig", ac.HTTPHandlers()))
	ac.WatchDiscovery()

	http.HandleFunc("/readonly", s.readOnlyHandler)

	// Expose administrative endpoints.
	http.Handle("/raft/", http.StripPrefix("/raft", server.RaftAdminHandler(s.raft, s.raftStorage)))

	// Set up RPC mechanisms.
	opm := server.NewOpMetric("master_rpc", "rpc")
	s.ctlHandler = &MasterCtlHandler{
		master: s.master,
		opm:    opm,
	}
	if err = rpc.RegisterName("MasterCtlHandler", s.ctlHandler); nil != err {
		return err
	}

	s.srvHandler = &MasterSrvHandler{
		master:     s.master,
		pendingSem: server.NewSemaphore(s.cfg.RejectReqThreshold),
		opm:        opm,
	}
	if err = rpc.RegisterName("MasterSrvHandler", s.srvHandler); nil != err {
		return err
	}

	log.Infof("listening on address %s", s.cfg.Addr)
	err = http.ListenAndServe(s.cfg.Addr, nil) // this blocks forever
	log.Fatalf("http listener returned error: %v", err)
	return
}

func (s *Server) readOnlyHandler(w http.ResponseWriter, r *http.Request) {
	server.ReadOnlyHandler(w, r, s.master.stateHandler)
}

//---------------------------------
// Mechanism for managing Curators
//---------------------------------

// MasterCtlHandler defines control-oriented methods that conform to the Go's RPC
// requirement.
type MasterCtlHandler struct {
	// Master that manages the Curators.
	master *Master

	// Per-RPC stats.
	opm *server.OpMetric
}

// RegisterCurator is called when a new Curator is added to the system.  It is assigned
// a persistent ID by the Master that it must use in all future communications.
func (h *MasterCtlHandler) RegisterCurator(req core.RegisterCuratorReq, reply *core.RegisterCuratorReply) error {
	op := h.opm.Start("RegisterCurator")
	defer op.EndWithBlbError(&reply.Err)

	reply.CuratorID, reply.Err = h.master.registerCurator(req.Addr)

	log.Infof("RegisterCurator: req %+v reply %+v", req, *reply)

	return nil
}

// CuratorHeartbeat is used to notify the Master that a Curator is still alive.
func (h *MasterCtlHandler) CuratorHeartbeat(req core.CuratorHeartbeatReq, reply *core.CuratorHeartbeatReply) error {
	op := h.opm.Start("CuratorHeartbeat")
	defer op.EndWithBlbError(&reply.Err)

	reply.Partitions, reply.Err = h.master.curatorHeartbeat(req.CuratorID, req.Addr)

	log.Infof("CuratorHeartbeat: req %+v reply %+v", req, *reply)

	return nil
}

// NewPartition is used to allocate a new partition to the provided curator.
func (h *MasterCtlHandler) NewPartition(req core.NewPartitionReq, reply *core.NewPartitionReply) error {
	op := h.opm.Start("NewPartition")
	defer op.EndWithBlbError(&reply.Err)

	reply.PartitionID, reply.Err = h.master.newPartition(req.CuratorID)

	log.Infof("NewPartition: req %+v reply %+v", req, *reply)

	return nil
}

//----------------------
// Tractserver start-up
//----------------------

// RegisterTractserver is called when a new Tractserver is added to the system.
// It is assigned a persistent ID by the Master that it must use in all future
// communications.
func (h *MasterCtlHandler) RegisterTractserver(req core.RegisterTractserverReq, reply *core.RegisterTractserverReply) error {
	op := h.opm.Start("RegisterTractserver")
	defer op.EndWithBlbError(&reply.Err)

	reply.TSID, reply.Err = h.master.registerTractserver(req.Addr)

	log.Infof("RegisterTractserver: req %+v reply %+v", req, *reply)

	return nil
}

// MasterTractserverHeartbeat handles heartbeats from tractservers. These are
// sent occasionally to learn about new curators and changes in curator
// locations.
func (h *MasterCtlHandler) MasterTractserverHeartbeat(req core.MasterTractserverHeartbeatReq, reply *core.MasterTractserverHeartbeatReply) error {
	op := h.opm.Start("MasterTractserverHeartbeat")
	defer op.EndWithBlbError(&reply.Err)

	reply.Curators, reply.Err = h.master.tractserverHeartbeat(req.TractserverID, req.Addr, req.Disks)

	log.Infof("MasterTractserverHeartbeat: req %+v reply %+v", req, *reply)

	return nil
}

func (h *MasterCtlHandler) rpcStats() map[string]string {
	return h.opm.Strings(
		"RegisterTractserver",
		"RegisterCurator",
		"CuratorHeartbeat",
		"NewPartition",
		"MasterTractserverHeartbeat",
	)
}

//-------------------------------
// Mechanism for serving clients
//-------------------------------

// MasterSrvHandler defines client-oriented methods that conform to the Go's RPC
// requirement. Different from MasterCtlHandler, MasterSrvHandler is rate-limited.
type MasterSrvHandler struct {
	master *Master

	// Semaphore used to limit the number of pending requests from the client.
	pendingSem server.Semaphore

	// Per-RPC stats.
	opm *server.OpMetric
}

func (h *MasterSrvHandler) rpcStats() map[string]string {
	return h.opm.Strings(
		"Create",
		"Lookup",
		"ListPartitions",
		"GetTractserverInfo",
	)
}

// The number of pending requests exceeds the limit.
var errBusy = errors.New("the master is too busy to serve the request")

// LookupCurator is used to locate the Curator for a blob that already exists.
func (h *MasterSrvHandler) LookupCurator(req core.LookupCuratorReq, reply *core.LookupCuratorReply) error {
	op := h.opm.Start("LookupCurator")
	defer op.EndWithBlbError(&reply.Err)

	if !h.pendingSem.TryAcquire() {
		op.TooBusy()
		return errBusy
	}
	defer h.pendingSem.Release()

	reply.Replicas, reply.Err = h.master.lookup(req.Blob.Partition())
	log.Infof("LookupCurator: req %+v reply %+v", req, *reply)

	return nil
}

// LookupPartition is used to locate the Curator for a partition.
func (h *MasterSrvHandler) LookupPartition(req core.LookupPartitionReq, reply *core.LookupPartitionReply) error {
	op := h.opm.Start("LookupPartition")
	defer op.EndWithBlbError(&reply.Err)

	if !h.pendingSem.TryAcquire() {
		op.TooBusy()
		return errBusy
	}
	defer h.pendingSem.Release()

	reply.Replicas, reply.Err = h.master.lookup(req.Partition)
	log.Infof("LookupCurator: req %+v reply %+v", req, *reply)

	return nil
}

// MasterCreateBlob is used to choose a curator to create a blob.
func (h *MasterSrvHandler) MasterCreateBlob(req core.MasterCreateBlobReq, reply *core.LookupCuratorReply) error {
	op := h.opm.Start("MasterCreateBlob")
	defer op.EndWithBlbError(&reply.Err)

	if !h.pendingSem.TryAcquire() {
		op.TooBusy()
		return errBusy
	}
	defer h.pendingSem.Release()

	reply.Replicas, reply.Err = h.master.newBlob()
	log.Infof("MasterCreateBlob: req %+v reply %+v", req, *reply)

	return nil
}

// ListPartitions returns all known partitions.
func (h *MasterSrvHandler) ListPartitions(req core.ListPartitionsReq, reply *core.ListPartitionsReply) error {
	op := h.opm.Start("ListPartitions")
	defer op.EndWithBlbError(&reply.Err)

	if !h.pendingSem.TryAcquire() {
		op.TooBusy()
		return errBusy
	}
	defer h.pendingSem.Release()

	reply.Partitions, reply.Err = h.master.getPartitions(core.CuratorID(0))

	return nil
}

// GetTractserverInfo returns a summary of tractservers.
func (h *MasterSrvHandler) GetTractserverInfo(req core.GetTractserverInfoReq, reply *core.GetTractserverInfoReply) error {
	op := h.opm.Start("GetTractserverInfo")
	defer op.EndWithBlbError(&reply.Err)

	if !h.pendingSem.TryAcquire() {
		op.TooBusy()
		return errBusy
	}
	defer h.pendingSem.Release()

	if !h.master.stateHandler.IsLeader() {
		reply.Err = core.ErrRaftNodeNotLeader
		return nil
	}

	reply.Info = h.master.getTractserverInfo()
	return nil
}
