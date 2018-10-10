// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package tractserver

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/westerndigitalcorporation/blb/internal/core"
	"github.com/westerndigitalcorporation/blb/internal/server"
	"github.com/westerndigitalcorporation/blb/pkg/failures"
	"github.com/westerndigitalcorporation/blb/pkg/rpc"
)

type curatorInfo struct {
	// These may be stale; a curator in one repl group could be scheduled on a machine,
	// die, get scheduled elsewhere, and a curator in a diff. repl group could be brought
	// up on that same machine.
	partitions []core.PartitionID
}

// Server is the RPC server for the TractStore
type Server struct {
	// The actual data storage.
	store *Store

	// Configuration parameters.
	cfg *Config

	// Aggregate connections to curators.
	ct CuratorTalker

	// The set of curators (indexed by address) that we heartbeat to.
	curators map[string]curatorInfo

	// Cached disk status.
	fsStatusCache []core.FsStatus

	// When was the cached disk status last updated.
	fsStatusCacheUpdated time.Time

	// Lock for 'curators', 'fsStatus' and 'fsStatusUpdated'.
	lock sync.Mutex

	// The connection to master.
	mc *RPCMasterConnection

	// Control handler.
	ctlHandler *TSCtlHandler

	// Service handler.
	srvHandler *TSSrvHandler
}

// NewServer creates a new Server. The server does not listen for or serve
// requests until Start() is called on it.
func NewServer(store *Store, ct CuratorTalker, mc *RPCMasterConnection, cfg *Config) *Server {
	return &Server{
		store:    store,
		ct:       ct,
		cfg:      cfg,
		curators: make(map[string]curatorInfo),
		mc:       mc,
	}
}

// Start starts the TractServer by launching goroutines to accept RPC requests.
func (s *Server) Start() (err error) {
	s.initTractserverID()

	// Set up status page.
	http.HandleFunc("/", s.statusHandler)
	//http.HandleFunc("/logs", health.HandleLogs)
	//http.HandleFunc("/loglevel", health.HandleLogsLevel)

	// Endpoint for shutting down the tractserver.
	http.HandleFunc("/_quit", server.QuitHandler)

	// Create control/service handlers.
	opm := server.NewOpMetric("tractserver_rpc", "rpc")
	s.ctlHandler = newTSCtlHandler(s, opm)
	s.srvHandler = newTSSrvHandler(s, opm)

	// Register the rpc handlers.
	if err = rpc.RegisterName("TSCtlHandler", s.ctlHandler); nil != err {
		return err
	}
	if err = rpc.RegisterName("TSSrvHandler", s.srvHandler); err != nil {
		return err
	}

	go s.masterHeartbeatLoop()
	go s.curatorHeartbeatLoop()

	log.Infof("tractserver id=%v listening on address %s", s.store.GetID(), s.cfg.Addr)
	err = http.ListenAndServe(s.cfg.Addr, nil) // this blocks forever
	log.Fatalf("http listener returned error: %v", err)
	return
}

// initTractserverID contacts the master to register this tractserver for service.
func (s *Server) initTractserverID() core.TractserverID {
	// We just started up and probably don't have any disks yet. Let's wait
	// until we add some disks.
	for seconds := 0; s.store.DiskCount() == 0; seconds++ {
		if seconds%60 == 3 {
			log.Infof("Waiting for disks to read TSID...")
		}
		time.Sleep(time.Second)
	}

	// Already registered.
	if id := s.store.GetID(); id.IsValid() {
		return id
	}

	// Check for manual override.
	if overrideID := core.TractserverID(s.cfg.OverrideID); overrideID.IsValid() {
		log.Infof("tractserver ID is being **OVERRIDDEN** to %d", overrideID)
		if _, err := s.store.SetID(overrideID); err != core.NoError {
			log.Fatalf("error storing OVERRIDDEN tractserver ID: %s", err)
		}
		return overrideID
	}

	// We haven't registered yet. Register with the master.
	for {
		reply, err := s.mc.RegisterTractserver(context.Background(), s.cfg.Addr)
		if err != core.NoError {
			log.Errorf("initTractserverID: failed to register tractserver with master, sleeping and retrying, err=%s", err)
			time.Sleep(s.cfg.RegistrationRetry)
			continue
		}

		log.Infof("registered TSID %s", reply.TSID)

		if id, err := s.store.SetID(reply.TSID); err != core.NoError {
			log.Fatalf("failed to persist the tractserver ID: %s", err)
		} else {
			return id
		}
	}
}

// masterHeartbeatLoop runs forever, sending heartbeats to the master.
func (s *Server) masterHeartbeatLoop() {
	masterTicker := time.NewTicker(s.cfg.MasterHeartbeatInterval)

	for {
		// Send the regularly scheduled heartbeat.
		beat, err := s.mc.MasterTractserverHeartbeat(context.Background(), s.store.GetID(), s.cfg.Addr, s.getStatusForHeartbeat())
		if err != core.NoError {
			log.Errorf("masterHeartbeatLoop: error sending heartbeat to master, sleeping and retrying, err=%s", err)
			time.Sleep(s.cfg.MasterHeartbeatRetry)
			continue
		}

		s.processMasterHeartbeat(beat)
		<-masterTicker.C
	}
}

// processMasterHeartbeat compares our view of active curators with the master's view,
// establishing connections to new curators and closing connections to stale curators.
func (s *Server) processMasterHeartbeat(beat core.MasterTractserverHeartbeatReply) {
	// Collect some stats for logging.
	alreadyConnected := 0
	newConnections := 0
	removedConnections := 0

	// We create a map of what's in the from-master heartbeat so that we can remove any
	// curators that aren't in it later.
	inBeat := make(map[string]bool)

	load := s.getLoad()

	// Using s.curators so we lock it.
	s.lock.Lock()
	for _, addr := range beat.Curators {
		inBeat[addr] = true
		if _, ok := s.curators[addr]; ok {
			alreadyConnected++
		} else {
			s.curators[addr] = curatorInfo{}
			go s.beatToCurator(nil, addr, nil, load)
			newConnections++
		}
	}
	for addr := range s.curators {
		if !inBeat[addr] {
			go s.ct.Close(addr)
			delete(s.curators, addr)
			removedConnections++
		}
	}
	s.lock.Unlock()

	if newConnections+removedConnections > 0 {
		log.Infof("processMasterHeartbeat: %d already connected to, %d new connections, %d dropped, total %d curator conns",
			alreadyConnected, newConnections, removedConnections, alreadyConnected+newConnections)
	}
}

// getLoad returns information about how loaded this TS is.
func (s *Server) getLoad() core.TractserverLoad {
	var load core.TractserverLoad
	for _, s := range s.getStatus() {
		load.NumTracts += s.NumTracts
		load.TotalSpace += s.TotalSpace
		// A disk's FS might have free space but we can't use it if the disk is unhealthy.
		if s.Status.Healthy && !s.Status.Full {
			load.AvailSpace += s.AvailSpace
		}
	}
	return load
}

// curatorHeartbeatLoop runs forever, regularly sending heartbeats to all curators in s.curators.
func (s *Server) curatorHeartbeatLoop() {
	// Maps from a partition ID to tracts that we store for that partition.
	// Used to send a subset of tracts to the right curator.
	var tractsByPart map[core.PartitionID][]core.TractID
	var shard uint64

	for range time.Tick(s.cfg.CuratorHeartbeatInterval) {
		// pickSubset removes a subset of tractsByPart.  When it's out of tracts we refresh it.
		if len(tractsByPart) == 0 {
			tractsByPart = s.store.GetSomeTractsByPartition(shard)
			shard++
		}

		// Send the same load vector to all curators.
		load := s.getLoad()

		// Protect s.curators as we iterate over it.
		s.lock.Lock()
		log.V(2).Infof("sending heartbeats to %d curators", len(s.curators))
		for c, ci := range s.curators {
			subset := s.pickSubset(ci.partitions, tractsByPart)
			go s.beatToCurator(ci.partitions, c, subset, load)
		}
		s.lock.Unlock()
	}
}

// Pick a subset of the tracts in 'all', up to tractsPerCuratorHeartbeat, that have a partition in 'parts'.
func (s *Server) pickSubset(parts []core.PartitionID, all map[core.PartitionID][]core.TractID) []core.TractID {
	var ret []core.TractID
	for _, p := range parts {
		budget := s.cfg.TractsPerCuratorHeartbeat - len(ret)
		if budget <= 0 {
			break
		} else if budget < len(all[p]) {
			ret = append(ret, all[p][0:budget]...)
			all[p] = all[p][budget:]
		} else {
			ret = append(ret, all[p]...)
			delete(all, p)
		}
	}
	return ret
}

// beatToCurator sents a heartbeat to the given curator.
func (s *Server) beatToCurator(parts []core.PartitionID, curatorAddr string, tracts []core.TractID, load core.TractserverLoad) {
	// Pull out what's corrupt and managed by this curator.
	beat := core.CuratorTractserverHeartbeatReq{
		TSID:    s.store.GetID(),
		Addr:    s.cfg.Addr,
		Corrupt: s.store.GetBadTracts(parts, s.cfg.BadTractsPerHeartbeat),
		Has:     tracts,
		Load:    load,
	}

	parts, err := s.ct.CuratorTractserverHeartbeat(curatorAddr, beat)
	if err != nil {
		log.Errorf("failed to beat to curator at %s, err=%s", curatorAddr, err)
		return
	}

	log.V(2).Infof("beat to curator at %s, sent %d corrupt, %d owned, got partitions %v",
		curatorAddr, len(beat.Corrupt), len(beat.Has), parts)

	// Update which partitions the curator owns and remove reported bad
	// tracts from failures map.
	s.lock.Lock()
	s.curators[curatorAddr] = curatorInfo{partitions: parts}
	s.store.removeTractsFromFailures(beat.Corrupt)
	s.lock.Unlock()
}

// getStatus retrieves disk status -- used by getLoad and status page. The
// caller should not mutate the returned slice.
func (s *Server) getStatus() []core.FsStatus {
	s.lock.Lock()
	defer s.lock.Unlock()

	// If the cached status is too stale, go down to the store layer to
	// retrieve the actual status and refresh the cache.
	if time.Now().After(s.fsStatusCacheUpdated.Add(s.cfg.DiskStatusCacheTTL)) {
		s.fsStatusCache = s.store.getStatus()
		s.fsStatusCacheUpdated = time.Now()
	}
	return s.fsStatusCache
}

// getStatusForHeartbeat is like getStatus, but clears some fields we don't need
// to send to the master.
func (s *Server) getStatusForHeartbeat() []core.FsStatus {
	disks := s.getStatus()
	out := make([]core.FsStatus, len(disks))
	for i, d := range disks {
		out[i] = d
		out[i].Ops = nil
	}
	return out
}

//-----------------
// Control handler
//-----------------

// TSCtlHandler handles all control messages.
type TSCtlHandler struct {
	// When failure service is enabled, what errors failed operations should return.
	opFailure *server.OpFailure

	// The server.
	server *Server

	// The actual data store.
	store *Store

	// The semaphore which is used to limit the number of pending requests.
	pendingSem server.Semaphore

	// Metrics we collect.
	opm *server.OpMetric
}

// newTSCtlHandler creates a new TSCtlHandler.
func newTSCtlHandler(s *Server, opm *server.OpMetric) *TSCtlHandler {
	cfg := s.cfg
	handler := &TSCtlHandler{
		server:     s,
		store:      s.store,
		pendingSem: server.NewSemaphore(cfg.RejectCtlReqThreshold),
		opm:        opm,
	}

	if cfg.UseFailure {
		handler.opFailure = server.NewOpFailure()
		if err := failures.Register("ts_control_failure", handler.opFailure.Handler); err != nil {
			log.Errorf("failed to register failure service: %s", err)
		}
	}
	return handler
}

// SetVersion sets the version of a tract.  This is done by the curator when it changes replication
// group membership.
func (h *TSCtlHandler) SetVersion(req core.SetVersionReq, reply *core.SetVersionReply) error {
	op := h.opm.Start("SetVersion")
	defer op.EndWithBlbError(&reply.Err)

	// Check failure service.
	if err := h.getFailure("SetVersion"); err != core.NoError {
		log.Errorf("SetVersion: failure service override, returning %s", err)
		*reply = core.SetVersionReply{Err: err}
		return nil
	}

	// Check pending request limit.
	if !h.pendingSem.TryAcquire() {
		op.TooBusy()
		log.Errorf("SetVersion: too busy, rejecting req")
		return errBusy
	}
	defer h.pendingSem.Release()

	if !h.store.HasID(req.TSID) {
		log.Infof("SetVersion: request has tsid %d, i have %d, rejecting", req.TSID, h.store.GetID())
		*reply = core.SetVersionReply{Err: core.ErrWrongTractserver}
		return nil
	}

	reply.NewVersion, reply.Err = h.store.SetVersion(req.ID, req.NewVersion, req.ConditionalStamp)

	log.Infof("SetVersion: req %+v reply %+v", req, *reply)

	return nil
}

// PullTract copies a tract from an existing tractserver.
func (h *TSCtlHandler) PullTract(req core.PullTractReq, reply *core.Error) error {
	op := h.opm.Start("PullTract")
	defer op.EndWithBlbError(reply)

	// Check failure service.
	if err := h.getFailure("PullTract"); err != core.NoError {
		log.Errorf("PullTract: failure service override, returning %s", err)
		*reply = err
		return nil
	}

	// Check pending request limit.
	if !h.pendingSem.TryAcquire() {
		op.TooBusy()
		log.Errorf("PullTract: too busy, rejecting req")
		return errBusy
	}
	defer h.pendingSem.Release()

	if !h.store.HasID(req.TSID) {
		log.Infof("PullTract: request has tsid %d, i have %d, rejecting", req.TSID, h.store.GetID())
		*reply = core.ErrWrongTractserver
		return nil
	}

	ctx := controlContext()
	*reply = h.store.PullTract(ctx, req.From, req.ID, req.Version)
	log.Infof("PullTract: req %+v reply %+v", req, *reply)

	return nil
}

// CheckTracts is a request from a curator to verify that we have the tracts the curator thinks we do.
func (h *TSCtlHandler) CheckTracts(req core.CheckTractsReq, reply *core.CheckTractsReply) error {
	op := h.opm.Start("CheckTracts")
	defer op.EndWithBlbError(&reply.Err)

	// Check failure service.
	if err := h.getFailure("CheckTracts"); err != core.NoError {
		log.Errorf("CheckTracts: failure service override, returning %s", err)
		*reply = core.CheckTractsReply{Err: err}
		return nil
	}

	// Check pending request limit.
	if !h.pendingSem.TryAcquire() {
		op.TooBusy()
		log.Errorf("CheckTracts: too busy, rejecting req")
		return errBusy
	}
	defer h.pendingSem.Release()

	if !h.store.HasID(req.TSID) {
		log.Infof("CheckTracts: request has tsid %d, i have %d, rejecting", req.TSID, h.store.GetID())
		*reply = core.CheckTractsReply{Err: core.ErrWrongTractserver}
		return nil
	}

	*reply = core.CheckTractsReply{Err: core.NoError}
	log.Infof("CheckTracts: req %d tracts reply %+v", len(req.Tracts), *reply)

	// Enqueue the check tracts request into the channel and the check will be
	// done by another goroutine.

	select {
	case h.store.checkTractsCh <- req.Tracts:

	case <-time.After(3 * time.Second):
		// Drop the check request after a certain timeout. This is fine given
		// curators will redo the check periodically.
		log.Errorf("Timeout on blocking on the check tracts channel, drop the CheckTracts request with %d tracts", len(req.Tracts))
		op.TooBusy()
	}

	return nil
}

// GCTract is a request to garbage collect the provided tracts as we don't need to store them anymore.
func (h *TSCtlHandler) GCTract(req core.GCTractReq, reply *core.Error) error {
	op := h.opm.Start("GCTract")
	defer op.EndWithBlbError(reply)

	if err := h.getFailure("GCTract"); err != core.NoError {
		log.Errorf("GCTract: failure service override, returning %s", err)
		*reply = err
		return nil
	}

	// Check pending request limit.
	if !h.pendingSem.TryAcquire() {
		op.TooBusy()
		log.Errorf("GCTract: too busy, rejecting req")
		return errBusy
	}
	defer h.pendingSem.Release()

	if !h.store.HasID(req.TSID) {
		log.Infof("GCTract: request has tsid %d, i have %d, rejecting", req.TSID, h.store.GetID())
		*reply = core.ErrWrongTractserver
		return nil
	}

	h.store.GCTracts(req.Old, req.Gone)
	*reply = core.NoError
	log.Infof("GCTract: req %+v reply %+v", req, *reply)
	return nil
}

// GetTSID returns the id of this tractserver. This is only used for testing
// purposes and not in production.
func (h *TSCtlHandler) GetTSID(req struct{}, reply *core.TractserverID) error {
	if id := h.store.GetID(); id.IsValid() {
		*reply = id
		return nil
	}
	return errors.New("unregistered tractserver")
}

// PackTracts instructs the tractserver to read a bunch of tracts from other
// tractservers and write them to one RS data chunk on the local tractserver.
func (h *TSCtlHandler) PackTracts(req core.PackTractsReq, reply *core.Error) error {
	op := h.opm.Start("PackTracts")
	defer op.EndWithBlbError(reply)

	if err := h.getFailure("PackTracts"); err != core.NoError {
		log.Errorf("PackTracts: failure service override, returning %s", err)
		*reply = err
		return nil
	}

	// Check pending request limit.
	if !h.pendingSem.TryAcquire() {
		op.TooBusy()
		log.Errorf("PackTracts: too busy, rejecting req")
		return errBusy
	}
	defer h.pendingSem.Release()

	if !h.store.HasID(req.TSID) {
		log.Infof("PackTracts: request has tsid %d, i have %d, rejecting", req.TSID, h.store.GetID())
		*reply = core.ErrWrongTractserver
		return nil
	}

	ctx := controlContext()
	*reply = h.store.PackTracts(ctx, req.Length, req.Tracts, req.ChunkID)
	log.Infof("PackTracts: req %+v reply %+v", req, *reply)

	return nil
}

// RSEncode instructs the tractserver to read a bunch of RS data chunks, perform
// the RS parity computation, and write out parity chunks to other tractservers.
func (h *TSCtlHandler) RSEncode(req core.RSEncodeReq, reply *core.Error) error {
	op := h.opm.Start("RSEncode")
	defer op.EndWithBlbError(reply)

	if err := h.getFailure("RSEncode"); err != core.NoError {
		log.Errorf("RSEncode: failure service override, returning %s", err)
		*reply = err
		return nil
	}

	// Check pending request limit.
	if !h.pendingSem.TryAcquire() {
		op.TooBusy()
		log.Errorf("RSEncode: too busy, rejecting req")
		return errBusy
	}
	defer h.pendingSem.Release()

	if !h.store.HasID(req.TSID) {
		log.Infof("RSEncode: request has tsid %d, i have %d, rejecting", req.TSID, h.store.GetID())
		*reply = core.ErrWrongTractserver
		return nil
	}

	ctx := controlContext()
	*reply = h.store.RSEncode(ctx, req.ChunkID, req.Length, req.Srcs, req.Dests, req.IndexMap)
	log.Infof("RSEncode: req %+v reply %+v", req, *reply)

	return nil
}

// CtlRead reads from a tract, bypassing request limits. CtlReads are not
// cancellable.
func (h *TSCtlHandler) CtlRead(req core.ReadReq, reply *core.ReadReply) error {
	op := h.opm.Start("CtlRead")
	defer op.EndWithBlbError(&reply.Err)

	if err := h.getFailure("CtlRead"); err != core.NoError {
		log.Errorf("CtlRead: failure service override, returning %s", err)
		*reply = core.ReadReply{Err: err}
		return nil
	}

	// Check pending request limit.
	if !h.pendingSem.TryAcquire() {
		op.TooBusy()
		log.Errorf("CtlRead: too busy, rejecting req")
		return errBusy
	}
	defer h.pendingSem.Release()

	ctx := controlContext()
	var b []byte
	b, reply.Err = h.store.Read(ctx, req.ID, req.Version, req.Len, req.Off)
	reply.Set(b, true)

	log.Infof("CtlRead: req %+v reply len %d Err %s", req, len(b), reply.Err)
	return nil
}

// CtlWrite does a write to a tract, bypassing request limits.
// Unlike regular Write, CtlWrite creates the tract when Off is zero.
func (h *TSCtlHandler) CtlWrite(req core.WriteReq, reply *core.Error) error {
	op := h.opm.Start("CtlWrite")
	defer op.EndWithBlbError(reply)

	if err := h.getFailure("CtlWrite"); err != core.NoError {
		log.Errorf("CtlWrite: failure service override, returning %s", err)
		*reply = err
		return nil
	}

	// Check pending request limit.
	if !h.pendingSem.TryAcquire() {
		op.TooBusy()
		log.Errorf("CtlWrite: too busy, rejecting req")
		return errBusy
	}
	defer h.pendingSem.Release()

	// Currently, we only support CtlWrite for RS chunks.
	if req.ID.Blob.Partition().Type() != core.RSPartition {
		*reply = core.ErrInvalidArgument
		return nil
	}

	// TODO: This feels a little messy. Is there a better way to do this?
	// (Without adding a whole new CtlCreate path?)
	ctx := controlContext()
	if req.Off == 0 {
		*reply = h.store.Create(ctx, req.ID, req.B, req.Off)
	} else {
		*reply = h.store.Write(ctx, req.ID, req.Version, req.B, req.Off)
	}

	lenB := len(req.B)
	rpc.PutBuffer(req.Get())

	log.Infof("CtlWrite: req ID %v Version %v len(B) %d, Off %d, reply %+v", req.ID, req.Version, lenB, req.Off, *reply)
	return nil
}

// CtlStatTract returns the size of a tract.
func (h *TSCtlHandler) CtlStatTract(req core.StatTractReq, reply *core.StatTractReply) error {
	op := h.opm.Start("CtlStatTract")
	defer op.EndWithBlbError(&reply.Err)

	// Check failure service.
	if err := h.getFailure("StatTract"); err != core.NoError {
		log.Errorf("StatTract: failure service override, returning %s", err)
		*reply = core.StatTractReply{Err: err}
		return nil
	}

	// Check pending request limit.
	if !h.pendingSem.TryAcquire() {
		op.TooBusy()
		log.Errorf("CtlStatTract: too busy, rejecting req")
		return errBusy
	}
	defer h.pendingSem.Release()

	ctx := controlContext()
	reply.Size, reply.ModStamp, reply.Err = h.store.Stat(ctx, req.ID, req.Version)

	log.Infof("CtlStatTract: req %+v reply %+v", req, *reply)
	return nil
}

func (h *TSCtlHandler) rpcStats() map[string]string {
	return h.opm.Strings(
		"CheckTracts",
		"GCTract",
		"PullTract",
		"SetVersion",
		"PackTracts",
		"RSEncode",
		"CtlRead",
		"CtlWrite",
		"CtlStatTract",
	)
}

// Return the error registered with the given operation 'op', if any.
func (h *TSCtlHandler) getFailure(op string) core.Error {
	if nil == h.opFailure {
		return core.NoError
	}
	return h.opFailure.Get(op)
}

//-----------------
// Service handler
//-----------------

// errBusy is returned if there are too many pending requests.
var errBusy = errors.New("the server is too busy to serve this request")

// TSSrvHandler handles all client requests.
type TSSrvHandler struct {
	// When failure service is enabled, what errors failed operations should return.
	opFailure *server.OpFailure

	// The server.
	server *Server

	// The actual data store.
	store *Store

	// The semaphore which is used to limit the number of pending requests.
	pendingSem server.Semaphore

	// Per-RPC info.
	opm *server.OpMetric

	inFlight *opTracker
}

// newTSSrvHandler creates a new TSSrvHandler.
func newTSSrvHandler(s *Server, opm *server.OpMetric) *TSSrvHandler {
	cfg := s.cfg
	handler := &TSSrvHandler{
		server:     s,
		store:      s.store,
		pendingSem: server.NewSemaphore(cfg.RejectReqThreshold),
		opm:        opm,
		inFlight:   newOpTracker(),
	}
	if cfg.UseFailure {
		handler.opFailure = server.NewOpFailure()
		if err := failures.Register("ts_service_failure", handler.opFailure.Handler); err != nil {
			log.Errorf("failed to register failure service: %s", err)
		}
	}
	return handler
}

// CreateTract creates a tract and writes data to it. Upon success, the tract
// can be further Read/Write'd, and will have version 1.
func (h *TSSrvHandler) CreateTract(req core.CreateTractReq, reply *core.Error) error {
	op := h.opm.Start("CreateTract")
	defer op.EndWithBlbError(reply)

	// Check failure service.
	if err := h.getFailure("CreateTract"); err != core.NoError {
		log.Errorf("Create: failure service override, returning %s", err)
		*reply = err
		return nil
	}

	// Check pending request limit.
	if !h.pendingSem.TryAcquire() {
		op.TooBusy()
		log.Errorf("CreateTract: too busy, rejecting req")
		return errBusy
	}
	defer h.pendingSem.Release()

	if !h.store.HasID(req.TSID) {
		log.Infof("CreateTract: request has tsid %d, i have %d, rejecting", req.TSID, h.store.GetID())
		*reply = core.ErrWrongTractserver
		return nil
	}

	ctx := contextWithPriority(context.Background(), mapPriority(req.Pri))
	*reply = h.store.Create(ctx, req.ID, req.B, req.Off)

	lenB := len(req.B)
	rpc.PutBuffer(req.Get())

	log.Infof("Create: req ID %v len(B) %d, Off %d, reply %+v", req.ID, lenB, req.Off, *reply)

	return nil
}

// Write does a write to a tract on this tractserver.
func (h *TSSrvHandler) Write(req core.WriteReq, reply *core.Error) error {
	op := h.opm.Start("Write")
	defer op.EndWithBlbError(reply)

	// Check failure service.
	if err := h.getFailure("Write"); err != core.NoError {
		log.Errorf("Write: failure service override, returning %s", err)
		*reply = err
		return nil
	}

	// Check pending request limit.
	if !h.pendingSem.TryAcquire() {
		op.TooBusy()
		log.Errorf("Write: too busy, rejecting req")
		return errBusy
	}
	defer h.pendingSem.Release()

	// Make sure the client can cancel this op by adding it to the in-flight table.
	ctx := h.inFlight.start(req.ReqID)
	if ctx == nil {
		log.Errorf("Write: new request w/existing ReqID, rejecting.  req: %+v", req)
		return errBusy
	}
	defer h.inFlight.end(req.ReqID)

	ctx = contextWithPriority(ctx, mapPriority(req.Pri))
	*reply = h.store.Write(ctx, req.ID, req.Version, req.B, req.Off)

	lenB := len(req.B)
	rpc.PutBuffer(req.Get())

	log.Infof("Write: req ID %v Version %v len(B) %d, Off %d, reply %+v", req.ID, req.Version, lenB, req.Off, *reply)
	return nil
}

// Read reads from a tract.
func (h *TSSrvHandler) Read(req core.ReadReq, reply *core.ReadReply) error {
	op := h.opm.Start("Read")
	defer op.EndWithBlbError(&reply.Err)

	// Check failure service.
	if err := h.getFailure("Read"); err != core.NoError {
		log.Errorf("Read: failure service override, returning %s", err)
		*reply = core.ReadReply{Err: err}
		return nil
	}

	// Check pending request limit.
	if !h.pendingSem.TryAcquire() {
		op.TooBusy()
		log.Errorf("Read: too busy, rejecting req")
		return errBusy
	}
	defer h.pendingSem.Release()

	// Make sure the client can cancel this op by adding it to the in-flight table.
	ctx := h.inFlight.start(req.ReqID)
	if ctx == nil {
		log.Errorf("Read: new request w/existing ReqID, rejecting.  req: %+v", req)
		return errBusy
	}
	defer h.inFlight.end(req.ReqID)

	// Actually do the op.
	ctx = contextWithPriority(ctx, mapPriority(req.Pri))
	var b []byte
	b, reply.Err = h.store.Read(ctx, req.ID, req.Version, req.Len, req.Off)
	reply.Set(b, true)

	log.Infof("Read: req %+v reply len %d Err %s", req, len(b), reply.Err)
	return nil
}

// StatTract returns the size of a tract.
func (h *TSSrvHandler) StatTract(req core.StatTractReq, reply *core.StatTractReply) error {
	op := h.opm.Start("StatTract")
	defer op.EndWithBlbError(&reply.Err)

	// Check failure service.
	if err := h.getFailure("StatTract"); err != core.NoError {
		log.Errorf("StatTract: failure service override, returning %s", err)
		*reply = core.StatTractReply{Err: err}
		return nil
	}

	// Check pending request limit.
	if !h.pendingSem.TryAcquire() {
		op.TooBusy()
		log.Errorf("StatTract: too busy, rejecting req")
		return errBusy
	}
	defer h.pendingSem.Release()

	ctx := contextWithPriority(context.Background(), mapPriority(req.Pri))
	reply.Size, reply.ModStamp, reply.Err = h.store.Stat(ctx, req.ID, req.Version)

	log.Infof("StatTract: req %+v reply %+v", req, *reply)
	return nil
}

// GetDiskInfo returns a summary of disk info in reply.
func (h *TSSrvHandler) GetDiskInfo(req core.GetDiskInfoReq, reply *core.GetDiskInfoReply) error {
	op := h.opm.Start("GetDiskInfo")
	defer op.EndWithBlbError(&reply.Err)

	// Check failure service.
	if err := h.getFailure("GetDiskInfo"); err != core.NoError {
		log.Errorf("GetDiskInfo: failure service override, returning %s", err)
		*reply = core.GetDiskInfoReply{Err: err}
		return nil
	}

	// Check pending request limit.
	if !h.pendingSem.TryAcquire() {
		op.TooBusy()
		log.Errorf("GetDiskInfo: too busy, rejecting req")
		return errBusy
	}
	defer h.pendingSem.Release()

	reply.Disks = h.server.getStatus()

	log.Infof("GetDiskInfo: req %+v reply %+v", req, *reply)
	return nil
}

// SetControlFlags changes control flags for a disk.
func (h *TSSrvHandler) SetControlFlags(req core.SetControlFlagsReq, reply *core.Error) error {
	op := h.opm.Start("SetControlFlags")
	defer op.EndWithBlbError(reply)

	// Check failure service.
	if err := h.getFailure("SetControlFlags"); err != core.NoError {
		log.Errorf("SetControlFlags: failure service override, returning %s", err)
		*reply = err
		return nil
	}

	// Check pending request limit.
	if !h.pendingSem.TryAcquire() {
		op.TooBusy()
		log.Errorf("SetControlFlags: too busy, rejecting req")
		return errBusy
	}
	defer h.pendingSem.Release()

	if req.Flags.DrainLocal > 0 {
		log.Errorf("SetControlFlags: DrainLocal not implemented yet")
		*reply = core.ErrInvalidArgument
	} else {
		*reply = h.store.SetControlFlags(req.Root, req.Flags)
	}

	log.Infof("SetControlFlags: req %+v reply %+v", req, *reply)
	return nil
}

// Cancel attempts to signal the pending operation identified by 'id' that it should be canceled.
func (h *TSSrvHandler) Cancel(id string, reply *core.Error) error {
	if h.inFlight.cancel(id) {
		*reply = core.NoError
	} else {
		*reply = core.ErrCancelFailed
	}
	return nil
}

func (h *TSSrvHandler) rpcStats() map[string]string {
	return h.opm.Strings(
		"CreateTract",
		"Read",
		"Write",
		"StatTract",
		"GetDiskInfo",
		"SetControlFlags",
	)
}

// Return the error registered with the given operation 'op', if any.
func (h *TSSrvHandler) getFailure(op string) core.Error {
	if h.opFailure == nil {
		return core.NoError
	}
	return h.opFailure.Get(op)
}

// opTracker keeps track of the contexts of active requests.
type opTracker struct {
	// Active requests.
	//
	// Maps from the client-supplied request ID to the function called to cancel the associated request.
	// Our current RPC mechanism doesn't allow access to the calling host:port, so we trust
	// the client to prepend their cancellation key with that.  This is a bit risky as clients
	// can cancel each other.
	active map[string]context.CancelFunc

	lock sync.Mutex
}

func newOpTracker() *opTracker {
	return &opTracker{active: make(map[string]context.CancelFunc)}
}

// start notes that we're starting a cancellable operation with id 'id'.
// returns 'nil' if the op is already running, in which case the caller should return an error 'up'.
func (s *opTracker) start(id string) context.Context {
	if len(id) == 0 {
		return context.Background()
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	if _, ok := s.active[id]; ok {
		log.Errorf("uh oh, duplicate op ID: %s", id)
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	s.active[id] = cancel
	return ctx
}

// cancel cancels the op with the provided ID.
//
// Returns true if the op exists and was told to cancel.
// Returns false if the op doesn't exist.
func (s *opTracker) cancel(id string) bool {
	if len(id) == 0 {
		return false
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	cancelFunc, ok := s.active[id]
	if ok {
		cancelFunc()
	}
	return ok
}

// end notes that we're ending a cancellable operation with id 'id'.
func (s *opTracker) end(id string) {
	if len(id) == 0 {
		return
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	if _, ok := s.active[id]; !ok {
		log.Errorf("programming error!  ending an op w/o a cancel func, id is %s", id)
	}
	delete(s.active, id)
}
