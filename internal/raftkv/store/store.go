// Copyright (c) 2016 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/westerndigitalcorporation/blb/internal/raftkv/db"
	"github.com/westerndigitalcorporation/blb/pkg/raft/raft"
	"github.com/westerndigitalcorporation/blb/pkg/raft/raftfs"
	"github.com/westerndigitalcorporation/blb/pkg/raft/raftrpc"
)

var (
	// ErrTimeout timeout error.
	ErrTimeout = errors.New("Timeout error")
	// ErrCAS represents a failed compare-and-swap operation.
	ErrCAS = errors.New("CAS failed")
)

const (
	dbFile    = "raft.db"
	dbFileTmp = "raft.db.tmp"
)

// Store is backed by Raft-replicated persistent database.
type Store struct {
	// Protects "leaderAddr" and "currentTerm".
	lock sync.Mutex
	// Address of the leader. Will be an empty string if the leader
	// is unknown.
	leaderAddr string
	// Current term (as notified by OnLeadershipChange).
	currentTerm uint64

	config Config

	// This lock is used to protect field 'db'. It's possible that
	// the store received a more up-to-date snapshot from leader so
	// we have to close current DB and restore from the received one.
	// This lock prevents all accesses to 'db' during the process of
	// close old DB -> open new DB.
	dbLock sync.RWMutex
	// The replicated state. Please note that the state is persisted
	// on disk and it's managed outside of Raft so it's possible that
	// Raft will ask the Store to apply entries or snapshots that are
	// already included in the state at startup time.
	db *db.DB

	raft *raft.Raft

	// These are touched only inside of Apply, which is guaranteed to be called
	// from a single goroutine by raft, so no locking is needed.
	checksumIdx uint64
	checksum    uint64

	// --- Metrics ---
	metricSnapshotLatency *prometheus.SummaryVec
}

// New creates a store object.
func New(config Config) *Store {
	sMgr, err := raftfs.NewFSSnapshotMgr(config.RaftDir)
	if err != nil {
		log.Fatalf("Failed to create snapshot manager: %v", err)
	}
	fState, err := raftfs.NewFSState(config.RaftDir)
	if err != nil {
		log.Fatalf("Failed to create FSState: %v", err)
	}

	storage := raft.NewStorage(sMgr, raft.NewFSLog(config.RaftDir, 10000), fState)
	tc := raft.TransportConfig{Addr: config.ID, MsgChanCap: 100}
	rpcc := raftrpc.RPCTransportConfig{SendTimeout: 1 * time.Second, RPCName: "MainRaft"}
	transport, err := raftrpc.NewRPCTransport(tc, rpcc)
	if err != nil {
		log.Fatalf("Failed to create RPC transport: %s", err)
	}
	raft := raft.NewRaft(config.Config, storage, transport)

	db := db.Open(filepath.Join(config.DBDir, dbFile))

	return &Store{
		raft:        raft,
		config:      config,
		db:          db,
		checksumIdx: ^uint64(0),
		metricSnapshotLatency: promauto.NewSummaryVec(prometheus.SummaryOpts{
			Subsystem: "raftkv",
			Name:      "snapshot_latency",
			Help:      "Latency of snapshot operations (save/restore)",
		}, []string{"op"}),
	}
}

// Start starts the store with given "members" as initial cluster
// configuration.
func (s *Store) Start(members []string) {
	s.raft.Start(s)
	s.raft.ProposeInitialMembership(members)
}

// Restart restarts the store from existing state on disk.
func (s *Store) Restart() {
	s.raft.Start(s)
}

// Join starts the store as a join member. It assumes someone will be
// responsible for issuing the reconfiguration request to leader.
func (s *Store) Join() {
	s.raft.Start(s)
}

// Propose proposes a command that will be replicated by Raft.
func (s *Store) Propose(cmd Command, timeout time.Duration) (res interface{}, err error) {
	pending := s.raft.Propose(cmdToBytes(cmd))

	select {
	case <-pending.Done:
		if pending.Err != nil {
			return pending.Err, nil
		}
	case <-time.After(timeout):
		return ErrTimeout, nil
	}

	return pending.Res, nil
}

// Get returns value of a given key.
func (s *Store) Get(key []byte, local bool, timeout time.Duration) ([]byte, error) {
	s.dbLock.RLock()
	defer s.dbLock.RUnlock()

	if !local {
		// Do a linearizable read.
		pending := s.raft.VerifyRead()
		select {
		case <-pending.Done:
			if pending.Err != nil {
				return nil, pending.Err
			}

		case <-time.After(timeout):
			return nil, ErrTimeout
		}
	}

	tx, err := s.db.RTxn()
	if err != nil {
		return nil, err
	}
	defer tx.Commit()
	return tx.Get(key), nil
}

// ListKeys returns all the keys in the store.
func (s *Store) ListKeys(prefix string, local bool, timeout time.Duration) ([][]byte, error) {
	s.dbLock.RLock()
	defer s.dbLock.RUnlock()

	if !local {
		// Do a linearizable read.
		pending := s.raft.VerifyRead()
		select {
		case <-pending.Done:
			if pending.Err != nil {
				return nil, pending.Err
			}

		case <-time.After(timeout):
			return nil, ErrTimeout
		}
	}

	tx, err := s.db.RTxn()
	if err != nil {
		return nil, err
	}
	defer tx.Commit()
	return tx.ListKeys([]byte(prefix)), nil
}

// AddNode adds a new node into Raft cluster.
func (s *Store) AddNode(node string) error {
	pending := s.raft.AddNode(node)
	<-pending.Done
	return pending.Err
}

// RemoveNode removes a node from Raft cluster.
func (s *Store) RemoveNode(node string) error {
	pending := s.raft.RemoveNode(node)
	<-pending.Done
	return pending.Err
}

// LeaderAddr returns address of leader. An empty string will be returned
// if the leader is unknown.
func (s *Store) LeaderAddr() string {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.leaderAddr
}

// IsStillTerm returns true if the current term (as far as we have been notified
// by raft) matches the given term.
func (s *Store) IsStillTerm(term uint64) bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	return term == s.currentTerm
}

// Apply implements fsm.Apply.
func (s *Store) Apply(ent raft.Entry) interface{} {
	s.dbLock.RLock()
	defer s.dbLock.RUnlock()

	// TODO: cache the index either at Store level or DB level.
	index, ok := s.db.Index()

	if ok && ent.Index <= index {
		// In recovery Raft will apply all entries that are not in snapshot to
		// FSM. The entries might have already been included in the on-disk state
		// because the snapshot might be staler than the state. Ignore these
		// entries in this case.
		return nil
	}

	container := bytesToCmd(ent.Cmd)
	log.Infof("%+v", container)

	// Checksum commands are read-only, so they don't need a RWTxn.
	switch cmd := container.Op.(type) {
	case ChecksumRequestCommand:
		return s.checksumRequest(cmd, ent.Index)
	case ChecksumVerifyCommand:
		s.checksumVerify(cmd)
		return nil
	}

	// Other commands may modify state.
	txn, err := s.db.RWTxn(ent.Index)
	if err != nil {
		log.Fatalf("Failed to get read-write transaction: %v", err)
		return nil
	}
	defer txn.Commit()

	switch cmd := container.Op.(type) {
	case PutCommand:
		log.Infof("Putting key: %q into DB", cmd.Key)
		txn.Put(cmd.Key, cmd.Value)
	case DelCommand:
		log.Infof("Deleting key: %q from DB", cmd.Key)
		txn.Delete(cmd.Key)
	case CASCommand:
		log.Infof("CAS the key of %q", cmd.Key)
		if v := txn.Get(cmd.Key); v != nil || !bytes.Equal(v, cmd.OldValue) {
			return ErrCAS
		}
		txn.Put(cmd.Key, cmd.NewValue)
	default:
		log.Fatalf("Unknown command type: %v", cmd)
	}

	return nil
}

// checksumRequest handles a checksum request command.
func (s *Store) checksumRequest(cmd ChecksumRequestCommand, index uint64) (res ChecksumResult) {
	tx, err := s.db.RTxn()
	if err != nil {
		log.Fatalf("Failed to get read transaction: %v", err)
	}
	res.NextKey, res.Checksum = tx.Checksum(cmd.StartKey, cmd.N)
	res.Index = index
	tx.Commit()

	// Store in volatile state and wait for the verify.
	s.checksumIdx = index
	s.checksum = res.Checksum
	return
}

// checksumVerify handles a checksum verify command. If there is a mismatch, it
// will kill the process by log.Fatalf.
func (s *Store) checksumVerify(cmd ChecksumVerifyCommand) {
	if s.checksumIdx != cmd.Index {
		// This may happen around node restarts or leader elections or membership changes.
		// It's harmless.
		log.Infof("Got checksum for wrong index: %v != %v", s.checksumIdx, cmd.Index)
	} else if s.checksum != cmd.Checksum {
		log.Fatalf("Consistency check failed! %v != %v", s.checksum, cmd.Checksum)
	}
}

// OnLeadershipChange implements fsm.OnLeadershipChange.
func (s *Store) OnLeadershipChange(b bool, term uint64, leader string) {
	s.lock.Lock()
	s.leaderAddr = leader
	s.currentTerm = term
	s.lock.Unlock()

	if b {
		log.Infof("%q becomes a leader for term %d", s.config.ID, term)
		// This goroutine will exit when the current term changes, so we can
		// start a new one each time we become leader.
		go s.proposeChecksums(term)
	}
}

// OnMembershipChange implements fsm.OnMembershipChange.
func (s *Store) OnMembershipChange(membership raft.Membership) {
	log.Infof("Membership changed: %+v", membership)
}

// Snapshot implements fsm.Snapshot.
func (s *Store) Snapshot() (raft.Snapshoter, error) {
	s.dbLock.RLock()

	snap, err := s.db.GetSnapshot()
	if err != nil {
		return nil, err
	}

	return &raftkvSnapshoter{
		metric:   s.metricSnapshotLatency.WithLabelValues("save"),
		snapshot: snap,
		lock:     &s.dbLock,
	}, nil
}

// proposeChecksums runs when this is the leader. It periodically proposes
// checksum request commands and then verify commands.
func (s *Store) proposeChecksums(thisTerm uint64) {
	n := s.config.ChecksumBatchSize
	var nextKey []byte

	for _ = range time.NewTicker(s.config.ChecksumInterval).C {
		// This goroutine is scoped to one leadership term. If the term has
		// changed, exit.
		if !s.IsStillTerm(thisTerm) {
			return
		}

		// First ask everyone (including ourself) to compute a checksum over a
		// subset of their state.
		cmd := Command{Op: ChecksumRequestCommand{
			StartKey: nextKey,
			N:        n,
		}}
		res, err := s.Propose(cmd, 10*time.Second)
		if err != nil {
			log.Infof("Error proposing checksum command: %v", err)
			continue
		}

		// Now ask everyone (who saw the previous command) to match theirs
		// against ours.
		ckRes := res.(ChecksumResult)
		cmd = Command{Op: ChecksumVerifyCommand{
			Index:    ckRes.Index,
			Checksum: ckRes.Checksum,
		}}
		_, err = s.Propose(cmd, 10*time.Second)
		if err != nil {
			log.Infof("Error proposing checksum verify command: %v", err)
			continue
		}

		// If we got this far, we can move to the next key range.
		nextKey = ckRes.NextKey
	}
}

type raftkvSnapshoter struct {
	metric   prometheus.Observer
	snapshot *db.Snapshot
	lock     *sync.RWMutex
}

func (s *raftkvSnapshoter) Release() {
	if err := s.snapshot.Done(); err != nil {
		log.Fatalf("Failed to release snapshot: %v", err)
	}
	s.lock.RUnlock()
}

func (s *raftkvSnapshoter) Save(writer io.Writer) error {
	log.Infof("Save is called.")

	// The on-disk state can be very large so we'd better compress
	// it.
	writer = snappy.NewBufferedWriter(writer)
	defer writer.(*snappy.Writer).Flush()

	defer addDurationSince(s.metric, time.Now())

	// Get the index of the last applied transaction to the DB.
	index, ok := s.snapshot.Index()
	if !ok {
		index = 0
	}

	// Serialize the last applied index to snapshot.
	binary.Write(writer, binary.LittleEndian, index)

	// Write DB state to snapshot.
	_, err := s.snapshot.WriteTo(writer)
	return err
}

// SnapshotRestore implements fsm.SnapshotRestore.
func (s *Store) SnapshotRestore(reader io.Reader, lastIndex, lastTerm uint64) {
	log.Infof("SnapshotRestore is called.")
	defer addDurationSince(s.metricSnapshotLatency.WithLabelValues("restore"), time.Now())

	reader = snappy.NewReader(reader)

	// Read out the serialized index of the snapshot. We could use "lastIndex"
	// passed by Raft, but Raft internally will propose commands(e.g.
	// VerifyLeader) to bump "lastIndex" so it's possible that even "lastIndex"
	// is larger than the last applied index of on-disk state but the snapshot is
	// still staler than the on-disk state. That's why we serialize the last applied
	// index of on-disk state to snapshot file ourselves.
	var snapIndex uint64
	binary.Read(reader, binary.LittleEndian, &snapIndex)
	log.Infof("Last applied index to snaphsot is %d", snapIndex)

	index, ok := s.db.Index()
	if ok && index >= snapIndex {
		// The snapshot might be staler than on-disk state. If so, skip it.
		log.Infof("On-disk state(%d) is more recent than snapshot(%d), skip.", index, lastIndex)
		return
	}

	dbDir := s.config.DBDir

	// Create a temporary file to store received snapshot.
	file, err := os.Create(filepath.Join(dbDir, dbFileTmp))
	if err != nil {
		log.Fatalf("Failed to create a temporary file: %v", err)
	}

	w, err := io.Copy(file, reader)
	if err != nil {
		log.Fatalf("Failed to restore form snapshot: %v", err)
	}

	// Hold the lock so all other accesses to db will be blocked.
	s.dbLock.Lock()
	defer s.dbLock.Unlock()

	// Close current DB.
	s.db.Close()

	// Make received DB effective.
	if err := os.Rename(filepath.Join(dbDir, dbFileTmp), filepath.Join(dbDir, dbFile)); err != nil {
		log.Fatalf("Failed to rename: %v", err)
	}

	// Reopen the DB with restored snapshot.
	s.db = db.Open(filepath.Join(dbDir, dbFile))
	log.Infof("Successfully restored from a snapshot file(size: %d)", w)
}

func addDurationSince(m prometheus.Observer, start time.Time) {
	m.Observe(time.Since(start).Seconds())
}
