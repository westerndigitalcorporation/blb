// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package raft

import (
	"crypto/rand"
	"encoding/binary"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	log "github.com/golang/glog"
)

// We use type "int32" to represent Raft state because we want to use atomic
// operations exposed by atomic package.
const (
	raftNotStarted int32 = iota
	raftStarted
)

// pendingEntry wraps an entry and a pending so once the "entry" is concluded the
// corresponding "pending" can be signaled.
type pendingEntry struct {
	entry   Entry    // The actual entry.
	pending *Pending // The pending object that should be signaled once the entry gets committed and applied.
}

// Represents one reconfiguration change (add/remove one server).
type reconfigChange struct {
	// Node ID.
	node string
	// True if the change is adding one node, false if it's removing a node.
	isAdd bool
}

// Represents an initial member configuration.
type initialConfigChange struct {
	Membership
}

// Raft implements a Raft node in a consensus group.
type Raft struct {
	transport    Transport         // Used to communicate with other nodes.
	storage      *Storage          // Used to persist/retrieve Raft's durable state.
	core         *core             // Raft core layer.
	propCh       chan pendingEntry // channel for command proposals from application.
	verifyReadCh chan *Pending     // channel for read-verify requests.
	reconfigCh   chan *Pending     // channel for initial/reconfiguration requests.
	config       Config            // Configurations of this node.
	fsm          FSM               // State machine implementation.
	fsmLoop      *fsmLoop          // The goroutine that interacts with FSM.
	snapLoop     *snapshotLoop     // The goroutine that takes snapshot.
	isStarted    int32             // Is the node started?

	// --- Metrics we collect ---
	metricProposeReqs    prometheus.Counter
	metricVerifyReadReqs prometheus.Counter
	// The index of last command that is applied to the state of a FSM(either
	// from a committed command or snapshot). We can use it to check if a node's
	// state is sufficiently up-to-date.
	metricAppliedIndex prometheus.Gauge
	// Latencies about committed user commands. Raft might commit commands internally
	// and these commands are not included. Will only be recorded by leader node.
	metricCommitsLats prometheus.Observer
	// Latencies about committed read requests. Note that read requests will not go to
	// Raft log even though we use word "commit" here. Will only be recorded by
	// leader node.
	metricReadsLats prometheus.Observer
	// Current state(stateLeader, stateFollower or stateCandidate).
	metricState prometheus.Gauge
	// Metrics about snapshot.
	metricSnapSave     prometheus.Counter
	metricSnapRestore  prometheus.Counter
	metricSnapshotSize prometheus.Gauge // Size of last taken snapshot(in bytes).
	// The two metrics below are measured in unit of millisecond.
	metricSnapSaveDuration    prometheus.Gauge // How long did last snapshot take?
	metricSnapRestoreDuration prometheus.Gauge // How long did it take to restore from last snapshot?
}

var (
	// Global metric sets for all rafts in this process.
	metricCurrentTermVec = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "raft",
		Name:      "current_term",
	}, []string{"cluster"})
	metricProposeReqsVec = promauto.NewCounterVec(prometheus.CounterOpts{
		Subsystem: "raft",
		Name:      "reqs_propose",
	}, []string{"cluster"})
	metricVerifyReadReqsVec = promauto.NewCounterVec(prometheus.CounterOpts{
		Subsystem: "raft",
		Name:      "reqs_verify_read",
	}, []string{"cluster"})
	metricAppliedIndexVec = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "raft",
		Name:      "applied_index",
	}, []string{"cluster"})
	metricStateVec = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "raft",
		Name:      "state",
	}, []string{"cluster"})
	metricCommitsLatsVec = promauto.NewSummaryVec(prometheus.SummaryOpts{
		Subsystem: "raft",
		Name:      "commits_lats",
	}, []string{"cluster"})
	metricReadsLatsVec = promauto.NewSummaryVec(prometheus.SummaryOpts{
		Subsystem: "raft",
		Name:      "reads_lats",
	}, []string{"cluster"})
	metricSnapSaveVec = promauto.NewCounterVec(prometheus.CounterOpts{
		Subsystem: "raft",
		Name:      "snap_save_count",
	}, []string{"cluster"})
	metricSnapRestoreVec = promauto.NewCounterVec(prometheus.CounterOpts{
		Subsystem: "raft",
		Name:      "snap_restore_count",
	}, []string{"cluster"})
	metricSnapSaveDurationVec = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "raft",
		Name:      "snap_save_duration",
	}, []string{"cluster"})
	metricSnapRestoreDurationVec = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "raft",
		Name:      "snap_restore_duration",
	}, []string{"cluster"})
	metricSnapshotSizeVec = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "raft",
		Name:      "snapshot_size",
	}, []string{"cluster"})
)

// NewRaft creates a Raft node. You have to call "Raft.Start/Restart/Join" before
// you can use it.
func NewRaft(config Config, storage *Storage, transport Transport) *Raft {
	if err := validateConfig(config); err != nil {
		log.Fatalf("Failed to validate Config object: %s", err)
		return nil
	}

	// Wrap the current State implementation into "metricState" so we can update
	// the metric of current term when it gets updated.
	metricCurrentTerm := metricCurrentTermVec.WithLabelValues(config.ClusterID)
	storage.State = &metricState{State: storage.State, m: metricCurrentTerm}

	return &Raft{
		transport:    transport,
		storage:      storage,
		config:       config,
		propCh:       make(chan pendingEntry, 1024),
		verifyReadCh: make(chan *Pending, 1024),
		reconfigCh:   make(chan *Pending, 1),
		isStarted:    raftNotStarted,

		// All the metrics
		metricProposeReqs:         metricProposeReqsVec.WithLabelValues(config.ClusterID),
		metricVerifyReadReqs:      metricVerifyReadReqsVec.WithLabelValues(config.ClusterID),
		metricAppliedIndex:        metricAppliedIndexVec.WithLabelValues(config.ClusterID),
		metricState:               metricStateVec.WithLabelValues(config.ClusterID),
		metricCommitsLats:         metricCommitsLatsVec.WithLabelValues(config.ClusterID),
		metricReadsLats:           metricReadsLatsVec.WithLabelValues(config.ClusterID),
		metricSnapSave:            metricSnapSaveVec.WithLabelValues(config.ClusterID),
		metricSnapRestore:         metricSnapRestoreVec.WithLabelValues(config.ClusterID),
		metricSnapSaveDuration:    metricSnapSaveDurationVec.WithLabelValues(config.ClusterID),
		metricSnapRestoreDuration: metricSnapRestoreDurationVec.WithLabelValues(config.ClusterID),
		metricSnapshotSize:        metricSnapshotSizeVec.WithLabelValues(config.ClusterID),
	}
}

// Start starts a new Raft node. If there is existing state on disk, that will
// be used to rejoin an existing cluster. Otherwise it will wait for an initial
// configuration.
func (r *Raft) Start(fsm FSM) {
	r.start(fsm)
}

// ProposeInitialMembership sets an initial membership for the cluster. Raft
// must be in a clean state to do this. The intended use is to call this on one
// node in the cluster, and that one will initialize the others. However, it is
// safe to propose the same initial membership on all the members at once.
func (r *Raft) ProposeInitialMembership(members []string) *Pending {
	if atomic.LoadInt32(&r.isStarted) == raftNotStarted {
		log.Fatalf("Raft hasn't started yet.")
	}

	// Pick a random epoch.
	var buf [8]byte
	rand.Read(buf[:])
	epoch := binary.LittleEndian.Uint64(buf[:])

	pending := &Pending{
		Done: make(chan struct{}, 1),
		ctx:  initialConfigChange{Membership{Members: members, Epoch: epoch}},
	}
	r.reconfigCh <- pending
	return pending
}

// Propose proposes a new command to the consensus group. The command will be
// proposed only if this node is the leader of the group. A Pending object
// will be returned so applications can block on it until the command concludes.
// Here "conlude" means either an error happens or the command is applied to state
// machine successfully.
// Also there's no timeout associated with a pending object. In the case of
// network partition or nodes failures that leader can not reach a quorum,
// the pending object might be blocked indefinitely, it's the applications'
// responsibilities to implement timeout. But a timeout on application side
// doesn't necessarily mean the command will be discarded by Raft, the command
// might still be applied to state machine eventually so applications should be
// careful about retrying requests which are either returned as an error
// "ErrNotLeaderAnymore" or "timeout" if their requests are not idempotent,
// given retrying their requests might cause a command to be applied to their
// state machine multiple times.
// Errors:
//   ErrNodeNotLeader
//     - If the node is not a leader.
//   ErrNotLeaderAnymore
//     - If the node was the leader when received the command, but stepped down
//       before the command gets committed to state. Please note when this error
//       is returned it's still possible that the command will be applied or has
//       been applied to applications.
func (r *Raft) Propose(cmd []byte) *Pending {
	return r.ProposeIfTerm(cmd, 0)
}

// ProposeIfTerm is similar to 'Propose', the only difference is the command
// will not get proposed if it can't have a term number which matches the term
// number specified by users. If term is 0, then terms are not required to match.
// Errors:
//   ErrNodeNotLeader
//     - If the node is not a leader.
//   ErrNotLeaderAnymore
//     - If the node was the leader when received the command, but stepped down
//       before the command gets committed to state. Please note when this error
//       is returned it's still possible that the command will be applied or has
//       been applied to applications.
//   ErrTermMismatch
//     - If the command can't get the term that's specified by users.
func (r *Raft) ProposeIfTerm(cmd []byte, term uint64) *Pending {
	if atomic.LoadInt32(&r.isStarted) == raftNotStarted {
		log.Fatalf("Raft hasn't started yet.")
	}
	r.metricProposeReqs.Inc()
	pending := &Pending{
		Done: make(chan struct{}, 1),
		term: term,
		// Records the timestamp that the request gets enqueued.
		start: time.Now(),
	}
	r.propCh <- pendingEntry{Entry{Type: EntryNormal, Cmd: cmd}, pending}
	return pending
}

// AddNode adds a new node to the cluster. A Pending object will be returned so
// applications can block until the request concludes.
// Errors:
//   ErrNodeNotLeader:
//     - If the node is not a leader.
//   ErrNotLeaderAnymore
//     - If the node was the leader when received the reuqest, but stepped down
//       before the reconfiguration gets committed. Please note when this error
//       is returned it's still possible that the reconfiguration will be
//       committed or has been committed.
//   ErrNodeExists:
//     - If the node to be added already exists in the current configuration.
//   ErrTooManyReqs:
//     - If the latest configuration has not been committed yet. We only allow
//       one pending reconfiguration at a time. You have to wait for the commit
//       of previous reconfiguration before issuing a new one.
func (r *Raft) AddNode(node string) *Pending {
	if atomic.LoadInt32(&r.isStarted) == raftNotStarted {
		log.Fatalf("Raft hasn't started yet.")
	}
	pending := &Pending{
		Done: make(chan struct{}, 1),
		ctx:  reconfigChange{node: node, isAdd: true},
	}
	r.reconfigCh <- pending
	return pending
}

// RemoveNode removes an existing node from the cluster. A Pending object will
// be returned so applications can block until the request concludes.
// Errors:
//   ErrNodeNotLeader:
//     - If the node is not a leader.
//   ErrNotLeaderAnymore
//     - If the node was the leader when received the reuqest, but stepped down
//       before the reconfiguration gets committed. Please note when this error
//       is returned it's still possible that the reconfiguration will be
//       committed or has been committed.
//   ErrNodeNotExists:
//     - If the node to be removed doesn't exist in the current configuration.
//   ErrTooManyReqs:
//     - If the latest configuration has not been committed yet. We only allow
//       one pending reconfiguration at a time. You have to wait for the commit
//       of previous reconfiguration before issuing a new one.
func (r *Raft) RemoveNode(node string) *Pending {
	if atomic.LoadInt32(&r.isStarted) == raftNotStarted {
		log.Fatalf("Raft hasn't started yet.")
	}
	pending := &Pending{
		Done: make(chan struct{}, 1),
		ctx:  reconfigChange{node: node, isAdd: false},
	}
	r.reconfigCh <- pending
	return pending
}

// VerifyRead verifies, at the time this method is called, that (1) the node is
// leader and there's no leader with a higher term exists, and (2) the node's
// state is at least as up-to-date as any previous leaders.
//
// Once the two requirements are met one can serve read requests from its
// local state in a linearizable way because it can ensure its state is
// up-to-date at the time VerifyRead method is called. Doing so can guarantee
// linearizability because by definition of linearizability means each operation
// appears to take effect atomically at some point between its invocation and
// completion, and we can guarantee the read response reflects a state at some
// point between VerifyRead method is called and the returned Pending object
// concludes.
//
// A Pending object will be returned and applications can block on it until
// the command conludes. If the command concludes with no error then
// applications can serve read requests from local state in a linearizable
// way. Otherwise linearizability of reading from local state is not
// guaranteed. There're two types of errors might be returned:
// Errors:
//   ErrNodeNotLeader:
//     - If the node is not a leader.
//   ErrNotLeaderAnymore
//     - If the node was the leader when received the reuqest, but stepped down
//       before the request has been verified.
func (r *Raft) VerifyRead() *Pending {
	if atomic.LoadInt32(&r.isStarted) == raftNotStarted {
		log.Fatalf("Raft hasn't started yet.")
	}
	r.metricVerifyReadReqs.Inc()
	pending := &Pending{
		Done: make(chan struct{}, 1),
		// Records the timestamp that the request gets enqueued.
		start: time.Now(),
	}
	r.verifyReadCh <- pending
	return pending
}

func (r *Raft) start(fsm FSM) {
	if !atomic.CompareAndSwapInt32(&r.isStarted, raftNotStarted, raftStarted) {
		log.Fatalf("Raft node already started")
	}

	r.core = newCore(r.config, r.storage)
	r.fsm = fsm
	r.snapLoop = &snapshotLoop{
		// The channel have capacity of one because:
		// 1. The enqueue operation should be non-blocking.
		// 2. We only do one snapshot at a time so it doesn't make sense to have
		//    multiple snapshot requests in queue.
		snapReqCh:      make(chan *snapshotReq, 1),
		snapshotDoneCh: make(chan SnapshotFileWriter, 1),
		snapMgr:        r.storage.SnapshotManager,
		// These metrics will be shared with snapshotLoop.
		metricSnapSave:         r.metricSnapSave,
		metricSnapSaveDuration: r.metricSnapSaveDuration,
		metricSnapshotSize:     r.metricSnapshotSize,
	}
	r.fsmLoop = &fsmLoop{
		fsmCh:             make(chan interface{}, 1),
		snapshotThreshold: r.config.SnapshotThreshold,
		fsm:               r.fsm,
		snapReqCh:         r.snapLoop.snapReqCh,
		// These metrics will be shared with fsmLoop.
		metricAppliedIndex:        r.metricAppliedIndex,
		metricSnapRestore:         r.metricSnapRestore,
		metricSnapRestoreDuration: r.metricSnapRestoreDuration,
	}
	go r.snapLoop.run()
	go r.fsmLoop.run()
	go r.coreLoop()
}

// coreLoop is the only goroutine that talks to 'core' layer.
func (r *Raft) coreLoop() {
	// Start timer.
	timerC := time.Tick(r.config.DurationPerTick)

	// It's possible that there's a snapshot file already when a node restarts,
	// restore from it first.
	r.maybeRestoreFromSnapshot()

	for {
		if r.core.state.name() == stateLeader {
			r.runLeader(timerC)
		} else {
			r.runNonLeader(timerC)
		}
	}
}

// runNonLeader keeps running the Raft until the node becomes leader.
func (r *Raft) runNonLeader(timerC <-chan time.Time) {
	log.Infof("Start running non-leader loop...")
	// First notify FSM loop that it's not in leader state.
	r.fsmLoop.fsmCh <- leadershipUpdate{
		isLeader: false,
		term:     r.storage.GetCurrentTerm(),
		leader:   r.core.leaderID,
	}

	// Record current Raft state in the metric.
	curState := r.core.state.name()
	r.metricState.Set(float64(stateToInt(curState)))
	// Record current leader. It might be an empty string if the leader is
	// unknown.
	curLeader := r.core.leaderID

	for r.core.state.name() != stateLeader {
		if r.core.state.name() != curState {
			// We stay in this loop as long the node is in non-leader state, but state
			// transition may still happen between candidate and follower states. We use
			// 'curState' to track state change between these two and update the metric
			// accordingly.
			curState = r.core.state.name()
			r.metricState.Set(float64(stateToInt(curState)))
		}
		if curLeader != r.core.leaderID {
			// Once the leader has been changed we should ask fsmLoop to notify
			// application as well.
			curLeader = r.core.leaderID
			r.fsmLoop.fsmCh <- leadershipUpdate{
				isLeader: false,
				term:     r.storage.GetCurrentTerm(),
				leader:   r.core.leaderID,
			}
		}

		commits := r.core.TakeNewlyCommitted()
		// If there's new snapshot received, restore from it.
		r.maybeRestoreFromSnapshot()

		// Handles committed entries from core layer.
		if len(commits) != 0 {
			commitTuples := make([]commitTuple, 0, len(commits))
			for _, commit := range commits {
				// We don't need to pair the committed entry with its corresponding pending
				// object becasue non-leader node can't propose.
				commitTuples = append(commitTuples, commitTuple{entry: commit})
			}
			// Notify the FSM loop about newly committed entries.
			r.fsmLoop.fsmCh <- commitsUpdate{commits: commitTuples}
		}

		r.sendMsgs()

		// We use two "selects" to prioritize timer channel because we want timer
		// channel to be triggered at a fixed interval without being affected by
		// other channels.
		select {
		case <-timerC:
			r.core.Tick()
			continue

		default:
			// Fall through
		}

		select {
		// Monitor incoming message channel.
		case msg := <-r.transport.Receive():
			r.core.HandleMsg(msg)

		case <-timerC:
			r.core.Tick()

		case writer := <-r.snapLoop.snapshotDoneCh:
			r.fsmSnapshotDone(writer)

		case pendingEnt := <-r.propCh:
			pendingEnt.pending.conclude(nil, ErrNodeNotLeader)

		case pending := <-r.verifyReadCh:
			pending.conclude(nil, ErrNodeNotLeader)

		case pending := <-r.reconfigCh:
			if init, ok := pending.ctx.(initialConfigChange); ok {
				err := r.core.proposeInitialMembership(init.Membership)
				pending.conclude(nil, err)
			} else {
				// As a follower, we can't process anything other than initial
				// configurations.
				pending.conclude(nil, ErrNodeNotLeader)
			}
		}
	}
}

// runLeader keeps running the Raft until the node steps down as leader of current term.
func (r *Raft) runLeader(timerC <-chan time.Time) {
	log.Infof("Start running leader loop...")
	// First notify FSM loop that it's in leader state.
	r.fsmLoop.fsmCh <- leadershipUpdate{
		isLeader: true,
		term:     r.storage.GetCurrentTerm(),
		leader:   r.core.leaderID,
	}
	r.metricState.Set(float64(stateToInt(stateLeader)))

	// Once a new leader is elected, it will propose a NOP command to commit all
	// entries of previous terms. This is not mentioned in Raft paper but it is
	// mentioned in (1) paper "Raft Reload: Do We Have Consensus"
	// (http://www.cl.cam.ac.uk/~ms705/pub/papers/2015-osr-raft.pdf) (section 4.4)
	// and (2) the newest Raft dissertation (section 6.4).
	//
	// Doing this solves two problems:
	//
	// (1). Avoid the livelock mentioned in paper (1), commands were blocked by the
	//      extra condition of commitment, these commands can not be committed
	//      unless some command in new term gets committed.
	//
	// (2). Implementing linearizable read, which is mentioned in Raft dissertation.
	//      Elected leader is guaranteed to have all committed commands in its log,
	//      but this doesn't mean the elected leader has applied all committed
	//      commands to its state machine. So in order to find out, the elected
	//      leader needs to propose a NOP command to its state machine and as soon
	//      as this no-op entry is committed, the leader's commit index will be at
	//      least as large as any other server's during its term.
	//
	//
	// We'll propose a NOP command and wait until it gets applied before entering the
	// loop below. The reason we'll wait until it gets applied because I think it
	// will make the logic clearer for following reasons:
	//
	//  - Once the NOP command of current term gets applied the node must have a
	//    state that is at least as up-to-date as any previous leaders, this makes
	//    the logic of linearizable read simpler.
	//
	//  - Once it's applied every commands committed later in this loop must be
	//    proposed by this node in current term, makes us easier to reason the code.
	//
	r.proposeNopAndWaitApplied(timerC)

	// Used to track commands that have been proposed but not committed in current term.
	var pendingCommands []concluder
	// Pending reconfiguration request
	var pendingReconfig *Pending

	// NOTE some invariants:
	//
	// 1. We can only accept(thus propose them) requests from clients after
	//    entering this loop.
	//
	// 2. Because we have committed and applied the NOP command proposed at
	//    the beginning of current term, all entries committed in this loop
	//    must be proposed by this node after entering the loop.
	//
	// 3. Entries proposed in this loop must have the same term(the current term
	//    of the node).
	//
	// 4. Entries are committed in the order they were proposed.
	//
	// 5. This is in leader state, it will never restore from a snapshot in
	//    this loop. (A node will only restore from snapshot at startup
	//    time or receives a snapshot from a leader). Which means if the
	//    node proposes an entry to Raft log and then applies it to its state
	//    in this loop, it must come from the log instead of the snapshot.
	//
	for r.core.state.name() == stateLeader {
		// See if there're any entries were committed in this round.
		commits := r.core.TakeNewlyCommitted()

		if len(commits) != 0 {
			commitTuples := make([]commitTuple, 0, len(commits))

			// Get the commit time.
			now := time.Now()

			for _, commit := range commits {
				switch commit.Type {
				case EntryNormal, EntryNOP:
					// We commit commands in a FIFO order in each term, so the committed
					// command must correspond to the first pending object in the queue.
					first := pendingCommands[0]
					pendingCommands = pendingCommands[1:]
					commitTuples = append(commitTuples, commitTuple{entry: commit, pending: first})
					if commit.Type == EntryNormal {
						// It's user command. Updates the metric of commands commit latency.
						r.metricCommitsLats.Observe(float64(now.Sub(first.(*Pending).start)) / 1e9)
					} else {
						// It's read requests. Updates the metric of reads latency.
						for _, p := range first.(pendingGroup) {
							r.metricReadsLats.Observe(float64(now.Sub(p.start)) / 1e9)
						}
					}

				case EntryConf:
					commitTuples = append(commitTuples, commitTuple{
						entry:   commit,
						pending: pendingReconfig})
					pendingReconfig = nil

				default:
					log.Fatalf("Unexpected type of entry: %+v", commit)
				}
			}
			// Notify the FSM loop about newly committed entries.
			r.fsmLoop.fsmCh <- commitsUpdate{commits: commitTuples}
		}

		r.sendMsgs()

		// We use two "selects" to prioritize timer channel because we want timer
		// channel to be triggered at a fixed interval without being affected by
		// other channels.
		select {
		// Simulate real time.
		case <-timerC:
			r.core.Tick()
			continue

		default:
			// Fall through
		}

		select {
		// Monitor incoming message channel.
		case msg := <-r.transport.Receive():
			r.core.HandleMsg(msg)

			// Simulate real time.
		case <-timerC:
			r.core.Tick()

		case writer := <-r.snapLoop.snapshotDoneCh:
			r.fsmSnapshotDone(writer)

		case pendingEnt := <-r.propCh:
			batchedProposal := []pendingEntry{pendingEnt}
			// Take out a batch of proposals and process them at once.
			batchedProposal = r.takeBatchedProposals(len(pendingCommands), batchedProposal)
			// Go through batched proposals, reject those with unmatched terms(with
			// exception for term number 0).
			entries := make([]Entry, 0, len(batchedProposal))
			for _, pendingEnt := range batchedProposal {
				if pendingEnt.pending.term != 0 &&
					pendingEnt.pending.term != r.storage.GetCurrentTerm() {
					// The command can't get the term required by users, we have to
					// reject it.
					pendingEnt.pending.conclude(nil, ErrTermMismatch)
					continue
				}
				pendingCommands = append(pendingCommands, pendingEnt.pending)
				entries = append(entries, pendingEnt.entry)
			}
			if len(entries) == 0 {
				// It's possible that all entries were rejected.
				continue
			}
			if err := r.core.Propose(entries...); err != nil {
				// Leader should not fail on proposing entries.
				// Leaving leader state can be only triggered by receiving a message
				// with a higher term(HandleMsg) or timeout(Tick) on leader side.
				log.Fatalf("bug: Propose is not supposed to return error %v", err)
			}

		case pending := <-r.verifyReadCh:
			// We'll drain all verify request requests in the channel and issue one
			// NOP command for them.
			//
			// TODO:  this is a temporary hack, should be optimized.
			var group pendingGroup
			group = append(group, pending)
		drain:
			for {
				select {
				case pending = <-r.verifyReadCh:
					group = append(group, pending)
				default:
					break drain
				}
			}
			// When the NOP command gets committed and applied, the whole group of
			// pending objects will be concluded.
			pendingCommands = append(pendingCommands, group)
			if err := r.core.Propose(Entry{Type: EntryNOP}); err != nil {
				log.Fatalf("bug: Propose is not supposed to return error %v", err)
			}

		case pending := <-r.reconfigCh:
			if pendingReconfig != nil {
				// Only allow one reconfiguration at a time.
				pending.conclude(nil, ErrTooManyPendingReqs)
				continue
			}
			if _, ok := pending.ctx.(initialConfigChange); ok {
				// If we're the leader, we must already have a configuration, so
				// we can't process another one.
				pending.conclude(nil, ErrAlreadyConfigured)
				continue
			}
			req := pending.ctx.(reconfigChange)
			var err error
			if req.isAdd {
				err = r.core.AddNode(req.node)
			} else {
				err = r.core.RemoveNode(req.node)
			}
			if err != nil {
				pending.conclude(nil, err)
				continue
			}
			pendingReconfig = pending
		}
	}

	log.Infof("Leaving leader state...")
	// The pending commands might be either committed or truncated in future.
	// For simplicity, we are pessimistic here and just return an error to
	// applications.
	for _, pending := range pendingCommands {
		pending.conclude(nil, ErrNotLeaderAnymore)
	}
	if pendingReconfig != nil {
		pendingReconfig.conclude(nil, ErrNotLeaderAnymore)
	}
}

// This method will be called once the node is elected as leader. This method will
// propose a NOP command as the first command of this term and wait until it gets
// applied(thus all commands of previous terms have already been applied). During
// the call the node will not process any requests from clients.
//
// It returns either the NOP command in current term gets committed and applied
// or the node steps down as leader. If the NOP command gets applied then the
// node must have a state that is at least as up-to-date as any previous leaders
// once the call returns.
func (r *Raft) proposeNopAndWaitApplied(timerC <-chan time.Time) {
	var pendingNOP *Pending

	// Propose a NOP command once it becomes a leader.
	log.V(10).Infof("Going to propose a NOP command")
	if err := r.core.Propose(Entry{Type: EntryNOP}); err != nil {
		log.Fatalf("No error should be returned when propose in leader state: %v", err)
	}

	for r.core.state.name() == stateLeader {
		commits := r.core.TakeNewlyCommitted()

		if len(commits) != 0 {
			commitTuples := make([]commitTuple, 0, len(commits))
			for _, commit := range commits {
				// These entries(except the NOP command proposed in current term) must
				// be proposed by previous leaders, we just simply hand them over to
				// the FSM loop without pairing them to correspoding pending objects.
				if commit.Type == EntryNOP && commit.Term == r.storage.GetCurrentTerm() {
					// We are only interested in the NOP command proposed in current term here.
					pendingNOP = &Pending{Done: make(chan struct{}, 1)}
					commitTuples = append(commitTuples, commitTuple{entry: commit, pending: pendingNOP})
				} else {
					// It's some entry of previous term.
					commitTuples = append(commitTuples, commitTuple{entry: commit})
				}
			}
			r.fsmLoop.fsmCh <- commitsUpdate{commits: commitTuples}
		}

		if pendingNOP != nil {
			// If it's not nil then the NOP command proposed in current term has
			// already been committed and sent to the FSM loop. Wait the
			// acknowledgement from the FSM loop.
			<-pendingNOP.Done
			return
		}

		r.sendMsgs()

		// We use two "selects" to prioritize timer channel because we want timer
		// channel to be triggered at a fixed interval without being affected by
		// other channels.
		select {
		// Simulate real time.
		case <-timerC:
			r.core.Tick()
			continue

		default:
			// Fall through
		}

		select {
		// Monitor incoming message channel.
		case msg := <-r.transport.Receive():
			r.core.HandleMsg(msg)

			// Simulate real time.
		case <-timerC:
			r.core.Tick()
		}
	}
}

func (r *Raft) takeBatchedProposals(curLen int, batchedProposals []pendingEntry) []pendingEntry {
	//
	// Batches proposals from the channel until one of the following happens:
	//
	//  1. The number of drained proposals along with pending proposals(proposed
	//     but not committed) can't exceed the "MaximumPendingProposals" specified
	//     in configuration.
	//
	//  2. The number of drained proposals can't exceed the "MaximumProposalsBatch"
	//     specified in configuration.
	//
	//	3. There's no more proposals in the channel.

drainLoop:
	for (r.config.MaximumPendingProposals == 0 ||
		uint32(curLen+len(batchedProposals)) < r.config.MaximumPendingProposals) &&
		uint32(len(batchedProposals)) < r.config.MaximumProposalBatch {
		// check for 1 and 2.
		select {
		case tuple := <-r.propCh:
			batchedProposals = append(batchedProposals, tuple)
		default:
			// check for 3
			break drainLoop
		}
	}
	return batchedProposals
}

// Handles a temporary snapshot file written by the fsm loop.
func (r *Raft) fsmSnapshotDone(writer SnapshotFileWriter) {
	meta, _ := r.storage.GetSnapshotMetadata()
	if meta != NilSnapshotMetadata &&
		meta.LastIndex >= writer.GetMetadata().LastIndex {
		// It's possible, though unlikely, that while the fsm loop was taking the
		// snapshot, a follower has persisted a more recent snapshot from leader,
		// thus the temporary snapshot file written by fsm loop is not as up-to-date
		// as the current snapshot file. If this ever happens, we'll just discard the
		// temporary snapshot file.
		log.Infof("A more recent snapshot exists, deleting the temporary snapshot from fsm Loop")
		writer.Abort()
		return
	}
	// Try to commit(make it visible) the temporary snapshot file.
	if err := writer.Commit(); err != nil {
		log.Errorf("Failed to commit snapshot: %v", err)
		return
	}
	// Trim the log once the snapshot becomes effective.
	r.core.TrimLog(writer.GetMetadata().LastIndex)
}

func (r *Raft) maybeRestoreFromSnapshot() {
	if r.core.needRestoreSnap {
		pending := &Pending{Done: make(chan struct{}, 1)}
		r.fsmLoop.fsmCh <- restoreUpdate{reader: r.storage.GetSnapshot(), pending: pending}
		<-pending.Done
		r.core.needRestoreSnap = false
	}
}

func (r *Raft) sendMsgs() {
	// Handle messages that need to be sent.
	for _, msg := range r.core.TakeAllMsgs() {
		msg.SetFromGUID(r.core.GUID)
		msg.SetToGUID(r.storage.GetGUIDFor(msg.GetTo()))
		msg.SetEpoch(r.core.getEpoch())
		r.transport.Send(msg)
	}
}

// metricState is a simple wrapper on top of a State implementation that updates
// the term metric whenever the term gets changed.
type metricState struct {
	State
	m prometheus.Gauge
}

func (s *metricState) SetCurrentTerm(term uint64) {
	s.State.SetCurrentTerm(term)
	s.m.Set(float64(term))
}

func (s *metricState) SaveState(voteFor string, term uint64) {
	s.State.SaveState(voteFor, term)
	s.m.Set(float64(term))
}
