// Copyright (c) 2017 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package curator

import (
	"container/heap"
	"encoding/binary"
	"hash"
	"hash/fnv"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/westerndigitalcorporation/blb/internal/core"
	"github.com/westerndigitalcorporation/blb/internal/curator/durable/state/fb"
)

const (
	// Cap the size of the pending task queue at this. We need some cap because
	// we store indexes as int32s, but it can be very high.
	pendingCap = 1 << 30

	// We dump complaints that we haven't heard in this much time.
	// NOTE: Shouldn't be shorter than the time for a TS to be unhealthy.
	complaintResetInterval = 60 * time.Second

	// We don't want to let clients put unbounded complaints into our map, so we cap it at this.
	blockedMapMax = 10000
)

type entWithHash struct {
	ent  *heapEntry
	hash uint64
}

type idAndErr struct {
	id  core.TractID
	err core.Error
}

type recovery struct {
	c *Curator

	// Used to pass IDs of completed tasks back to detectLoop.
	completed chan idAndErr

	// Holds the main priority queue of pending tasks.
	pendingLock sync.Mutex
	pending     entryHeap

	// Only used in detectLoop, so no locking needed:

	// Map of entries that have been added to the pending heap and not completed
	// yet, plus a hash that lets us quickly check if an entry needs to be
	// updated without actually looking at it (and needed to lock pendingLock).
	//
	// Note that actively working tasks are still in this map, even though
	// they're not in the priority queue anymore!
	//
	// Also note that even though this structure contains *heapEntry's, you
	// can't dereference them without pendingLock, since the actual entry memory
	// is covered by that lock.
	entryMap map[core.TractID]entWithHash

	// Host status from tractserverMonitor.
	status tsStatus

	// A client-generated map of tract ID -> tractserver IDs that clients are
	// blocked on. We use two maps and swap them periodically to coarsely expire
	// complaints. blocked1 is the current interval, blocked2 is the previous
	// interval.
	blocked1, blocked2 map[core.TractID]TSIDSet

	// Tractserver-reported corrupt tracts, mapping from the tract ID (which can
	// be an RS chunk ID) to which tractservers have corrupt copies of that
	// data.
	//
	// Note that tractservers will only report corruption at most once per
	// read/write error. If the corruption is detected by a scrub, we might only
	// get one report per week. If the "corruption" is a missing tract, we'll
	// only get a report when the checkTractsLoop sends it again, which could
	// also be a very long time. So we shouldn't remove things from corrupt
	// unless we're confident they're fixed.
	corrupt map[core.TractID]TSIDSet

	// These are used to remove tracts/chunks from corrupt if they are deleted.
	// corruptGen should have the same set of keys as corrupt.
	corruptGen map[core.TractID]bool
	currentGen bool

	// Set of tracts and RS chunks that are currently unrecoverable because too
	// many replicas or pieces are missing. Values are generation (as in
	// corruptGen).
	unrecoverable map[core.TractID]bool

	// Every time this ticks, we dump the older client complaints in 'blocked'
	// to ensure that we don't let complaints caused by transient issues crowd
	// out recent actual issues.
	lastBlockedReset time.Time

	// Counts changes per iteration.
	changed int

	// Metrics (use atomics so no locking needed)
	metricBlocked       prometheus.Gauge // The number of tracts that are blocking client's operations.
	metricCorrupt       prometheus.Gauge // The number of tracts that are reported as corrupt (at least one copy).
	metricQueued        prometheus.Gauge // The number of tracts or chunks in the pending queue.
	metricActive        prometheus.Gauge // The number of tracts or chunks that are being recovered.
	metricUnrecoverable prometheus.Gauge // The number of tracts or chunks that are unrecoverable.
}

func newRecovery(c *Curator, metricPrefix string) *recovery {
	r := &recovery{
		c:             c,
		completed:     make(chan idAndErr, 1000),
		entryMap:      make(map[core.TractID]entWithHash, 1000),
		blocked1:      make(map[core.TractID]TSIDSet),
		blocked2:      make(map[core.TractID]TSIDSet),
		corrupt:       make(map[core.TractID]TSIDSet),
		corruptGen:    make(map[core.TractID]bool),
		unrecoverable: make(map[core.TractID]bool),

		metricBlocked:       promauto.NewGauge(prometheus.GaugeOpts{Subsystem: metricPrefix, Name: "blocking_tracts"}),
		metricCorrupt:       promauto.NewGauge(prometheus.GaugeOpts{Subsystem: metricPrefix, Name: "corrupt_tracts"}),
		metricQueued:        promauto.NewGauge(prometheus.GaugeOpts{Subsystem: metricPrefix, Name: "recovery_queued"}),
		metricActive:        promauto.NewGauge(prometheus.GaugeOpts{Subsystem: metricPrefix, Name: "recovery_active"}),
		metricUnrecoverable: promauto.NewGauge(prometheus.GaugeOpts{Subsystem: metricPrefix, Name: "unrecoverable"}),
	}
	go r.detectLoop()
	go r.runnerLoop()
	return r
}

func (r *recovery) stats(m map[string]interface{}) {
	/* TODO: read values from these
	m["BlockingClients"] = r.metricBlocked.Read()
	m["CorruptTracts"] = r.metricCorrupt.Read()
	m["RecoveryQueued"] = r.metricQueued.Read()
	m["RecoveryActive"] = r.metricActive.Read()
	m["Unrecoverable"] = r.metricUnrecoverable.Read()
	*/
}

func (r *recovery) detectLoop() {
	for {
		if !r.c.stateHandler.IsLeader() {
			// If we lost leadership, remove all in-progress work.
			r.clearPending()
			r.c.blockIfNotLeader()
		}

		op := r.c.internalOpM.Start("DetectLoop")

		r.currentGen = !r.currentGen

		r.removeCompletedTasks()         // Remove completed tasks from entryMap.
		r.updateCorrupt()                // Merge in corruption reports.
		r.updateBlocked()                // Merge in client blocked complaints.
		r.status = r.c.tsMon.getStatus() // Get a snapshot of the health of all tractservers.

		// Look at all tracts and RS chunks and schedule work if any are incomplete.
		r.changed = 0
		r.c.stateHandler.ForEachTract(r.eachTract, r.c.stateHandler.IsLeader)
		r.c.stateHandler.ForEachRSChunk(r.eachChunk, r.c.stateHandler.IsLeader)

		r.pruneDeletedCorrupt()       // Remove tracts from corrupt if they don't exist anymore.
		r.pruneDeletedUnrecoverable() // Remove tracts from unrecoverable if they don't exist anymore.

		r.metricUnrecoverable.Set(float64(len(r.unrecoverable)))

		op.End()

		if r.changed == 0 {
			time.Sleep(time.Second) // Don't spin.
		} else {
			log.V(2).Infof("updated %d queue entries", r.changed)
		}
	}
}

func (r *recovery) updateCorrupt() {
	// Merge new reports of corruption with our current list of corrupt tracts.
	for tract, hosts := range r.c.getCorruptBatch() {
		r.corrupt[tract] = r.corrupt[tract].Merge(hosts)
		r.corruptGen[tract] = r.currentGen
	}
	r.metricCorrupt.Set(float64(len(r.corrupt)))
}

func (r *recovery) updateBlocked() {
	// Throw out older buffered user complaints every now and then. The user can always complain again.
	// We do this even if we don't act on any of the complaints.
	now := time.Now()
	if now.Sub(r.lastBlockedReset) > complaintResetInterval {
		log.V(2).Infof("dumping blocked information, had %d+%d entries", len(r.blocked1), len(r.blocked2))
		r.blocked2 = r.blocked1
		r.blocked1 = make(map[core.TractID]TSIDSet)
		r.lastBlockedReset = now
	}

	// Pull any pending user complaints out of the complaining channel and put them into the complaint map.
loop:
	for len(r.blocked1) < blockedMapMax {
		select {
		case complaint := <-r.c.blockedChan:
			// De-duplicate complaints.
			if s, ok := r.blocked1[complaint.id]; !ok || !s.Contains(complaint.tsid) {
				r.blocked1[complaint.id] = s.Add(complaint.tsid)
				log.V(1).Infof("got complaint from client: %+v, now has %d",
					complaint, r.blocked1[complaint.id].Len())
			}
		default:
			break loop
		}
	}

	// Take union of maps to find actual number of tracts blocked.
	totalBlocked := len(r.blocked2)
	for id := range r.blocked1 {
		if _, ok := r.blocked2[id]; !ok {
			totalBlocked++
		}
	}
	r.metricBlocked.Set(float64(totalBlocked))
}

func (r *recovery) removeCompletedTasks() {
	for {
		select {
		case comp := <-r.completed:
			delete(r.entryMap, comp.id)
			// If this succeeded, we can assume we fixed all the corruption.
			if comp.err == core.NoError {
				delete(r.corrupt, comp.id)
				delete(r.corruptGen, comp.id)
			}
		default:
			return
		}
	}
}

func (r *recovery) pruneDeletedCorrupt() {
	for id, gen := range r.corruptGen {
		if gen != r.currentGen {
			delete(r.corrupt, id)
			delete(r.corruptGen, id)
		}
	}
}

func (r *recovery) pruneDeletedUnrecoverable() {
	for id, gen := range r.unrecoverable {
		if gen != r.currentGen {
			delete(r.unrecoverable, id)
		}
	}
}

func (r *recovery) pruneCorruptTractSet(id core.TractID, t *fb.TractF) {
	// If "corrupt" has tsids that aren't in this tract's replicated set
	// anymore, then we can remove them.
	set, ok := r.corrupt[id]
	if !ok {
		return
	}

	// Mark as seen in this generation.
	r.corruptGen[id] = r.currentGen

	l := set.Len()
	for i := 0; i < l; i++ {
		if !fb.HostsContains(t, set.Get(i)) {
			// If any are missing, recreate corrupt set excluding unknown ids.
			var newSet TSIDSet
			for i = 0; i < l; i++ {
				tsid := set.Get(i)
				if fb.HostsContains(t, tsid) {
					newSet = newSet.Add(tsid)
				}
			}
			if newSet.Len() == 0 {
				delete(r.corrupt, id)
				delete(r.corruptGen, id)
			} else {
				r.corrupt[id] = newSet
			}
			return
		}
	}
}

func (r *recovery) eachTract(id core.TractID, t *fb.TractF) {
	r.pruneCorruptTractSet(id, t)
	task, hash, unrec := r.tractTask(id, t)
	r.syncTask(id, task, hash, unrec)
}

func (r *recovery) tractTask(id core.TractID, t *fb.TractF) (task, uint64, bool) {
	h := fnv.New64a()

	if t.HostsLength() == 0 {
		return nil, 0, false // Not replicated
	}

	var badTs TSIDSet
	blocking := int8(0)
	for i := 0; i < t.HostsLength(); i++ {
		ts := core.TractserverID(t.Hosts(i))
		bad := false
		if _, ok := r.status.down[ts]; ok {
			bad = true
		} else if r.corrupt[id].Contains(ts) {
			bad = true
		} else if r.blocked1[id].Contains(ts) || r.blocked2[id].Contains(ts) {
			if _, ok := r.status.unhealthy[ts]; ok {
				bad = true
				blocking = 1
			}
		}
		if bad {
			badTs = badTs.Add(ts)
			hashTSID(h, ts)
		}
	}

	if l := badTs.Len(); l == 0 {
		// The hopefully-common case of a healthy repl set for a tract.
		return nil, 0, false
	} else if l == t.HostsLength() {
		// The hopefully-never case of possibly lost data.
		log.Errorf("all %d copies of tract %s are bad; CANNOT REREPLICATE", t.HostsLength(), id)
		return nil, 0, true
	}

	h.Write([]byte{byte(blocking)})

	return &replTask{
		id:       id,
		factor:   int8(t.HostsLength()),
		blocking: blocking,
		badTs:    badTs,
	}, h.Sum64(), false
}

func (r *recovery) pruneCorruptChunkSet(baseID core.RSChunkID, c *fb.RSChunkF) {
	for idx := 0; idx < c.HostsLength(); idx++ {
		expectedTSID := core.TractserverID(c.Hosts(idx))
		id := baseID.Add(idx).ToTractID()

		// If "corrupt" has tsids that aren't supposed to hold this piece
		// anymore, we can remove them. Note there's only supposed to be one
		// copy of any piece.
		set, ok := r.corrupt[id]
		if !ok {
			continue
		}

		// Mark as seen in this generation.
		r.corruptGen[id] = r.currentGen

		// Prune others besides expectedTSID.
		if !set.Contains(expectedTSID) {
			delete(r.corrupt, id)
			delete(r.corruptGen, id)
		} else if set.Len() != 1 {
			r.corrupt[id] = TSIDSet{}.Add(expectedTSID)
		}
	}
}

func (r *recovery) eachChunk(id core.RSChunkID, c *fb.RSChunkF) {
	r.pruneCorruptChunkSet(id, c)
	task, hash, unrec := r.chunkTask(id, c)
	r.syncTask(id.ToTractID(), task, hash, unrec)
}

func (r *recovery) chunkTask(baseID core.RSChunkID, c *fb.RSChunkF) (task, uint64, bool) {
	h := fnv.New64a()
	n := c.DataLength()
	m := c.HostsLength() - n

	var badTs TSIDSet
	blocking := int8(0)
	for idx := 0; idx < c.HostsLength(); idx++ {
		ts := core.TractserverID(c.Hosts(idx))
		tid := baseID.Add(idx).ToTractID()
		bad := false
		if _, ok := r.status.down[ts]; ok {
			bad = true
		} else if r.corrupt[tid].Contains(ts) {
			bad = true
		} else if r.blocked1[tid].Contains(ts) || r.blocked2[tid].Contains(ts) {
			if _, ok := r.status.unhealthy[ts]; ok {
				bad = true
				blocking = 1
			}
		}
		if bad {
			badTs = badTs.Add(ts)
			hashTSID(h, ts)
		}
	}

	if l := badTs.Len(); l == 0 {
		// The hopefully-common case of a healthy chunk.
		return nil, 0, false
	} else if l > m {
		// The hopefully-never case of possibly lost data.
		log.Errorf("RS(%d,%d) chunk %s has %d pieces remaining; CANNOT RECONSTRUCT", n, m, baseID, n+m-l)
		return nil, 0, true
	}

	h.Write([]byte{byte(blocking)})

	return &rsTask{
		id:       baseID,
		n:        int8(n),
		m:        int8(m),
		blocking: blocking,
		badTs:    badTs,
	}, h.Sum64(), false
}

func (r *recovery) syncTask(id core.TractID, newTask task, newHash uint64, unrec bool) {
	if eh, ok := r.entryMap[id]; ok {
		if newTask == nil {
			// If we already have this task and shouldn't, delete it.
			r.removeEntry(eh.ent)
			delete(r.entryMap, id)
			r.changed++
		} else if newHash != eh.hash {
			// If we already have this task, maybe update it.
			r.fixEntry(eh.ent, newTask)
			r.entryMap[id] = entWithHash{ent: eh.ent, hash: newHash}
			r.changed++
		}
	} else if newTask != nil {
		// Not present, we should push it on the heap.
		ent := r.pushTask(newTask)
		if ent != nil {
			r.entryMap[id] = entWithHash{ent: ent, hash: newHash}
			r.changed++
		}
	}

	if unrec {
		r.unrecoverable[id] = r.currentGen
	} else {
		delete(r.unrecoverable, id)
	}
}

func calcScore(t task) int32 {
	// We want prioritize tasks by "what data is at most risk of unrecoverable loss?" In other
	// words, what has the highest probability of data loss in the next short amount of time? As a
	// very simple model, let's say p is the probability of a given replica failing in the next
	// short fixed amount of time. (p should be very small) For replicated data, we have loss if all
	// good replicas fail at the same time, so p^g, where g is the number of good replicas left. For
	// RS data, we have loss if (g-n+1) out of g pieces fail, where g is the number of good pieces.
	// That's roughly choose(g,g-n+1) * p^(g-n+1). But we can simplify the coefficient to just g and
	// the order will come out the same.
	//
	// t.Priority() returns a coefficient and exponent. Since p^-1 > any coefficient, we're
	// effectively sorting first by exponent (higher e -> lower priority) and second by coefficient
	// (higher c -> higher priority). We also give a bonus for blocking clients, that overrides any
	// coefficient but not exponent.
	//
	// This produces example priorities from high to low:
	// - RS( 10,3) with 10 good, blocking client
	// - RS(  6,3) with  6 good, blocking client
	// - replicated with 1 good, blocking client
	// - RS( 10,3) with 10 good
	// - RS(  6,3) with  6 good
	// - replicated with 1 good
	// - RS( 10,3) with 11 good, blocking client
	// - RS(  6,3) with  7 good, blocking client
	// - replicated with 2 good, blocking client
	// - RS( 10,3) with 11 good
	// - RS(  6,3) with  7 good
	// - replicated with 2 good
	// ...
	c, e, blocking := t.Priority()
	score := int32(e*1000 - c)
	if blocking {
		score -= 500
	}
	return score
}

func (r *recovery) unlockPendingLock() {
	l := int64(len(r.pending))
	r.pendingLock.Unlock()
	r.metricQueued.Set(float64(l))
}

func (r *recovery) clearPending() {
	r.pendingLock.Lock()
	r.pending = r.pending[:0]
	r.unlockPendingLock()
	r.entryMap = make(map[core.TractID]entWithHash)
}

func (r *recovery) pushTask(t task) *heapEntry {
	r.pendingLock.Lock()
	defer r.unlockPendingLock()

	if len(r.pending) >= pendingCap {
		// We shouldn't have this many, but if we do, just drop new ones.
		log.Errorf("dropping new task")
		return nil
	}
	ent := &heapEntry{task: t, score: calcScore(t)}
	heap.Push(&r.pending, ent)
	return ent
}

func (r *recovery) popTask() (task, bool) {
	r.pendingLock.Lock()
	defer r.unlockPendingLock()

	if len(r.pending) == 0 {
		return nil, false
	}
	return heap.Pop(&r.pending).(*heapEntry).task, true
}

func (r *recovery) fixEntry(ent *heapEntry, newTask task) {
	r.pendingLock.Lock()
	defer r.unlockPendingLock()

	ent.task = newTask
	ent.score = calcScore(newTask)

	idx := int(ent.index)
	if idx >= 0 {
		heap.Fix(&r.pending, idx)
	}
}

func (r *recovery) removeEntry(ent *heapEntry) {
	r.pendingLock.Lock()
	defer r.unlockPendingLock()

	idx := int(ent.index)
	if idx >= 0 {
		heap.Remove(&r.pending, idx)
	}
}

func (r *recovery) runnerLoop() {
	for {
		task, ok := r.popTask()
		if !ok {
			time.Sleep(time.Second)
			continue
		}

		// Do bandwidth-based flow control in this goroutine, so tasks get
		// started in close to priority order.
		r.c.recoveryBwLim.Take(task.Bandwidth())

		go r.runTask(task)
	}
}

func (r *recovery) runTask(t task) {
	log.Infof("%s: starting task", t.ID())
	r.metricActive.Add(1)
	err := t.Run(r.c)
	r.metricActive.Add(-1)
	log.Infof("%s: ending task, result %s", t.ID(), err)
	r.completed <- idAndErr{t.ID(), err}
}

type task interface {
	ID() core.TractID   // an ID for the task (tract ID or RS chunk ID)
	Bandwidth() float32 // approx. how many bytes this will transfer

	// approx. priority of losing data, in the form c⋅p^e, and whether we're
	// blocking a client
	Priority() (c, e int, blocking bool)

	// actually run the task
	Run(*Curator) core.Error
}

// implements task
type replTask struct {
	id       core.TractID
	factor   int8
	blocking int8
	badTs    TSIDSet
}

func (rt *replTask) ID() core.TractID {
	return rt.id
}

func (rt *replTask) Bandwidth() float32 {
	// We don't know how big this tract actually is. Assume it's full.
	return float32(core.TractLength)
}

func (rt *replTask) Priority() (c, e int, blocking bool) {
	return 1, int(rt.factor) - rt.badTs.Len(), rt.blocking != 0
}

func (rt *replTask) Run(c *Curator) core.Error {
	return c.replicateTract(rt.id, rt.badTs.ToSlice())
}

// implements task
type rsTask struct {
	id       core.RSChunkID
	n, m     int8
	blocking int8
	badTs    TSIDSet
}

func (rt *rsTask) ID() core.TractID {
	return rt.id.ToTractID()
}

func (rt *rsTask) Bandwidth() float32 {
	// one TS has to read n*length bytes and write (bad-1)*length bytes
	return float32((int(rt.n) + rt.badTs.Len() - 1) * RSPieceLength)
}

func (rt *rsTask) Priority() (c, e int, blocking bool) {
	good := int(rt.n) + int(rt.m) - rt.badTs.Len()
	return good, good - int(rt.n) + 1, rt.blocking != 0
}

func (rt *rsTask) Run(c *Curator) core.Error {
	return c.reconstructChunk(rt.id, rt.badTs.ToSlice())
}

type heapEntry struct {
	task  task
	score int32 // lower score == higher priority
	index int32 // index >= 0 means in pending heap; -1 means actively working
}

type entryHeap []*heapEntry

func (eh entryHeap) Len() int { return len(eh) }

func (eh entryHeap) Less(i int, j int) bool { return eh[i].score < eh[j].score }

func (eh entryHeap) Swap(i int, j int) {
	eh[i], eh[j] = eh[j], eh[i]
	eh[i].index, eh[j].index = int32(i), int32(j)
}

func (eh *entryHeap) Push(enti interface{}) {
	ent := enti.(*heapEntry)
	ent.index = int32(len(*eh))
	*eh = append(*eh, ent)
}

func (eh *entryHeap) Pop() interface{} {
	ent := (*eh)[len(*eh)-1]
	ent.index = -1
	*eh = (*eh)[:len(*eh)-1]
	return ent
}

func hashTSID(w hash.Hash, ts core.TractserverID) {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], uint32(ts))
	w.Write(buf[:])
}
