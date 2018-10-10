// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT
//
// A Manager schedules and execute requests on one disk.

package tractserver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	dto "github.com/prometheus/client_model/go"

	log "github.com/golang/glog"
	"github.com/westerndigitalcorporation/blb/internal/core"
	"github.com/westerndigitalcorporation/blb/internal/server"
	"github.com/westerndigitalcorporation/blb/pkg/disk"
)

const (
	graveyardPrefix = "DELETED-AT-%d-"

	// How many names to ask for at once from readdir.
	readdirChunkSize = 1000

	// Don't try to look for flag files more often than this.
	flagFilesCheckInterval = 5 * time.Minute

	// Subdirectory of root containing tracts/deleted tracts.
	tractDir = "data"
	delDir   = "deleted"
)

var (
	// Internal error returned when a request is canceled.
	errCanceled = errors.New("canceled")

	// OpMetric to record counts and latencies of disk ops. Does not count time
	// in the queue.
	opm = server.NewOpMetric("tractserver_disk", "disk", "op")

	// Other metrics.
	metricWaitTime = promauto.NewSummaryVec(prometheus.SummaryOpts{
		Subsystem: "tractserver",
		Name:      "queue_wait",
		Help:      "wait time for operations to hit the front of the queue",
	}, []string{"disk"})
	metricQueueLength = promauto.NewSummaryVec(prometheus.SummaryOpts{
		Subsystem: "tractserver",
		Name:      "queue_length",
		Help:      "length of the operation queue",
	}, []string{"disk"})
	metricSpace = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "tractserver",
		Name:      "space",
		Help:      "disk space avail/total",
	}, []string{"disk", "type"})
	metricTracts = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "tractserver",
		Name:      "tracts",
		Help:      "number of tracts existing/deleted/unknown",
	}, []string{"disk", "type"})
)

// A Manager prioritizes I/O to a disk and executes it.
//
// This is the set of errors that can be returned from methods on Manager and the implied semantics:
//
// core.NoError -- everything was fine.
// core.ErrEOF -- could not read as much data as asked to.
// core.ErrNoSpace -- the filesystem is full.
// core.ErrInvalidArgument -- a bad file descriptor or similar, shouldn't see this but don't know what steps to take.
// core.ErrCorruptData -- the tract is corrupt and should be reported to the curator.
// core.ErrIO -- the filesystem may be corrupt, all tracts are suspect.
// core.ErrDiskRemoved -- Stop has been called.
type Manager struct {
	// Requests are enqueued here.
	queue *PriorityQueue

	// This is the mount point of the disk we're managing.
	root string

	// This is the path to the tract directory (root+"/data").
	tractRoot string

	// This is the path to the deleted tract directory (root+"/deleted").
	delRoot string

	// A shorter name of the disk, for metrics.
	name string

	// Global configuration for a tract server.
	config *Config

	// Holds current target worker count. Use with atomic ops.
	workers uint64

	// Holds count of open files + directories. Use with atomic ops.
	openFiles int64

	// Stopped flag
	stopped int64

	// Information compiled from observed behavior and a lock to prevent races.
	lock   sync.Mutex
	status core.DiskStatus

	lastFlagFileCheck time.Time
}

// NewManager creates a new Manager to manage the disk rooted at 'root'.
//
// NewManager is thread-safe in that it will never produce *corrupt data* when
// handling parallel mutations to a file, but it may produce inconsistent data.
func NewManager(root string, config *Config) (*Manager, error) {
	log.V(1).Infof("[%s mgr]: creating new manager", root)

	root = filepath.Clean(root)
	tractRoot := filepath.Join(root, tractDir)
	delRoot := filepath.Join(root, delDir)

	if fi, e := os.Stat(tractRoot); e != nil || !fi.IsDir() {
		os.Mkdir(tractRoot, 0700)
	}

	if fi, e := os.Stat(delRoot); e != nil || !fi.IsDir() {
		os.Mkdir(delRoot, 0700)
	}

	// Make sure root exists and is a directory.
	var e error
	tractsFile, e := os.Open(tractRoot)
	if e != nil {
		log.Errorf("couldn't open tracts directory, err=%s", e)
		return nil, e
	}
	defer tractsFile.Close()

	var fi os.FileInfo
	if fi, e = tractsFile.Stat(); nil != e {
		log.Errorf("[%s mgr]: stat of root failed", root)
		return nil, e
	}
	if !fi.Mode().IsDir() {
		log.Errorf("tract dir %s is not a directory", tractRoot)
		return nil, fmt.Errorf("tract dir %s must be directory", tractRoot)
	}

	// Do we want to use a limit on the queue?  It would be weird to reject
	// a close request.  How would that be handled?  I think limiting might
	// be better done higher up.
	m := &Manager{
		queue:     NewPriorityQueue(0),
		root:      root,
		tractRoot: tractRoot,
		delRoot:   delRoot,
		name:      shortName(root),
		config:    config,
		status: core.DiskStatus{
			Root:    root,
			Full:    false,
			Healthy: true,
			Flags:   loadControlFlags(root),
		},
		lastFlagFileCheck: time.Now(),
	}

	m.setWorkers(config.Workers)

	go m.sweepDeletedTracts()

	return m, nil
}

// shortName turns "/disk/x/data/..." into "x".
func shortName(root string) string {
	parts := strings.Split(root, "/")
	if parts[0] == "" && parts[1] == "disk" {
		return parts[2]
	}
	return root
}

// Open creates (if necessary) and opens a file. The opaque value returned is a
// file handle and may be used in future calls to Manager.
func (m *Manager) Open(ctx context.Context, id core.TractID, flags int) (interface{}, core.Error) {
	op, err := m.schedule(ctx, openRequest{id: id, flags: flags})
	if err == core.NoError {
		return op.(openReply).f, err
	}
	return nil, err
}

// Close closes an Open'd file. After calling Close on a file, the caller must
// not use the file handle for any future operation (even if Close returns an
// error).
func (m *Manager) Close(f interface{}) core.Error {
	_, err := m.schedule(context.TODO(), closeRequest{f: f.(*disk.ChecksumFile)})
	return err
}

// Delete removes the data identified by tract 'id'.
func (m *Manager) Delete(id core.TractID) core.Error {
	_, err := m.schedule(context.TODO(), deleteRequest{id: id})
	return err
}

// Write writes len(b) bytes from 'b' starting at offset 'off' in the open tract 'f'.
func (m *Manager) Write(ctx context.Context, f interface{}, b []byte, off int64) (int, core.Error) {
	op, err := m.schedule(ctx, writeRequest{f: f.(*disk.ChecksumFile), b: b, off: off})
	if err == core.NoError {
		return op.(writeReply).n, err
	}
	return 0, err
}

// Read reads len(b) bytes into 'b' from offset 'off' in the open tract 'f'.
func (m *Manager) Read(ctx context.Context, f interface{}, b []byte, off int64) (int, core.Error) {
	op, err := m.schedule(ctx, readRequest{f: f.(*disk.ChecksumFile), b: b, off: off})
	if err == core.NoError || err == core.ErrEOF {
		return op.(readReply).n, err
	}
	return 0, err
}

// Scrub checks whether or not the tract 'id' is corrupt.
func (m *Manager) Scrub(id core.TractID) (int64, core.Error) {
	ctx := contextWithPriority(context.Background(), LowPri)
	op, err := m.schedule(ctx, scrubRequest{id: id})
	if err == core.NoError {
		return op.(scrubReply).size, err
	}
	return 0, err
}

// Size returns the size of the data stored in the tract.
func (m *Manager) Size(f interface{}) (int64, core.Error) {
	op, err := m.schedule(context.TODO(), statRequest{f.(*disk.ChecksumFile)})
	if err == core.NoError {
		return op.(statReply).size, err
	}
	return 0, err
}

// Getxattr returns the value of the xattr named 'name' in the open tract 'f'.
func (m *Manager) Getxattr(f interface{}, name string) ([]byte, core.Error) {
	op, err := m.schedule(context.TODO(), getxattrRequest{f: f.(*disk.ChecksumFile), name: name})
	if err == core.NoError {
		return op.(getxattrReply).value, err
	}
	return nil, err
}

// Setxattr sets the xattr named 'name' to value 'value' in the open tract 'f'.
func (m *Manager) Setxattr(f interface{}, name string, value []byte) core.Error {
	_, err := m.schedule(context.TODO(), setxattrRequest{f: f.(*disk.ChecksumFile), name: name, value: value})
	return err
}

// OpenDir returns a value that can be used to read all tract ids.
func (m *Manager) OpenDir() (interface{}, core.Error) {
	op, err := m.schedule(context.TODO(), opendirRequest{m.tractRoot})
	if err == core.NoError {
		return op.(opendirReply).d, err
	}
	return nil, err
}

// ReadDir reads a set of tract ids.
func (m *Manager) ReadDir(d interface{}) ([]core.TractID, core.Error) {
	op, err := m.schedule(context.TODO(), readdirRequest{d.(*os.File)})
	if err == core.NoError {
		return op.(readdirReply).tracts, err
	}
	return nil, err
}

// CloseDir closes the directory.
func (m *Manager) CloseDir(d interface{}) core.Error {
	_, err := m.schedule(context.TODO(), closedirRequest{d.(*os.File)})
	return err
}

// refreshStatus updates various fields in m.status.
// Must be called with m.lock.
func (m *Manager) refreshStatus() {
	// Fill out information presumably useful for load balancing.
	m.status.QueueLen = m.queue.Len()

	sum, ok := metricWaitTime.WithLabelValues(m.name).(prometheus.Summary)
	if !ok {
		return
	}
	var value dto.Metric
	if sum.Write(&value) != nil {
		return
	}
	m.status.AvgWaitMs = int(*value.Summary.SampleSum / float64(*value.Summary.SampleCount) * 1000)
}

// Status returns lightweight health information about this disk.
func (m *Manager) Status() core.DiskStatus {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.refreshStatus()
	return m.status
}

// Statfs returns information about the underlying filesystem.
func (m *Manager) Statfs() core.FsStatus {
	ctx := context.TODO()

	// First count files (this may take a little time).
	var numTracts, numDeletedFiles, numUnknownFiles int
	m.iterateDir(ctx, m.tractRoot, func(rep readdirReply) {
		// Everything in here should be tracts, non-tracts are unknown.
		numTracts += len(rep.tracts)
		numUnknownFiles += len(rep.dead) + len(rep.unknown)
	})

	// Next count deleted files.
	m.iterateDir(ctx, m.delRoot, func(rep readdirReply) {
		// Everything in here should be deleted. Other files are unknown.
		numDeletedFiles += len(rep.dead)
		numUnknownFiles += len(rep.tracts) + len(rep.unknown)
	})

	// Then do the statfs syscall.
	op, err := m.schedule(ctx, statfsRequest{})
	if err != core.NoError {
		return core.FsStatus{}
	}

	// Add our counts.
	s := op.(statfsReply).status
	s.NumTracts = numTracts
	s.NumDeletedTracts = numDeletedFiles
	s.NumUnknownFiles = numUnknownFiles

	metricSpace.WithLabelValues(m.name, "total").Set(float64(s.TotalSpace))
	metricSpace.WithLabelValues(m.name, "avail").Set(float64(s.AvailSpace))

	metricTracts.WithLabelValues(m.name, "tracts").Set(float64(numTracts))
	metricTracts.WithLabelValues(m.name, "deleted").Set(float64(numDeletedFiles))
	metricTracts.WithLabelValues(m.name, "unknown").Set(float64(numUnknownFiles))

	return s
}

// SetControlFlags sets flags for this disk. Note that flags may take effect
// even if an error is returned.
func (m *Manager) SetControlFlags(flags core.DiskControlFlags) core.Error {
	if m.isStopped() {
		return core.ErrDiskRemoved
	}

	m.lock.Lock()
	m.status.Flags = flags
	m.lock.Unlock()
	return saveControlFlags(m.root, flags)
}

// SetConfig notifies the Manager of a new configuration. Not all fields support
// dynamic configuration (only one does so far: worker count).
func (m *Manager) SetConfig(ncfg *Config) {
	m.setWorkers(ncfg.Workers)
}

// Stop stops the manager and all associated goroutines.
func (m *Manager) Stop() {
	if old := atomic.SwapInt64(&m.stopped, 1); old != 0 {
		// Already stopped.
		return
	}

	// From here, 'schedule' will return an error instead of enqueueing anything besides
	// closeRequest, closedirRequest, and exitRequest. The sweep goroutine will also exit
	// eventually.

	// Now we'd like to wait until we have no more open files. There may still be
	// openRequests in the queue, though, so we can't just wait until the open file count
	// drops to zero, we need to wait until the queue is empty also. Don't block on this.
	go func() {
		for m.queue.Len() > 0 || atomic.LoadInt64(&m.openFiles) > 0 {
			time.Sleep(10 * time.Millisecond)
		}

		// Queue is empty _and_ all open files are closed. Now we know that nothing
		// can open any more files in the future. We can stop the workers.
		m.setWorkers(0)
	}()
}

// String returns the name of the disk, for logging.
func (m *Manager) String() string {
	return m.name
}

//
// Implementation
//

// Schedule adds 'op' to the pending work queue. If the context contains a priority, that priority
// is used for queue priority.
//
// If the error returned is not core.NoError, the returned interface may be nil or otherwise invalid.
// Otherwise, the caller can cast the returned interface{} to the type that matches the request.
func (m *Manager) schedule(ctx context.Context, op interface{}) (interface{}, core.Error) {
	if m.isStopped() {
		switch op.(type) {
		case exitRequest, closeRequest, closedirRequest:
			// Allow these to go through so that we can clean up properly.
		default:
			return nil, core.ErrDiskRemoved
		}
	}

	done := make(chan reply)
	pri := priorityFromContext(ctx)
	req := request{done: done, priority: pri, op: op, enqueueTime: time.Now(), ctx: ctx}

	metricQueueLength.WithLabelValues(m.name).Observe(float64(m.queue.Len()))

	if err := m.queue.TryPush(req); err != nil {
		log.Fatalf("Shouldn't have had a dropped request, disabled max queue limit")
	}
	reply := <-done
	return reply.op, m.toBlbError(reply.err)
}

// Translate the disk-level error to a core.Error and also snoop on it to update state.
func (m *Manager) toBlbError(err error) core.Error {
	if err == nil {
		return core.NoError
	}

	if e, ok := core.BlbError(err); ok {
		return e
	}

	switch pe := err.(type) {
	case *os.PathError:
		err = pe.Err
	case *os.SyscallError:
		err = pe.Err
	case *os.LinkError:
		err = pe.Err
	case *disk.XattrError:
		return core.ErrBadVersion
	}

	switch err {
	case errCanceled:
		return core.ErrCanceled
	case disk.ErrCorruptData:
		return core.ErrCorruptData
	case disk.ErrInvalidFlag:
		return core.ErrInvalidArgument
	case io.EOF:
		return core.ErrEOF
	case syscall.ENOENT:
		return core.ErrNoSuchTract
	case syscall.EIO, syscall.EROFS:
		m.lock.Lock()
		m.status.Healthy = false
		m.lock.Unlock()
		return core.ErrIO
	case syscall.ENOSPC:
		m.lock.Lock()
		m.status.Full = true
		m.lock.Unlock()
		return core.ErrNoSpace
	default:
		log.Errorf("assuming unknown error type %+v is corruption", err)
		return core.ErrCorruptData
	}
}

// isStopped returns true if this manager has been stopped. This is true if and only if
// there are zero workers.
func (m *Manager) isStopped() bool {
	return atomic.LoadInt64(&m.stopped) != 0
}

// setWorkers changes the number of io worker goroutines for this Manager.
func (m *Manager) setWorkers(nworkers int) {
	new := uint64(nworkers)
	old := atomic.SwapUint64(&m.workers, new)
	for ; old > new; old-- {
		m.schedule(context.Background(), exitRequest{})
	}
	for ; old < new; old++ {
		go m.ioWorker()
	}
	log.Infof("worker count for disk %s is now %d", m.name, nworkers)
}

// Loop forever, executing disk requests.
func (m *Manager) ioWorker() {
	for {
		req := m.queue.Pop().(request)
		metricWaitTime.WithLabelValues(m.name).Observe(float64(time.Since(req.enqueueTime)) / 1e9)
		reply := m.execute(req)
		req.done <- reply
		if _, ok := req.op.(exitRequest); ok {
			break
		}
	}
}

// execute actually executes the disk request 'req', returning the result as a *reply.
func (m *Manager) execute(generic request) reply {
	switch req := generic.op.(type) {
	case openRequest:
		// We only check for cancellation at open time, as cancelling a close would
		// be weird and confusing.
		defer opm.Start(m.name, "open").End()
		select {
		case <-generic.ctx.Done():
			return reply{errCanceled, nil}
		default:
			f, e := disk.NewChecksumFile(m.toPath(req.id), req.flags)
			atomic.AddInt64(&m.openFiles, 1)
			return reply{e, openReply{f}}
		}
	case closeRequest:
		defer opm.Start(m.name, "close").End()
		atomic.AddInt64(&m.openFiles, -1)
		return reply{req.f.Close(), nil}
	case deleteRequest:
		defer opm.Start(m.name, "delete").End()
		return m.executeDelete(req)
	case finishDeleteRequest:
		defer opm.Start(m.name, "finishdelete").End()
		return m.executeFinishDelete(req)
	case readRequest:
		defer opm.Start(m.name, "read").End()
		n, e := req.f.ReadAt(req.b, req.off)
		return reply{e, readReply{n}}
	case writeRequest:
		defer opm.Start(m.name, "write").End()
		n, e := req.f.WriteAt(req.b, req.off)
		return reply{e, writeReply{n}}
	case scrubRequest:
		defer opm.Start(m.name, "scrub").End()
		return m.executeScrub(req)
	case statRequest:
		defer opm.Start(m.name, "stat").End()
		size, e := req.f.Size()
		return reply{e, statReply{size}}
	case getxattrRequest:
		defer opm.Start(m.name, "getxattr").End()
		v, err := req.f.Getxattr(req.name)
		return reply{err, getxattrReply{v}}
	case setxattrRequest:
		defer opm.Start(m.name, "setxattr").End()
		e := req.f.Setxattr(req.name, req.value)
		return reply{e, nil}
	case opendirRequest:
		defer opm.Start(m.name, "opendir").End()
		d, e := os.Open(req.dir)
		atomic.AddInt64(&m.openFiles, 1)
		return reply{e, opendirReply{d}}
	case readdirRequest:
		defer opm.Start(m.name, "readdir").End()
		rep, e := m.executeReaddir(req)
		return reply{e, rep}
	case closedirRequest:
		defer opm.Start(m.name, "closedir").End()
		atomic.AddInt64(&m.openFiles, -1)
		return reply{req.d.Close(), nil}
	case statfsRequest:
		defer opm.Start(m.name, "statfs").End()
		return m.executeStatfs()
	case exitRequest:
		return reply{}
	}

	log.Fatalf("unknown request %+v", generic)
	return reply{}
}

// sweepDeletedTracts finds "deleted" (renamed) tracts and deletes them for real.
func (m *Manager) sweepDeletedTracts() {
	for range time.Tick(m.config.SweepTractInterval) {
		if m.isStopped() {
			return
		}

		m.doSweep(time.Now())
	}
}

// doSweep does a single sweep for deleted tracts.
func (m *Manager) doSweep(now time.Time) {
	ctx := contextWithPriority(context.Background(), LowPri)

	const batchSize = 100
	names := make([]string, 0, batchSize)

	m.iterateDir(ctx, m.delRoot, func(rep readdirReply) {
		for _, name := range rep.dead {
			var tm int64
			if n, e := fmt.Sscanf(name, graveyardPrefix, &tm); e != nil || n < 1 {
				continue
			}
			if now.Sub(time.Unix(tm, 0)) < m.config.UnlinkTractDelay {
				continue
			}
			names = append(names, name)
			if len(names) == cap(names) {
				m.schedule(ctx, finishDeleteRequest{names})
				names = names[:0]
			}
		}
	})
	if len(names) > 0 {
		m.schedule(ctx, finishDeleteRequest{names})
	}
}

// toPath turns a tract ID into a path on the file system.
func (m *Manager) toPath(id core.TractID) string {
	return filepath.Join(m.tractRoot, id.String())
}

// executeDelete executes a delete request.
func (m *Manager) executeDelete(req deleteRequest) reply {
	// For safety against bugs and gross operator error, instead of unlinking,
	// we rename these and unlink them later in a sweep.
	prefix := fmt.Sprintf(graveyardPrefix, time.Now().Unix())
	path := m.toPath(req.id)
	newPath := filepath.Join(m.delRoot, prefix+filepath.Base(path))

	// This will sync delDir, but not tractDir. That's ok: if somehow the tract
	// reappears in tractDir, it'll just be extra and will be GC-ed again later.
	return reply{disk.Rename(path, newPath), nil}
}

// executeFinishDelete executes a finishDelete request.
func (m *Manager) executeFinishDelete(req finishDeleteRequest) reply {
	for _, name := range req.files {
		log.V(1).Infof("[%s mgr]: unlinking tract %s", m.root, name)
		e := os.Remove(filepath.Join(m.delRoot, name))
		// We just do this for the side-effects of checking the various possible errno values.
		m.toBlbError(e)
	}
	return reply{nil, nil}
}

func (m *Manager) executeScrub(req scrubRequest) reply {
	// Always specify O_DROPCACHE for scrubbing.
	f, e := disk.NewChecksumFile(m.toPath(req.id), os.O_RDONLY|disk.O_DROPCACHE)
	if e != nil {
		return reply{e, scrubReply{0}}
	}

	bytes, scrubErr := f.Scrub()

	if e = f.Close(); nil != e {
		return reply{e, scrubReply{0}}
	}

	return reply{scrubErr, scrubReply{bytes}}
}

// executeReaddir does a Readdirnames call and processes the results.
func (m *Manager) executeReaddir(req readdirRequest) (reply readdirReply, err error) {
	var names []string
	names, err = req.d.Readdirnames(readdirChunkSize)
	for _, n := range names {
		if strings.HasPrefix(n, "DELETED-AT-") {
			reply.dead = append(reply.dead, n)
		} else if t, e := core.ParseTractID(n); e == nil {
			if t.Blob == core.ZeroBlobID {
				// This is a metadata tract. Hide this from above layers (so that the
				// curator doesn't tell us to GC it).
				continue
			}
			reply.tracts = append(reply.tracts, t)
		} else {
			reply.unknown = append(reply.unknown, n)
		}
	}
	return
}

// executeStatfs executes a statfs request.
func (m *Manager) executeStatfs() reply {
	// Retrieve info about the filesystem.
	var stat syscall.Statfs_t
	if err := syscall.Statfs(m.tractRoot, &stat); err != nil {
		return reply{err, nil}
	}

	// Stuff metric information into a map.
	ops := map[string]string{
		"wait_time":    server.SummaryString(metricWaitTime.WithLabelValues(m.name)),
		"queue_length": server.SummaryString(metricQueueLength.WithLabelValues(m.name)),
	}
	for _, op := range []string{
		"open", "close", "delete", "write", "read", "scrub", "size", "stat",
		"readdir", "getxattr", "setxattr", "statfs",
	} {
		ops[op] = opm.String(m.name, op)
	}

	avail := uint64(stat.Bsize) * stat.Bavail

	m.lock.Lock()

	// Leave some breathing room so that we don't push to the disk capacity limit.
	m.status.Full = avail < m.config.DiskLowThreshold

	// Check flag files if it's been a while. We don't do this automatically,
	// only in response to a Statfs. But the tractserver periodically calls
	// Statfs, so we can piggyback on that.
	if time.Since(m.lastFlagFileCheck) > flagFilesCheckInterval {
		m.lock.Unlock()
		flags := loadControlFlags(m.root)
		m.lock.Lock()
		m.status.Flags = flags
		m.lastFlagFileCheck = time.Now()
	}

	// Update queue length and average wait time.
	m.refreshStatus()

	fsStatus := core.FsStatus{
		// (tract/file counts will be filled in by Manager.Statfs)
		AvailSpace: avail,
		TotalSpace: uint64(stat.Bsize) * stat.Blocks,
		Status:     m.status,
		Ops:        ops,
	}
	m.lock.Unlock()

	return reply{nil, statfsReply{status: fsStatus}}
}

// iterateDir iterates over a directory on the disk in batches by scheduling opendir,
// readdir, and closedir calls through the manager. It must not be called from a worker
// goroutine! It will call 'f' on each batch (of unspecified size). If opendir or any
// readdir calls return an error (besides EOF), that error will be returned (and logged).
func (m *Manager) iterateDir(ctx context.Context, dir string, f func(readdirReply)) (err core.Error) {
	var op interface{}
	op, err = m.schedule(ctx, opendirRequest{dir})
	if err != core.NoError {
		log.Errorf("openDir(%q) error: %s", dir, err)
		return
	}
	d := op.(opendirReply).d
	defer m.schedule(ctx, closedirRequest{d})
	for {
		op, err = m.schedule(ctx, readdirRequest{d})
		if err != core.NoError {
			if err != core.ErrEOF {
				log.Errorf("readDir(%q) error: %s", dir, err)
			}
			return err
		}
		f(op.(readdirReply))
	}
}
