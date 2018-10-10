// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package loadblb

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/beorn7/perks/quantile"

	log "github.com/golang/glog"
	client "github.com/westerndigitalcorporation/blb/client/blb"
	"github.com/westerndigitalcorporation/blb/internal/core"
)

// Byte size constants
const (
	KB = 1024               // How many bytes are there in a kilobyte,
	MB = 1024 * 1024        // ... and in a megabyte,
	GB = 1024 * 1024 * 1024 // ... and in a gigabyte.
)

type handlerResult struct {
	err   error
	stats string // Throughput and latency stats.
}

// eventStats is used to record some basic stats during the processing of
// events. It keeps track of processed bytes and latency for processing each
// event. Its stringer returns a formatted result including total bytes,
// throughput and latency distribution.
//
// Does its own locking.
type eventStats struct {
	start time.Time // Start time.

	lock     sync.Mutex       // Protect below.
	byteSize int64            // How many bytes read/written.
	lat      *quantile.Stream // Operation latency.
}

func newStat() *eventStats {
	objectives := map[float64]float64{0.1: 0.05, 0.5: 0.05, 0.9: 0.01, 0.99: 0.001, 0.9999: 0.000001}
	return &eventStats{start: time.Now(), lat: quantile.NewTargeted(objectives)}
}

func (s *eventStats) update(n int64, d time.Duration) {
	s.lock.Lock()
	s.byteSize += n
	s.lat.Insert(float64(d) / 1e9)
	s.lock.Unlock()
}

func (s *eventStats) String() string {
	s.lock.Lock()
	defer s.lock.Unlock()

	elapsedInSec := time.Now().Sub(s.start).Seconds()
	sizeInMB := float64(s.byteSize) / MB
	str := fmt.Sprintf("processed bytes: %f MB\n", sizeInMB)
	str += fmt.Sprintf("throughput: %f MB/sec\n", sizeInMB/elapsedInSec)
	str += fmt.Sprintf("latency distribution:\n")
	for _, quantile := range []float64{0.1, 0.5, 0.9, 0.99, 0.9999} {
		str += fmt.Sprintf("%g=%.3f ms\n", quantile*100, s.lat.Query(quantile)*1000)
	}
	return str
}

// LoadHandler accepts events and executes the desired I/O operations. When
// any error is encountered, it stops and sends the result over 'DoneC' chan.
// One can also call 'Stop' explicitly to stop the running of the handler and a
// nil error will be sent over the chan.
//
// Whenever an error is encountered or user calls 'Stop' explicitly, the handler
// stops accepting/processing events.
type LoadHandler struct {
	DoneC chan handlerResult // Where should the result sent to?

	cfg    HandlerConfig    // Configuration parameters.
	eventC chan interface{} // An internal chan for storing incoming events.
	stop   uint32           // stop flag.

	// Stats. Doing their own locking.
	readStat  *eventStats
	writeStat *eventStats

	lock    sync.Mutex      // Protect below.
	created []client.BlobID // Keep track of created blobs so we can clean them up.
}

// NewLoadHandler creates a new load handler
func NewLoadHandler(cfg HandlerConfig) *LoadHandler {
	return &LoadHandler{
		DoneC:  make(chan handlerResult, 1),
		cfg:    cfg,
		eventC: make(chan interface{}, cfg.QueueLength),
	}
}

// Start starts the handler. 'collections' maps readers to their associated blob
// collections.
func (h *LoadHandler) Start(collections map[string]*blobCollection) {
	h.readStat = newStat()
	h.writeStat = newStat()
	log.Infof("starting %d worker goroutines", h.cfg.NumWorkers)
	for i := 0; i < h.cfg.NumWorkers; i++ {
		go func(id int, collections map[string]*blobCollection) {
			log.Infof("worker #%d started", id)
			if err := h.worker(id, collections); err != nil {
				h.toStop(err)
			}
		}(i, collections)
	}
}

// Accept requests to add a slice of events to be processed by the handler.
// It's possible that some (even all) of these events are dropped by the handler
// if it's too busy.
func (h *LoadHandler) Accept(events []interface{}) {
	if atomic.LoadUint32(&h.stop) == 1 {
		return
	}
	for _, e := range events {
		select {
		case h.eventC <- e:
		default:
		}
	}
}

// Stop stops the handler with nil error.
func (h *LoadHandler) Stop() {
	h.toStop(nil)
}

// Singal stopC with the given error.
func (h *LoadHandler) toStop(err error) {
	if atomic.CompareAndSwapUint32(&h.stop, 0, 1) {
		// Compute stats.
		stats := fmt.Sprintf("  Write stats:\n%s  Read stats:\n%s",
			h.writeStat.String(), h.readStat.String())

		// Signal doneC.
		h.DoneC <- handlerResult{err: err, stats: stats}
	}
}

// worker pulls from eventC and execute the I/Os. It returns whenver an error is
// encountered or the handler stops by others.
func (h *LoadHandler) worker(id int, collections map[string]*blobCollection) error {
	// Create a client driver. Per-worker client is used to avoid sharing
	// connection cache between different workers (and thus leads to
	// unrealistic load ratio between curator and tractserver).
	cli := client.NewClient(h.cfg.Options)

	// Create a buffer. We will reuse it for all events.
	b := make([]byte, 8*MB)
	var off int64
	var err error

	for event := range h.eventC {
		// Check if the 'stop' flag is set.
		// Strictly speaking this is not necessary and won't affect the
		// test result. However, we want to stop the workers right away
		// if any error is encountered.
		if atomic.LoadUint32(&h.stop) == 1 {
			return nil
		}

		// Do the hard work to talk to blb.
		start := time.Now()
		switch e := event.(type) {
		case readEvent:
			off, err = h.handleRead(collections, cli, b, e)
			h.readStat.update(off, time.Now().Sub(start))
		case writeEvent:
			off, err = h.handleWrite(collections, cli, b, e)
			h.writeStat.update(off, time.Now().Sub(start))
		default:
			err = fmt.Errorf("unknown event type")
		}
		if err != nil {
			return err
		}
	}

	return nil
}

func (h *LoadHandler) handleRead(collections map[string]*blobCollection, cli *client.Client, b []byte, e readEvent) (int64, error) {
	log.V(1).Infof("processing a read request of size %d", e.size)
	off := int64(0)

	// Select a random blob.
	id, size, ok := collections[e.reader].getRandRecent(e.recentN)
	if !ok {
		return off, nil
	}
	blob, operr := cli.Open(id, "r")
	if nil != operr {
		return off, operr
	}

	// Verify the length.
	if length, err := blob.ByteLength(); err != nil {
		return off, err
	} else if size != length {
		return off, fmt.Errorf("wrong size, exp %d and got %d", size, length)
	}

	// Do we want to read the entire blob or only part of it?
	if e.size > 0 && e.size < size {
		size = e.size
	}

	// Use the buffer to read data.
	for size > 0 {
		end := minInt(int(size), len(b))
		n, err := blob.Read(b[:end])
		if err != nil {
			return off, err
		}
		if !verifyBytes(blob.ID(), off, b[:end]) {
			return off, fmt.Errorf("mismatched read result")
		}
		size -= int64(n)
		off += int64(n)
	}
	// Sanity check.
	if size != 0 {
		return off, fmt.Errorf("failed to read the requested amount of data")
	}

	return off, nil
}

func (h *LoadHandler) handleWrite(collections map[string]*blobCollection, cli *client.Client, b []byte, e writeEvent) (int64, error) {
	log.V(1).Infof("processing a write request of size %d", e.size)
	off := int64(0)

	// Create a blob.
	blob, err := cli.Create()
	if nil != err {
		return off, err
	}
	h.lock.Lock()
	h.created = append(h.created, blob.ID())
	h.lock.Unlock()

	// Use the buffer to write data.
	size := e.size
	for size > 0 {
		s := minInt(int(size), len(b))
		fillBytes(blob.ID(), off, b[:s])
		n, err := blob.Write(b[:s])
		if nil != err {
			return off, err
		}
		size -= int64(n)
		off += int64(n)
	}
	// Sanity check.
	if size != 0 {
		return off, fmt.Errorf("failed to write the requested amount of data")
	}

	// Add the blob to the given collections.
	for _, r := range e.readers {
		collections[r].put(blob.ID(), e.size)
	}

	return off, nil
}

// Cleanup removes created blobs.
func (h *LoadHandler) Cleanup() {
	cli := client.NewClient(h.cfg.Options)
	h.lock.Lock()
	for _, blob := range h.created {
		cli.Delete(context.Background(), blob)
	}
	h.lock.Unlock()
}

//====== helpers ======//

func maxInt64(a, b int64) int64 {
	if a >= b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a <= b {
		return a
	}
	return b
}

// fillBytes fills 'buf' with deterministic bytes. The first byte is computed
// from 'computeBase' and each following byte equlas the previous one plus one.
func fillBytes(id client.BlobID, off int64, buf []byte) {
	base := computeBase(id, off)
	for i := range buf {
		buf[i] = base
		base++
	}
}

// verifyBytes verifies bytes in 'buf' is computed from 'fillBytes'.
func verifyBytes(id client.BlobID, off int64, buf []byte) bool {
	base := computeBase(id, off)
	for _, b := range buf {
		if b != base {
			return false
		}
		base++
	}
	return true
}

// A deterministic mapping from (blob, offset) to a byte.
func computeBase(id client.BlobID, off int64) byte {
	bid := core.BlobID(id)
	x := uint32(bid.Partition()) ^ uint32(bid.ID()) ^ uint32(off>>32) ^ uint32(off)
	return byte(x) ^ byte(x>>8) ^ byte(x>>16) ^ byte(x>>24)
}
