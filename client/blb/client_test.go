// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package blb

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/westerndigitalcorporation/blb/pkg/slices"

	"github.com/westerndigitalcorporation/blb/internal/core"
)

// A tsTraceLog collects a set of trace entries from fake tractservers.
type tsTraceLog struct {
	lock sync.Mutex
	log  []tsTraceEntry
}

// add appends an entry
func (log *tsTraceLog) add(e tsTraceEntry) core.Error {
	log.lock.Lock()
	defer log.lock.Unlock()
	log.log = append(log.log, e)
	return core.NoError
}

// checkLength checks that the length of the trace log matches the expected length.
func (log *tsTraceLog) checkLength(t *testing.T, length int) {
	if len(log.log) != length {
		t.Errorf("trace log length mismatch %v != %v", len(log.log), length)
	}
}

// check checks that the specified entry is present.
// The tract id itself is embedded in addr (along with the index of the
// tractserver), so we don't bother checking id.
func (log *tsTraceLog) check(t *testing.T, write bool, addr string, version, length int, off int64) {
	for _, e := range log.log {
		if write {
			if e.write == write &&
				e.addr == addr &&
				e.version == version &&
				e.length == length &&
				e.off == off {
				return
			}
		} else {
			// Reads randomly pick a host to read from so we don't verify the address.
			if e.write == write &&
				e.version == version &&
				e.length == length &&
				e.off == off {
				return
			}
		}
	}
	t.Errorf("trace log missing entry %v %v %v %v %v", write, addr, version, length, off)
}

// newClient creates a Client suitable for testing. The trace function given
// should return core.NoError for a read/write to proceed, and something else to
// inject an error. The disableBackupReads parameter allows the backup read feature
// to be turned off when tests only need to send reads to a single tract.
// Note: this mechanism doesn't provide a way to inject an error into a write
// _and_ have the write reflected on the tractserver anyway. We can test that
// later.
func newClient(trace tsTraceFunc) *Client {
	options := Options{
		DisableRetry: true,
		DisableCache: true,
	}
	cli := newBaseClient(&options)
	cli.master = newMemMasterConnection([]string{"1", "2", "3"})
	cli.curators = newMemCuratorTalker()
	cli.tractservers = newMemTractserverTalker(trace)
	return cli
}

// newTracingClient creates a Client suitable for testing. It's connected to a
// fake master, three fake curators (each responsible for one partition), and as
// many tractservers as there are tract copies (the fake curator places each
// copy on a separate fake tractserver).  The trace log returned can be used to
// check the exact calls made to each tractserver.
func newTracingClient() (*Client, *tsTraceLog) {
	traceLog := new(tsTraceLog)
	return newClient(traceLog.add), traceLog
}

// makeData generates a slightly random byte string of the given length, which
// can be used to check that reads return written data.
func makeData(n int) []byte {
	b := make([]byte, n)
	for i := 0; i < n; i += 64 {
		b[i] = byte(rand.Intn(256))
	}
	return b
}

// checkWrite does blob.Write and fails the test on error or short write.
func checkWrite(t *testing.T, blob *Blob, p []byte) {
	n, err := blob.Write(p)
	if err != nil || n != len(p) {
		t.Fatal("error or short write", n, len(p), err)
	}
}

// checkRead does blob.Read and fails the test on error or short read.
func checkRead(t *testing.T, blob *Blob, length int) []byte {
	p := make([]byte, length)
	n, err := blob.Read(p)
	if err != nil || n != length {
		t.Fatal("error or short read", n, length, err)
	}
	return p
}

// testWriteRead does a single write at the given length and offset, then a
// single read, and checks that the read returns the written data.
func testWriteRead(t *testing.T, blob *Blob, length int, off int64) {
	blob.Seek(off, os.SEEK_SET)
	p1 := makeData(length)
	checkWrite(t, blob, p1)

	blob.Seek(off, os.SEEK_SET)
	p2 := checkRead(t, blob, length)
	if !bytes.Equal(p1, p2) {
		t.Errorf("data mismatch")
	}
}

// testWrite does a single write at the given length and offset.
func testWrite(t *testing.T, blob *Blob, length int, off int64) {
	blob.Seek(off, os.SEEK_SET)
	p1 := makeData(length)
	checkWrite(t, blob, p1)
}

// testWriteReadInParts does a series of sequential writes at the given length
// and offset, and then a series of sequential reads, and checks that the data
// matches.
func testWriteReadInParts(t *testing.T, blob *Blob, length int, off int64, writeparts, readparts int) {
	blob.Seek(off, os.SEEK_SET)
	p1 := makeData(length)
	p := p1[:]

	for i := 0; i < writeparts-1; i++ {
		checkWrite(t, blob, p[:length/writeparts])
		p = p[length/writeparts:]
	}
	checkWrite(t, blob, p)

	blob.Seek(off, os.SEEK_SET)
	var p2 []byte
	for i := 0; i < readparts-1; i++ {
		p2 = append(p2, checkRead(t, blob, length/readparts)...)
	}
	p2 = append(p2, checkRead(t, blob, length-len(p2))...)

	if !bytes.Equal(p1, p2) {
		t.Errorf("data mismatch")
	}
}

func createClient() *Client {
	cli, _ := newTracingClient()
	return cli
}

// createBlob blob and fails the test on errors.
func createBlob(t *testing.T, cli *Client) *Blob {
	if cli == nil {
		cli = createClient()
	}
	blob, e := cli.Create()
	if e != nil {
		t.Fatal("can't create blob", e)
	}
	return blob
}

func TestWriteReadSimple(t *testing.T) {
	testWriteRead(t, createBlob(t, nil), 222, 0)
}

func TestWriteReadCrossTracts(t *testing.T) {
	// a little more than three tracts
	testWriteRead(t, createBlob(t, nil), 3*core.TractLength+4444, 0)
}

func TestWriteReadSmallOffset(t *testing.T) {
	testWriteRead(t, createBlob(t, nil), 333, 5555)
}

func TestWriteReadAnotherTractOffset(t *testing.T) {
	// small offset a little into the third tract
	testWriteRead(t, createBlob(t, nil), 444, 2*core.TractLength+999)
}

func TestWriteReadCrossTractWithOffset(t *testing.T) {
	// add small numbers to be a little off from tract boundaries
	testWriteRead(t, createBlob(t, nil), 3*core.TractLength+8765, 2*core.TractLength+27)
}

func TestWriteReadCrossTractWithOffsetInParts(t *testing.T) {
	testWriteReadInParts(t, createBlob(t, nil), 3*core.TractLength+8765, 2*core.TractLength+27, 7, 5)
}

func TestWriteReadTraced(t *testing.T) {
	cli, trace := newTracingClient()
	blob := createBlob(t, cli)
	testWriteRead(t, blob, 3*core.TractLength+3000000, 2*core.TractLength+2000000)

	// Check that we got the expected writes: four tracts in total, three replicas each.
	//          t  write addr                         ver length               off
	trace.check(t, true, "ts-0000000100000001:0000-0", 1, 0, 0)
	trace.check(t, true, "ts-0000000100000001:0000-1", 1, 0, 0)
	trace.check(t, true, "ts-0000000100000001:0000-2", 1, 0, 0)
	trace.check(t, true, "ts-0000000100000001:0001-0", 1, 0, 0)
	trace.check(t, true, "ts-0000000100000001:0001-1", 1, 0, 0)
	trace.check(t, true, "ts-0000000100000001:0001-2", 1, 0, 0)
	trace.check(t, true, "ts-0000000100000001:0002-0", 1, core.TractLength-2000000, 2000000)
	trace.check(t, true, "ts-0000000100000001:0002-1", 1, core.TractLength-2000000, 2000000)
	trace.check(t, true, "ts-0000000100000001:0002-2", 1, core.TractLength-2000000, 2000000)
	trace.check(t, true, "ts-0000000100000001:0003-0", 1, core.TractLength, 0)
	trace.check(t, true, "ts-0000000100000001:0003-1", 1, core.TractLength, 0)
	trace.check(t, true, "ts-0000000100000001:0003-2", 1, core.TractLength, 0)
	trace.check(t, true, "ts-0000000100000001:0004-0", 1, core.TractLength, 0)
	trace.check(t, true, "ts-0000000100000001:0004-1", 1, core.TractLength, 0)
	trace.check(t, true, "ts-0000000100000001:0004-2", 1, core.TractLength, 0)
	trace.check(t, true, "ts-0000000100000001:0005-0", 1, 2000000+3000000, 0)
	trace.check(t, true, "ts-0000000100000001:0005-1", 1, 2000000+3000000, 0)
	trace.check(t, true, "ts-0000000100000001:0005-2", 1, 2000000+3000000, 0)

	// Check the reads. The client prefers reading from the first replica per tract.
	//          t  write  addr                         ver length               off
	trace.check(t, false, "ts-0000000100000001:0002-0", 1, core.TractLength-2000000, 2000000)
	trace.check(t, false, "ts-0000000100000001:0003-0", 1, core.TractLength, 0)
	trace.check(t, false, "ts-0000000100000001:0004-0", 1, core.TractLength, 0)
	trace.check(t, false, "ts-0000000100000001:0005-0", 1, 2000000+3000000, 0)

	// And check that those were the only calls.
	trace.checkLength(t, 22)
}

func TestWriteReadTracedExactlyOneTract(t *testing.T) {
	cli, trace := newTracingClient()
	blob := createBlob(t, cli)
	testWriteRead(t, blob, core.TractLength, core.TractLength)

	trace.check(t, true, "ts-0000000100000001:0000-0", 1, 0, 0)
	trace.check(t, true, "ts-0000000100000001:0000-1", 1, 0, 0)
	trace.check(t, true, "ts-0000000100000001:0000-2", 1, 0, 0)
	trace.check(t, true, "ts-0000000100000001:0001-0", 1, core.TractLength, 0)
	trace.check(t, true, "ts-0000000100000001:0001-1", 1, core.TractLength, 0)
	trace.check(t, true, "ts-0000000100000001:0001-2", 1, core.TractLength, 0)
	trace.check(t, false, "ts-0000000100000001:0001-0", 1, core.TractLength, 0)
	trace.checkLength(t, 7)
}

func (cli *Client) setupBackupClient(maxNumBackups int, overrideDelay bool) (chan<- time.Time, <-chan bool, <-chan bool) {
	behavior := BackupReadBehavior{
		Enabled:       true,
		MaxNumBackups: maxNumBackups,
		Delay:         2 * time.Millisecond,
	}
	cli.backupReadState = makeBackupReadState(behavior)
	bch := make(chan time.Time)
	rdone := make(chan bool)
	bdone := make(chan bool)
	if overrideDelay {
		cli.backupReadState.delayFunc = func(_ time.Duration) <-chan time.Time {
			// send a time value to this channel to unblock.
			<-bch
			return time.After(0 * time.Second)
		}
		readDone = func() {
			rdone <- true
		}
		backupPhaseDone = func() {
			bdone <- true
		}
	}
	randOrder = func(n int) []int {
		order := make([]int, n)
		for i := 0; i < n; i++ {
			order[i] = i
		}
		return order
	}
	return bch, rdone, bdone
}

func TestReadOneTractBackups(t *testing.T) {
	cli, trace := newTracingClient()
	bch, rdone, bdone := cli.setupBackupClient(1, true) // 2 backup requests

	// Write a blob and check the writes are logged.
	blob := createBlob(t, cli)
	blob.Seek(core.TractLength, os.SEEK_SET)
	p1 := makeData(core.TractLength)
	checkWrite(t, blob, p1)

	trace.check(t, true, "ts-0000000100000001:0000-0", 1, 0, 0)
	trace.check(t, true, "ts-0000000100000001:0000-1", 1, 0, 0)
	trace.check(t, true, "ts-0000000100000001:0000-2", 1, 0, 0)
	trace.check(t, true, "ts-0000000100000001:0001-0", 1, core.TractLength, 0)
	trace.check(t, true, "ts-0000000100000001:0001-1", 1, core.TractLength, 0)
	trace.check(t, true, "ts-0000000100000001:0001-2", 1, core.TractLength, 0)

	// Test each request scenario by overriding backupRequestFunc
	// Does the first request succeed before backup is sent work?
	sendPrimary := make(chan bool)
	sendBackup := make(chan bool)
	backupRequestFunc = func(
		ctx context.Context,
		cli *Client,
		reqID string,
		tract *core.TractInfo,
		thisB []byte,
		thisOffset int64,
		nReaders int,
		order []int) <-chan tractResultRepl {

		resultCh := make(chan tractResultRepl)
		<-sendPrimary
		go cli.readOneTractBackupReplicated(ctx, reqID, tract, thisB, thisOffset, order, 0, resultCh)

		<-sendBackup
		go cli.readOneTractBackupReplicated(ctx, reqID, tract, thisB, thisOffset, order, 1, resultCh)
		return resultCh
	}

	go func() {
		blob.Seek(core.TractLength, os.SEEK_SET)
		p := make([]byte, core.TractLength)
		n, err := blob.Read(p)
		if err != nil || n != core.TractLength {
			t.Fatal("error or short read", n, core.TractLength, err)
		}
	}()

	// release fake sleep
	sendPrimary <- true
	bch <- time.Time{}

	// wait for the rpc to finish. otherwise we will race with checking the trace entry.
	<-rdone

	// check the read is there.
	trace.check(t, false, "ts-0000000100000001:0001-0", 1, core.TractLength, 0)

	// check that we can exit
	sendBackup <- true
	bch <- time.Time{}
	<-bdone
}

func TestReadFailoverWithBackups(t *testing.T) {
	_ = func(e tsTraceEntry) core.Error {
		// Reads from the first two tractservers fail.
		if !e.write && !strings.HasSuffix(e.addr, "2") {
			return core.ErrRPC
		}
		return core.NoError
	}
	// TODO test all backups fail and read uses the fallback path
}

func TestWriteFailSimple(t *testing.T) {
	fail := func(e tsTraceEntry) core.Error {
		// All writes to last tractserver fail
		if e.write && strings.HasSuffix(e.addr, "2") {
			return core.ErrRPC
		}
		return core.NoError
	}
	blob := createBlob(t, newClient(fail))
	_, err := blob.Write(makeData(100))
	if err == nil {
		t.Errorf("write succeeded when it shouldn't have")
	}
}

func TestWriteFailLonger(t *testing.T) {
	fail := func(e tsTraceEntry) core.Error {
		// Writes to the last tract on the first tractserver fail.
		if e.write && e.addr == "ts-0000000100000001:0004-0" {
			return core.ErrRPC
		}
		return core.NoError
	}
	blob := createBlob(t, newClient(fail))
	_, err := blob.Write(makeData(core.TractLength * 5))
	if err == nil {
		t.Errorf("write succeeded when it shouldn't have")
	}
}

func TestReadFailover(t *testing.T) {
	fail := func(e tsTraceEntry) core.Error {
		// Reads from the first two tractservers fail.
		if !e.write && !strings.HasSuffix(e.addr, "2") {
			return core.ErrRPC
		}
		return core.NoError
	}
	cli := newClient(fail)
	testWriteRead(t, createBlob(t, cli), 3*core.TractLength+8765, 2*core.TractLength+27)
}

func TestLengths(t *testing.T) {
	blob := createBlob(t, nil)
	length := 2*core.TractLength + 3335
	checkWrite(t, blob, makeData(length))

	info, err := blob.Stat()
	if err != nil {
		t.Fatalf("error getting tract length: %s", err)
	} else if info.NumTracts != 3 {
		t.Fatalf("wrong number of tracts: %d != %d", info.NumTracts, 3)
	}

	numBytes, err := blob.ByteLength()
	if err != nil {
		t.Fatalf("error getting byte length: %s", err)
	} else if int(numBytes) != length {
		t.Fatalf("wrong byte length: %d != %d", numBytes, length)
	}
}

func TestSeekEnd(t *testing.T) {
	blob := createBlob(t, nil)

	checkWrite(t, blob, makeData(1000))

	off, err := blob.Seek(-200, os.SEEK_END)
	if err != nil {
		t.Fatalf("error seeking: %s", err)
	} else if off != 800 {
		t.Fatalf("seek returned wrong offset: %d", off)
	}

	// Make sure it actually updated the offset.
	checkWrite(t, blob, makeData(1000))
	length, err := blob.ByteLength()
	if err != nil {
		t.Fatalf("error getting byte length: %s", err)
	} else if length != 1800 {
		t.Fatalf("wrong byte length: %d != %d", length, 1800)
	}
}

// Test padded bytes for a hole in a tract are all 0's.
func TestPadding(t *testing.T) {
	// Create a blob with a single tract.
	blob := createBlob(t, nil)
	len := core.TractLength / 3

	// Fill the first 1/3 of the tract with random bytes and verify.
	testWriteRead(t, blob, len, 0)

	// Fill the last 1/3 of the tract with random bytes and verify.
	testWriteRead(t, blob, len, int64(len)*2)

	// Fill the byte slice with random bytes and use it to read the middle
	// 1/3 of the tract. The result should be all zeros.
	b := makeData(len)
	blob.Seek(int64(len), os.SEEK_SET)
	if n, err := blob.Read(b); nil != err || n != len {
		t.Fatalf("failed to read blob")
	}
	for _, bb := range b {
		if 0 != bb {
			t.Fatalf("padding should be 0")
		}
	}
}

// Read from a blob of a single tract where the first half is empty.
func TestHalfEmptyTract(t *testing.T) {
	blob := createBlob(t, nil)
	len := core.TractLength / 2

	// Fill the second half of the tract with random bytes and verify.
	testWriteRead(t, blob, len, int64(len))

	// Read the first half back and it should be padded with zeros.
	exp := make([]byte, len)
	blob.Seek(0, os.SEEK_SET)
	got := checkRead(t, blob, len)
	if 0 != bytes.Compare(exp, got) {
		t.Fatalf("failed to read blob")
	}
}

// Read from a blob of a single tract where there is a hole at the middle.
func TestHoleInTract(t *testing.T) {
	blob := createBlob(t, nil)
	len := core.TractLength / 3

	// Fill the first 1/3 of the tract with random bytes and verify.
	testWriteRead(t, blob, len, 0)

	// Fill the last 1/3 of the tract with random bytes and verify.
	testWriteRead(t, blob, len, int64(len)*2)

	// Read the middle 1/3 back and it should be padded with zeros.
	exp := make([]byte, len)
	blob.Seek(int64(len), os.SEEK_SET)
	got := checkRead(t, blob, len)
	if 0 != bytes.Compare(exp, got) {
		t.Fatalf("failed to read blob")
	}
}

// Read from a blob of two tracts with a hole across the tract boundary.
func TestHoleCrossTracts(t *testing.T) {
	blob := createBlob(t, nil)
	len := core.TractLength / 2

	// Fill the first half of the first tract with random bytes and verify.
	testWriteRead(t, blob, len, 0)
	// Fill the second half of the second tract with random bytes and verify.
	testWriteRead(t, blob, len, int64(len)*3)
	// After the above writes, there is a hole in the blob that starts
	// from the middle of the first tract and ends at the middle of the
	// second tract -- the hole spans across the two tracts' boundary.

	info, err := blob.Stat()
	if nil != err {
		t.Fatalf("failed to stat the blob: %s", err)
	}
	if 2 != info.NumTracts {
		t.Fatalf("failed to create 2 tracts")
	}

	// Read the hole out and it should be padded with zeros.
	exp := make([]byte, len*2)
	blob.Seek(int64(len), os.SEEK_SET)
	got := checkRead(t, blob, len*2)
	if 0 != bytes.Compare(exp, got) {
		t.Fatalf("failed to read blob")
	}
}

// Read from a blob of three tracts, where the second one is totally empty.
func TestHoleEntireTract(t *testing.T) {
	blob := createBlob(t, nil)

	// Fill the first tract with random bytes and verify.
	testWriteRead(t, blob, core.TractLength, 0)
	// Fill the third tract with random bytes and verify.
	testWriteRead(t, blob, core.TractLength, core.TractLength*2)
	// After the above writes, the second tract is empty and becomes a hole
	// in the blob.

	info, err := blob.Stat()
	if nil != err {
		t.Fatalf("failed to stat the blob: %s", err)
	}
	if 3 != info.NumTracts {
		t.Fatalf("failed to create 3 tracts")
	}

	// Read the middle tract and it should be padded with zeros.
	exp := make([]byte, core.TractLength)
	blob.Seek(core.TractLength, os.SEEK_SET)
	got := checkRead(t, blob, core.TractLength)
	if 0 != bytes.Compare(exp, got) {
		t.Fatalf("failed to read blob")
	}
}

// Request to read bytes that goes beyond the end of a blob and verify the
// number of bytes we actually read.
func TestShortRead(t *testing.T) {
	blob := createBlob(t, nil)
	len := core.TractLength / 2

	// Fill the first half of the first tract with random bytes and verify.
	testWriteRead(t, blob, len, 0)

	// Try to read the entire tract out. We should get an EOF and only read
	// what we wrote above.
	blob.Seek(0, os.SEEK_SET)
	b := make([]byte, core.TractLength)
	n, err := blob.Read(b)
	if len != n || io.EOF != err {
		t.Fatalf("expected (%d, %s) and got (%d, %s)", len, io.EOF, n, err)
	}
}

// Test listing blobs.
func TestListBlobs(t *testing.T) {
	var blobs, out []string
	cli := newClient(nil)
	// create enough to spread across partitions
	for i := 0; i < 100; i++ {
		blobs = append(blobs, createBlob(t, cli).ID().String())
	}

	iter := cli.ListBlobs(context.Background())
	for {
		ids, err := iter()
		if err != nil {
			t.Fatalf("ListBlobs: error iterating: %v", err)
		}
		if ids == nil {
			break
		}
		for _, id := range ids {
			out = append(out, id.String())
		}
	}

	if !slices.EqualStrings(blobs, out) {
		t.Errorf("ListBlobs: expected %v, got %v", blobs, out)
	}
}
