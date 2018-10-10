// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package testblb

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/golang/glog"
	"github.com/westerndigitalcorporation/blb/client/blb"
)

// TestWriteReadLong writes data to blb, reads it out and verifies we get what we wrote.
func (tc *TestCase) TestWriteReadLong() error {
	var killed uint32
	errCh := make(chan error)
	wg := &sync.WaitGroup{}

	kill := func() {
		atomic.StoreUint32(&killed, 1)
		wg.Wait()
	}

	// Start writer goroutines.
	for i := 0; i < tc.testCfg.NumWriters; i++ {
		wg.Add(1)
		go tc.writerLoop(i, wg, &killed, errCh)
	}

	runTimer := time.NewTimer(tc.testCfg.TestTime)
	select {
	case <-runTimer.C: // Stop the test when the timer expires.
		kill()
		log.Infof("test done")
		return nil

	case err := <-errCh: // Stop the test when there is an error.
		kill()
		return err
	}
}

// Write random bytes to blb.
func (tc *TestCase) writerLoop(index int, wg *sync.WaitGroup, killed *uint32, errCh chan error) {
	log.Infof("writer #%d started", index)
	lower := tc.testCfg.WriteSizeLowMB * mb
	upper := tc.testCfg.WriteSizeHighMB * mb
	inbuf := make([]byte, upper)
	outbuf := make([]byte, upper)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	ticker := time.NewTicker(tc.testCfg.WriteInterval)
	defer ticker.Stop()
	defer wg.Done()

	for {
		if atomic.LoadUint32(killed) != 0 {
			return
		}

		<-ticker.C
		// Create a blob. The repl factor should be in [2, MaxReplFactor].
		replFactor := r.Intn(tc.testCfg.MaxReplFactor-1) + 2
		blob, err := tc.c.Create(blb.ReplFactor(replFactor))
		if nil != err {
			errCh <- fmt.Errorf("failed to create blob: %s", err)
			return
		}

		// Generate some random bytes.
		size := lower + r.Intn(upper-lower+1)
		size = size / 8 * 8 // Round down to the nearest multiple of 8.
		in := inbuf[:size]
		rand.Read(in)

		// Write the bytes to the blob.
		if _, err := blob.Write(in); nil != err {
			errCh <- fmt.Errorf("failed to write blob: %s", err)
			return
		}

		// Read the bytes back from the blob.
		blob.Seek(0, os.SEEK_SET)
		out := outbuf[:size]
		if _, err := blob.Read(out); nil != err {
			errCh <- fmt.Errorf("failed to read blob: %s", err)
			return
		}

		// Compare the two.
		if 0 != bytes.Compare(in, out) {
			errCh <- fmt.Errorf("failed to read data back")
			return
		}
		log.Infof("verified blob: size=%dmb, repl factor=%d", size/mb, replFactor)
	}
}
