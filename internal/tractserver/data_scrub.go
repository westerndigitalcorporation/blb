// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package tractserver

import (
	"time"

	log "github.com/golang/glog"

	"github.com/westerndigitalcorporation/blb/internal/core"
	"github.com/westerndigitalcorporation/blb/pkg/tokenbucket"
)

// scrubDisk scrubs the tracts on the disk, forever.
func (s *Store) scrubDisk(diskno int, d Disk) {
	// We use 0 as capacity for token bucket limiter, which means we'll wait
	// for "s.config.ScrubRate" tokens to be refilled every time we call "Take",
	// which is fine for our usage here.
	tb := tokenbucket.New(0, 0)

	for {
		// In case of errors, empty disk, etc., don't spin.  PL-1113
		time.Sleep(5 * time.Minute)

		rate := s.Config().ScrubRate
		if rate < 1024 {
			continue
		}
		tb.SetRate(float32(rate), 0)

		dir, err := d.OpenDir()
		if err == core.ErrDiskRemoved {
			return
		}
		if err != core.NoError {
			log.Errorf("aborting disk scrub for disk %d, failed to open dir, err=%s", diskno, err)
			continue
		}

		log.Infof("scrub of disk %d starting", diskno)

		var scrubbed, ttracts, ok, bad int
		var bytes int64
		start := time.Now()

		for {
			tracts, terr := d.ReadDir(dir)
			if terr != core.NoError {
				break
			}
			ttracts += len(tracts)
			for _, tract := range tracts {
				// If we can't lock it someone else is probably scrubbing it by virtue of reading or writing it.
				if !s.tryLockTract(tract, READ) {
					log.V(5).Infof("tract %s is busy, won't scrub this iteration", tract)
					continue
				}

				// Scrub returns how many bytes it read. We use this to throttle scrubbing to s.config.ScrubRate bytes/sec.
				n, err := d.Scrub(tract)
				s.unlock(tract, READ)

				if err == core.ErrDiskRemoved {
					return
				}

				// This might sleep so we want to unlock the tract before calling it.
				tb.Take(float32(n))

				// Collect and log some stats.
				if s.maybeReportError(tract, err) {
					bad++
				} else {
					ok++
				}
				scrubbed++
				bytes += n
				if scrubbed%10 == 0 {
					logStats(diskno, start, scrubbed, ok, bad, ttracts, bytes, 2)
				}
			}
		}
		d.CloseDir(dir)
		logStats(diskno, start, scrubbed, ok, bad, ttracts, bytes, 0)
	}
}

func logStats(diskno int, start time.Time, scrubbed, ok, bad, total int, bytes int64, level log.Level) {
	elapsed := time.Now().Sub(start)
	bps := bytes / (1 + int64(elapsed.Seconds()))
	log.V(level).Infof("scrub of disk %d: %d/%d tracts total, %d ok %d bad, %d bytes in %s (%d bytes/sec)", diskno, scrubbed, total, ok, bad, bytes, elapsed, int(bps))
}
