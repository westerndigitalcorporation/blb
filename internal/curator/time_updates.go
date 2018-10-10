// Copyright (c) 2017 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package curator

import (
	"time"

	log "github.com/golang/glog"
	"github.com/westerndigitalcorporation/blb/internal/core"
	"github.com/westerndigitalcorporation/blb/internal/curator/durable/state"
)

const (
	// How large the update times queue can get. We silently drop if we have
	// more updates than this.
	maxTimeQueue = 500000
	// How large a single batch we can send to raft. Corresponds to up to ~480KB
	// encoded command size, and since we use a 1s ticker, at least 20K blob
	// opens per second, which should cover expected load for a while.
	// The maximum size of a raft command is 1MB, so we have to stay under that.
	maxTimeBatch = 20000
)

func (c *Curator) touchBlob(id core.BlobID, now int64, forRead, forWrite bool) {
	var mtime, atime int64
	if forWrite {
		mtime = now
	}
	if forRead {
		atime = now
	}
	c.lock.Lock()
	if len(c.timeUpdates) < maxTimeQueue {
		c.timeUpdates = append(c.timeUpdates, state.UpdateTime{
			Blob:  id,
			MTime: mtime,
			ATime: atime,
		})
	}
	c.lock.Unlock()
}

func (c *Curator) updateTimesLoop() {
	ticker := time.NewTicker(1 * time.Second)

	for {
		<-ticker.C
		c.blockIfNotLeader()

		// Grab a batch of updates.
		c.lock.Lock()
		n := len(c.timeUpdates)
		if n > maxTimeBatch {
			n = maxTimeBatch
		}
		batch := c.timeUpdates[:n]
		c.timeUpdates = c.timeUpdates[n:]
		c.lock.Unlock()

		if len(batch) == 0 {
			continue
		}

		// Do the update.
		err := c.stateHandler.UpdateTimes(mergeUpdates(batch))
		switch err {
		case core.NoError:
			// Update successful.
		case core.ErrRaftNodeNotLeader, core.ErrRaftNotLeaderAnymore:
			// Lost leadership, just drop these updates.
			log.Errorf("updateTimesLoop: error applying batch of len %d: %s; dropping", n, err)
		case core.ErrRaftTimeout, core.ErrRaftTooManyPendingReqs:
			// We are still leader, just busy. Retry updates.
			log.Infof("updateTimesLoop: too busy applying batch of len %d: %s; retrying", n, err)
			c.lock.Lock()
			c.timeUpdates = append(c.timeUpdates, batch...)
			c.lock.Unlock()
		}
	}
}

// Merges time updates to the same blob, to reduce the amount of data sent
// through raft.
func mergeUpdates(batch []state.UpdateTime) (out []state.UpdateTime) {
	m := make(map[core.BlobID]int) // index in out
	for _, tu := range batch {
		if i, ok := m[tu.Blob]; ok {
			// merge
			if tu.MTime > out[i].MTime {
				out[i].MTime = tu.MTime
			}
			if tu.ATime > out[i].ATime {
				out[i].ATime = tu.ATime
			}
		} else {
			// add
			m[tu.Blob] = len(out)
			out = append(out, tu)
		}
	}
	return
}
