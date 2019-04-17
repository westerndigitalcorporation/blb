// Copyright (c) 2017 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package blb

import (
	"context"

	"github.com/klauspost/reedsolomon"

	log "github.com/golang/glog"
	"github.com/westerndigitalcorporation/blb/internal/core"
	"github.com/westerndigitalcorporation/blb/internal/curator/storageclass"
	"github.com/westerndigitalcorporation/blb/internal/server"
	"github.com/westerndigitalcorporation/blb/pkg/rpc"
)

const (
	defaultMaxReconstructInFlight = 1
)

// ReconstructBehavior lets clients control when the client attempts client-side RS
// reconstruction (instead of just retrying and waiting for the curator to do it).
type ReconstructBehavior struct {
	Enabled     bool
	MaxInFlight int
	// TODO: add time-based behavior here, e.g. retry for 1 minute, then reconstruct
}

type reconstructState struct {
	ReconstructBehavior
	sem server.Semaphore
}

func makeReconstructState(behavior ReconstructBehavior) reconstructState {
	s := reconstructState{ReconstructBehavior: behavior}
	if s.Enabled {
		n := s.MaxInFlight
		if n == 0 {
			n = defaultMaxReconstructInFlight
		}
		s.sem = server.NewSemaphore(n)
	}
	return s
}

func (cli *Client) shouldReconstruct(tract *core.TractInfo) bool {
	// Can disable client-wide.
	if !cli.reconstructState.Enabled {
		return false
	}
	// Make sure it's a class we know about.
	cls := storageclass.Get(tract.RS.Class)
	if cls == nil {
		return false
	}
	// And that we have the right number of hosts.
	n, m := cls.RSParams()
	if len(tract.RS.OtherHosts) != n+m {
		return false
	}
	return true
}

func (cli *Client) reconstructOneTract(
	ctx context.Context,
	result *tractResult,
	tract *core.TractInfo,
	thisB []byte,
	offset int64,
	length int) {
	// Note that this offset and length are into the RS pieces, not the tract.
	cli.reconstructState.sem.Acquire()
	defer cli.reconstructState.sem.Release()

	// We checked this above so this shouldn't fail.
	n, m := storageclass.Get(tract.RS.Class).RSParams()

	// At this point, we know that we want to read part of an RS chunk, and the
	// "direct read" has already failed. We need n pieces of data to
	// reconstruct. Let's try reading all of them (except the one we just failed
	// to read) in parallel and use the first n that come in.
	type piece struct {
		idx int
		res []byte
		err core.Error
	}
	pieces := make(chan piece, n+m)
	requests := make([]int, 0, n+m)
	targetIdx := -1

	// First count and sanity-check.
	for i, host := range tract.RS.OtherHosts {
		if tract.RS.OtherTSIDs[i] == tract.RS.TSID {
			targetIdx = i
			continue
		}
		if host == "" {
			continue
		}
		requests = append(requests, i)
	}

	if targetIdx < 0 {
		// OtherTSIDs didn't contain our TSID? Shouldn't happen.
		log.Errorf("rs reconstruct %s: tsid mismatch", tract.Tract)
		*result = tractResult{len(thisB), 0, core.ErrInvalidArgument, ""}
		return
	}

	if len(requests) < n {
		// We don't have enough alive pieces to request.
		log.Errorf("rs reconstruct %s: not enough pieces %d < %d", tract.Tract, len(requests), n)
		*result = tractResult{len(thisB), 0, core.ErrHostNotExist, ""}
		return
	}

	// Do the actual requests.
	nctx, cancel := context.WithCancel(ctx)
	for _, i := range requests {
		go func(i int) {
			// reqID allows ts-ts cancellation of reads.
			reqID := core.GenRequestID()
			rsTract := tract.RS.BaseChunk.Add(i).ToTractID()
			p := piece{idx: i}
			// Don't use ReadInto here, because that would require us to pre-allocate all the
			// memory, instead of having rpc/gob allocate it for us when a response comes in.
			p.res, p.err = cli.tractservers.Read(nctx, tract.RS.OtherHosts[i], nil, reqID, rsTract, core.RSChunkVersion, length, offset)
			// length is already clipped to fall within the RS piece, so short
			// reads are unexpected here. Treat them as an error.
			if (p.err == core.NoError || p.err == core.ErrEOF) && len(p.res) != length {
				p.err = core.ErrShortRead
			}
			pieces <- p
		}(i)
	}

	// Collect responses and stop when we have n.
	var lastErr core.Error
	inFlight, good := len(requests), 0
	data := make([][]byte, n+m)
	for good < n && inFlight > 0 {
		p := <-pieces
		inFlight--
		if p.err != core.NoError && p.err != core.ErrEOF {
			lastErr = p.err
			log.V(1).Infof("rs reconstruct %s: error %s from %s index %d", tract.Tract, lastErr, tract.RS.OtherHosts[p.idx], p.idx)
			continue
		}
		log.V(1).Infof("rs reconstruct %s: read from %s index %d", tract.Tract, tract.RS.OtherHosts[p.idx], p.idx)
		good++
		data[p.idx] = p.res
		defer rpc.PutBuffer(p.res, true)
	}

	cancel()

	// We didn't get enough.
	if good < n {
		// lastErr must be something other than NoError/EOF here, since we must have taken that
		// branch at least once, otherwise we would have good >= n (starting with inFlight >= n).
		log.Errorf("rs reconstruct %s: got %d < %d pieces, last err: %s", tract.Tract, good, n, lastErr)
		*result = tractResult{len(thisB), 0, lastErr, ""}
		return
	}

	// We got enough, try reconstructing!
	enc, e := reedsolomon.New(n, m)
	if e != nil {
		*result = tractResult{len(thisB), 0, core.ErrInvalidArgument, ""}
		return
	}
	// Reconstruct into our destination. Use ReconstructData so we don't waste time rebuilding parity.
	data[targetIdx] = thisB[0:0:length]
	e = enc.ReconstructData(data)
	out := data[targetIdx]
	if e != nil || len(out) != length || &out[0] != &thisB[0] {
		// None of these cases should happen.
		log.Errorf("rs reconstruct error from RS library: %s", e)
		*result = tractResult{len(thisB), 0, core.ErrCorruptData, ""}
		return
	}

	for i := length; i < len(thisB); i++ {
		thisB[i] = 0 // Pad with zeros. See comment in readOneTractReplicated.
	}

	log.V(1).Infof("rs reconstruct %s: success", tract.Tract)

	// Even if the tractserver doesn't think this was an EOF (because it was packed with other
	// data), we might have requested less than the caller asked for. That counts as an EOF too.
	err := core.NoError
	if length < len(thisB) {
		err = core.ErrEOF
	}
	*result = tractResult{len(thisB), length, err, ""}
}
