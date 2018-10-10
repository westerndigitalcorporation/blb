// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package curator

import (
	"github.com/westerndigitalcorporation/blb/internal/core"
)

// TractserverTalker is an abstraction of the transport layer between the Curator and the Tractserver.
type TractserverTalker interface {
	// SetVersion asks the tractserver (tsid, addr) to set the version of 'id' to newVersion.
	// This will only succeed if the version at the tractserver is newVersion or newVersion-1.
	SetVersion(addr string, tsid core.TractserverID, id core.TractID, newVersion int, conditionalStamp uint64) core.Error

	// PullTract asks the tractserver at 'addr' with id 'tsid' to pull the tract with id 'id' and
	// version 'version' from the source hosts 'from', each of which should own a copy of the tract.
	PullTract(addr string, tsid core.TractserverID, from []string, id core.TractID, version int) core.Error

	// CheckTracts asks the tractserver at 'addr' with id 'tsid' if it has the tracts 'tracts'.
	CheckTracts(addr string, tsid core.TractserverID, tracts []core.TractState) core.Error

	// GCTract tells the tractserver at 'addr' that the tracts in 'old' are obsolete and the blob that
	// has the tracts in 'gone' has been deleted.
	//
	// Any host may delete the tracts in 'old' (or any older version thereof).
	// The tracts in 'gone' are safe to be deleted from any host as the blobs are permanently gone.
	//
	// Returns any error encountered when trying to talk to the tractserver.
	GCTract(addr string, tsid core.TractserverID, old []core.TractState, gone []core.TractID) core.Error

	// CtlStatTract is like the client StatTract, but bypasses request limits.
	CtlStatTract(addr string, tsid core.TractserverID, id core.TractID, version int) core.StatTractReply

	// PackTracts asks the tractserver to create a chunk with a bunch of tracts in it.
	// It will pull data from other tractservers and write it to the local disk.
	PackTracts(addr string, tsid core.TractserverID, length int, tracts []*core.PackTractSpec, id core.RSChunkID) core.Error

	// RSEncode asks the tractserver to pull N chunks from other tractservers, compute M
	// parity chunks, and write the parity chunks to other tractservers.
	RSEncode(addr string, tsid core.TractserverID, id core.RSChunkID, length int, srcs, dests []core.TSAddr, indexMap []int) core.Error
}
