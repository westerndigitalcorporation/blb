// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package core

import "github.com/westerndigitalcorporation/blb/pkg/rpc"

// This file describes the RPC interface exported by the tractserver.

// Control methods (sent by master, curator, tractserver):

// SetVersionMethod is the method name for curator to tractserver set tract version.
const SetVersionMethod = "TSCtlHandler.SetVersion"

// SetVersionReq is sent by the Curator to set the version of a tract.
type SetVersionReq struct {
	// The tractserver that the curator expects it's talking to.
	TSID TractserverID

	// The ID of the tract we're setting the version on.
	ID TractID

	// What version does the curator want the tractserver to increase to?
	// This will be one more than the current durable version.
	NewVersion int

	// If this is nonzero, the SetVersion is made conditional on the
	// modification stamp of the tract being equal to this value.
	ConditionalStamp uint64
}

// SetVersionReply is a reply to a SetVersionReq.
type SetVersionReply struct {
	// Was there an error handling this request?
	Err Error

	// What version does the tractserver have now?
	NewVersion int
}

// CheckTractsMethod is the method name for curator to check that the tractserver is hosting the data the curator expects it to.
const CheckTractsMethod = "TSCtlHandler.CheckTracts"

// TractState is a pair of TractID and version number.
type TractState struct {
	ID      TractID
	Version int
}

// CheckTractsReq is sent from a Curator to a Tractserver verify that it's
// hosting the tracts that the curator thinks it's hosting.
type CheckTractsReq struct {
	// The tractserver that the curator expects it's talking to.
	TSID TractserverID

	// What tracts must the tractserver host?
	Tracts []TractState
}

// CheckTractsReply is the reply to a CheckTractsReq
type CheckTractsReply struct {
	// NoError if everything went OK, otherwise an error representing what
	// went wrong.
	Err Error
}

// GCTractMethod is the method name for garbage collecting a tract.
const GCTractMethod = "TSCtlHandler.GCTract"

// GCTractReq is a request to a tractserver to garbage collect some tracts.
type GCTractReq struct {
	// The tractserver that the curator expects it's talking to.
	TSID TractserverID

	// Outdated tracts that the tractserver can GC.
	Old []TractState

	// Tracts that belong to a deleted blob.
	Gone []TractID
}

// PullTractMethod is the method name for curator to tractserver to pull tract. Reply is Error.
const PullTractMethod = "TSCtlHandler.PullTract"

// PullTractReq is a request from a curator to a tractserver to pull the tract (ID, Version) from 'From'.
type PullTractReq struct {
	// The tractserver that the curator expects it's talking to.
	TSID TractserverID

	// What hosts should we try to pull the tract from? Multiple hosts are
	// provided in case error is encountered and retry is performed from
	// different sources.
	From []string

	// What is the ID of the tract?
	ID TractID

	// And what is the version?
	Version int
}

// GetTSIDMethod is the method name for the GetTSID method. Request is struct{},
// reply is core.TractserverID.
const GetTSIDMethod = "TSCtlHandler.GetTSID"

// TSAddr is a pair of TSID and hostname that refers to a tractserver.
type TSAddr struct {
	ID   TractserverID // If 0, don't write out this parity chunk.
	Host string
}

// PackTractsMethod is the method name for the PackTracts RPC. Request is
// PackTractsReq, reply is Error.
const PackTractsMethod = "TSCtlHandler.PackTracts"

// PackTractSpec describes one tract to pack.
type PackTractSpec struct {
	ID      TractID  // tract id
	From    []TSAddr // host(s) that it can be read from
	Version int      // expected version
	Offset  int      // destination offset in packed tract
	Length  int      // expected length
}

// PackTractsReq is a request to the TS to pack mulitple regular data tracts
// into a data chunk and store it locally, in preparation for erasure coding.
// The length will probably be greater than the standard tract length.
type PackTractsReq struct {
	TSID    TractserverID    // The tractserver that the curator expects it's talking to.
	Length  int              // Total length of packed chunk. TS will pad with zeros.
	Tracts  []*PackTractSpec // Tracts to pack. Must be ordered by Offset and must not overlap.
	ChunkID RSChunkID        // What chunk id to write to.
}

// RSEncodeMethod is the method name for RSEncode. Request is RSEncodeReq, reply is Error.
const RSEncodeMethod = "TSCtlHandler.RSEncode"

// RSEncodeReq is a request to perform erasure coding or reconstruction on a chunk.
// For Reed-Solomon with N data and M parity pieces, there should be exactly N Srcs
// and M Dests. This tractserver will read the N pieces from other
// tractservers (or itself), perform the calculation, and then write out the
// coded parity or reconstructed data pieces to other tractservers (or itself).
// All pieces must be the same length. Since holding (N+M)*L bytes in memory
// might be prohibitive, the IO and calculation may be carried out in smaller
// pieces.
type RSEncodeReq struct {
	TSID     TractserverID // The tractserver that the curator expects it's talking to.
	ChunkID  RSChunkID     // The starting chunk id for this RS chunk.
	Length   int           // Total length of each piece of the chunk.
	Srcs     []TSAddr      // Where to read the N data pieces from.
	Dests    []TSAddr      // Where to write the M parity pieces to.
	IndexMap []int         // Data indexes for reconstruction (see below).
}

// To simplify the implementation, we use the same RPC for coding and reconstruction.
// IndexMap[i] specifies the index of [Srcs||Dests][i] in the RS chunk (as if Srcs and
// Dests were concatenated).
//
// For coding, IndexMap should be nil. That will be interpreted as the identity map: the
// addresses in Srcs correspond to their respective data pieces, and the addresses in
// Dests correspond to parity pieces, so the TS will read all the data and write out all
// parity.
//
// For reconstruction, provide an IndexMap of length N+M. For example, suppose we were
// doing RS(5, 3) and we lost the second data piece (index 1), and the first parity
// piece (index 5). I.e. we have 0_234|_67. Then the curator might ask a TS to pull
// pieces 0, 2, 3, 4, and 6, and write out 1 and 5. It would send an IndexMap:
// [0, 2, 3, 4, 6, 1, 5, -1], which means: Srcs[0] has piece 0, Srcs[1] has piece 2,
// Srcs[2] has piece 3, etc.; Dests[0] should get piece 1, Dests[1] should get piece 5,
// and don't write anything to Dests[2]. (But note that len(Dests) must still be 3! Fill
// it with zero values.)

// ---

// Service methods (sent by clients):

// CreateTractMethod is the method name for curator to tractserver create tract. Reply is Error.
const CreateTractMethod = "TSSrvHandler.CreateTract"

// CreateTractReq is sent from a client to a tractserver to ask the tractserver
// to create a tract with id 'ID' and writes the bytes to it.
type CreateTractReq struct {
	TSID TractserverID
	ID   TractID
	B    []byte
	Off  int64
	Pri  Priority

	// Local-only flag to indicate whether B is exclusively owned.
	bExclusive bool
}

// WriteMethod is the method name for client to tractserver write. Reply is Error.
const WriteMethod = "TSSrvHandler.Write"

// CtlWriteMethod is the method name for tractserver to tractserver write/create.
const CtlWriteMethod = "TSCtlHandler.CtlWrite"

// WriteReq is sent from the client to a tractserver to write to a tract.
type WriteReq struct {
	ID      TractID
	Version int
	B       []byte
	Off     int64
	Pri     Priority

	// ID for cancellation.
	ReqID string

	// Local-only flag to indicate whether B is exclusively owned.
	bExclusive bool
}

// ReadMethod is the method name for client to tractserver read.
const ReadMethod = "TSSrvHandler.Read"

// CtlReadMethod is the method name for tractserver to tractserver read.
const CtlReadMethod = "TSCtlHandler.CtlRead"

// ReadReq is the request for reading data from an existing file.
type ReadReq struct {
	ID      TractID
	Version int
	Len     int
	Off     int64
	Pri     Priority

	// ID for cancellation.
	ReqID string

	// Other replicas that can serve alternate reads.
	OtherHosts []string
}

// ReadReply is the reply for ReadReq.
type ReadReply struct {
	Err Error
	B   []byte

	// Local-only flag to indicate whether B is exclusively owned.
	bExclusive bool
}

// StatTractMethod is the method name for client to tractserver stat tract.
const StatTractMethod = "TSSrvHandler.StatTract"

// CtlStatTractMethod is the method name for curator to tractserver stat tract.
const CtlStatTractMethod = "TSCtlHandler.CtlStatTract"

// StatTractReq is a request to stat an existing tract.
type StatTractReq struct {
	ID      TractID
	Version int
	Pri     Priority
}

// StatTractReply is a reply to a StatTractReq.
type StatTractReply struct {
	Err  Error
	Size int64

	// ModStamp is a value that can be used to detect writes to a tract. If two
	// stat calls return the same modstamp, the tract was definitely not written
	// to in between those calls. If two calls return different mod stamps, the
	// tract may have been written to.
	ModStamp uint64
}

// GetDiskInfoMethod is the method name for GetDiskInfo. Request is GetDiskInfoReq, reply
// is GetDiskInfoReply.
const GetDiskInfoMethod = "TSSrvHandler.GetDiskInfo"

// GetDiskInfoReq is a request for disk info.
type GetDiskInfoReq struct {
}

// GetDiskInfoReply is the reply for GetDiskInfo.
type GetDiskInfoReply struct {
	// Disks has one value for each disk in the tractserver.
	Disks []FsStatus
	Err   Error
}

// SetControlFlagsMethod is the method name for SetControlFlags. Request is
// SetControlFlags, reply is core.Error.
const SetControlFlagsMethod = "TSSrvHandler.SetControlFlags"

// SetControlFlagsReq requests to change control flags on one disk on the tractserver.
type SetControlFlagsReq struct {
	Root  string
	Flags DiskControlFlags
}

// CancelReqMethod is the method name for canceling a client request.
//
// The request type is the string passed in a previous request's ReqID field.
// The reply type is a core.Error.
const CancelReqMethod = "TSSrvHandler.Cancel"

// The following implement the rpc.BulkData interface:
func (c *CreateTractReq) Get() ([]byte, bool)  { b := c.B; c.B = nil; return b, c.bExclusive }
func (c *CreateTractReq) Set(b []byte, e bool) { c.B, c.bExclusive = b, e }
func (w *WriteReq) Get() ([]byte, bool)        { b := w.B; w.B = nil; return b, w.bExclusive }
func (w *WriteReq) Set(b []byte, e bool)       { w.B, w.bExclusive = b, e }
func (r *ReadReply) Get() ([]byte, bool)       { b := r.B; r.B = nil; return b, r.bExclusive }
func (r *ReadReply) Set(b []byte, e bool)      { r.B, r.bExclusive = b, e }

var (
	// Assert that these implement rpc.BulkData.
	_ rpc.BulkData = (*CreateTractReq)(nil)
	_ rpc.BulkData = (*WriteReq)(nil)
	_ rpc.BulkData = (*ReadReply)(nil)
)
