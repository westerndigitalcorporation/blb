// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package core

import "time"

// This file describes the RPC interface exported by the curator.

// Control methods (sent by master, curator, tractserver):

// CuratorTractserverHeartbeatMethod is the method name for tractserver to curator heartbeat.
const CuratorTractserverHeartbeatMethod = "CuratorCtlHandler.CuratorTractserverHeartbeat"

// CuratorTractserverHeartbeatReq is sent from a Tractserver to a Curator.
type CuratorTractserverHeartbeatReq struct {
	// The master assigned ID for the tractserver.
	TSID TractserverID

	// Where can we reach the tractserver?
	Addr string

	// What tracts are corrupt on this tractserver?  These tracts may not be owned by the
	// curator receiving this message, in which case the curator can ignore the entries it
	// doesn't own.
	Corrupt []TractID

	// A subset of tracts owned by this tractserver that we think the curator
	// manages.
	Has []TractID

	// Load state of this tractserver.
	Load TractserverLoad
}

// TractserverLoad describes load state of a tractserver.
type TractserverLoad struct {
	// How many tracts is this TS currently storing?
	NumTracts int

	// In bytes.
	AvailSpace uint64
	TotalSpace uint64
}

// CuratorTractserverHeartbeatReply is the reply to a CuratorTractserverHeartbeatReq.
type CuratorTractserverHeartbeatReply struct {
	// What partitions does the curator have?
	Partitions []PartitionID

	// NoError if everything went OK, otherwise an error representing what
	// went wrong.
	Err Error
}

// Service methods (sent by clients):

// CreateBlobMethod is sent from the client to the curator to create a blob.
const CreateBlobMethod = "CuratorSrvHandler.CreateBlob"

// CreateBlobReq is sent from the client to a curator to create a blob.
type CreateBlobReq struct {
	// The required replication factor for the blob we're creating.
	Repl int

	// The initial storage hint for the blob.
	Hint StorageHint

	// Expiry time.
	Expires time.Time
}

// CreateBlobReply is a reply to a CreateBlobReq sent from the curator to the client.
type CreateBlobReply struct {
	// Was there an error?  If so, it's here.
	Err Error

	// The ID of the blob.
	ID BlobID
}

// ExtendBlobMethod is the method name for client to curator to extend blob.
const ExtendBlobMethod = "CuratorSrvHandler.ExtendBlob"

// ExtendBlobReq is the request message for extending a blob.
type ExtendBlobReq struct {
	// The ID of the blob the client wants to extend.
	Blob BlobID

	// If the number of tracts in the blob is less than this, more tracts will be added.
	NumTracts int
}

// ExtendBlobReply is the reply message to an ExtendBlobReq.
type ExtendBlobReply struct {
	Err Error

	// Information for newly allocated tracts.
	NewTracts []TractInfo
}

// AckExtendBlobMethod is the method name for client to curator to acknowledge
// extending blob.
const AckExtendBlobMethod = "CuratorSrvHandler.AckExtendBlob"

// AckExtendBlobReq is the request message for extending a blob. Reply is Error.
type AckExtendBlobReq struct {
	// The ID of the blob the client wants to extend.
	Blob BlobID

	// Successfully created tracts.
	Tracts []TractInfo
}

// AckExtendBlobReply is the reply message for AckExtendBlobReq.
type AckExtendBlobReply struct {
	// Number of tracts after extending the blob.
	NumTracts int

	Err Error
}

// DeleteBlobMethod is the method name for client to curator delete blob. Request is BlobID, reply is Error.
const DeleteBlobMethod = "CuratorSrvHandler.DeleteBlob"

// UndeleteBlobMethod is the method name for client to curator request to undelete a blob.
// Request is BlobID, reply is Error.
const UndeleteBlobMethod = "CuratorSrvHandler.UndeleteBlob"

// SetMetadataMethod is the method name for client to curator request to change
// blob metadata. Request is SetMetadataReq, reply is Error.
const SetMetadataMethod = "CuratorSrvHandler.SetMetadata"

// SetMetadataReq asks the curator to change some blob metadata.
type SetMetadataReq struct {
	Blob BlobID

	// Only changing Hint, MTime, ATime, Expires is supported. Other fields are ignored.
	Metadata BlobInfo
}

// GetTractsMethod is the method name for client to curator get tracts.
const GetTractsMethod = "CuratorSrvHandler.GetTracts"

// GetTractsReq is sent by a client who wants to read or write existing tracts in a blob.
type GetTractsReq struct {
	// What blob does the client want information for?
	Blob BlobID

	// Get tracts [Start, End) for the blob.
	Start, End int

	// Is the client intending to open this blob for reading or writing (or both)?
	ForRead, ForWrite bool
}

// GetTractsReply is the reply to a GetTractsReq.
type GetTractsReply struct {
	// The tracts that the user asked for.
	// If there are no tracts in the range the user asked for, this is nil.
	// If there are no tracts in the blob, this is nil.
	//
	// Otherwise, Tracts[0] is the GetTractsReq.Start-th tract in the blob,
	// and Tracts contains as many entries as possible, up to (End-Start).
	Tracts []TractInfo

	// Was there any error encountered?  If so, the rest of the fields should be ignored.
	Err Error
}

// StatBlobMethod is the method name for client to curator stat blob. Request is BlobID.
const StatBlobMethod = "CuratorSrvHandler.StatBlob"

// StatBlobReply is sent in response to a request to Stat a blob.
type StatBlobReply struct {
	// Was there any error encountered in processing this request?
	// If so, the rest of the fields in this message should be ignored.
	Err Error

	Info BlobInfo
}

// ReportBadTSMethod lets clients report tractserver errors to the curator.
const ReportBadTSMethod = "CuratorSrvHandler.ReportBadTS"

// ReportBadTSReq lets clients report tractserver errors to the curator.
type ReportBadTSReq struct {
	// What tract or RS chunk is problematic?
	ID TractID

	// What host is the client blocked on?
	Bad string

	// What operation were they trying to do? ("read" or "write")
	Operation string

	// What error did they get?
	GotError Error

	// If operation is a read, was the client able to complete the read from
	// another tractserver, or reconstruct erasure-coded data? (If so, the
	// curator may choose to treat this as lower priority.)
	CouldRecover bool
}

// FixVersionMethod is the method called by a client when an IO on the tractserver fails due to incorrect
// version information.
const FixVersionMethod = "CuratorSrvHandler.FixVersion"

// FixVersionReq is a client request to verify that the version(s) of the
// provided tract are correct.
type FixVersionReq struct {
	// Info is the tract ID, version pair the client was having trouble with.
	Info TractInfo

	// Bad is the address of the tractserver who rejected our version.
	Bad string
}

// ListBlobsMethod is the method name for clients to ask the curator for existing blobs.
const ListBlobsMethod = "CuratorSrvHandler.ListBlobs"

// ListBlobsReq is the client request for a list of blob keys.
type ListBlobsReq struct {
	Partition PartitionID
	Start     BlobKey
}

// ListBlobsReply is the result of a ListBlobs call.
type ListBlobsReply struct {
	Keys []BlobKey
	Err  Error
}
