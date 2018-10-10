// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package core

import "time"

// This file describes the RPC interface exported by the master.

// Control methods (sent by master, curator, tractserver):

// RegisterCuratorMethod is the method name for curator to master registration.
const RegisterCuratorMethod = "MasterCtlHandler.RegisterCurator"

// RegisterCuratorReq is sent by a new curator to the master to register it as available
// to serve metadata.
type RegisterCuratorReq struct {
	// Where can we reach the curator?
	Addr string
}

// RegisterCuratorReply is sent by the master to a curator in response to a
// RegisterCuratorReq.
type RegisterCuratorReply struct {
	// An ID identifying this curator that must be persisted and used in all further
	// communications.
	CuratorID CuratorID

	// lib.NoError if everything went OK, otherwise an error representing
	// what went wrong.
	Err Error
}

// RegisterTractserverMethod is the method name for tractserver to master registration.
const RegisterTractserverMethod = "MasterCtlHandler.RegisterTractserver"

// RegisterTractserverReq is sent by a new tractserver to the master to register it as available
// to serve data.
type RegisterTractserverReq struct {
	// Where we can reach this tractserver.
	Addr string
}

// RegisterTractserverReply is sent by the master to a tractserver in response to a
// RegisterTractserverReq.
type RegisterTractserverReply struct {
	// An ID identifying this tractserver that must be persisted and used in all further
	// communications.
	TSID TractserverID

	// lib.NoError if everything went OK, otherwise an error representing
	// what went wrong.
	Err Error
}

// CuratorHeartbeatMethod is the method name for curator to master heartbeat.
const CuratorHeartbeatMethod = "MasterCtlHandler.CuratorHeartbeat"

// CuratorHeartbeatReq is sent from the primary curator to the master.
type CuratorHeartbeatReq struct {
	// What is the ID of this group?
	CuratorID CuratorID

	// Where can we reach the curator?
	Addr string
}

// CuratorHeartbeatReply is the reply to a curator heartbeat. This message
// piggybacks partition assignment info so far.
type CuratorHeartbeatReply struct {
	// What partitions does the curator own?
	Partitions []PartitionID

	// lib.NoError is everything went OK, otherwise an error representing
	// what went wrong.
	Err Error
}

// MasterTractserverHeartbeatMethod is the method name for tractserver to master heartbeat.
const MasterTractserverHeartbeatMethod = "MasterCtlHandler.MasterTractserverHeartbeat"

// MasterTractserverHeartbeatReq is sent from a Tractserver to Master.
type MasterTractserverHeartbeatReq struct {
	// What is the ID of this tractserver?
	TractserverID TractserverID

	// Where can we reach the tractserver?
	Addr string

	// Per-disk status
	Disks []FsStatus
}

// FsStatus is heavyweight status information for a disk.  It includes
// information about the underlying file system and per-operation statistics.
type FsStatus struct {
	Status DiskStatus        // Basic health information observed during operation.
	Ops    map[string]string // If not nil, per-op information.

	// On-disk information.
	NumTracts        int    // Number of tracts.
	NumDeletedTracts int    // How many tracts have been deleted but not GC-ed?
	NumUnknownFiles  int    // How many files exist but don't look like tracts or former tracts?
	AvailSpace       uint64 // Available space in bytes on the filesystem.
	TotalSpace       uint64 // Total space in bytes on the filesystem.
}

// DiskStatus is lightweight information about how a disk is doing.
type DiskStatus struct {
	// Root path
	Root string

	// Is the disk full?
	Full bool

	// Has the disk seen any filesystem corruption?
	Healthy bool

	// What's the current queue length?
	QueueLen int

	// What's the average wait time of an op?
	AvgWaitMs int

	// Manual control flags.
	Flags DiskControlFlags
}

// DiskControlFlags are flags that an administrator can manually set on a disk
// to control tractserver behavior.
type DiskControlFlags struct {
	// Stop allocating new tracts to this disk, but don't change anything else.
	StopAllocating bool

	// Slowly report this many tracts per second as "corrupt" (non-zero value
	// implies StopAllocating).
	Drain int

	// Slowly try to move this many tracts per second to other disks (non-zero
	// value implies StopAllocating).
	DrainLocal int
}

// MasterTractserverHeartbeatReply is the reply to a
// MasterTractserverHeartbeatReq.
type MasterTractserverHeartbeatReply struct {
	// Each entry in Curators is the current primary of a Curator replica set.
	Curators []string

	// NoError if everything went OK, otherwise an error representing what
	// went wrong.
	Err Error
}

// NewPartitionMethod is the method name for curator to master new partition.
const NewPartitionMethod = "MasterCtlHandler.NewPartition"

// NewPartitionReq is sent by a curator to request ownership of an additional PartitionID.
type NewPartitionReq struct {
	// What is the ID of the requesting curator?
	CuratorID CuratorID
}

// NewPartitionReply allocates a new partition ID to the curator.
type NewPartitionReply struct {
	// The new partition that the curator now owns.
	PartitionID PartitionID

	// NoError if everything went OK, otherwise an error representing
	// what went wrong.
	Err Error
}

// Service methods (sent by clients):

// LookupCuratorMethod is the method name for client to master blob lookup.
const LookupCuratorMethod = "MasterSrvHandler.LookupCurator"

// LookupCuratorReq is sent by a client who wants to access a blob.
type LookupCuratorReq struct {
	// Find the curator that owns this blobid.
	Blob BlobID
}

// LookupCuratorReply is sent in response to a LookupCuratorReq.
type LookupCuratorReply struct {
	// Replicas[0] is the primary curator, and the rest are secondaries.
	Replicas []string

	// NoError if everything went OK, otherwise an error representing
	// what went wrong.
	Err Error
}

// LookupPartitionMethod is the method name for client to master blob lookup.
const LookupPartitionMethod = "MasterSrvHandler.LookupPartition"

// LookupPartitionReq is sent by a client who wants to access a blob.
type LookupPartitionReq struct {
	// Find the curator that owns this partition.
	Partition PartitionID
}

// LookupPartitionReply is sent in response to a LookupPartitionReq.
type LookupPartitionReply struct {
	// Replicas[0] is the primary curator, and the rest are secondaries.
	Replicas []string

	// NoError if everything went OK, otherwise an error representing
	// what went wrong.
	Err Error
}

// MasterCreateBlobMethod is the method name for client to master new blob. Reply is LookupCuratorReply.
const MasterCreateBlobMethod = "MasterSrvHandler.MasterCreateBlob"

// MasterCreateBlobReq is sent from the client to the master to create a blob.
type MasterCreateBlobReq struct {
	// TODO: Locality constraints could go here.
}

// ListPartitionsMethod is the method name for client to master list partitions.
const ListPartitionsMethod = "MasterSrvHandler.ListPartitions"

// ListPartitionsReq is the request for the master to list partitions.
type ListPartitionsReq struct {
}

// ListPartitionsReply is the response to a ListPartitions request. It contains
// all known partition ids.
type ListPartitionsReply struct {
	Partitions []PartitionID
	Err        Error
}

// GetTractserverInfoMethod is the method name to get an overall tractserver summary.
const GetTractserverInfoMethod = "MasterSrvHandler.GetTractserverInfo"

// GetTractserverInfoReq requests a tractserver summary from the master.
type GetTractserverInfoReq struct {
}

// TractserverInfo is info about one tractserver.
type TractserverInfo struct {
	ID            TractserverID
	Addr          string
	Disks         []FsStatus
	LastHeartbeat time.Time
}

// GetTractserverInfoReply is a summary of the disks in the cluster.
type GetTractserverInfoReply struct {
	Info []TractserverInfo
	Err  Error
}
