// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package core

import (
	"io"

	log "github.com/golang/glog"

	"github.com/westerndigitalcorporation/blb/pkg/raft/raft"
)

// Error is our own defined error type for sending errors over an RPC layer.
type Error int

const (
	// NoError means no error.
	NoError = Error(iota)

	//------ Replication/tractserver level errors ------//

	// ErrHaveNewerVersion is returned when the Curator asks us to increment our version
	// number but our version is higher.
	ErrHaveNewerVersion

	// ErrBadVersion is returned if the provided version number is bad.
	ErrBadVersion

	// ErrVersionMismatch is returned if there is a tract version mismatch.
	ErrVersionMismatch

	// ErrNoSuchTract is returned when an operation requires a tract to exist but it does not.
	ErrNoSuchTract

	// ErrDiskRemoved is returned for all disk calls after Stop has been called on the disk.
	ErrDiskRemoved

	// ErrStampChanged is returned for a conditional SetVersion if the stamp changed.
	ErrStampChanged

	// ErrShortRead is returned if we get less data than we wanted, in a context
	// where that is unexpected/disallowed.
	ErrShortRead

	// ErrNoVersion is returned when there isn't a version set on a tract.
	ErrNoVersion

	// ErrGeneralDisk is returned from the BadDisk mock disk.
	ErrGeneralDisk

	// ErrWrongTractserver is returned if the curator thinks it's sending a request to one tractserver but another one received it.
	ErrWrongTractserver

	//------ Errors from the disk level ------//

	// ErrAlreadyExists is returned when users want to create a duplicate file.
	ErrAlreadyExists

	// ErrFileNotFound is returned when users want to access a unexisting file.
	ErrFileNotFound

	// ErrCorruptData is returned if a block is corrupted when read, or if a write unavoidably
	// causes a corruption.
	ErrCorruptData

	// ErrNoSpace is returned when a disk fills up while writing a block.
	ErrNoSpace

	// ErrEOF is returned when reach the end of a file.
	ErrEOF

	// ErrInitManager is returned if a disk.Manager cannot be initialized.
	ErrInitManager

	// ErrRemoveFile is returned if os.Remove fails.
	ErrRemoveFile

	// ErrIO is returned if there is an OS-level IO error.
	// The file is probably corrupt in this case.
	ErrIO

	// ErrNoAttribute is returned if the specified attribute was not found.
	ErrNoAttribute

	//-------- Errors from the curator level ------//

	// ErrInvalidArgument is returned if an argument is bad or confusing (eg negative size)
	ErrInvalidArgument

	// ErrBlobFull is returned if the blob has hit its tract limit.
	ErrBlobFull

	// ErrGenBlobID is returned if we fail to generate a BlobID.
	ErrGenBlobID

	// ErrHostExist is returned if we already have a host in our state but an RPC implies otherwise.
	ErrHostExist

	// ErrHostNotExist is returned if we don't have a host in our state but an RPC implies otherwise.
	ErrHostNotExist

	// ErrAllocHost is returned if we fail to allocate a host.
	ErrAllocHost

	// ErrExtendConflict is returned if the client tries to acknowledge the
	// extension of a blob with existing tracts. Probably it means that
	// another client is concurrently extending the blob and succeeds before
	// this one finishes. The failed client can retry upon this error.
	ErrExtendConflict

	//------ Errors from the master level ------//

	// ErrBadCuratorID means an invalid curator ID was provided.
	ErrBadCuratorID

	// ErrBadHosts means that invalid or insufficient hostnames were provided.
	ErrBadHosts

	// ErrCantFindCuratorAddr means can't locate curator address of a given curator group.
	ErrCantFindCuratorAddr

	// ErrExceedNewPartitionQuota means the daily quota for new partitions has reached.
	ErrExceedNewPartitionQuota

	//------ Client driver errors ------//

	// ErrNetworkConn is returned if we fail to connection to a host.
	ErrNetworkConn

	// ErrInvalidState is returned if we find data in our state that doesn't
	// make sense or is inconsistent.
	ErrInvalidState

	//------ Errors from any level ------//

	// ErrTooBusy means the server is too busy to do whatever it was asked to do.
	ErrTooBusy

	// ErrTooBig is returned if the client is asking for too much data allocation.
	ErrTooBig

	// ErrNoSuchBlob is returned if the blob doesn't exist.
	ErrNoSuchBlob

	// ErrRPC is returned when the RPC layer errors during sending/receiving.
	ErrRPC

	// ErrStaleLeader is returned when the received message is sent from a stale leader of a repl group
	ErrStaleLeader

	//------ Error from the raft level ------//

	// ErrRaftTimeout means a proposal timed out, most likely because the
	// quorum is not available due to node/network failures. The user should
	// retry but with caution -- there is chance that the operation succeeds
	// after the timeout signal is received. If the operation is not
	// idempotent, the user needs to verify before retrying.
	ErrRaftTimeout

	// ErrRaftNodeNotLeader means a node is not a leader.
	ErrRaftNodeNotLeader

	// ErrRaftNotLeaderAnymore means a leader has stepped down.
	ErrRaftNotLeaderAnymore

	// ErrRaftTooManyPendingReqs means too many pending requests.
	ErrRaftTooManyPendingReqs

	// ErrLeaderContinuityBroken is returned if a Raft leader has not been leader for a continuous duration.
	ErrLeaderContinuityBroken

	//------ Meta-error ------//

	// ErrUnknown is an error that we're not really sure about.
	ErrUnknown

	// ErrNotYetImplemented is returned if the method or feature isn't implemented yet.
	ErrNotYetImplemented

	// ErrCanceled is returned when a request is canceled.
	ErrCanceled

	// ErrCancelFailed is returned when there was no request to cancel.
	ErrCancelFailed

	// ErrReadOnlyStorageClass means that the client is trying to write to a
	// storage class that doesn't support writing (i.e. erasure-coded). The
	// client can request that the blob be transitioned to a replicated storage
	// class and try again.
	ErrReadOnlyStorageClass

	// ErrConflictingState means an update to raft state failed because it
	// conflicted with other changes.
	ErrConflictingState

	// ErrReadOnlyMode is returned when a raft-based service is temporarily in
	// read-only mode (likely during a rolling upgrade).
	ErrReadOnlyMode

	// ErrDrainDisk is a fake error that should be treated the same as
	// ErrCorruptData or ErrIO.
	ErrDrainDisk
)

var description = map[Error]string{
	NoError: "no error",

	// Replication/tractserver level errors.
	ErrHaveNewerVersion: "setversion called with lower version than we have",
	ErrBadVersion:       "setversion called with invalid version number",
	ErrVersionMismatch:  "tract version mismatch",
	ErrNoSuchTract:      "tract does not exist",
	ErrDiskRemoved:      "operation on disk after it has been removed",
	ErrStampChanged:     "modification stamp changed",
	ErrShortRead:        "short read in unexpected context",
	ErrNoVersion:        "server does not have a version number for this tract",
	ErrGeneralDisk:      "general disk error (testing)",
	ErrWrongTractserver: "wrong tractserver",

	// Errors from the disk level.
	ErrAlreadyExists: "file already exists",
	ErrFileNotFound:  "file was not found",
	ErrCorruptData:   "block checksum is invalid, data is corrupt",
	ErrNoSpace:       "ran out of space, possibly wrote partial block",
	ErrEOF:           "end of file",
	ErrInitManager:   "error initializing a disk.Manager",
	ErrRemoveFile:    "error removing file",
	ErrIO:            "I/O level error",
	ErrNoAttribute:   "attribute not found",

	// Errors from the curator level.
	ErrInvalidArgument: "invalid argument",
	ErrBlobFull:        "blob has hit tract limit",
	ErrGenBlobID:       "failed to generate blob ID",
	ErrHostExist:       "host already exists in view",
	ErrHostNotExist:    "host does not exist in view",
	ErrAllocHost:       "failed to allocate host",
	ErrExtendConflict:  "failed to extend blob possibly due to conflict",

	// Errors from the master level.
	ErrBadCuratorID:            "curator ID is invalid",
	ErrBadHosts:                "hostnames invalid",
	ErrCantFindCuratorAddr:     "can't find curator address of the given curator group",
	ErrExceedNewPartitionQuota: "the quota for new partitions has reached. wait for it to get refilled",

	// Client driver errors.
	ErrNetworkConn:  "network connection error",
	ErrInvalidState: "invalid state",

	// Errors from any level, really.
	ErrTooBusy:     "too busy",
	ErrTooBig:      "request is too large",
	ErrNoSuchBlob:  "blob does not exist, cannot succeed without it",
	ErrRPC:         "RPC-level error",
	ErrStaleLeader: "a newer leader in this group has been seen, the request has been ignored",

	// Error from the raft level.
	ErrRaftTimeout:            "raft: proposal timed out",
	ErrRaftNodeNotLeader:      "raft: node is not leader",
	ErrRaftNotLeaderAnymore:   "raft: not leader anymore",
	ErrRaftTooManyPendingReqs: "raft: too many pending requests",
	ErrLeaderContinuityBroken: "raft: node was not raft leader for entire operation",

	// Meta-error.
	ErrUnknown:           "unknown error!!!! contact a programming professional to diagnose",
	ErrNotYetImplemented: "not yet implemented",
	ErrCanceled:          "request canceled",
	ErrCancelFailed:      "couldn't cancel request",

	ErrReadOnlyStorageClass: "read-only storage class",
	ErrConflictingState:     "conflicting state in transaction",
	ErrReadOnlyMode:         "raft read-only mode",
	ErrDrainDisk:            "fake error to drain disk",
}

// String returns a human readable error message.
func (e Error) String() string {
	if s, ok := description[e]; ok {
		return s
	}
	return "NO DESCRIPTION FOR ERROR FIX THIS"
}

// Error returns a golang error object with an error message corresponding to
// this core.Error.
func (e Error) Error() error {
	if e == NoError {
		return nil
	} else if e == ErrEOF {
		// io.EOF is treated specially by the Go standard library and is
		// required to make a Blob properly satisfy the Reader interface.
		return io.EOF
	}
	return goError(e)
}

// Is checks whether the generic Go error 'g' is actually the receiver Blb error
// underneath.
func (e Error) Is(g error) bool {
	b, ok := g.(goError)
	return ok && (Error)(b) == e
}

// goError is a wrapper type to make our Error act like Go's 'error'
type goError Error

// Error implements the 'error' interface.
func (g goError) Error() string {
	return (Error)(g).String()
}

// BlbError gets the underlying core.Error from an error.
func BlbError(err error) (Error, bool) {
	e, ok := err.(goError)
	return Error(e), ok
}

// IsRetriableBlbError checks if this is 1) core.Error 2) retriable
func IsRetriableBlbError(err error) bool {
	if goerr, ok := err.(goError); ok {
		return IsRetriableError(Error(goerr))
	}
	return false
}

// IsRetriableError checks if we should retry on a given returned error.
// We consider errors that might be transient to be retriable errors.
func IsRetriableError(err Error) bool {
	switch err {
	case ErrRPC, // Failed to connect to a host, retry connecting it.
		// Raft related errors, might be transient so we should retry.
		ErrRaftTimeout,
		ErrRaftNodeNotLeader,
		ErrRaftNotLeaderAnymore,
		ErrRaftTooManyPendingReqs,
		ErrLeaderContinuityBroken,
		// This is only during upgrades. Worth it to try again.
		ErrReadOnlyMode,
		// Retry until curator registers with master.
		ErrCantFindCuratorAddr,
		// Reconnect?
		ErrNetworkConn,
		// Wait tract servers register with curator.
		ErrAllocHost,
		// Make sense to backoff a little bit and retry.
		ErrTooBusy,
		// Can't match the version of tract server, retry with curator to get updated version?
		ErrVersionMismatch,
		// Extend blob failed due to conflicts.
		ErrExtendConflict:
		return true
	}
	return false
}

// FromRaftError translets Raft errors defined in raft pacakge to
// `core.Error` types to be sent over the wire.
func FromRaftError(err error) Error {
	switch err {
	case nil:
		return NoError
	case raft.ErrNodeNotLeader:
		return ErrRaftNodeNotLeader
	case raft.ErrNotLeaderAnymore:
		return ErrRaftNotLeaderAnymore
	case raft.ErrTooManyPendingReqs:
		return ErrRaftTooManyPendingReqs
	case raft.ErrTermMismatch:
		return ErrLeaderContinuityBroken
	case raft.ErrNodeExists, raft.ErrNodeNotExists, raft.ErrAlreadyConfigured:
		// These errors should never appear in a context where we have to convert a raft
		// error to a blb error, but if they do, just return ErrUnknown, don't crash.
		log.Errorf("Unexpected Raft error: %v", err)
		return ErrUnknown
	default:
		log.Fatalf("Unexpected Raft error is returned: %v", err)
		return ErrUnknown
	}
}
