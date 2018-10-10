// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package raft

import (
	"io"
)

// FSM is the interface that should be implemented by applications. It contains
// a list of callbacks which will be called by Raft to notify that certain events
// happen.
// NOTE all callbacks of FSM will be called from the same goroutine so that you don't
// need to worry about race conditions in different callbacks.
type FSM interface {
	// Apply will be called once a command is committed in a Raft group. This callback
	// is called from a single goroutine to ensure that the state updates are applied
	// in the same order as they arrived. Application must apply the command in the
	// callback and can optionally return the result to it so that the one who proposed
	// the command can get the result from Pending.Res.
	//
	// Note that the behavior of the command should be deterministic, otherwise the
	// applications might end up in an inconsistent state.
	Apply(Entry) (result interface{})

	// OnLeadershipChange will be called to notify applications the change of
	// leader status of Raft. 'isLeader' tells application whether it is in
	// leader state or in non-leader state. 'term' tells application the
	// current term of the Raft node. 'leader' tells application the Raft
	// address of the leader node, it will be an empty string if the leader
	// is unknown yet, but once the the leader is known Raft will call this
	// callback with the address of the leader.
	//
	// The term number can be used in an application specific way. For example,
	// it's possible that multiple Raft leaders exist at the same time, but only
	// one leader can be effective, it is because internally Raft uses term
	// number to detect stale leaders, applications who also want one elected
	// primary to intereact with outsiders can also attach term number in their
	// messages so outsiders can use it to detect stale primaries. Another use
	// case is if applications want to implement the logic like read and then
	// modify based on the state which were read, they can also attach the term
	// number in modify command so when modify command is applied applications
	// can compare the term number attached in modify command to the term number
	// of the command itself so if they don't match there might be a potential
	// conflict modification from another leader. By guaranteeing that the term
	// when applications read the state and the term of the command which will
	// modify state machine are the same, there can't be a conflict change from
	// another leader in between.
	OnLeadershipChange(isLeader bool, term uint64, leader string)

	// OnMembershipChange will be called to notify applications of membership
	// changes over time.
	OnMembershipChange(membership Membership)

	// Snapshot is called once a snapshot is needed to be taken to compact log.
	// Applications should return an implementation of "Snapshoter" whose method
	// "Save" will be called to dump the actual state in a different goroutine.
	// The state that will be dumped in "Snapshoter.Save" MUST be the state at
	// the point the callback "Snapshot" is called.
	//
	// Once the "Snapshot" is returned further mutations might be applied in Apply
	// callback. However, these mutations should not affect the state that is
	// being dumped in "Snapshoter.Save".
	//
	// If any erorr is returned no snapshot will be taken.
	Snapshot() (Snapshoter, error)

	// SnapshotRestore is called once applications need to restore its state from
	// snapshot. Once it's called applications need to discard its current state
	// and restore its state from the given reader. Also the index and term number
	// of last applied command to the snapshot is passed to the applications.
	SnapshotRestore(reader io.Reader, lastIndex, lastTerm uint64)
}

// Snapshoter encapsulates a point-in-time state of applications and should be
// implemented by applications so that Raft can dump the state to a an underlying
// snapshot file.
type Snapshoter interface {
	// Save is the callback that will be called by Raft. When this callback is
	// called applications should write the serialized state to the given
	// "writer". This callback will be called concurrently with "FSM.Apply" so
	// it should be implemented in a way that concurrent mutations made in
	// "FSM.Apply" will have no effects to the state that is being dumped.
	//
	// If any error is returned the snapshot file will be discarded.
	Save(writer io.Writer) error

	// Release will be called by Raft once Raft is done(either succeed or fail)
	// with the snapshot. Applications should release any resources that are
	// assoicated the snapshot when this callback is called.
	Release()
}
