// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package raft

import (
	"bytes"
	"fmt"
	"io"
)

// Msg defines the interface that all message types should implement.
type Msg interface {
	GetTerm() uint64
	SetTerm(uint64)
	GetTo() string
	SetTo(string)
	GetFrom() string
	SetFrom(string)
	GetToGUID() uint64
	SetToGUID(uint64)
	GetFromGUID() uint64
	SetFromGUID(uint64)
	GetEpoch() uint64
	SetEpoch(uint64)
}

// BaseMsg contains all common fields of different message types.
type BaseMsg struct {
	Term     uint64 // The current term number of sender.
	To       string // The ID of receiver.
	From     string // The ID of sender.
	ToGUID   uint64 // The GUID of receiver.
	FromGUID uint64 // The GUID of sender.
	Epoch    uint64 // The epoch ID.
}

// String converts BaseMsg to a human-readable string.
func (b *BaseMsg) String() string {
	return fmt.Sprintf("Base{Term:%d, To:%q, From:%q}", b.Term, b.To, b.From)
}

// GetTerm returns the current term of sender.
func (b *BaseMsg) GetTerm() uint64 {
	return b.Term
}

// SetTerm sets the current term of sender.
func (b *BaseMsg) SetTerm(term uint64) {
	b.Term = term
}

// GetTo returns the ID of receiver.
func (b *BaseMsg) GetTo() string {
	return b.To
}

// SetTo sets the ID of receiver.
func (b *BaseMsg) SetTo(to string) {
	b.To = to
}

// GetFrom gets the ID of sender.
func (b *BaseMsg) GetFrom() string {
	return b.From
}

// SetFrom sets the ID of sender.
func (b *BaseMsg) SetFrom(from string) {
	b.From = from
}

// GetToGUID returns the GUID of receiver.
func (b *BaseMsg) GetToGUID() uint64 {
	return b.ToGUID
}

// SetToGUID sets the GUID of receiver.
func (b *BaseMsg) SetToGUID(to uint64) {
	b.ToGUID = to
}

// GetFromGUID gets the GUID of sender.
func (b *BaseMsg) GetFromGUID() uint64 {
	return b.FromGUID
}

// SetFromGUID sets the GUID of sender.
func (b *BaseMsg) SetFromGUID(from uint64) {
	b.FromGUID = from
}

// GetEpoch gets the epoch id.
func (b *BaseMsg) GetEpoch() uint64 {
	return b.Epoch
}

// SetEpoch sets the epoch id.
func (b *BaseMsg) SetEpoch(epoch uint64) {
	b.Epoch = epoch
}

// AppEnts will be used by leader to:
//
//	1. Update followers' logs.
//	2. Probe the last agreed point between follower's log and log of itself.
//	3. Send as a heartbeat message to maintain its leadership.
//
type AppEnts struct {
	// 'term' is provided by BaseMsg.Term
	// 'leaderId' is provied by BaseMsg.From
	BaseMsg

	// PrevLogIndex and PrevLogTerm will be used as consistency check.
	// Follower will only accept the AppEnts request if an entry with term
	// 'PrevLogTerm' and index 'PrevLogIndex' exists in its log.
	PrevLogIndex uint64
	PrevLogTerm  uint64

	// The entries that need to be appended to follower's log.
	// It can be nil if it's just a pure heartbeat message or a probing message.
	Entries []Entry

	// The index that can be safely committed by follower. Leader is the only one
	// who decides which index of entry can be committed, and leader will use this
	// field to piggyback the commit index to followers so followers can apply
	// committed commands to their state machines.
	LeaderCommit uint64
}

// String converts AppEnts to a human-readable string.
func (a *AppEnts) String() string {
	var buf bytes.Buffer
	buf.WriteString("AppEnts{")
	buf.WriteString(a.BaseMsg.String())
	buf.WriteString(fmt.Sprintf(", PrevIdx:%d, PrevTerm:%d, Commit:%d, Entries:%d", a.PrevLogIndex, a.PrevLogTerm, a.LeaderCommit, len(a.Entries)))
	buf.WriteString("}")
	return buf.String()
}

// AppEntsResp is the response of its corresponding AppEnts request.
type AppEntsResp struct {
	BaseMsg
	// Whether its corresponding AppEnts request succeeded or not.
	Success bool

	// If its corresponding AppEnts succeeded, 'Index' will be the index of the last
	// matched entry in follower's log identified by that request, so leader can
	// update 'matchIndex' of follower.
	//
	// If its corresponding AppEnts failed, 'Index' will be the index used for
	// consisntecy check(AppEnts.PrevLogIndex) so leader can update follower's
	// 'nextIdx' for next probing.
	//
	// 'Index' field is not specified in paper because the paper phrases the
	// protocol in terms of RPC, response is paired with its request it can always
	// derive this 'Index' field from its corresponding request.
	// However, we decoupled our transport layer from Raft implementation so we
	// need to keep track of this ourselves.
	Index uint64

	// Hint from follower. If 'Hint' is not 0 leader should decrement the
	// 'nextIndex' to 'Hint'. We use 'Hint' to bypass all log entries that
	// a follower is missing, instead of sending one probing message per
	// log entry.
	// Stale response with a stale 'Hint' is fine. In that case a leader might
	// just probe from a stale position. We implement the follower and leader
	// in a way that processing a stale message is still correct.
	Hint uint64
}

// String converts AppEntsResp to a human-readable string.
func (a *AppEntsResp) String() string {
	var buf bytes.Buffer
	buf.WriteString("AppEntsResp{")
	buf.WriteString(a.BaseMsg.String())
	buf.WriteString(fmt.Sprintf(", Success:%v, Idx:%d, Hint:%d", a.Success, a.Index, a.Hint))
	buf.WriteString("}")
	return buf.String()
}

// VoteReq will be sent by candidate to campaign for a new leadership.
type VoteReq struct {
	// 'term' is provided by BaseMsg.Term
	// 'candidateId' is provied by BaseMsg.From
	BaseMsg

	// The index and term number of last entry in candidate's log. Candidate can
	// be elected as leader only if it has "sufficient" up-to-dated log history.
	LastLogIndex uint64
	LastLogTerm  uint64
}

// String converts VoteReq to a human-readable string.
func (v *VoteReq) String() string {
	var buf bytes.Buffer
	buf.WriteString("VoteReq{")
	buf.WriteString(v.BaseMsg.String())
	buf.WriteString(fmt.Sprintf(", LastLogIdx:%d, LastLogTerm:%d", v.LastLogIndex, v.LastLogTerm))
	buf.WriteString("}")
	return buf.String()
}

// VoteResp is the response of VoteReq.
type VoteResp struct {
	BaseMsg
	// Whether its corresponding VoteReq is granted or not.
	Granted bool
}

// String converts VoteResp to a human-readable string.
func (v *VoteResp) String() string {
	var buf bytes.Buffer
	buf.WriteString("VoteResp{")
	buf.WriteString(v.BaseMsg.String())
	buf.WriteString(fmt.Sprintf(", Granted:%v", v.Granted))
	buf.WriteString("}")
	return buf.String()
}

// InstallSnapshot will be sent by leader to ship its snapshot to followers.
type InstallSnapshot struct {
	// 'term' is provided by BaseMsg.Term
	// 'leaderId' is provied by BaseMsg.From
	BaseMsg

	// The index of last applied command included in this snapshot.
	LastIndex uint64
	// The term of last applied command included in this snapshot.
	LastTerm uint64

	// The latest applied membership change in snapshot.
	Membership Membership

	// The payload of a snapshot.
	//
	// This message must be created with Body pointing to actual payload.
	// Transport is responsible for sending it out on sender side and pointing
	// it to actual payload on receiver side.
	//
	// Body MUST be closed after the messge is sent out on sender side or
	// processed on receiver side.
	Body io.ReadCloser
}

// String converts InstallSnapshot to a human-readable string.
func (i *InstallSnapshot) String() string {
	var buf bytes.Buffer
	buf.WriteString("InstallSnapshot{")
	buf.WriteString(i.BaseMsg.String())
	buf.WriteString(fmt.Sprintf(", LastIndex:%d, LastTerm:%d", i.LastIndex, i.LastTerm))
	buf.WriteString(fmt.Sprintf(", Membership:%+v", i.Membership))
	buf.WriteString("}")
	return buf.String()
}
