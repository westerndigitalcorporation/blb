// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package tractserver

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"time"

	"github.com/westerndigitalcorporation/blb/internal/core"
	"github.com/westerndigitalcorporation/blb/pkg/disk"
)

// The Priority of a request. We map client-supplied priorities to our own scale
// to reduce coupling.
type Priority int

// Pre-defined priority levels.
const (
	LowPri     Priority = 10
	MedPri     Priority = 20
	HighPri    Priority = 30
	ScrubPri            = LowPri  // Scrub at low priority.
	ControlPri          = HighPri // Control requests get high priority.
)

// Maps priority from RPC values to our own scale.
func mapPriority(pri core.Priority) Priority {
	switch pri {
	case core.Priority_LOW:
		return LowPri
	case core.Priority_HIGH:
		return HighPri
	}
	return MedPri
}

//
// request is used internally by the Manager
//

type request struct {
	ctx context.Context

	// After the request finishes a *Reply is sent via this channel to the user.
	done chan reply

	// All higher priority requests will be executed before any lower.
	priority Priority

	// Marks when the operation was enqueued.
	enqueueTime time.Time

	// Specific request type. See below for possible types.
	op interface{}
}

type openRequest struct {
	id    core.TractID
	flags int
}

type closeRequest struct {
	f *disk.ChecksumFile
}

type deleteRequest struct {
	id core.TractID
}

type finishDeleteRequest struct {
	files []string
}

type readRequest struct {
	f   *disk.ChecksumFile
	b   []byte
	off int64
}

type writeRequest struct {
	f   *disk.ChecksumFile
	b   []byte
	off int64
}

type statRequest struct {
	f *disk.ChecksumFile
}

type scrubRequest struct {
	id core.TractID
}

type getxattrRequest struct {
	f    *disk.ChecksumFile
	name string
}

type setxattrRequest struct {
	f     *disk.ChecksumFile
	name  string
	value []byte
}

type opendirRequest struct {
	dir string
}

type readdirRequest struct {
	d *os.File
}

type closedirRequest struct {
	d *os.File
}

type statfsRequest struct {
}

type exitRequest struct {
}

func (r request) String() string {
	var buf bytes.Buffer

	switch specific := r.op.(type) {
	case openRequest:
		buf.WriteString(fmt.Sprintf("open %s, flags %#x", specific.id, specific.flags))
	case scrubRequest:
		buf.WriteString(fmt.Sprintf("scrub id %s", specific.id))
	case statRequest:
		buf.WriteString(fmt.Sprintf("stat %p", specific.f))
	case closeRequest:
		buf.WriteString(fmt.Sprintf("close %p", specific.f))
	case deleteRequest:
		buf.WriteString(fmt.Sprintf("delete %s", specific.id))
	case readRequest:
		buf.WriteString(fmt.Sprintf("read %p len=%d off=%d", specific.f, len(specific.b), specific.off))
	case writeRequest:
		buf.WriteString(fmt.Sprintf("write %p len=%d off=%d", specific.f, len(specific.b), specific.off))
	case getxattrRequest:
		buf.WriteString(fmt.Sprintf("getxattr %p name=%s", specific.f, specific.name))
	case setxattrRequest:
		buf.WriteString(fmt.Sprintf("setxattr %p name=%s value=%v", specific.f, specific.name, specific.value))
	case opendirRequest:
		buf.WriteString(fmt.Sprintf("opendir dir=%s", specific.dir))
	case readdirRequest:
		buf.WriteString("readdir")
	case closedirRequest:
		buf.WriteString("closedir")
	case statfsRequest:
		buf.WriteString("statfs")
	case exitRequest:
		buf.WriteString("exit")
	}
	return buf.String()
}

// Used with the priority queue to order requests.
func (r request) Less(q interface{}) bool {
	e := q.(request)

	if r.priority != e.priority {
		return r.priority > e.priority
	}

	return r.enqueueTime.Before(e.enqueueTime)
}

//
// reply is internally used by the manager.
//

type reply struct {
	err error

	// one of the reply types below.
	op interface{}
}

type openReply struct {
	f *disk.ChecksumFile
}

type readReply struct {
	n int
}

type writeReply struct {
	n int
}

type statReply struct {
	size int64
}

type scrubReply struct {
	size int64
}

type getxattrReply struct {
	value []byte
}

type opendirReply struct {
	d *os.File
}

type readdirReply struct {
	tracts  []core.TractID // Well-formed tracts.
	dead    []string       // Files that have been deleted but could be undeleted by an operator.
	unknown []string       // Files we don't know about.
}

type closedirReply struct {
}

type statfsReply struct {
	status core.FsStatus
}

// Context functions:

type contextKey int

const priorityKey contextKey = iota

func contextWithPriority(parent context.Context, pri Priority) context.Context {
	return context.WithValue(parent, priorityKey, pri)
}

func priorityFromContext(ctx context.Context) Priority {
	if pri, ok := ctx.Value(priorityKey).(Priority); ok {
		return pri
	}
	return MedPri
}

func controlContext() context.Context {
	return contextWithPriority(context.Background(), ControlPri)
}
