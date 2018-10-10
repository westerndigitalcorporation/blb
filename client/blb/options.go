// Copyright (c) 2017 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package blb

import (
	"context"
	"time"

	"github.com/westerndigitalcorporation/blb/internal/core"
)

// This is the client API to storage options:

// Create options:

// StorageDefault causes the blob to be created with the default storage hint.
func StorageDefault(o *createOptions) { o.hint = core.StorageHint_DEFAULT }

// StorageHot causes the blob to be created with an intention for hot storage.
func StorageHot(o *createOptions) { o.hint = core.StorageHint_HOT }

// StorageWarm causes the blob to be created with an intention for warm storage.
func StorageWarm(o *createOptions) { o.hint = core.StorageHint_WARM }

// StorageCold causes the blob to be created with an intention for cold storage.
func StorageCold(o *createOptions) { o.hint = core.StorageHint_COLD }

// ReplFactor causes the blob to be created with replication factor n.
func ReplFactor(n int) createOpt { return func(o *createOptions) { o.repl = n } }

// WithExpires causes the blob to be created with an expiration time.
func WithExpires(e time.Time) createOpt { return func(o *createOptions) { o.expires = e } }

// CreatePriHigh gives high priority to all disk operations related to this blob.
func CreatePriHigh(o *createOptions) { o.pri = core.Priority_HIGH }

// CreatePriMedium gives medium priority to all disk operations related to this blob.
func CreatePriMedium(o *createOptions) { o.pri = core.Priority_MEDIUM }

// CreatePriLow gives low priority to all disk operations related to this blob.
func CreatePriLow(o *createOptions) { o.pri = core.Priority_LOW }

// CreateContext associates a context with this Create call.
func CreateContext(ctx context.Context) createOpt { return func(o *createOptions) { o.ctx = ctx } }

// Open options:

// OpenPriHigh gives high priority to all disk operations related to this blob.
func OpenPriHigh(o *openOptions) { o.pri = core.Priority_HIGH }

// OpenPriMedium gives medium priority to all disk operations related to this blob.
func OpenPriMedium(o *openOptions) { o.pri = core.Priority_MEDIUM }

// OpenPriLow gives low priority to all disk operations related to this blob.
func OpenPriLow(o *openOptions) { o.pri = core.Priority_LOW }

// OpenContext associates a context with this Open call.
func OpenContext(ctx context.Context) openOpt { return func(o *openOptions) { o.ctx = ctx } }

// Implementation details:

// createOptions contains creation parameters for a blob.
type createOptions struct {
	repl    int
	hint    core.StorageHint
	expires time.Time
	pri     core.Priority
	ctx     context.Context
}

var defaultCreateOptions = createOptions{
	repl: 3,
	hint: core.StorageHint_DEFAULT,
	pri:  core.Priority_TSDEFAULT,
}

type createOpt func(*createOptions)

type openOptions struct {
	ctx context.Context
	pri core.Priority
}

var defaultOpenOptions = openOptions{
	pri: core.Priority_TSDEFAULT,
}

type openOpt func(*openOptions)
