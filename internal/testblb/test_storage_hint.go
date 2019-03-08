// Copyright (c) 2017 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package testblb

import (
	"context"
	"fmt"
	"time"

	client "github.com/westerndigitalcorporation/blb/client/blb"
	"github.com/westerndigitalcorporation/blb/internal/core"
)

// TestStorageHint tests functionality related to storage hints and classes.
func (tc *TestCase) TestStorageHint() error {
	blob, err := tc.c.Create() // defaults
	if err != nil {
		return err
	}
	info, err := blob.Stat()
	if err != nil {
		return err
	}
	if info.Hint != core.StorageHintDEFAULT {
		return fmt.Errorf("wrong storage hint: %s != %s", info.Hint, core.StorageHintDEFAULT)
	}
	if info.Class != core.StorageClassREPLICATED {
		return fmt.Errorf("wrong storage class: %s != %s", info.Class, core.StorageClassREPLICATED)
	}
	if info.Repl != 3 { // default from client library
		return fmt.Errorf("wrong repl factor: %d != %d", info.Repl, 3)
	}

	blob, err = tc.c.Create(client.StorageCold, client.ReplFactor(7))
	if err != nil {
		return err
	}

	info, err = blob.Stat()
	if err != nil {
		return err
	}
	if info.Hint != core.StorageHintCOLD {
		return fmt.Errorf("wrong storage hint: %s != %s", info.Hint, core.StorageHintCOLD)
	}
	// created with REPLICATED class even with a separate hint
	if info.Class != core.StorageClassREPLICATED {
		return fmt.Errorf("wrong storage class: %s != %s", info.Class, core.StorageClassREPLICATED)
	}
	if info.Repl != 7 {
		return fmt.Errorf("wrong repl factor: %d != %d", info.Repl, 7)
	}

	// Try to change things.
	newMTime := time.Now().Add(-77 * time.Hour).Truncate(time.Second)
	newExp := time.Now().Add(3 * 24 * time.Hour).Truncate(time.Second)
	info.MTime = newMTime
	info.Hint = core.StorageHintHOT
	info.Expires = newExp
	err = tc.c.SetMetadata(context.Background(), blob.ID(), info)
	if err != nil {
		return err
	}

	// Stat again.
	info, err = blob.Stat()
	if info.Hint != core.StorageHintHOT {
		return fmt.Errorf("wrong storage hint: %s != %s", info.Hint, core.StorageHintHOT)
	}
	if !info.MTime.Equal(newMTime) {
		return fmt.Errorf("wrong mtime: %s != %s", info.MTime, newMTime)
	}
	if !info.Expires.Equal(newExp) {
		return fmt.Errorf("wrong expires: %s != %s", info.Expires, newExp)
	}

	return nil
}
