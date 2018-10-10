// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package testblb

import (
	"fmt"
	"time"

	client "github.com/westerndigitalcorporation/blb/client/blb"
)

// TestTimes tests mtime/atime/expires functionality.
func (tc *TestCase) TestTimes() error {
	// Create a blob and fill it with one tract of data.
	blob, err := tc.c.Create()
	if err != nil {
		return err
	}
	id := blob.ID()

	var t1, t2, t3 time.Time

	// get original times
	if info, err := blob.Stat(); err != nil {
		return err
	} else if info.MTime.IsZero() || info.ATime.IsZero() || !info.MTime.Equal(info.ATime) || !info.Expires.IsZero() {
		return fmt.Errorf("zero or mismatched times on new blob")
	} else {
		t1 = info.MTime
	}

	// open for write to update mtime
	if _, err := tc.c.Open(id, "w"); err != nil {
		return err
	}

	// let curator batch update times and check stat
	time.Sleep(2 * time.Second)
	if info, err := blob.Stat(); err != nil {
		return err
	} else if !info.MTime.After(t1) {
		return fmt.Errorf("mtime was not updated 1")
	} else if info.ATime.After(t1) {
		return fmt.Errorf("atime was updated 1")
	} else {
		t2 = info.MTime
	}

	// open for read to update atime
	if _, err := tc.c.Open(id, "r"); err != nil {
		return err
	}

	// let curator batch update times and check stat
	time.Sleep(2 * time.Second)
	if info, err := blob.Stat(); err != nil {
		return err
	} else if info.MTime.After(t2) {
		return fmt.Errorf("mtime was updated 2")
	} else if !info.ATime.After(t2) {
		return fmt.Errorf("atime was not updated 2")
	} else {
		t3 = info.ATime
	}

	// write again and make sure time updated
	if _, err := tc.c.Open(id, "w"); err != nil {
		return err
	}
	time.Sleep(2 * time.Second)
	if info, err := blob.Stat(); err != nil {
		return err
	} else if !info.MTime.After(t3) {
		return fmt.Errorf("mtime was not updated 3")
	} else if info.ATime.After(t3) {
		return fmt.Errorf("atime was updated 3")
	}

	// check round-trip expiry time (stuck here instead of making a new case to
	// keep things faster)
	hour := time.Now().Add(time.Hour)
	blob, err = tc.c.Create(client.WithExpires(hour))
	if err != nil {
		return err
	}
	if info, err := blob.Stat(); err != nil {
		return err
	} else if !info.Expires.Equal(hour) {
		return fmt.Errorf("didn't get expiry time back")
	}

	return nil
}
