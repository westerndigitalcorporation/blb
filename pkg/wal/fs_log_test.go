// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package wal

import (
	"io/ioutil"
	"testing"

	test "github.com/westerndigitalcorporation/blb/pkg/testutil"
)

func TestLogCreation(t *testing.T) {
	dir := tempDir(t)

	l, err := OpenFSLog(dir)
	if err != nil {
		t.Fatalf("Failed to create log in %q: %v", dir, err)
	}

	id, empty := l.FirstID()
	if !empty {
		t.Errorf("New log wasn't empty, gave first id=%d", id)
	}

	id, empty = l.LastID()
	if !empty {
		t.Errorf("New log wasn't empty, gave last id=%d", id)
	}

	l.Close()
}

func TestLogReopen(t *testing.T) {
	dir := tempDir(t)

	l, err := OpenFSLog(dir)
	if err != nil {
		t.Fatalf("Failed to create log in %q: %v", dir, err)
	}

	l.Close()

	l1, err := OpenFSLog(dir)
	if err != nil {
		t.Fatalf("Failed to reopen log: %v", err)
	}

	l1.Close()
}

func tempDir(t *testing.T) string {
	dir, err := ioutil.TempDir(test.TempDir(), "WAL-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	return dir
}
