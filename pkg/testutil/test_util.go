// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT
//
// This contains a few functions to help writing tests. If you want to put
// something in a temporary directory, put them in test.TempDir(), or a directory
// within it. Also, put this in a file named main_test.go in your package, and
// temp directories will be cleaned up automatically on successful runs:
/*

package mypkg

import (
	"testing"

	"infra/lib/testutil"
)

func TestMain(m *testing.M) {
	testutil.TestMain(m)
}

*/

package testutil

import (
	"flag"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"
)

var tempDir, createdBase string

// TempDir gets a temp directory that's exclusive to this process (but not
// necessarily other tests in the same process). You might want to use
// ioutil.TempDir on the result to create a directory exclusive to a particular
// test.
func TempDir() string {
	if tempDir == "" {
		var err error
		tempDir, err = ioutil.TempDir(getBase(), filepath.Base(os.Args[0]))
		if err != nil {
			log.Fatalf("Couldn't create temp dir: %s", err)
		}
	}
	return tempDir
}

// Get a base temp dir. Create one if it doesn't exist.
func getBase() string {
	// Try TMPDIR first.
	if tmp := os.Getenv("TMPDIR"); tmp != "" {
		return tmp
	}
	// Otherwise just make one in the current directory.
	wd, err := os.Getwd()
	if nil != err {
		log.Fatalf("could not get the current dir: %s", err)
	}
	// Note that "*.test" is in .gitignore, so this will be ignored by git
	// anywhere in the repo.
	base := time.Now().Format("20060102.150405.test")
	tmp := filepath.Join(wd, base)
	if err := os.Mkdir(tmp, 0755); nil != err && !os.IsExist(err) {
		log.Fatalf("failed to create tmp dir: %s", tmp)
	}
	createdBase = tmp
	return tmp
}

func cleanup() {
	if tempDir != "" {
		os.RemoveAll(tempDir)
	}
	if createdBase != "" {
		os.RemoveAll(createdBase)
	}
}

// TestMain should be called from your package TestMain to ensure that the process
// temp directory is cleaned up on successful runs.
func TestMain(m *testing.M) {
	flag.Parse()
	ret := m.Run()
	if ret == 0 {
		cleanup()
	}
	os.Exit(ret)
}
