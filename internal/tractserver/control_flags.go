// Copyright (c) 2017 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT
//
// These functions manage a set of simple flag files on each disk. We keep them
// in simple files (as opposed to something like json) so that they can be
// easily examined and created/modified by simple tools or by hand.

package tractserver

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"

	"github.com/westerndigitalcorporation/blb/internal/core"
)

const flagFilePerms = 0644

func controlFilePaths(root string) (stopAllocatingPath, drainPath, drainLocalPath string) {
	stopAllocatingPath = filepath.Join(root, "stopallocating")
	drainPath = filepath.Join(root, "drain")
	drainLocalPath = filepath.Join(root, "drainlocal")
	return
}

func loadControlFlags(root string) core.DiskControlFlags {
	stopAllocatingPath, drainPath, drainLocalPath := controlFilePaths(root)
	return core.DiskControlFlags{
		StopAllocating: boolFromFlagFile(stopAllocatingPath),
		Drain:          intFromFlagFile(drainPath),
		DrainLocal:     intFromFlagFile(drainLocalPath),
	}
}

func saveControlFlags(root string, flags core.DiskControlFlags) (err core.Error) {
	stopAllocatingPath, drainPath, drainLocalPath := controlFilePaths(root)
	mergeErr := func(e core.Error) {
		if e != core.NoError {
			err = e
		}
	}
	mergeErr(boolToFlagFile(stopAllocatingPath, flags.StopAllocating))
	mergeErr(intToFlagFile(drainPath, flags.Drain))
	mergeErr(intToFlagFile(drainLocalPath, flags.DrainLocal))
	return
}

func intFromFlagFile(path string) int {
	if data, err := ioutil.ReadFile(path); err == nil {
		if i, err := strconv.Atoi(string(data)); err == nil {
			return i
		}
		return 1 // assume 1 if we can't parse an integer out of the contents
	}
	return 0
}

func intToFlagFile(path string, i int) core.Error {
	if i > 0 {
		data := strconv.Itoa(i) + "\n"
		if e := ioutil.WriteFile(path, []byte(data), flagFilePerms); e != nil {
			return core.ErrIO
		}
	} else {
		os.Remove(path) // ignore errors from Remove, assuming they're just ENOENT
	}
	return core.NoError
}

func boolFromFlagFile(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func boolToFlagFile(path string, v bool) core.Error {
	if v {
		if e := ioutil.WriteFile(path, nil, flagFilePerms); e != nil {
			return core.ErrIO
		}
	} else {
		os.Remove(path) // ignore errors from Remove, assuming they're just ENOENT
	}
	return core.NoError
}
