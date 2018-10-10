// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package testutil

import (
	"fmt"
	"os"
	"path/filepath"
)

// FindBinDir finds a directory that contains our built binaries. It should work
// in both experimental and upcode. If it fails to find such a directory, it
// returns the empty string.
func FindBinDir() string {
	// ""build/bin" for the root of upcode. "go/bin" might also work for upcode
	// if binaries haven't been copied to "build/bin".
	relPaths := []string{"build/bin", "go/bin"}

	var wd string
	var err error

	if wd, err = os.Getwd(); err != nil {
		return ""
	}

	for wd != "/" {
		for _, relPath := range relPaths {
			path := filepath.Join(wd, relPath)
			if fi, err := os.Stat(path); err == nil && fi.IsDir() {
				return path
			}
		}
		wd = filepath.Dir(wd)
	}

	return ""
}

// CheckBinaries verifies if we have the given binaries in the given dir
// 'binDir'. The name of the first missing binary, if any, is returned as an
// error. Otherwise returns nil.
func CheckBinaries(binDir string, binaries []string) error {
	for _, b := range binaries {
		if fi, err := os.Stat(filepath.Join(binDir, b)); os.IsNotExist(err) || (fi.Mode().Perm()&0100) == 0 {
			return fmt.Errorf("failed to find binary %q in directory %q", b, binDir)
		}
	}
	return nil
}
