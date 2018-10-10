// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT
//
// Darwin (MacOS) syscall related stuff goes here.
//
// +build darwin

package disk

import "syscall"

// Constants for syscalls.
const (
	// Don't care about Fadvise on Mac.
	POSIX_FADV_RANDOM   = 0
	POSIX_FADV_DONTNEED = 0

	// Don't need xattr namespace.
	xattrNamespace = ""

	// Error returned if the attribute was not found.
	ENOATTR = syscall.ENOATTR

	// Darwin doesn't have this.
	O_NOATIME = 0
)

// Fadvise implements mockFile.
func (f *regularFile) Fadvise(offset int64, length int64, advice int) (err error) {
	return
}
