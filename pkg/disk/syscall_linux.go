// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT
//
// Linux syscall related stuff goes here.
//
// +build linux

package disk

import (
	"syscall"

	// #include <fcntl.h>
	"C"
)

// Constants for syscalls.
const (
	// Parameters to be used by fadvise.
	POSIX_FADV_RANDOM   = C.POSIX_FADV_RANDOM
	POSIX_FADV_DONTNEED = C.POSIX_FADV_DONTNEED

	// Mandatory namespace for xattr names.
	xattrNamespace = "user."

	// Error returned if the attribute was not found.
	ENOATTR = syscall.ENODATA

	// Disable all atime updates.
	O_NOATIME = syscall.O_NOATIME
)

// Fadvise implements mockFile.
func (f *regularFile) Fadvise(offset int64, length int64, advice int) (err error) {
	// Directly call posix_fadvise as it's not exposed as a normal Go call.
	_, _, e1 := syscall.Syscall6(syscall.SYS_FADVISE64, f.File.Fd(), uintptr(offset), uintptr(length), uintptr(advice), 0, 0)
	if e1 != 0 {
		err = e1
	}
	return
}
