// Copyright (c) 2017 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package wal

import (
	"io"
	"syscall"
	"unsafe"
)

func writevfull(fd uintptr, b1, b2, b3 []byte) error {
	iov := []syscall.Iovec{
		{&b1[0], uint64(len(b1))},
		{&b2[0], uint64(len(b2))},
		{&b3[0], uint64(len(b3))},
	}
	n, _, errno := syscall.Syscall(syscall.SYS_WRITEV, fd, uintptr(unsafe.Pointer(&iov[0])), uintptr(len(iov)))
	if errno != 0 {
		return errno
	}
	if int(n) != len(b1)+len(b2)+len(b3) {
		return io.ErrShortWrite
	}
	return nil
}
