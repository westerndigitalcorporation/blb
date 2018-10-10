// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT
//
// This file contains mechanism to manipulate extended file attributes.

package disk

import (
	"fmt"
	"strings"
	"syscall"
	"unsafe"
)

//
// XattrError type.
//

// XattrError records an error and the operation, file path and attribute name
// that caused it.
type XattrError struct {
	Op   string  // What is the operation?
	Fd   uintptr // What is the file descriptor?
	Name string  // What is the name of the attribute?
	Err  error   // What is the error?
}

// Error implements error.
func (x *XattrError) Error() string {
	return fmt.Sprintf("xattr error, op=%s, fd=%v, name=%s, error=%s", x.Op, x.Fd, x.Name, x.Err)
}

func newXattrError(op string, fd uintptr, name string, err error) *XattrError {
	return &XattrError{
		Op:   op,
		Fd:   fd,
		Name: name,
		Err:  err,
	}
}

// convertCStrings converts a set of NULL-terminated UTF-8 strings to a set of
// go strings.
func convertCStrings(cstrings []byte) []string {
	return strings.FieldsFunc(string(cstrings), func(c rune) bool { return 0 == c })
}

// Fgetxattr gets an extended attribute value.
func Fgetxattr(fd uintptr, name string) ([]byte, error) {
	// We will first try a small-sized buffer.
	xname := xattrNamespace + name
	size := 8
	value := make([]byte, size)
	size, err := fgetxattr(fd, xname, value, size)
	if nil == err {
		return value[:size], nil
	}

	// Return the error if it's not complaining about the buffer size.
	if syscall.ERANGE != err {
		return nil, newXattrError("getxattr", fd, name, err)
	}

	// We're here. It means that the buffer size is too small. Ask for the
	// proper size directly. When 'value' is nil, getxattr returns current
	// size of the named attribute.
	size, err = fgetxattr(fd, xname, nil, 0)
	if nil != err {
		return nil, newXattrError("getxattr", fd, name, err)
	}

	// Read the value out.
	if size <= 0 {
		return nil, newXattrError("getxattr", fd, name, fmt.Errorf("size cannot be non-positive"))
	}
	value = make([]byte, size)
	if size, err = fgetxattr(fd, xname, value, size); nil != err {
		return nil, newXattrError("getxattr", fd, name, err)
	} else if size != len(value) {
		// Sanity check.
		return nil, newXattrError("getxattr", fd, name, fmt.Errorf("size doesn't match"))
	}
	return value, nil
}

// Fsetxattr sets an extended attribute value.
func Fsetxattr(fd uintptr, name string, value []byte) error {
	if err := fsetxattr(fd, xattrNamespace+name, value, len(value)); nil != err {
		return newXattrError("setxattr", fd, name, err)
	}
	return nil
}

//
// Helper.
//

// Remove the namespace prefix from xattr names.
func cleanNamespace(in []string) (out []string) {
	for _, s := range in {
		out = append(out, strings.TrimPrefix(s, xattrNamespace))
	}
	return
}

//
// Internal syscall implementation. We don't use things like syscall.Getxattr
// directly so that the implementation can be shared between linux and darwin.
//

// Convert Errno to error.
func convertErrno(errno syscall.Errno) (err error) {
	if 0 != errno {
		err = errno
	}
	return
}

// fgetxattr is the internal implementation of Fgetxattr. See "man fgetxattr"
// for details.
func fgetxattr(fd uintptr, name string, value []byte, size int) (int, error) {
	// Convert name to a pointer to NUL-terminated array of bytes.
	cname, err := syscall.BytePtrFromString(name)
	if nil != err {
		return 0, fmt.Errorf("failed to convert name %q: %s", name, err)
	}

	var b *byte
	if 0 != len(value) {
		b = &value[0]
	}

	r, _, errno := syscall.Syscall6(
		syscall.SYS_FGETXATTR,
		fd,
		uintptr(unsafe.Pointer(cname)),
		uintptr(unsafe.Pointer(b)),
		uintptr(size),
		0,
		0,
	)
	return int(r), convertErrno(errno)
}

// fsetxattr is the internal implementation of Fsetxattr. See "man fsetxattr"
// for details.
func fsetxattr(fd uintptr, name string, value []byte, size int) error {
	// Convert name to a pointer to NUL-terminated array of bytes.
	cname, err := syscall.BytePtrFromString(name)
	if nil != err {
		return fmt.Errorf("failed to convert name %q: %s", name, err)
	}

	var b *byte
	if 0 != len(value) {
		b = &value[0]
	}

	_, _, errno := syscall.Syscall6(
		syscall.SYS_FSETXATTR,
		fd,
		uintptr(unsafe.Pointer(cname)),
		uintptr(unsafe.Pointer(b)),
		uintptr(size),
		0,
		0,
	)
	return convertErrno(errno)
}
