// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package tractserver

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/westerndigitalcorporation/blb/internal/core"
)

// Disk is a wrapper around an actual disk that we use for testing.
//
// Disk is thread-safe in general, BUT: each individual tract must be used in a
// way that is compatible with a read-write lock. That is, multiple threads may
// have a single tract open for read at once, but at most one thread may have a
// tract open for writing. If multiple threads try to write to a tract
// concurrently, it may get corrupted. Disk may return an error in that case,
// but is not required to.
type Disk interface {
	// Open opens a tract for I/O or metadata changes.
	// Returns a file handle for the opened tract and core.NoError on success,
	// another error otherwise.
	Open(ctx context.Context, id core.TractID, flags int) (interface{}, core.Error)

	// Close closes a file handle created by calling Open.
	// Returns core.NoError if the file was opened and successfully closed.
	// Returns another error otherwise.
	Close(f interface{}) core.Error

	// Write writes to the provided tract.
	Write(ctx context.Context, f interface{}, b []byte, off int64) (int, core.Error)

	// Read reads from the provided tract.
	Read(ctx context.Context, f interface{}, b []byte, off int64) (int, core.Error)

	// Scrub scrubs the provided tract (read and verify checksums, then discard data).
	// Returns the size of the file.
	Scrub(id core.TractID) (int64, core.Error)

	// Size returns the size of the provided tract.
	Size(f interface{}) (int64, core.Error)

	// Delete removes the provided tract.
	Delete(id core.TractID) core.Error

	// OpenDir gets an iterator over the full set of tracts on the disk. The
	// value returned can be passed to ReadDir and CloseDir.
	OpenDir() (interface{}, core.Error)

	// ReadDir reads some tract IDs from the disk. It will return ErrEOF when
	// it's done iterating through the directory.
	ReadDir(d interface{}) ([]core.TractID, core.Error)

	// CloseDir closes a directory iterator opened by OpenDir.
	CloseDir(d interface{}) core.Error

	// Getxattr gets the value of xattr named 'name' in the open tract 'f'.
	Getxattr(f interface{}, name string) ([]byte, core.Error)

	// Setxattr sets xattr named 'name' to value 'value' in the open tract 'f'.
	Setxattr(f interface{}, name string, value []byte) core.Error

	// Statfs returns heavyweight information about the disk under management.
	Statfs() core.FsStatus

	// Return lightweight information about the health of the disk under management.
	Status() core.DiskStatus

	// Sets control flags for this disk. This is persistent: these flags will
	// persist unless changed with another call to SetControlFlags, or another
	// mechanism specific to this disk.
	SetControlFlags(core.DiskControlFlags) core.Error

	// Stop causes the disk to release all allocated resources and return ErrDiskRemoved
	// to all subsequent calls except Close, CloseDir, and Status.
	Stop()
}

// MemDisk is a memory-only implementation of the Disk interface that is useful for testing.
// Files are stored in a map.
type MemDisk struct {
	lock     sync.Mutex
	fds      map[core.TractID]uint32
	files    map[uint32][]byte
	versions map[uint32]int
	open     map[uint32]bool
	xattrs   map[uint32]map[string][]byte
	nextFD   uint32
	status   core.DiskStatus
}

// NewMemDisk returns a new MemDisk.
func NewMemDisk() *MemDisk {
	md := &MemDisk{
		fds:      make(map[core.TractID]uint32),
		files:    make(map[uint32][]byte),
		versions: make(map[uint32]int),
		open:     make(map[uint32]bool),
		xattrs:   make(map[uint32]map[string][]byte),
		status:   core.DiskStatus{Full: false, Healthy: true},
	}
	md.status.Root = fmt.Sprintf("mem:%p", md)
	return md
}

// Status returns lightweight disk status.
func (m *MemDisk) Status() core.DiskStatus {
	return m.status
}

// Open opens a tract.
func (m *MemDisk) Open(ctx context.Context, id core.TractID, flags int) (interface{}, core.Error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.fds == nil {
		return nil, core.ErrDiskRemoved
	}

	// If it's already there.
	if fd, ok := m.fds[id]; ok {
		if (flags&os.O_CREATE != 0) && (flags&os.O_EXCL != 0) {
			return 0, core.ErrAlreadyExists
		}
		if flags&os.O_TRUNC != 0 {
			m.files[fd] = nil
		}
		m.open[fd] = true
		return fd, core.NoError
	}

	// It's not there.
	if flags&os.O_CREATE == 0 {
		return 0, core.ErrNoSuchTract
	}
	fd := m.nextFD
	m.fds[id] = fd
	m.files[fd] = nil
	m.open[fd] = true
	m.nextFD++
	return fd, core.NoError
}

// Close closes an open tract.
func (m *MemDisk) Close(f interface{}) core.Error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.fds == nil {
		return core.ErrDiskRemoved
	}

	fd := f.(uint32)
	if _, ok := m.open[fd]; !ok {
		return core.ErrInvalidArgument
	}
	delete(m.open, fd)
	return core.NoError
}

// Write writes to the MemDisk.
func (m *MemDisk) Write(_ context.Context, f interface{}, b []byte, off int64) (int, core.Error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.fds == nil {
		return 0, core.ErrDiskRemoved
	}

	fd := f.(uint32)
	if _, ok := m.open[fd]; !ok {
		return 0, core.ErrInvalidArgument
	}

	oldSize := int64(len(m.files[fd]))
	newSize := off + int64(len(b))

	if newSize > oldSize {
		old := m.files[fd]
		m.files[fd] = make([]byte, newSize)
		copy(m.files[fd], old)
	}

	n := copy(m.files[fd][off:], b)
	if n != len(b) {
		return n, core.ErrNoSpace
	}
	return n, core.NoError
}

// Read reads from the MemDisk.
func (m *MemDisk) Read(_ context.Context, f interface{}, b []byte, off int64) (int, core.Error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.fds == nil {
		return 0, core.ErrDiskRemoved
	}

	fd := f.(uint32)
	if _, ok := m.open[fd]; !ok {
		return 0, core.ErrInvalidArgument
	}

	n := copy(b, m.files[fd][off:])
	return n, core.NoError
}

// Scrub just assumes everything is good.
func (m *MemDisk) Scrub(id core.TractID) (int64, core.Error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.fds == nil {
		return 0, core.ErrDiskRemoved
	}

	fd, ok := m.fds[id]
	if !ok {
		return 0, core.ErrInvalidArgument
	}

	return int64(len(m.files[fd])), core.NoError
}

// Size returns the size in bytes of the provided tract.
func (m *MemDisk) Size(f interface{}) (int64, core.Error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.fds == nil {
		return 0, core.ErrDiskRemoved
	}

	fd := f.(uint32)
	if _, ok := m.open[fd]; !ok {
		return 0, core.ErrInvalidArgument
	}

	return int64(len(m.files[fd])), core.NoError
}

// Delete removes a tract from the MemDisk.
func (m *MemDisk) Delete(id core.TractID) core.Error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.fds == nil {
		return core.ErrDiskRemoved
	}

	fd, ok := m.fds[id]
	if !ok {
		return core.ErrNoSuchTract
	}

	delete(m.fds, id)
	delete(m.files, fd)
	delete(m.versions, fd)
	delete(m.open, fd)
	delete(m.xattrs, fd)
	return core.NoError
}

// OpenDir returns a value that can be used to read all tract ids.
func (m *MemDisk) OpenDir() (interface{}, core.Error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.fds == nil {
		return nil, core.ErrDiskRemoved
	}

	out := make([]core.TractID, 0, len(m.fds))
	for t := range m.fds {
		if t.Blob == core.ZeroBlobID {
			// This is a metadata tract. Hide this from above layers (so that the
			// curator doesn't tell us to GC it).
			continue
		}
		out = append(out, t)
	}
	return &out, core.NoError
}

// ReadDir reads a set of tract ids.
func (m *MemDisk) ReadDir(d interface{}) ([]core.TractID, core.Error) {
	ids := d.(*[]core.TractID)
	// return half at once, rounded up
	if len(*ids) == 0 {
		return nil, core.ErrEOF
	}
	n := len(*ids)/2 + 1
	half := (*ids)[:n]
	*ids = (*ids)[n:]
	return half, core.NoError
}

// CloseDir closes the directory.
func (m *MemDisk) CloseDir(d interface{}) core.Error {
	return core.NoError
}

// Getxattr gets the value for the named xattr.
func (m *MemDisk) Getxattr(f interface{}, name string) ([]byte, core.Error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.fds == nil {
		return nil, core.ErrDiskRemoved
	}

	fd := f.(uint32)
	if _, ok := m.open[fd]; !ok {
		return nil, core.ErrInvalidArgument
	}

	xattr, ok := m.xattrs[fd]
	if !ok {
		return nil, core.ErrNoSuchTract
	}
	return xattr[name], core.NoError
}

// Setxattr sets the value for the named xattr.
func (m *MemDisk) Setxattr(f interface{}, name string, value []byte) core.Error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.fds == nil {
		return core.ErrDiskRemoved
	}

	fd := f.(uint32)
	if _, ok := m.open[fd]; !ok {
		return core.ErrInvalidArgument
	}

	_, ok := m.xattrs[fd]
	if !ok {
		m.xattrs[fd] = make(map[string][]byte)
	}
	b := make([]byte, len(value))
	copy(b, value)
	m.xattrs[fd][name] = b
	return core.NoError
}

// Statfs returns fake information about the underlying "file system"
func (m *MemDisk) Statfs() core.FsStatus {
	return core.FsStatus{Status: m.status}
}

// SetControlFlags sets flags for this disk.
func (m *MemDisk) SetControlFlags(flags core.DiskControlFlags) core.Error {
	m.status.Flags = flags
	return core.NoError
}

// Stop releases resources associated with this disk.
func (m *MemDisk) Stop() {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.fds = nil
	m.files = nil
	m.versions = nil
	m.open = nil
	m.xattrs = nil
}

// SimpleDisk is an implementation of Disk that returns a certain core.Err for every operation.
type simpleDisk struct {
	err    core.Error
	status core.DiskStatus
}

func newSimpleDisk(err core.Error, status core.DiskStatus) Disk {
	return &simpleDisk{err: err, status: status}
}

// Status returns a DiskStatus object indicating that the disk is unhealthy and full.
func (b *simpleDisk) Status() core.DiskStatus {
	return b.status
}

// Open returns an error.
func (b *simpleDisk) Open(context.Context, core.TractID, int) (interface{}, core.Error) {
	return 0, b.err
}

// Close returns an error.
func (b *simpleDisk) Close(interface{}) core.Error {
	return b.err
}

// Write returns an error.
func (b *simpleDisk) Write(context.Context, interface{}, []byte, int64) (int, core.Error) {
	return 0, b.err
}

// Read returns an error.
func (b *simpleDisk) Read(context.Context, interface{}, []byte, int64) (int, core.Error) {
	return 0, b.err
}

// Scrub returns an error.
func (b *simpleDisk) Scrub(id core.TractID) (int64, core.Error) {
	return 0, b.err
}

// Size returns an error.
func (b *simpleDisk) Size(interface{}) (int64, core.Error) {
	return 0, b.err
}

// Delete returns an error.
func (b *simpleDisk) Delete(core.TractID) core.Error {
	return b.err
}

// OpenDir returns an error.
func (b *simpleDisk) OpenDir() (interface{}, core.Error) {
	return nil, b.err
}

// ReadDir returns no tracts (note: not b.err, to satisfy the open/read/close protocol).
func (b *simpleDisk) ReadDir(d interface{}) ([]core.TractID, core.Error) {
	return nil, core.ErrEOF
}

// CloseDir returns an error.
func (b *simpleDisk) CloseDir(d interface{}) core.Error {
	return b.err
}

// Getxattr returns an error.
func (b *simpleDisk) Getxattr(interface{}, string) ([]byte, core.Error) {
	return nil, b.err
}

// Setxattr returns an error.
func (b *simpleDisk) Setxattr(interface{}, string, []byte) core.Error {
	return b.err
}

// Statfs returns an empty status.
func (b *simpleDisk) Statfs() core.FsStatus {
	return core.FsStatus{}
}

// SetControlFlags sets flags for this disk.
func (b *simpleDisk) SetControlFlags(flags core.DiskControlFlags) core.Error {
	return b.err
}

func (b *simpleDisk) Stop() {
	b.err = core.ErrDiskRemoved
}
