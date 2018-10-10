// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT
//
// This package implements a basic interface to Blb as a filesystem using FUSE.
// Creating, listing, reading, writing, and removing blobs are supported.
//
// This is not for production use! It's intended for diagnostics only.

package fuse

import (
	"fmt"
	"io"
	"os"
	"sync/atomic"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"

	client "github.com/westerndigitalcorporation/blb/client/blb"
	"github.com/westerndigitalcorporation/blb/internal/core"
)

// MountState holds information about a current mount.
type MountState struct {
	path   string       // the path we mounted on
	err    atomic.Value // an error value returned from fuse, or nil if no error so far
	exited atomic.Value // nil if the fuse server goroutine is still running, non-nil if not
}

// Mount mounts a Blb FUSE filesystem on the given path and runs the FUSE server
// in a goroutine. It returns immediately.
func Mount(c *client.Client, path string) *MountState {
	ms := &MountState{path: path}
	go ms.mount(c)
	return ms
}

// mount is the actual mount process.
func (ms *MountState) mount(c *client.Client) {
	defer ms.exited.Store("true")

	conn, err := fuse.Mount(
		ms.path,
		fuse.FSName("blb"), // TODO: put server hostname here
		fuse.Subtype("blbfs"),
		// Blb likes lots of readahead. Note that the kernel will issue still
		// reads in 128KB blocks, no matter how large this is (Linux, at least).
		fuse.MaxReadahead(1<<20),
		// Blb likes larger writes.
		fuse.WritebackCache(),
		// There's no harm in async reads, so allow that.
		fuse.AsyncRead(),
	)
	if err != nil {
		ms.err.Store(err)
		return
	}
	defer conn.Close()

	blbfs := &blbFS{c: c}
	err = fs.Serve(conn, blbfs)
	if err != nil {
		ms.err.Store(err)
		return
	}

	// check if the mount process has an error to report
	<-conn.Ready
	if conn.MountError != nil {
		ms.err.Store(conn.MountError)
	}
}

// Unmount tries to unmount an existing FUSE mount.
func (ms *MountState) Unmount() error {
	return fuse.Unmount(ms.path)
}

// String returns a string representation of the state of this mount.
func (ms *MountState) String() string {
	return fmt.Sprintf("on %q, error %v, exited %v",
		ms.path, ms.err.Load(), ms.Exited())
}

// Exited returns true if the FUSE goroutine has exited.
func (ms *MountState) Exited() bool {
	return ms.exited.Load() != nil
}

type blbFS struct {
	c *client.Client
}

func (b *blbFS) Root() (fs.Node, error) {
	return (*rootDir)(b), nil
}

type rootDir blbFS

func (r *rootDir) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Inode = 1
	a.Mode = os.ModeDir | 0555
	return nil
}

func (r *rootDir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	switch name {
	case "blobs":
		return (*blobDir)(r), nil
	case "meta":
		return (*metaDir)(r), nil
	}
	return nil, fuse.ENOENT
}

func (r *rootDir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	return []fuse.Dirent{
		{Inode: 2, Name: "meta", Type: fuse.DT_Dir},
		{Inode: 3, Name: "blobs", Type: fuse.DT_Dir},
	}, nil
}

type metaDir blbFS

func (m *metaDir) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Inode = 2
	a.Mode = os.ModeDir | 0555
	return nil
}

func (m *metaDir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	return []fuse.Dirent{
		{Inode: 4, Name: "create", Type: fuse.DT_File},
	}, nil
}

func (m *metaDir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	switch name {
	case "create":
		return (*createNode)(m), nil
	}
	return nil, fuse.ENOENT
}

type createNode blbFS

func (c *createNode) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Inode = 4
	a.Mode = 0444
	return nil
}

func (c *createNode) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	// TODO: allow adding create options
	blob, err := c.c.Create()
	if err != nil {
		return nil, err
	}
	resp.Flags |= fuse.OpenDirectIO // need this or we won't be able to read
	return fs.DataHandle([]byte(blob.ID().String())), nil
}

type blobDir blbFS

func (b *blobDir) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Inode = 3
	a.Mode = os.ModeDir | 0555
	return nil
}

func (b *blobDir) ReadDirAll(ctx context.Context) (out []fuse.Dirent, err error) {
	// TODO: Do this in chunks. Depends on https://github.com/bazil/fuse/issues/58
	bi := b.c.ListBlobs(ctx)
	for {
		var ids []client.BlobID
		ids, err = bi()
		if err != nil || ids == nil {
			return
		}
		for _, id := range ids {
			// Use blob id directly as inode. blob ids always have a non-zero
			// partition id, so as long as our internal inodes have zeros in the
			// upper 32 bits, they won't conflict.
			out = append(out,
				fuse.Dirent{Inode: uint64(id), Name: id.String(), Type: fuse.DT_File})
		}
	}
}

func (b *blobDir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	id, err := core.ParseBlobID(name)
	if err != nil {
		return nil, fuse.ENOENT
	}
	blob, err := b.c.Open(client.BlobID(id), "s")
	if err != nil {
		return nil, translateError(err)
	}
	return (*blobNode)(blob), nil
}

func (b *blobDir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	id, err := core.ParseBlobID(req.Name)
	if err != nil {
		return fuse.ENOENT
	}
	err = b.c.Delete(ctx, client.BlobID(id))
	return translateError(err)
}

// We use *client.Blob as both Node and Handle. There's basically no state kept
// for "open" blobs, so this keeps things simpler.
type blobNode client.Blob

func (n *blobNode) Attr(ctx context.Context, attr *fuse.Attr) error {
	blob := (*client.Blob)(n)
	attr.Inode = uint64(blob.ID())
	info, err := blob.Stat()
	if err != nil {
		return err
	}
	length, err := blob.ByteLength()
	if err != nil {
		return err
	}
	attr.Size = uint64(length)
	// Round up to a 4KB block size, since we're storing things on ext4 in the end.
	const underlyingBlockSize = 4096
	attr.Blocks = uint64(length+underlyingBlockSize-1) / underlyingBlockSize / 512
	if info.Class == core.StorageClass_REPLICATED {
		attr.Mode = 0644
	} else {
		attr.Mode = 0444
	}
	attr.Mtime = info.MTime
	attr.Atime = info.ATime
	attr.BlockSize = core.TractLength
	return nil
}

func (n *blobNode) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	blob := (*client.Blob)(n)
	data := resp.Data[:req.Size] // caller has allocated a buffer with sufficient capacity
	s, err := blob.ReadAt(data, req.Offset)
	if err != nil && err != io.EOF {
		return err
	}
	resp.Data = data[:s]
	return nil
}

func (n *blobNode) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	blob := (*client.Blob)(n)
	s, err := blob.WriteAt(req.Data, req.Offset)
	if err != nil {
		return err
	}
	resp.Size = s
	return nil
}

func translateError(err error) error {
	// translate some errors specially for FUSE
	if core.ErrNoSuchBlob.Is(err) {
		return fuse.ENOENT
	}
	return err
}
