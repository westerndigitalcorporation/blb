// Copyright (c) 2017 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT
//
// This file is heavily based on gob{Client,Server}Codec in Go's net/rpc package.
//
// bulkGobCodec is an attempt to reduce the amount of data copying when using Go RPC for
// sending large messages. It implements a slight variation on the gob codecs. Messages
// are encoded as follows:
// 1. gob-encoded request (or response) header
// 2. gob-encoded body
// 3. length of bulk data (32 bit little-endian)
// 4. crc32 of 1, 2, and 3 (little-endian)
// 5. if length is not zero: bulk data
// 6. if length is not zero: crc32 of bulk data (little-endian)
//
// To indicate that a message has bulk data, it should implement the BulkData interface
// below. Note that Get() should clear the []byte member so that gob doesn't also try to
// encode it. A given type must either always or never implement BulkData; it is not
// safe to change whether a type does or doesn't (or change which field is the bulk
// data), since that will change its encoding on the wire.
//
// When using this codec, note that request bodies must be passed as pointers to Send,
// otherwise they can't implement the interface.

package rpc

import (
	"bufio"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net/rpc"
)

// BulkData is an interface that lets a struct expose a single field as bulk data.
// The data may be exclusively owned by the caller or not; the bool value is
// true if it is.
type BulkData interface {
	Get() ([]byte, bool) // extract and return bulk data and exclusive flag
	Set([]byte, bool)    // put bulk data back and exclusive flag in the struct
}

var (
	errChecksumMismatch = errors.New("checksum mismatch in rpc")
	crcTable            = crc32.MakeTable(crc32.Castagnoli)
)

// bulkGobCodec implements both rpc.ClientCodec and rpc.ServerCodec.
type bulkGobCodec struct {
	rwc io.ReadWriteCloser

	// Readers/Writers are wrapped like: gob(crc(bufio(rwc))), so that we can
	// control what gets crc'd.
	decBuf *bufio.Reader
	dec    *gob.Decoder
	encBuf *bufio.Writer
	enc    *gob.Encoder

	wCrc, rCrc uint32
	closed     bool
}

func newBulkGobCodec(conn io.ReadWriteCloser) *bulkGobCodec {
	c := &bulkGobCodec{rwc: conn}
	c.decBuf = bufio.NewReader(conn)
	c.dec = gob.NewDecoder(c)
	c.encBuf = bufio.NewWriter(conn)
	c.enc = gob.NewEncoder(c)
	return c
}

// The codec itself acts as a checksumming writer and reader:
func (c *bulkGobCodec) Write(p []byte) (n int, err error) {
	n, err = c.encBuf.Write(p)
	c.wCrc = crc32.Update(c.wCrc, crcTable, p[:n])
	return
}

func (c *bulkGobCodec) Read(p []byte) (n int, err error) {
	n, err = c.decBuf.Read(p)
	c.rCrc = crc32.Update(c.rCrc, crcTable, p[:n])
	return
}

// Trick gob into thinking that this is a buffered reader (because it is).
func (c *bulkGobCodec) ReadByte() (byte, error) {
	panic("not implemented")
}

func (c *bulkGobCodec) WriteRequest(r *rpc.Request, body interface{}) (err error) {
	if err = c.writeBulk(r, body); err != nil {
		c.Close()
	}
	return
}

func (c *bulkGobCodec) ReadResponseHeader(r *rpc.Response) error {
	// 1. gob-encoded response header
	c.rCrc = 0
	return c.dec.Decode(r)
}

func (c *bulkGobCodec) ReadResponseBody(body interface{}) (err error) {
	return c.readBulkBody(body)
}

func (c *bulkGobCodec) ReadRequestHeader(r *rpc.Request) error {
	// 1. gob-encoded request header
	c.rCrc = 0
	return c.dec.Decode(r)
}

func (c *bulkGobCodec) ReadRequestBody(body interface{}) (err error) {
	return c.readBulkBody(body)
}

func (c *bulkGobCodec) WriteResponse(r *rpc.Response, body interface{}) (err error) {
	if err = c.writeBulk(r, body); err != nil {
		c.Close()
	}
	return
}

func (c *bulkGobCodec) Close() error {
	if c.closed {
		// Only call c.rwc.Close once; otherwise the semantics are undefined.
		return nil
	}
	c.closed = true
	return c.rwc.Close()
}

func (c *bulkGobCodec) writeBulk(reqOrResp, body interface{}) (err error) {
	var bulkData []byte
	var exclusive bool
	if bb, isBulk := body.(BulkData); isBulk {
		bulkData, exclusive = bb.Get()
	}

	// 1. gob-encoded request (or response) header
	c.wCrc = 0
	if err = c.enc.Encode(reqOrResp); err != nil {
		return
	}
	// 2. gob-encoded body
	if err = c.enc.Encode(body); err != nil {
		return
	}
	// 3. length of bulk data (32 bit little-endian)
	if err = binary.Write(c, binary.LittleEndian, uint32(len(bulkData))); err != nil {
		return
	}
	// 4. crc32 of 1, 2, and 3 (little-endian)
	if err = binary.Write(c, binary.LittleEndian, c.wCrc); err != nil {
		return
	}
	if len(bulkData) > 0 {
		// 5. bulk data
		// Note that bufio.Writer will pass this write directly though to c.rwc
		// once it flushes its buffer and has more than one buffer's worth of
		// data to write, so most of the data won't be copied more than once.
		c.wCrc = 0
		if _, err = c.Write(bulkData); err != nil {
			return
		}
		PutBuffer(bulkData, exclusive)
		// 6. crc32 of bulk data (little-endian)
		if err = binary.Write(c, binary.LittleEndian, c.wCrc); err != nil {
			return
		}
	}
	return c.encBuf.Flush()
}

func (c *bulkGobCodec) readBulkBody(body interface{}) (err error) {
	var bulkData []byte
	var exclusive bool
	bb, isBulk := body.(BulkData)
	if isBulk {
		// Get a preallocated slice from the body, if it has one.
		bulkData, exclusive = bb.Get()
	}

	// 2. gob-encoded body
	if err = c.dec.Decode(body); err != nil {
		return
	}
	// 3. length of bulk data (32 bit little-endian)
	var bulkLen uint32
	if err = binary.Read(c, binary.LittleEndian, &bulkLen); err != nil {
		return
	}
	// 4. crc32 of 1, 2, and 3 (little-endian)
	haveCrc := c.rCrc
	var wantCrc uint32
	if err = binary.Read(c, binary.LittleEndian, &wantCrc); err != nil {
		return
	}
	if wantCrc != haveCrc {
		return errChecksumMismatch
	}
	if bulkLen > 0 {
		if !isBulk {
			return fmt.Errorf("type %T doesn't implement BulkData", body)
		}
		if cap(bulkData) >= int(bulkLen) {
			bulkData = bulkData[:bulkLen]
		} else {
			bulkData = GetBuffer(int(bulkLen))
			exclusive = true
		}
		// 5. bulk data
		// ReadFull + bufio.Reader will do a direct read from the Reader once the buffer
		// (default 4KB) is exhausted and there's more than one buffer's worth of data to
		// read, so most of the data won't be copied more than once here.
		c.rCrc = 0
		if _, err = io.ReadFull(c, bulkData); err != nil {
			return
		}
		// 6. crc32 of bulk data (little-endian)
		haveCrc = c.rCrc
		if err = binary.Read(c, binary.LittleEndian, &wantCrc); err != nil {
			return
		}
		// Allow zero to mean "don't check this crc", so caller can choose not
		// to compute a crc here.
		if wantCrc != 0 && wantCrc != haveCrc {
			return errChecksumMismatch
		}
		bb.Set(bulkData, exclusive)
	}
	return
}
