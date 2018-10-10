// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	log "github.com/golang/glog"
)

// MaxRecordDataLen is the largest data buffer in bytes that can be stored
// with a Record.
const MaxRecordDataLen = 1024 * 1024

// ErrRecordDataTooBig is the error returned when trying to store a record
// with an excessively large data buffer.
var ErrRecordDataTooBig = fmt.Errorf("Data too big, can be at most %d bytes", MaxRecordDataLen)

// ErrCorruptData is returned when we find a crc mismatch or other invalid data.
var ErrCorruptData = fmt.Errorf("Corrupt data")

// Records are written to disk in a persistent format that includes
// a checksum to verify data integrity.
// The on-disk layout looks like (all values are little endian):
// --------------------------------------------
// | ID (8 bytes)                             |
// --------------------------------------------
// | datalen (4 bytes) | data (datalen bytes) |
// --------------------------------------------
// | ....              | csum (4 bytes)       |
// --------------------------------------------
// The checksum is calculated based on all bytes that precede it including
// the ID.

// This is opaque, pre-calculated data used by the hash/crc32 package
// to speed up CRC calculations.
var crc32Table = crc32.MakeTable(crc32.Castagnoli)

// serialize serializes the record to the given writer. Returns any error.
func (r *Record) serialize(w io.Writer) error {
	if len(r.Data) > MaxRecordDataLen {
		return ErrRecordDataTooBig
	}

	// Try to use writev for larger writes so we don't have to copy, but do a
	// plain write for small ones to avoid overhead. This cutoff was empirically
	// determined. The performance difference is pretty small in any case.
	if len(r.Data) > 112 {
		if f, ok := w.(*os.File); ok {
			var smallBuf [16]byte
			binary.LittleEndian.PutUint64(smallBuf[0:8], r.ID)
			binary.LittleEndian.PutUint32(smallBuf[8:12], uint32(len(r.Data)))

			csum := crc32.Update(0, crc32Table, smallBuf[0:12])
			csum = crc32.Update(csum, crc32Table, r.Data)
			binary.LittleEndian.PutUint32(smallBuf[12:16], csum)

			return writevfull(f.Fd(), smallBuf[0:12], r.Data, smallBuf[12:16])
		}
	}

	// Copy to single buffer and do the write in one call.
	buf := make([]byte, len(r.Data)+16)
	binary.LittleEndian.PutUint64(buf[0:8], r.ID)
	binary.LittleEndian.PutUint32(buf[8:12], uint32(len(r.Data)))
	copy(buf[12:], r.Data)
	csum := crc32.Update(0, crc32Table, buf[0:12+len(r.Data)])
	binary.LittleEndian.PutUint32(buf[12+len(r.Data):], csum)
	n, err := w.Write(buf)
	if n != len(buf) {
		return io.ErrShortWrite
	}
	return err
}

// deserializeRecord is the opposite of serialize. It validates the checksums
// of the data as is reads it, so if the function returns successfully the
// data should be correct.
// Returns io.EOF if there was no data to read, io.ErrUnexpectedEOF
// if the reader ran out of data in the middle of a record, or an
// appropriate error, if any.
func deserializeRecord(reader io.Reader) (Record, error) {
	var smallBuf [12]byte

	if _, err := io.ReadFull(reader, smallBuf[:]); err != nil {
		if err != io.EOF {
			// Don't be noisy for expected cases.
			log.Errorf("Read ID+len failed: %v", err)
		}
		return Record{}, err
	}

	recLen := binary.LittleEndian.Uint32(smallBuf[8:12])

	// We haven't verified the checksum yet, so recLen might be gibberish. Be careful.
	if recLen > MaxRecordDataLen {
		log.Errorf("Too big record data: len=%d", recLen)
		return Record{}, ErrCorruptData
	}

	bigBuf := make([]byte, recLen+4)
	if _, err := io.ReadFull(reader, bigBuf); err == io.EOF {
		log.Errorf("Read data+csum eof: %v", err)
		return Record{}, io.ErrUnexpectedEOF
	} else if err != nil {
		log.Errorf("Read data+csum failed: %v", err)
		return Record{}, err
	}

	csum := crc32.Update(0, crc32Table, smallBuf[0:12])
	csum = crc32.Update(csum, crc32Table, bigBuf[:recLen])

	expectedCsum := binary.LittleEndian.Uint32(bigBuf[recLen:])
	if csum != expectedCsum {
		log.Errorf("Checksum mismatch: %d != %d", csum, expectedCsum)
		return Record{}, ErrCorruptData
	}

	return Record{
		ID:   binary.LittleEndian.Uint64(smallBuf[0:8]),
		Data: bigBuf[:recLen],
	}, nil
}
