// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package wal

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	test "github.com/westerndigitalcorporation/blb/pkg/testutil"
)

func TestRecordSerialize(t *testing.T) {
	r := Record{
		Data: []byte("I am data, hear me roar"),
		ID:   uint64(42),
	}
	roundTripRecord(r, t)
}

func TestRecordSerializeEmptyData(t *testing.T) {
	r := Record{
		Data: []byte{},
		ID:   1 << 55,
	}
	roundTripRecord(r, t)
}

func roundTripRecord(r Record, t *testing.T) {
	var buf bytes.Buffer
	err := r.serialize(&buf)
	if err != nil {
		t.Fatalf("Failed to serialize record: %v", err)
	}

	outRec, err := deserializeRecord(&buf)
	if err != nil {
		t.Fatalf("Failed to deserialize record: %v", err)
	}

	if !recordsEqual(r, outRec) {
		t.Errorf("Record not the same: %+v vs %+v", r, outRec)
	}

	roundTripToFile(r, t)
}

func roundTripToFile(r Record, t *testing.T) {
	name := filepath.Join(test.TempDir(), "waltest")
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}

	err = r.serialize(f)
	if err != nil {
		t.Fatalf("Failed to serialize record: %v", err)
	}

	f.Seek(0, os.SEEK_SET)
	outRec, err := deserializeRecord(f)
	if err != nil {
		t.Fatalf("Failed to deserialize record: %v", err)
	}

	if !recordsEqual(r, outRec) {
		t.Errorf("Record not the same: %+v vs %+v", r, outRec)
	}
}

func recordsEqual(a, b Record) bool {
	return a.ID == b.ID && bytes.Equal(a.Data, b.Data)
}

func TestRecordDeserializeTooShort(t *testing.T) {
	minRecordLen := 16
	for i := 0; i < minRecordLen; i++ {
		d := make([]byte, i)
		buf := bytes.NewReader(d)
		_, err := deserializeRecord(buf)
		if i == 0 && err != io.EOF {
			t.Errorf("Unexpected error deserializing empty buffer")
		} else if i > 0 && err != io.ErrUnexpectedEOF {
			t.Errorf("Unexpected success deserializing too small buffer, len=%d", len(d))
		}
	}
}

func TestRecordDeserializeCorrupt(t *testing.T) {
	r := Record{
		Data: []byte("I am data, hear me roar"),
		ID:   uint64(42),
	}
	var writeBuf bytes.Buffer
	err := r.serialize(&writeBuf)
	if err != nil {
		t.Fatalf("Failed to serialize record: %v", err)
	}

	d := writeBuf.Bytes()
	d[2] = ^d[2]
	buf := bytes.NewReader(d)
	_, err = deserializeRecord(buf)
	if err == nil {
		t.Errorf("Unexpected success deserializing corrupt buffer")
	}
}

func TestRecordDataTooBig(t *testing.T) {
	r := Record{
		Data: make([]byte, MaxRecordDataLen+1),
		ID:   uint64(5),
	}

	var buf bytes.Buffer
	err := r.serialize(&buf)
	if err != ErrRecordDataTooBig {
		t.Errorf("Unexpected error from serialize, got %v", err)
	}
}
