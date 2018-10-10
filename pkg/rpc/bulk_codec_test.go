// Copyright (c) 2017 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package rpc

import (
	"bytes"
	"math/rand"
	"net/rpc"
	"testing"
)

type testMsg struct {
	Field int
	Data  []byte
}

func (t *testMsg) Get() ([]byte, bool)  { b := t.Data; t.Data = nil; return b, true }
func (t *testMsg) Set(b []byte, e bool) { t.Data = b }

type closeBuffer struct {
	bytes.Buffer
}

func (cb *closeBuffer) Close() error { return nil }

func TestBulkCodecRequest(t *testing.T) {
	buf := &closeBuffer{bytes.Buffer{}}

	bulk := make([]byte, 8<<20)
	rand.Read(bulk)
	inBody := &testMsg{Field: 777, Data: bulk}
	var _ BulkData = inBody // assert that it implements BulkData

	inReq := &rpc.Request{ServiceMethod: "method", Seq: 12345}
	cc := newBulkGobCodec(buf)
	cc.WriteRequest(inReq, inBody)

	sc := newBulkGobCodec(buf)
	var outReq rpc.Request
	if err := sc.ReadRequestHeader(&outReq); err != nil {
		t.Fatal(err)
	}
	if outReq.ServiceMethod != inReq.ServiceMethod || outReq.Seq != inReq.Seq {
		t.Fatal("mismatch")
	}

	var outBody testMsg
	if err := sc.ReadRequestBody(&outBody); err != nil {
		t.Fatal(err)
	}
	if outBody.Field != inBody.Field || !bytes.Equal(outBody.Data, bulk) {
		t.Fatal("mismatch")
	}
}

func TestBulkCodecResponse(t *testing.T) {
	buf := &closeBuffer{bytes.Buffer{}}

	bulk := make([]byte, 8<<20)
	rand.Read(bulk)
	inBody := &testMsg{Field: 777, Data: bulk}

	inResp := &rpc.Response{ServiceMethod: "method", Seq: 12345, Error: "none"}
	sc := newBulkGobCodec(buf)
	sc.WriteResponse(inResp, inBody)

	cc := newBulkGobCodec(buf)
	var outResp rpc.Response
	if err := cc.ReadResponseHeader(&outResp); err != nil {
		t.Fatal(err)
	}
	if outResp.ServiceMethod != inResp.ServiceMethod || outResp.Seq != inResp.Seq || outResp.Error != inResp.Error {
		t.Fatal("mismatch")
	}

	var outBody testMsg
	if err := cc.ReadResponseBody(&outBody); err != nil {
		t.Fatal(err)
	}
	if outBody.Field != inBody.Field || !bytes.Equal(outBody.Data, bulk) {
		t.Fatal("mismatch")
	}
}
