// Copyright (c) 2017 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package raftrpc

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"
	"time"

	log "github.com/golang/glog"

	"github.com/westerndigitalcorporation/blb/pkg/raft/raft"
	test "github.com/westerndigitalcorporation/blb/pkg/testutil"
)

func sendSnapshot(endpoint string, stream io.Reader) error {
	// Give it a 5 seconds time out to wait for the server to start listening
	timeout := time.Now().Add(5 * time.Second)
	req, err := http.NewRequest("POST", endpoint, stream)
	if err != nil {
		return err
	}

	var resp *http.Response
	clt := &http.Client{}
	for {
		if resp, err = clt.Do(req); err != nil {
			if time.Now().After(timeout) && strings.Contains(err.Error(), "connection refused") {
				return err
			}
			time.Sleep(time.Second)
			continue
		}
		break
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("error: %s", resp.Status)
	}
	return nil
}

// Test sending snapshot request.
func TestSendSnapshot(t *testing.T) {
	addr := fmt.Sprintf("localhost:%d", test.GetFreePort())
	transport, err := NewRPCTransport(raft.TransportConfig{Addr: addr}, DefaultRPCTransportConfig)
	if err != nil {
		t.Fatalf("Failed to create transport: %v", err)
	}

	transport.StartStandaloneRPCServer()

	go func() {
		// Keep consuming received requests.
		for {
			msg := <-transport.Receive()
			snap := msg.(*raft.InstallSnapshot)
			snap.Body.Close()
			log.Infof("Received snapshot: %+v", snap)
		}
	}()

	// Sending a valid snapshot should succeed.
	snapshotMsg := raft.InstallSnapshot{
		BaseMsg:    raft.BaseMsg{To: addr, From: "test"},
		LastIndex:  10,
		LastTerm:   1,
		Membership: raft.Membership{Index: 5, Term: 1, Members: []string{addr}},
		Body:       ioutil.NopCloser(bytes.NewReader([]byte("hello world"))),
	}
	stream, err := getSnapshotStream(snapshotMsg)
	if err != nil {
		t.Fatalf("Failed to get snapshot stream: %v", err)
	}
	if err := sendSnapshot(fmt.Sprintf("http://%s%s", addr, transport.snapshotEndpoint()), stream); err != nil {
		t.Fatalf("Failed to send snapshot request: %v", err)
	}

	// Sending a bad request(header length is too large) should fail.
	stream, err = getSnapshotStream(snapshotMsg)
	if err != nil {
		t.Fatalf("Failed to get snapshot stream: %v", err)
	}
	data, err := ioutil.ReadAll(stream)
	if err != nil {
		t.Fatalf("%v", err)
	}
	var bufLen bytes.Buffer
	// The header length is too large
	binary.Write(&bufLen, binary.LittleEndian, uint32(200*1024))
	// Stitch the invalid header length to the rest of the body.
	stream = bytes.NewReader(append(bufLen.Bytes(), data[4:]...))
	if err := sendSnapshot(fmt.Sprintf("http://%s%s", addr, transport.snapshotEndpoint()), stream); err == nil {
		t.Fatalf("Bad request should fail")
	}
}
