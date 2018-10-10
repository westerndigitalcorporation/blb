// Copyright (c) 2017 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package state

import (
	"bytes"
	"testing"

	"github.com/westerndigitalcorporation/blb/internal/core"
)

func TestRSChunk(t *testing.T) {
	id := core.RSChunkID{Partition: 0xabcdef01, ID: 0x654321654321}
	if key2rschunkID(rschunkID2Key(id)) != id {
		t.Errorf("round-trip RSChunkID failed")
	}
}

func TestRSChunkAsTractID(t *testing.T) {
	id := core.RSChunkID{Partition: 0xabcdef01, ID: 0x654321654321}
	asRSC := rschunkID2Key(id)
	asTID := tractID2Key(id.ToTractID())
	if !bytes.Equal(asRSC, asTID) {
		t.Errorf("RSChunkID should encode the same both ways")
	}
}
