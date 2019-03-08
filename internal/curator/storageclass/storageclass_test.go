// Copyright (c) 2019 David Reiss. All rights reserved.
// SPDX-License-Identifier: MIT

package storageclass

import (
	"testing"

	"github.com/westerndigitalcorporation/blb/internal/core"
	"github.com/westerndigitalcorporation/blb/internal/curator/durable/state/fb"
)

var testBlob = fb.GetRootAsBlobF(fb.BuildBlob(&fb.Blob{
	Tracts: []*fb.Tract{
		&fb.Tract{
			Version: 3,
			Hosts:   []core.TractserverID{5, 6, 7},
		},
		&fb.Tract{
			Version:   3,
			Rs83Chunk: &core.TractID{8, 3},
		},
		&fb.Tract{
			Version:    3,
			Rs125Chunk: &core.TractID{12, 5},
		},
	},
}), 0)

func TestRepl(t *testing.T) {
	repl := Get(core.StorageClassREPLICATED)
	if repl == nil {
		t.Fatalf("no storage class for REPLICATED")
	}

	if repl.ID() != core.StorageClassREPLICATED {
		t.Error("wrong ID")
	}

	var tract fb.TractF
	if testBlob.Tracts(&tract, 0); repl.Has(&tract) != true {
		t.Errorf("this should be a repl tract")
	}
	if testBlob.Tracts(&tract, 1); repl.Has(&tract) != false {
		t.Errorf("this should not be a repl tract")
	}
	if testBlob.Tracts(&tract, 2); repl.Has(&tract) != false {
		t.Errorf("this should not be a repl tract")
	}
}

func TestRS83(t *testing.T) {
	rs83 := Get(core.StorageClassRS_8_3)
	if rs83 == nil {
		t.Fatalf("no storage class for RS_8_3")
	}

	if rs83.ID() != core.StorageClassRS_8_3 {
		t.Error("wrong ID")
	}

	n, m := rs83.RSParams()
	if n != 8 || m != 3 {
		t.Errorf("RSParams wrong")
	}

	var tract fb.TractF
	if testBlob.Tracts(&tract, 0); rs83.Has(&tract) != false {
		t.Errorf("this should not be a rs83 tract")
	}
	if testBlob.Tracts(&tract, 1); rs83.Has(&tract) != true {
		t.Errorf("this should be a rs83 tract")
	}
	if testBlob.Tracts(&tract, 2); rs83.Has(&tract) != false {
		t.Errorf("this should not be a rs83 tract")
	}

	testBlob.Tracts(&tract, 1)
	cid := rs83.GetRS(&tract)
	expected := core.TractID{8, 3}
	if cid.ToTractID() != expected {
		t.Errorf("GetRS wrong")
	}

	var ts fb.Tract
	id := core.RSChunkID{7, 7}
	rs83.Set(&ts, id)
	if *ts.Rs83Chunk != id.ToTractID() {
		t.Errorf("Set didn't work")
	}

	rs83.Clear(&ts)
	if ts.Rs83Chunk != nil {
		t.Errorf("Clear didn't work")
	}
}
