// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package loadblb

import (
	"encoding/json"
	"reflect"
	"testing"

	client "github.com/westerndigitalcorporation/blb/client/blb"
)

// unmarshal decodes the JSON encoded bytes into a VariateConfig object.
func unmarshal(b []byte, t *testing.T) (c VariateConfig) {
	if err := json.Unmarshal(b, &c); err != nil {
		t.Fatalf("failed to unmarshal JSON object: %s", err)
	}
	return
}

// TestParseVaraites tests parsing variates from their JSON encode bytes.
func TestParseVariates(t *testing.T) {
	tests := [][]byte{
		[]byte(`{
		"Name": "Constant",
		"Parameters": 888
		}`),

		[]byte(`{
		"Name": "Uniform",
		"Seed": 123,
		"Parameters": {"Lower": 10, "Upper": 100}
		}`),

		[]byte(`{
		"Name": "Exponential",
		"Seed": 452,
		"Parameters": 23.5
		}`),

		[]byte(`{
		"Name": "Normal",
		"Seed": 3812,
		"Parameters": {"Mean": 10, "Stddev": 100.72834}
		}`),

		[]byte(`{
		"Name": "Poisson",
		"Seed": 12342,
		"Parameters": 89e+3
		}`),

		[]byte(`{
		"Name": "Pareto",
		"Seed": 7824,
		"Parameters": {"Xm": 10, "Alpha": 100.72834}
		}`),
	}

	for _, test := range tests {
		cfg := unmarshal(test, t)
		cfg.Parse().Sample()
	}
}

// TestParseConfigFromJSON tests parsing a graph config object from its JSON
// encoded bytes.
func TestParseConfigFromJSON(t *testing.T) {
	ticks := int64(200)
	rateV := VariateConfig{Name: "Constant", Parameters: []byte("2000")}
	sizeV := VariateConfig{Name: "Constant", Parameters: []byte("8388608")}
	replV := VariateConfig{Name: "Constant", Parameters: []byte("3")}

	// Create a single writer,
	writer := WriteGenConfig{
		generatorConfig: generatorConfig{
			Name:  "writer",
			Start: 0,
			End:   ticks / 2,
			Rate:  rateV,
			Size:  sizeV,
		},
		Readers:    []string{"reader"},
		ReplFactor: replV,
	}
	// ... a single reader,
	reader := ReadGenConfig{
		generatorConfig: generatorConfig{
			Name:  "reader",
			Start: ticks / 2,
			End:   ticks,
			Rate:  rateV,
			Size:  sizeV,
		},
		RecentN: 0,
	}
	// ... and a handler.
	handler := HandlerConfig{
		Options: client.Options{
			Cluster: "m0,m1,m2",
		},
		RetryTimeout: "3m",
		NumWorkers:   20,
		QueueLength:  20000,
	}

	// Make the graph.
	exp := GraphConfig{
		Writers:      []WriteGenConfig{writer},
		Readers:      []ReadGenConfig{reader},
		Handler:      handler,
		TickInterval: "1s",
	}
	exp.parseDuration()

	// Compose the JSON encoded bytes.
	b := []byte(`{
	"Writers": [{
		"Name": "writer",
		"Start": 0,
		"End": 100,
		"Rate": {
			"Name": "Constant",
			"Parameters": 2000
		},
		"Size": {
			"Name": "Constant",
			"Parameters": 8388608
		},
		"Readers": ["reader"],
		"ReplFactor": {
			"Name": "Constant",
			"Parameters": 3
		}
	}],

	"Readers": [{
		"Name": "reader",
		"Start": 100,
		"End": 200,
		"Rate": {
			"Name": "Constant",
			"Parameters": 2000
		},
		"Size": {
			"Name": "Constant",
			"Parameters": 8388608
		},
		"RecentN": 0
	}],

	"Handler":{
		"Cluster": "m0,m1,m2",
		"RetryTimeout": "3m",
		"NumWorkers": 20,
		"QueueLength": 20000
	},

	"TickInterval": "1s"
	}`)

	// Parse the bytes.
	var got GraphConfig
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("failed to parse JSON encoded bytes: %s", err)
	}
	got.parseDuration()

	// Compare the two.
	if !reflect.DeepEqual(exp, got) {
		t.Errorf("decoded object is not equal to the expected")
	}
}
