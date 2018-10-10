// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package failures

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"reflect"
	"sync"
	"testing"
	"time"

	log "github.com/golang/glog"
)

var once sync.Once

var (
	testAddr = "localhost:7991"
	testURL  = "http://" + testAddr + DefaultFailureServicePath
)

var (
	// Channels which keep track of call histories.
	dropCalls  = make(chan json.RawMessage, 100)
	delayCalls = make(chan json.RawMessage, 100)
	hddCalls   = make(chan json.RawMessage, 100)
)

func dropHandler(msg json.RawMessage) error {
	dropCalls <- msg
	return nil
}

func delayHandler(msg json.RawMessage) error {
	delayCalls <- msg
	return nil
}

func hddHandler(msg json.RawMessage) error {
	hddCalls <- msg
	return nil
}

func setup() {
	Register("drop_prob", dropHandler)
	Register("delay_prob", delayHandler)
	Register("hdd_limit", hddHandler)
	Init()
	go http.ListenAndServe(testAddr, nil)

	// Since the failure service start listening on the port asynchronously,
	// We have to wait until it starts accepting requests.
	maxTries := 5
	for i := 0; i < maxTries; i++ {
		_, err := http.Get(testURL)
		if err != nil {
			time.Sleep(5000 * time.Millisecond)
		} else {
			return
		}
	}
	log.Fatalf("Failed to connect to failure service after %d tries, this might happen if the service has not started yet", maxTries)
}

// Set current failure configuration using a json string. HTTP status code of
// POST request will be returned.
func postJSON(json string, t *testing.T) (statusCode int) {
	resp, err := http.Post(testURL, "application/json", bytes.NewBuffer([]byte(json)))
	if err != nil {
		t.Fatalf("Failed to issue POST request: %v", err)
	}
	return resp.StatusCode
}

// Return current failure service configuration as a json string.
func getJSON(t *testing.T) string {
	resp, err := http.Get(testURL)
	if err != nil {
		t.Errorf("Failed to issue Get request: %v", err)
	}
	jsonData, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Errorf("Failed to read HTTP data: %v", err)
	}
	return string(jsonData)
}

// Assert two json string are equivalent.
func assertSame(json1, json2 string, t *testing.T) {
	var m1 interface{}
	var m2 interface{}
	if err := json.Unmarshal([]byte(json1), &m1); err != nil {
		t.Fatalf("Failed to decode json data: %v", err)
	}
	if err := json.Unmarshal([]byte(json2), &m2); err != nil {
		t.Fatalf("Failed to decode json data: %v", err)
	}
	if !reflect.DeepEqual(m1, m2) {
		t.Fatalf("Inconsistent json data: %q %q", json1, json2)
	}
}

// Verify if a handler is supposed be called or not, and if it's supposed to
// be called, verify if it's called with expected value.
func assertCall(callCh chan json.RawMessage, msg json.RawMessage, isCalled bool, t *testing.T) {
	if !isCalled {
		// The call is not supposed to be called, the channel should be empty.
		select {
		case <-callCh:
			t.Fatalf("The handler is not supposed to be called!")
		default:
			// No call detected.
			return
		}
		return
	}

	// We expect the handler was called with value v.
	calledMsg := <-callCh

	if calledMsg == nil && msg == nil {
		return
	}
	assertSame(string(msg), string(calledMsg), t)
}

// Reset entire failure service configuration.
func resetTest(t *testing.T) {
	postJSON("{}", t)
	// Clear all call history.
	for {
		select {
		case <-dropCalls:
		case <-delayCalls:
		case <-hddCalls:
		default:
			return
		}
	}
}

// Test we have correct initial configuration.
func TestInitialConfig(t *testing.T) {
	once.Do(setup)
	resetTest(t)
	assertSame(`{"drop_prob":null, "delay_prob":null, "hdd_limit":null}`, getJSON(t), t)

	// No handler should be called.
	assertCall(dropCalls, nil, false, t)
	assertCall(delayCalls, nil, false, t)
	assertCall(hddCalls, nil, false, t)
}

// Test we can't register a duplicate key.
func TestRegisterDuplicate(t *testing.T) {
	once.Do(setup)
	resetTest(t)
	if err := Register("drop_prob", func(v json.RawMessage) error { return nil }); err == nil {
		t.Fatalf("expected returning an error for registering a duplicate key")
	}
}

// Test set value of a single key.
func TestSimplePostOneKey(t *testing.T) {
	once.Do(setup)
	resetTest(t)
	postJSON(`{"drop_prob": {"1": 0.3}}`, t)
	assertSame(`{"drop_prob": {"1": 0.3}, "delay_prob":null, "hdd_limit":null}`, getJSON(t), t)

	// Drop handler should be called with json.RawMessage {"1": 0.3}.
	assertCall(dropCalls, json.RawMessage(`{"1": 0.3}`), true, t)
	// The other two should not be called.
	assertCall(delayCalls, nil, false, t)
	assertCall(hddCalls, nil, false, t)
}

// Test set values of multiple keys.
func TestSimplePostMultipKeys(t *testing.T) {
	once.Do(setup)
	resetTest(t)
	postJSON(`{"drop_prob": {"1": 0.3}, "hdd_limit": 10}`, t)
	assertSame(`{"drop_prob": {"1": 0.3}, "delay_prob":null, "hdd_limit":10}`, getJSON(t), t)

	// Drop handler should be called with json.RawMessage {"1": 0.3}.
	assertCall(dropCalls, json.RawMessage(`{"1": 0.3}`), true, t)
	// Delay handler should not be called.
	assertCall(delayCalls, nil, false, t)
	// HDD handler should be called with 10.
	assertCall(hddCalls, json.RawMessage("10"), true, t)
}

// Test posting invalid data, see if failure service can return expected errors.
func TestPostInvalidData(t *testing.T) {
	once.Do(setup)
	resetTest(t)

	// Initialize failure configuration.
	postJSON(`{"drop_prob": {"1": 0.3}, "hdd_limit": 10}`, t)
	assertSame(`{"drop_prob": {"1": 0.3}, "delay_prob":null, "hdd_limit":10}`, getJSON(t), t)

	// Drop handler should be called with json.RawMessage {"1": 0.3}.
	assertCall(dropCalls, json.RawMessage(`{"1": 0.3}`), true, t)
	// Delay handler should not be called.
	assertCall(delayCalls, nil, false, t)
	// HDD handler should be called with 10.
	assertCall(hddCalls, json.RawMessage("10"), true, t)

	// Test posting invalid json data.
	if status := postJSON("not valid json data", t); status != http.StatusBadRequest {
		t.Fatalf("expected returning BadRequest for invalid json data")
	}

	// Test posting valid json data, but invalid(not registered) key
	if status := postJSON(`{"unknown_key": 1, "hdd_limit": 1}`, t); status != http.StatusBadRequest {
		t.Fatalf("expected returning BadRequest for unregistered key")
	}

	// And configuration shouldn't be affected.
	assertSame(`{"drop_prob": {"1": 0.3}, "delay_prob":null, "hdd_limit":10}`, getJSON(t), t)

	// No handler should be called.
	assertCall(dropCalls, nil, false, t)
	assertCall(delayCalls, nil, false, t)
	assertCall(hddCalls, nil, false, t)
}

// Test overwriting and resetting configuration.
func TestOverwriteAndResetConfig(t *testing.T) {
	once.Do(setup)
	resetTest(t)

	initialConfig := `{
		"drop_prob": {"1": 0.3, "2": 0.3},
		"hdd_limit": 10,
		"delay_prob": {"2": 0.9}
	}`

	postJSON(initialConfig, t)
	// Verify we have set initial configuration successfully.
	assertSame(initialConfig, getJSON(t), t)

	// Drop handler should be called with json.RawMessage {"1": 0.3, "2":0.3}.
	assertCall(dropCalls, json.RawMessage(`{"1": 0.3, "2": 0.3}`), true, t)
	// Drop handler should be called with map {"2": 0.9}.
	assertCall(delayCalls, json.RawMessage(`{"2": 0.9}`), true, t)
	// HDD handler should be called with 10.
	assertCall(hddCalls, json.RawMessage("10"), true, t)
	// Now we post a new config with only "hdd_limit" set, "hdd_limit" should be
	// updated and other keys("drop_prob" and "delay_prob") should be reset.
	update := `{"hdd_limit": 100}`
	postJSON(update, t)

	expectedConfig := `{
			"drop_prob": null,
			"hdd_limit": 100,
			"delay_prob": null
		}`
	// See if we get expected config after update.
	assertSame(expectedConfig, getJSON(t), t)

	// Drop handler should be called with "nil".
	assertCall(dropCalls, nil, true, t)
	// Delay handler should be called with "nil".
	assertCall(delayCalls, nil, true, t)
	// HDD handler should be called with 100.
	assertCall(hddCalls, json.RawMessage("100"), true, t)

	// Explicitly set "hdd_limit" to nil.
	update = `{"hdd_limit": null}`
	postJSON(update, t)

	// Drop handler should not be called.
	assertCall(dropCalls, nil, false, t)
	// Delay handler should not be called.
	assertCall(delayCalls, nil, false, t)
	// HDD handler should be called with "nil".
	assertCall(hddCalls, nil, true, t)
}

// Test set same value of a registered key, the handler should still be called.
func TestSetSameValue(t *testing.T) {
	once.Do(setup)
	resetTest(t)

	initialConfig := `{
		"drop_prob": {"1": 0.3, "2": 0.3},
		"hdd_limit": 10,
		"delay_prob": {"2": 0.9}
	}`

	postJSON(initialConfig, t)
	// Verify we have set initial configuration successfully.
	assertSame(initialConfig, getJSON(t), t)

	// Drop handler should be called with json.RawMessage {"1": 0.3, "2":0.3}.
	assertCall(dropCalls, json.RawMessage(`{"1": 0.3, "2": 0.3}`), true, t)
	// Drop handler should be called with map {"2": 0.9}.
	assertCall(delayCalls, json.RawMessage(`{"2": 0.9}`), true, t)
	// HDD handler should be called with 10.
	assertCall(hddCalls, json.RawMessage("10"), true, t)

	// Now we gonna post a new configuration with same "hdd_limit".
	update := `{"hdd_limit": 10}`
	postJSON(update, t)

	// Drop handler should be called with "nil".
	assertCall(dropCalls, nil, true, t)
	// Delay handler should be called with "nil".
	assertCall(delayCalls, nil, true, t)
	// HDD handler should be called with same value -- 10.
	assertCall(hddCalls, json.RawMessage("10"), true, t)
}
