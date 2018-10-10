// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package server

import (
	"encoding/json"
	"sync"

	log "github.com/golang/glog"

	"github.com/westerndigitalcorporation/blb/internal/core"
)

// OpFailure is used by failure service and maps operation names to their
// registered failure errors.
type OpFailure struct {
	// When failure service is enabled, what errors failed operations should
	// return and the lock for them.
	lock     sync.Mutex
	failures map[string]core.Error
}

// NewOpFailure creates a new OpFailure.
func NewOpFailure() *OpFailure {
	return &OpFailure{failures: make(map[string]core.Error)}
}

// Get returns the registered error for the given operation 'op'. NoError is
// returned if no error is registered for 'op'.
func (f *OpFailure) Get(op string) core.Error {
	f.lock.Lock()
	defer f.lock.Unlock()
	return f.failures[op]
}

// Handler is the method to be registered with the failure service that handles
// the update of failure configurations.
func (f *OpFailure) Handler(config json.RawMessage) error {
	f.lock.Lock()
	defer f.lock.Unlock()

	log.Infof("received new failure config: %s", string(config))
	if config == nil {
		f.failures = make(map[string]core.Error)
		return nil
	}

	var failures map[string]core.Error
	if err := json.Unmarshal(config, &failures); nil != err {
		log.Errorf("failed to unmarshal config: %s", err)
		return err
	}
	f.failures = failures
	return nil
}
