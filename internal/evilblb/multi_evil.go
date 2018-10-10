// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package evilblb

import (
	"encoding/json"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/westerndigitalcorporation/blb/internal/evilblb/failimpl"
	"github.com/westerndigitalcorporation/blb/internal/evilblb/topology"
)

func init() {
	registerEvil(MultiEvil{})
}

// MultiEvil is the evil type that contains a combination of other evil
// types (can also include MultiEvil itself). When a MultiEvil type is
// applied all evils included in this object will be applied at the same
// time.
type MultiEvil struct {
	Evils []EvilTask
}

// Duration implements EvilTask. It returns the maximum duration of
// evils included in this MultiEvil object.
func (m MultiEvil) Duration() time.Duration {
	var max time.Duration
	for _, evil := range m.Evils {
		if evil.Duration() > max {
			max = evil.Duration()
		}
	}
	return max
}

// Validate implements EvilTask.
func (m MultiEvil) Validate() error {
	for _, evil := range m.Evils {
		if err := evil.Validate(); err != nil {
			// Return the first non-nil error.
			return err
		}
	}
	return nil
}

// Apply implements EvilTask.
func (m MultiEvil) Apply(topo *topology.Topology, failer *failimpl.Failer) func() error {
	var wg sync.WaitGroup

	// Stores revert actions for all evils to be applied.
	reverters := make([]func() error, len(m.Evils))

	log.Infof("Applying all(total: %d) the evils included in the MultiEvil.",
		len(m.Evils))

	for i := range m.Evils {
		wg.Add(1)
		// Apply the evil asynchronously.
		go func(idx int) {
			reverters[idx] = m.Evils[idx].Apply(topo, failer)
			wg.Done()
		}(i)
	}

	// Wait all evils to be applied.
	wg.Wait()

	// The revert action of a MultiEvil is all the revert actions of evils
	// included in it.
	return func() error {
		log.Infof("Running revert actions of the MultiEvil...")

		errs := make([]error, len(reverters))
		var wg sync.WaitGroup

		// Running all revert actions in parallel.
		for i, reverter := range reverters {
			if reverter != nil {
				wg.Add(1)
				go func(idx int) {
					errs[idx] = reverters[idx]()
					wg.Done()
				}(i)
			}
		}

		// Wait all revert actions to finish.
		wg.Wait()

		for _, err := range errs {
			if err != nil {
				// Return first non-nil error.
				return err
			}
		}
		return nil
	}
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (m *MultiEvil) UnmarshalJSON(data []byte) error {
	var cfgMap map[string]json.RawMessage
	if err := json.Unmarshal(data, &cfgMap); err != nil {
		return err
	}

	var evilJSONs []json.RawMessage
	if err := json.Unmarshal(cfgMap["Evils"], &evilJSONs); err != nil {
		return err
	}
	for _, evilJSON := range evilJSONs {
		e, err := unmarshalJSON2Evil(evilJSON)
		if err != nil {
			return err
		}
		m.Evils = append(m.Evils, e)
	}
	return nil
}
