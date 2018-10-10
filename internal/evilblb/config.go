// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package evilblb

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/westerndigitalcorporation/blb/internal/evilblb/failimpl"
	"github.com/westerndigitalcorporation/blb/internal/evilblb/topology"
)

// Store the mappings from the name of an evil to the type of the evil.
// It's used to decode an evil object from JSON encoded data.
var evilTypes = make(map[string]reflect.Type)

// EvilTask is the interface that an Evil must implement.
//
// To add an evil:
//
// 1. Add a new type in evil_newevilname.go.  It must implement EvilTask.
//
// 2. Register the task in an init() function, eg:
// func init() {
//         registerEvil(NewEvilName{})
// }
//
// 3. Add the evil in a config file.  The config file will fail to parse if you messed up.
type EvilTask interface {
	// Validate should return nil if the task was configured correctly
	// and a non-nil error otherwise.
	Validate() error

	// Duration specifies how long to wait between calling Apply and
	// calling the revert function returned from Apply.
	Duration() time.Duration

	// Apply applies the evil to the cluster described by topology.
	// The provided Failer is used to cause the actual errors in the cluster.
	// Returns a function that, if not nil, should be called after Duration() to revert the evil.
	Apply(*topology.Topology, *failimpl.Failer) func() error
}

// Register an evil type.
func registerEvil(e EvilTask) {
	typ := reflect.TypeOf(e)
	evilTypes[typ.Name()] = typ
}

// Duration is a wrapper on time.Duration that can be decoded by a JSON decoder.
type Duration struct {
	time.Duration
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (d *Duration) UnmarshalJSON(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("nil data for a duration")
	}

	var err error
	if data[0] == '"' {
		sd := string(data[1 : len(data)-1])
		d.Duration, err = time.ParseDuration(sd)
		return err
	}

	var id int64
	id, err = json.Number(string(data)).Int64()
	d.Duration = time.Duration(id)
	return err
}

// Params stores parameters of a configuration.
type Params struct {
	// 'User' is the username used to SSH to remote machines to
	// inject failures. It must have the sudo privilege.
	User string

	// Masters are the masters addresses of a Blb cluster.
	Masters []string

	// MinGapBetweenEvils defines the minimum gap between evils.
	MinGapBetweenEvils Duration

	// MaxGapBetweenEvils defines the maximum gap between evils.
	MaxGapBetweenEvils Duration

	// TotalLength defines the total duration of  a EvilBlb instance.
	TotalLength Duration
}

// Config contains the configuration of a EvilBlb instance.
type Config struct {
	// Params stores parameters of the configuration.
	Params Params

	// Evils defines the evils that will be injected. EvilBlb wil pick a
	// random evil among 'Evils' at a time and inject to the Blb cluster.
	Evils []EvilTask
}

// Validate validates if the configuration is valid.
func (c *Config) Validate() error {
	if c.Params.User == "" {
		return fmt.Errorf("'User' can not be nil")
	}
	if len(c.Params.Masters) == 0 {
		return fmt.Errorf("Must provide master addresses")
	}
	if c.Params.MaxGapBetweenEvils.Duration < c.Params.MinGapBetweenEvils.Duration {
		return fmt.Errorf("MaxGapBetweenEvils must >= MinGapBetweenEvils")
	}
	if len(c.Evils) == 0 {
		return fmt.Errorf("Must have at least one evil type provided")
	}

	// Validate provided evils.
	for _, evil := range c.Evils {
		if e := evil.Validate(); e != nil {
			return e
		}
	}
	return nil
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (c *Config) UnmarshalJSON(data []byte) error {
	var cfgMap map[string]json.RawMessage
	if err := json.Unmarshal(data, &cfgMap); err != nil {
		return err
	}

	if err := json.Unmarshal(cfgMap["Params"], &c.Params); err != nil {
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
		c.Evils = append(c.Evils, e)
	}
	return nil
}

func unmarshalJSON2Evil(data []byte) (task EvilTask, err error) {
	var evilMap map[string]json.RawMessage
	if err = json.Unmarshal(data, &evilMap); err != nil {
		return
	}
	// Find out which type of evil is by looking up the name.
	var name string
	if err = json.Unmarshal(evilMap["Name"], &name); err != nil {
		return
	}
	typ, ok := evilTypes[name]
	if !ok {
		err = fmt.Errorf("Evil %q is not registered", name)
		return
	}
	v := reflect.New(typ)
	e := v.Interface()
	err = json.Unmarshal(data, e)
	if err != nil {
		return
	}
	// Every evil must implement EvilTask.
	task, ok = e.(EvilTask)
	if !ok {
		err = fmt.Errorf("unmarshall'd thing doesn't impl EvilTask: task=%+v", e)
		return
	}
	return task, nil
}
