// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package failures

import (
	"encoding/json"
	"fmt"
	"sync"
)

type configuration struct {
	// Configurations of all registered handlers, initially "nil", means no failure gets injected.
	configs  map[string]*json.RawMessage
	handlers map[string]func(json.RawMessage) error // Key->Handler mapping.
	lock     sync.Mutex                             // Protect fields above.
}

// Register a failure handler under the given key of failure configuration.
func (c *configuration) register(key string, handler func(json.RawMessage) error) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if _, ok := c.handlers[key]; ok {
		return fmt.Errorf("Key %q is already registered.", key)
	}
	c.handlers[key] = handler
	// Configuration has "nil" value initially. The configuration value of failure
	// handler is opaque to failure service, it will be interpreted by implementers
	// of failure handlers, but a `nil` value means the falure configuration is empty
	// and there should be no failure gets injected.
	c.configs[key] = nil
	return nil
}

// Serialize configuration object to JSON format.
func (c *configuration) MarshalJSON() ([]byte, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	return json.Marshal(c.configs)
}

// Apply the updates posted from users to current configuration.
func (c *configuration) applyUpdates(updates map[string]*json.RawMessage) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// First check if the update is valid or not.
	if err := c.checkUpdate(updates); err != nil {
		return err
	}

	for key, curValue := range c.configs {
		// Get the value of the key in "updates", if the key is not specified in
		// "updates", nil will be returned, which is desired behaviour.
		updateValue := updates[key]

		if updateValue != nil {
			// The value of the key is specified in "updates", we gonna invoke
			// the handler and update current configuration.
			if err := c.handlers[key](*updateValue); err != nil {
				return err
			}
		}

		if updateValue == nil && curValue != nil {
			// The value of the key is missing in updates but exists in current
			// configuration, we gonna invoke the handler with "nil" and update current
			// configuration.
			if err := c.handlers[key](nil); err != nil {
				return err
			}
		}

		c.configs[key] = updateValue
	}

	// Done
	return nil
}

// Check if the updates posted from users are valid or not.
func (c *configuration) checkUpdate(updates map[string]*json.RawMessage) error {
	// All keys included in "updates" must be registered.
	for key := range updates {
		if _, ok := c.configs[key]; !ok {
			return fmt.Errorf("Key %q is not registered", key)
		}
	}
	return nil
}
