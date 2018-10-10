// Copyright (c) 2018 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package dyconfig

type registerKey struct {
}

// RegisterKey is the used to unregistered the callback for a config.
type RegisterKey *registerKey

// Register will register your config as dynamically changeable and will call the registered callback
// when it's changed.
//
// Provide a unique key for each registered config. You can register the same config struct with same key
// for multiple times, but all following registering will only add callback function to a internal data
// struct. If you don't want to share the config with other service, set the second parameter to be true.
// It adds the service name(ops, postproc) to be part of the key in config storage.
//
// The third parameter has to be a concrete struct, not a pointer. And the fourth parameter(the callback)
// has to be a function that takes one parameter that shares the same type as the third parameter and returns
// no value.
//
// The function will block on getting the config from storage when it's the first time an unique config is
// registered. It gets the bytes from config storage, serializes them to a struct and calls the callback
// to notify the caller of this function, then it keeps an in-memory instance of that config and spins off
// a goroutine to watch the change of the config(associated with the given key) in storage. If an in-memory
// instance already exists for the config, then the instance will be passed to the callback immediately.
func Register(key string, serviceInKey bool, defaults interface{}, callback interface{}) (RegisterKey, error) {
	return new(registerKey), nil
}
