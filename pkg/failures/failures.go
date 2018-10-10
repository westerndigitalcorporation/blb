// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

// Package failures implements failure service. Failure service maintains a global
// failure configuration object and provides a RESTful API for clients to access
// and modify the configuration.
//
// You can think the failure configuration object is a map, service can add a
// key to the map by registering a failure handler under that key. The value of
// that key has type "json.RawMessage" and has initial value "nil". It's up to
// the implementers of failure handlers to interpret it.
//
// Failure handler is a function which will be called when the value of its
// registered key gets set or reset, the handler must have type:
//
//		func(value json.RawMessage) error
//
// Clients can read current failure configuration by issuing a HTTP GET request
// to failure service and it will return all configurations in JSON format with each
// top-level key represents a registered failure handler and the value represents
// current configuration of that handler.
//
// Clients can modify failure configuration by issuing a POST request to failure
// service with the new configuration in JSON format. Each POST request will
// overwrite the entire configuration, but clients don't need to specify
// configuration of all registered failure handlers, failure service will
// treat the missing configurations in POST request to have the value "null(nil)".
//
// Below is an example of using failure service to simulate dropping messages. It's
// used by Raft implementation.
//
// (1) First you have to implement a handler that can be called by failure
//	   service. The handler will be associated with a key of failure service
//	   so that when the value of that key gets updated the failure service will
//	   call the handler with the updated value. It's up to implementors of the
//	   handler to define what the format of the value('config') will be. It
//	   might be a float, string or another json string, etc. The handler will
//	   deserialize the value to its expected type. Below is an example:
//
//			func (d *MsgDropper) dropHandler(config json.RawMessage) error {
//				d.lock.Lock()
//				defer d.lock.Unlock()
//
//				log.Infof("Receive new config: %s", string(config))
//
//				if config == nil {
//					// Clear failures.
//					d.msgDropProb = make(map[string]float32)
//					return nil
//				}
//
//				var dropMap map[string]float32
//				if err := json.Unmarshal(config, &dropMap); err != nil {
//					// So if the value is not an expected type, just return an error to clients.
//					return err
//				}
//
//				// Reset failures.
//				d.msgDropProb = dropMap
//				return nil
//			}
//
//
// (2) You have to associate the handler with a given key of the failure
//	   service. So when the value of that key gets updated the handler will be
//	   called with the updated value.
//
//			failures.Register("msg_drop_prob", msgDropper.dropHandler)
//
// (3) You can check the failure configuration by issuing a GET request to the
//	   failure service:
//
//			curl http://<host>:<failure_port>/<failure_service_path>
//
// (4) You can update failure configuration by issuing a POST request to the
//	   failure service. Here is an example of updating the configuration of
//	   dropping messages.
//
//			curl http://<host>:<failure_port>/<failure_service_path> -XPOST -d \
//			'{"msg_drop_prob": {"host1:3110": 1.0, "host2:3110": 0.0}}'
//
// So the handler "dropHandler" will be called with the new value
// '{"msg_drop_prob": {"host1:3110": 1.0, "host2":3110: 0.0}}'. It's up to
// "dropHandler" to deserialize the value to its expected type(a map in this
// case).
//
// If we want to reset all failures we can just issue an empty JSON object "{}"
// so all configurations of registered handlers will be reset to nil.
//
// Here is an example of using the command line tool 'curl' to modify failure parameters:
// curl localhost:port -XPOST -d '{"field":value}'
package failures

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

// DefaultFailureServicePath is the path that the failure service handler will
// be mounted on, by default.
const DefaultFailureServicePath = "/__failure__"

var (
	config = configuration{
		configs:  make(map[string]*json.RawMessage),
		handlers: make(map[string]func(json.RawMessage) error),
	}
)

// Init mounts the failure service on the default path on the default http mux.
func Init() {
	InitWithPathAndMux(http.DefaultServeMux, DefaultFailureServicePath)
}

// InitWithPathAndMux mounts the failure service on the given path and mux.
func InitWithPathAndMux(mux *http.ServeMux, path string) {
	mux.HandleFunc(path, failureHTTPHandler)
}

// Register registers a failure handler to a given key of failure configuration.
// You can not register a failure handler to a key which has already been
// registered.
func Register(key string, handler func(json.RawMessage) error) error {
	return config.register(key, handler)
}

func failureHTTPHandler(writer http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "GET":
		doGet(writer, req)
	case "POST":
		doPost(writer, req)
	default:
		replyError(writer, fmt.Sprintf("Unsupported method %s", req.Method), http.StatusMethodNotAllowed)
	}
}

func doGet(writer http.ResponseWriter, req *http.Request) {
	enc := json.NewEncoder(writer)
	enc.Encode(&config)
}

func doPost(writer http.ResponseWriter, req *http.Request) {
	// Read json data posted from clients.
	jsonData, err := ioutil.ReadAll(req.Body)
	if err != nil {
		replyError(writer, err.Error(), http.StatusBadRequest)
		return
	}

	// Decode the json data into a map object.
	var updates map[string]*json.RawMessage
	dec := json.NewDecoder(bytes.NewBuffer(jsonData))
	if err = dec.Decode(&updates); err != nil {
		replyError(writer, err.Error(), http.StatusBadRequest)
		return
	}

	// Apply the new state to the failure configration.
	err = config.applyUpdates(updates)
	if err != nil {
		replyError(writer, err.Error(), http.StatusBadRequest)
		return
	}
}

func replyError(w http.ResponseWriter, errorStr string, code int) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(code)
	fmt.Fprintln(w, errorStr)
}
