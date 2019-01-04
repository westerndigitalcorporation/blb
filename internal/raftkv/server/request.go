// Copyright (c) 2016 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package server

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
)

// request represents a key-value request from clients.
type request struct {
	method string // DELETE/PUT/GET
	key    []byte

	// --- GET ----
	local bool // Is this a local read?
	list  bool // Is this a list?

	// --- PUT ---
	value    []byte
	cas      bool // Is this a compare and swap put?
	oldValue []byte
}

// Convert a HTTP request to a KV request.
func buildRequest(req *http.Request) (*request, error) {
	key := []byte(req.URL.Path[1:])

	query, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, err
	}

	kvReq := new(request)
	kvReq.method = req.Method
	kvReq.key = key

	switch kvReq.method {
	case "GET":
		if _, ok := query["local"]; ok {
			kvReq.local = true
		}
		kvReq.list = len(key) == 0

	case "PUT":
		if len(key) == 0 {
			return nil, fmt.Errorf("key can not be empty")
		}
		data, err := ioutil.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		kvReq.value = data
		if v, ok := query["cas"]; ok {
			kvReq.oldValue = []byte(v[0])
			kvReq.cas = true
		}

	case "DELETE":
		if len(key) == 0 {
			return nil, fmt.Errorf("key can not be empty")
		}

	default:
		return nil, fmt.Errorf("invalid request type")
	}

	return kvReq, nil
}
