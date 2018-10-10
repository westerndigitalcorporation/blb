// Copyright (c) 2017 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package testblb

import (
	"fmt"
	"net/http"
)

// TestReadOnly tests the read-only mode of the curator.
func (tc *TestCase) TestReadOnly() error {
	// Create a blob.
	blob, err := tc.noRetryC.Create()
	if err != nil {
		return err
	}

	// Set all curators to read-only.
	setCuratorsReadOnly(tc, true)

	// Writing should failed (can't extend).
	if _, err = blob.Write([]byte("hello")); err == nil {
		return fmt.Errorf("write should have failed")
	}

	// Set them back.
	setCuratorsReadOnly(tc, false)

	// Should succeed.
	if _, err = blob.Write([]byte("hello")); err != nil {
		return err
	}

	return nil
}

func setReadOnly(addrs []string, value bool) error {
	// addrs is addresses of several servers in a raft group, one of which is
	// the leader. We'll just make the request on all of them, and succeed if
	// we get a success.
	for _, addr := range addrs {
		url := fmt.Sprintf("http://%s/readonly?mode=", addr)
		if value {
			url += "true"
		} else {
			url += "false"
		}
		if resp, err := http.Post(url, "text/plain", nil); err == nil && resp.StatusCode == 200 {
			return nil
		}
	}
	return fmt.Errorf("all requests failed")
}

func setCuratorsReadOnly(tc *TestCase, value bool) error {
	for _, cg := range tc.bc.Curators() {
		var addrs []string
		for _, c := range cg {
			addrs = append(addrs, c.ServiceAddr())
		}
		if err := setReadOnly(addrs, value); err != nil {
			return err
		}
	}
	return nil
}
