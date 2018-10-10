// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package testblb

// TestMasterConsistency tests the consistency check functionality of the
// master. First it checks that it can pass a check. Then it corrupts the state
// of one master and checks that it causes a check failure.
func (tc *TestCase) TestMasterConsistency() error {
	// It should pass a consistency check to start.
	capture := tc.captureLogs()
	if err := capture.WaitFor("m:@@@ consistency check passed"); err != nil {
		return err
	}

	// Corrupt the state of one master directly. If this is the leader, the
	// others will fail. If this is a follower, it will fail.
	capture = tc.captureLogs()
	m0 := tc.bc.Masters()[0]
	if err := m0.SetFailureConfig(map[string]interface{}{"corrupt_master_state": true}); err != nil {
		return err
	}
	if err := capture.WaitFor("m:@@@ consistency check failed"); err != nil {
		return err
	}

	return nil
}
