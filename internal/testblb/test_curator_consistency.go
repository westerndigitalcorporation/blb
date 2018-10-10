// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package testblb

// TestCuratorConsistency tests the consistency check functionality of the
// curator. First it checks that it can pass a check. Then it corrupts the state
// of one curator and checks that it causes a check failure.
func (tc *TestCase) TestCuratorConsistency() error {
	// It should pass a consistency check to start.
	capture := tc.captureLogs()
	if err := capture.WaitFor("c:@@@ consistency check passed"); err != nil {
		return err
	}

	// Corrupt the state of one curator directly. If this is the leader, the
	// others will fail. If this is a follower, it will fail.
	capture = tc.captureLogs()
	c00 := tc.bc.Curators()[0][0]
	if err := c00.SetFailureConfig(map[string]interface{}{"corrupt_curator_state": true}); err != nil {
		return err
	}
	if err := capture.WaitFor("c:@@@ consistency check failed"); err != nil {
		return err
	}

	return nil
}
