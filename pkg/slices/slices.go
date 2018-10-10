// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package slices

// ContainsString returns true if slice contains value a
func ContainsString(slice []string, a string) bool {
	for _, b := range slice {
		if b == a {
			return true
		}
	}
	return false
}

// EqualStrings compares if two (unsorted) string slices have the same elements
func EqualStrings(slice1, slice2 []string) bool {
	switch {
	case slice1 == nil && slice2 == nil:
		return true
	case len(slice1) != len(slice2):
		return false
	}

	m := map[string]bool{}
	for _, s1 := range slice1 {
		m[s1] = true
	}
	for _, s2 := range slice2 {
		if !m[s2] {
			return false
		}
	}
	return true
}
