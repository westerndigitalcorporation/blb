// Copyright (c) 2019 David Reiss. All rights reserved.
// SPDX-License-Identifier: MIT

package core

// ParseStorageHint returns the StorageHint that matches the given string (case-sensitive).
func ParseStorageHint(s string) (StorageHint, bool) {
	for v, k := range EnumNamesStorageHint {
		if s == k {
			return v, true
		}
	}
	return StorageHint(0), false
}

func (c StorageClass) String() string { return EnumNamesStorageClass[c] }
func (h StorageHint) String() string  { return EnumNamesStorageHint[h] }
func (p Priority) String() string     { return EnumNamesPriority[p] }
