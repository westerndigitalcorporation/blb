// Copyright (c) 2017 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package core

import "unsafe"

// SizeofTractID is the size of a TractID value in bytes. Declared here so other
// packages don't have to import unsafe.
const SizeofTractID = int(unsafe.Sizeof(TractID{}))
