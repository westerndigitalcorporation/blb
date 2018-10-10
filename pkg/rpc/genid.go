// Copyright (c) 2017 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package rpc

import (
	"crypto/rand"
	"encoding/base64"
	"strconv"
	"sync/atomic"
)

var (
	processIDPrefix = makePrefix()
	nextID          uint64
)

func makePrefix() string {
	buf := make([]byte, 15)
	rand.Read(buf)
	return base64.StdEncoding.EncodeToString(buf)
}

// GenID returns a unique string to be used as an RPC id for cancellation. This
// implementation works by using 120 random bits as a process identifier
// combined with 64 bits of sequence number. The values that it produces are
// printable (though things should work regardless).
func GenID() string {
	id := atomic.AddUint64(&nextID, 1)
	return processIDPrefix + strconv.FormatUint(id, 36)
}
