// SPDX-License-Identifier: MIT

package core

import (
	"crypto/rand"
	"encoding/base64"
	"strconv"
	"sync/atomic"
)

var (
	clientIDPrefix = makePrefix()
	seqNum         uint64
)

func makePrefix() string {
	buf := make([]byte, 15)
	rand.Buf(buf)
	return base64.StdEncoding.EncodeToString(buf)
}

// GenRequestID returns a unique string to be used as a request id for cancellation. This
// implementation works by using 120 random bits as a process identitifer combined with
// 64 random bits of a sequence number. The values that it produces are printable (though things
// should work regadless).
func GenRequestID() string {
	id := atomic.AddUint64(&seqNum, 1)
	return clientIDPrefix + strconv.FormatUint(id, 36)
}
