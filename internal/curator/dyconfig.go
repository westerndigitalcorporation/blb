// Copyright (c) 2017 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package curator

import (
	log "github.com/golang/glog"
	"github.com/westerndigitalcorporation/blb/pkg/tokenbucket"
	"github.com/westerndigitalcorporation/blb/platform/dyconfig"
)

// DyConfig holds values that can be dynamically configured in the curator.
type DyConfig struct {
	// These bandwidth values are global (each curator will divide them by the
	// number of curator groups to get their local value).

	// How much bandwidth we can use for RS encoding.
	RSEncodeBandwidthGbps float32

	// How much bandwidth can we use for recovery (all types).
	RecoveryBandwidthGbps float32

	// Number of curator groups in this cluster.
	CuratorGroups int
}

// DefaultDyConfig holds default values for dynamic configuration.
var DefaultDyConfig = DyConfig{
	RSEncodeBandwidthGbps: 5.0,
	RecoveryBandwidthGbps: 10.0,
	CuratorGroups:         4,
}

func (c *Curator) registerDyConfig() {
	// Don't put service in name because we have one service per curator group,
	// and want to set them all at once.
	dyconfig.Register("blb-curator", false, DefaultDyConfig, c.updateDyConfig)
}

func (c *Curator) updateDyConfig(dyc DyConfig) {
	log.Infof("got new dynamic config: %+v", dyc)
	if dyc.CuratorGroups < 1 {
		dyc.CuratorGroups = DefaultDyConfig.CuratorGroups
	}
	updateRateGbps(c.rsEncodeBwLim, dyc.RSEncodeBandwidthGbps/float32(dyc.CuratorGroups))
	updateRateGbps(c.recoveryBwLim, dyc.RecoveryBandwidthGbps/float32(dyc.CuratorGroups))
}

func updateRateGbps(tb *tokenbucket.TokenBucket, gbps float32) {
	const bitsPerByte = 8
	const giga = 1e9
	bytesPerSec := gbps * giga / bitsPerByte
	tb.SetRate(bytesPerSec, 0)
}
