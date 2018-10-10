// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package blb

import (
	"context"
	"strconv"
	"sync"

	log "github.com/golang/glog"
	"github.com/westerndigitalcorporation/blb/internal/core"
)

// memMasterConnection is a mock in-memory implementation of
// MasterConnection. This should only be used for testing purposes.
type memMasterConnection struct {
	// List of curators managed by the master.
	curators []string

	// Mapping from PartitionID's to curator addresses.
	curatorMap map[core.PartitionID]string

	// Next curator to allocate new blob on.
	next int

	// Lock for 'curatorMap' and 'next'.
	lock sync.Mutex
}

// newMemMasterConnection creates a new MemMasterConnection.
// 'curators' is addresses of the MemCuratorConnection's to be managed
// by the MemMasterConnection. It is required that 'curators' can be
// converted to valid integers so that they will be mapped to
// PartitionID's.
func newMemMasterConnection(curators []string) MasterConnection {
	if 0 == len(curators) {
		log.Fatalf("no curator is found")
		return nil
	}

	m := &memMasterConnection{
		curators:   curators,
		curatorMap: make(map[core.PartitionID]string),
	}

	// Update curatorMap.
	for _, c := range curators {
		partition, err := strconv.Atoi(c)
		if nil != err {
			log.Fatalf("failed to create memMasterConnection: %s cannot be converted to integer", c)
		}
		m.curatorMap[core.PartitionID(partition)] = c
	}

	return m
}

// MasterCreateBlob allocates a new blob on the next curator.
func (m *memMasterConnection) MasterCreateBlob(ctx context.Context) (curator string, err core.Error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	curator = m.curators[m.next]
	m.next = (m.next + 1) % len(m.curators)
	return curator, core.NoError
}

// LookupPartition returns which curator is responsible for 'partition'.
func (m *memMasterConnection) LookupPartition(ctx context.Context, partition core.PartitionID) (curator string, err core.Error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	curator, ok := m.curatorMap[partition]
	if !ok {
		log.Infof("unknown partition: %v", partition)
		return "", core.ErrNoSuchBlob
	}
	return curator, core.NoError
}

// ListPartitions returns all existing partitions.
func (m *memMasterConnection) ListPartitions(ctx context.Context) (partitions []core.PartitionID, err core.Error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	for partition := range m.curatorMap {
		partitions = append(partitions, partition)
	}
	return
}

// GetTractserverInfo gets tractserver state.
func (m *memMasterConnection) GetTractserverInfo(ctx context.Context) (info []core.TractserverInfo, err core.Error) {
	return nil, core.NoError
}
