// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package raft

import (
	"github.com/westerndigitalcorporation/blb/pkg/wal"
)

// walCache wraps a wal.Log implementation and provides an in-memory circular
// cache on top of that. walCache will cache a given number of last appended
// entries so accessing to these entries will not incur actual disk I/O. The
// benchmark shows this simple caching strategy can boost the performance of
// Raft a lot.
//
// Because Raft will NOT access and modify its log concurrently so the log
// implementation doesn't have to be thread-safe. For example, iteration of
// of the log and mutation to the log can't happen concurrently. Also the the
// adjacent entries in the log will always have consecutive IDs. With these
// two facts we can simplify the implementation of the cache.
//
// Now "Trim" and "Truncate" will invalidate all cached entries for simplicity,
// given "Trim" and "Truncate" should not happen too frequently.
type walCache struct {
	// Cached entries. The length of the "entries" determines how many entries
	// we can cache. An entry with ID "i" will be stored to "i%len(entries)"-th
	// entry.
	entries []wal.Record

	// Actual log.
	log wal.Log
}

// Create a new cache on top of a wal.Log. "capacity" can't be 0.
func newWALCache(capacity uint, log wal.Log) wal.Log {
	if capacity == 0 {
		panic("capacity of the cache can't be 0.")
	}
	return &walCache{entries: make([]wal.Record, capacity), log: log}
}

func (c *walCache) FirstID() (id uint64, empty bool) {
	// Underlying wal.FsLog already cached the first ID in file name, it should be fast.
	return c.log.FirstID()
}

func (c *walCache) LastID() (id uint64, empty bool) {
	// Underlying wal.FsLog already cached the last ID, call it directly.
	return c.log.LastID()
}

func (c *walCache) GetIterator(start uint64) wal.Iterator {
	cache := c.entries[start%uint64(len(c.entries))]
	if cache.ID != 0 && cache.ID == start {
		// Hit the cache. Raft will not call 'GetIterator' directly, it will call
		// "Entries", which will call "GetIterator", "Next", "Record" ... "Close"
		// internally. Given Raft will not call "Append", "Entries", "Truncate",
		// etc concurrently so iteration and mutation to the entries will not
		// happen concurrently.
		return &walCacheIterator{entries: c.entries, curID: start}
	}
	return c.log.GetIterator(start)
}

func (c *walCache) Truncate(lastToKeep uint64) error {
	// For simplicity, we invalidate all cached entries here.
	c.entries = make([]wal.Record, len(c.entries))
	return c.log.Truncate(lastToKeep)
}

func (c *walCache) Trim(discardUpTo uint64) error {
	// For simplicity, we invalidate all cached entries here.
	c.entries = make([]wal.Record, len(c.entries))
	return c.log.Trim(discardUpTo)
}

func (c *walCache) Close() {
	c.log.Close()
}

func (c *walCache) Append(records ...wal.Record) error {
	err := c.log.Append(records...)
	if err != nil {
		// In case of errors it's possible that a prefix of records get appended
		// to the log, but the caller will panic if any error is returned so the
		// invariant that we always cache the last certain number of entries is still
		// maintained.
		return err
	}
	// Cache these successfully appended entries.
	// When Append is called there must be no iteration going on, so it's safe to
	// mutate the cached entries without lock.
	for _, record := range records {
		c.entries[record.ID%uint64(len(c.entries))] = record
	}
	return nil
}

type walCacheIterator struct {
	entries []wal.Record
	curID   uint64
	record  wal.Record
}

func (iter *walCacheIterator) Next() bool {
	// No need to hold a lock here given while iteration is going on there can't
	// any mutations to the log.
	cache := iter.entries[iter.curID%uint64(len(iter.entries))]
	if cache.ID != 0 && cache.ID == iter.curID {
		iter.record = cache
		iter.curID++
		return true
	}
	// Cache miss.
	// Given Raft will not Append to and iterate the log concurrently so the cached
	// entries will always be the last N entries of the log. Once we find a cache
	// miss we can conclude that we have reached the end of the log.
	return false
}

func (iter *walCacheIterator) Record() wal.Record {
	return iter.record
}

func (iter *walCacheIterator) Err() error {
	return nil
}

func (iter *walCacheIterator) Close() error {
	return nil
}
