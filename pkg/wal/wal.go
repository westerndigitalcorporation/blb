// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

// Package wal (Write-Ahead Log) exposes a mechanism for durably storing
// a sequence of binary records.
package wal

// Log represents a sequence of binary records.
type Log interface {
	// FirstID returns the ID of the first record currently stored
	// in the log or returns empty as true if there are no records
	// in the log.
	FirstID() (id uint64, empty bool)

	// LastID returns the ID of the last record currently stored
	// in the log or returns empty as true if there are no recrods
	// in the log.
	LastID() (id uint64, empty bool)

	// GetIterator returns an iterator that can be used to read a range of
	// the log, starting at the record with ID 'start' or the first record
	// after where it would be if there is no record with that ID.
	// Appending to the Log while iterating is safe, but removing
	// (via Trim or Truncate) the partion of the log containing the
	// current location of the iterator will result in undefined
	// behavior.
	GetIterator(start uint64) Iterator

	// Appends adds the given records to the end of the log. Blocks
	// until the records are durably written to disk.
	// The IDs of each record must be sequential values.
	// If Append fails, the only guarentee is that Records that were
	// successfully written by previous calls to Append should still
	// be readable.
	Append(...Record) error

	// Truncate removes any records with an ID greater than 'lastToKeep'.
	// If Truncate fails, a subset of the Records that should have
	// been removed may still be present and readable.
	Truncate(lastToKeep uint64) error

	// Trim provides a hint to the log that it is safe to discard
	// any records with IDs less than or equal to the provided ID.
	// The log can discard fewer records than that (including
	// possibly discarding no records) if it is inefficient to do so.
	Trim(discardUpTo uint64) error

	// Close releases any resources used by the Log. Close should be
	// called on all log instances and the log must not be used after
	// Close is called.
	Close()
}

// Record represents a record stored in a log.
type Record struct {
	Data []byte
	ID   uint64
}

// Iterator is a generic interator for a Log.
type Iterator interface {
	// Next advances the iterator. It returns true if it was able to
	// advance to the next record or false if there are no more records
	// or an error occurred. Use Err() to tell the difference.
	Next() bool

	// Record returns the current record. Next must be called before
	// every call to Record.
	Record() Record

	// Err() return any error that occurred during iteration or nil
	// if no error occurred.
	Err() error

	// Close releases any resources associated with the iterator and
	// returns an error, if any occurred during iteration. The iterator
	// must not be used after Close is called.
	Close() error
}
