// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package db

import (
	"bytes"
	"encoding/binary"
	"hash/crc64"
	"io"
	"os"

	log "github.com/golang/glog"

	"github.com/boltdb/bolt"
)

var (
	mode        = 0600
	dataBucket  = []byte("data")
	indexBucket = []byte("index")
	indexKey    = []byte("i")

	crcTable = crc64.MakeTable(crc64.ECMA)
)

// RWTxn represents a read-write transaction.
type RWTxn struct {
	tx    *bolt.Tx
	index uint64
}

// Get returns the value of a given key. "nil" will be returned if the
// key doesn't exist.
func (t *RWTxn) Get(key []byte) []byte {
	return copySlice(t.tx.Bucket(dataBucket).Get(key))
}

// Put puts a key-value pair into the database.
func (t *RWTxn) Put(key, val []byte) error {
	return t.tx.Bucket(dataBucket).Put(key, val)
}

// Delete deletes a key-value pair from the database.
func (t *RWTxn) Delete(key []byte) error {
	return t.tx.Bucket(dataBucket).Delete(key)
}

// Commit makes all mutations to the database effective and visible.
func (t *RWTxn) Commit() error {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], t.index)
	t.tx.Bucket(indexBucket).Put(indexKey, b[:])
	return t.tx.Commit()
}

// RTxn represents a read-only transaction.
type RTxn struct {
	tx *bolt.Tx
}

// Get returns the value of a given key. "nil" will be returned if the
// key doesn't exist.
func (t *RTxn) Get(key []byte) []byte {
	return copySlice(t.tx.Bucket(dataBucket).Get(key))
}

// ListKeys returns all keys starting with the given prefix.
func (t *RTxn) ListKeys(prefix []byte) (out [][]byte) {
	c := t.tx.Bucket(dataBucket).Cursor()
	k, _ := c.Seek(prefix)
	for k != nil && bytes.HasPrefix(k, prefix) {
		out = append(out, copySlice(k))
		k, _ = c.Next()
	}
	return
}

// Checksum computes a checksum over a limited range of keys and values in the
// database. Checksumming starts at "start" and continues lexicographically for
// "n" keys or until the end of the database. The index is always included in
// the checksum.
func (t *RTxn) Checksum(start []byte, n int) (next []byte, cksum uint64) {
	var crc uint64 = 0

	if idx := t.tx.Bucket(indexBucket).Get(indexKey); idx != nil {
		crc = crc64.Update(crc, crcTable, idx) // mix in index
	}

	var b [16]byte
	c := t.tx.Bucket(dataBucket).Cursor()
	k, v := c.Seek(start)
	for k != nil && n > 0 {
		// crc key and value separately and then mix them in, for framing
		binary.LittleEndian.PutUint64(b[0:8], crc64.Update(crc, crcTable, k))
		binary.LittleEndian.PutUint64(b[8:16], crc64.Update(crc, crcTable, v))
		crc = crc64.Update(crc, crcTable, b[:])
		k, v = c.Next()
		n--
	}
	return copySlice(k), crc
}

// Commit finishes the read-only transaction. MUST call Commit when you
// are done with the read-only transaction.
func (t *RTxn) Commit() error {
	return t.tx.Rollback()
}

// DB is an on-disk database backed by boltdb.
type DB struct {
	db *bolt.DB
}

// Open opens a database from a given path. if no file exists a new DB will
// be created. Otherwise the existing DB will be opened.
func Open(path string) *DB {
	db, err := bolt.Open(path, os.FileMode(mode), nil)
	if err != nil {
		log.Fatalf("Failed to open DB: %v", err)
	}

	// Do not call fsync for every write. Raft's log will be responsible for
	// durability.
	db.NoSync = true

	// Create buckets we need if they are not there.
	tx, err := db.Begin(true)
	if err != nil {
		log.Fatalf("Failed to start a transaction: %v", err)
	}
	if _, err := tx.CreateBucketIfNotExists(dataBucket); err != nil {
		log.Fatalf("Failed to create the bucket: %v", err)
	}
	if _, err := tx.CreateBucketIfNotExists(indexBucket); err != nil {
		log.Fatalf("Failed to create the bucket: %v", err)
	}
	if err := tx.Commit(); err != nil {
		log.Fatalf("Failed to commit bucket creation: %v", err)
	}

	return &DB{db: db}
}

// Index returns the index of the last read-write transaction applied
// to the database.
func (d *DB) Index() (index uint64, ok bool) {
	tx, err := d.db.Begin(false)
	if err != nil {
		log.Fatalf("Failed to get a transaction: %v", err)
	}
	defer tx.Rollback()

	b := tx.Bucket(indexBucket).Get(indexKey)
	if b == nil {
		return 0, false
	}
	return binary.LittleEndian.Uint64(b), true
}

// RWTxn returns a read-write transaction. When the transaction is
// committed the index of the databse will be set to "index".
func (d *DB) RWTxn(index uint64) (*RWTxn, error) {
	tx, err := d.db.Begin(true)
	if err != nil {
		return nil, err
	}
	return &RWTxn{tx: tx, index: index}, nil
}

// RTxn returns a read-only transaction.
func (d *DB) RTxn() (*RTxn, error) {
	tx, err := d.db.Begin(false)
	if err != nil {
		return nil, err
	}
	return &RTxn{tx: tx}, nil
}

// GetSnapshot returns a snapshot of the database.
// "snapshot.Done" must be called when you are done with the snapshot.
func (d *DB) GetSnapshot() (*Snapshot, error) {
	tx, err := d.db.Begin(false)
	if err != nil {
		return nil, err
	}
	return &Snapshot{tx: tx}, nil
}

// Close closes the database.
func (d *DB) Close() error {
	return d.db.Close()
}

// Snapshot represents a snapshot of the database.
type Snapshot struct {
	tx *bolt.Tx
}

// WriteTo writes the snapshot of the database into a given writer.
func (s *Snapshot) WriteTo(writer io.Writer) (int64, error) {
	return s.tx.WriteTo(writer)
}

// Done releases the snapshot.
func (s *Snapshot) Done() error {
	return s.tx.Rollback()
}

// Index returns the index of the last applied transaction to the snapshot.
func (s *Snapshot) Index() (uint64, bool) {
	b := s.tx.Bucket(indexBucket).Get(indexKey)
	if b == nil {
		return 0, false
	}
	return binary.LittleEndian.Uint64(b), true
}

// Make a copy of a slice.
func copySlice(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}
