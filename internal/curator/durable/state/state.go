// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package state

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"os"
	"time"

	"github.com/boltdb/bolt"
	"github.com/golang/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	log "github.com/golang/glog"
	"github.com/westerndigitalcorporation/blb/internal/core"
	pb "github.com/westerndigitalcorporation/blb/internal/curator/durable/state/statepb"
	"github.com/westerndigitalcorporation/blb/internal/curator/storageclass"
	"github.com/westerndigitalcorporation/blb/pkg/failures"
)

var (
	partitionBucket = []byte("partition") // Bucket that stores all partition metadata.
	blobBucket      = []byte("blob")      // Bucket that stores all blob metadata.
	rschunkBucket   = []byte("rschunk")   // Bucket that stores RSChunks.
	metaBucket      = []byte("metadata")  // Bucket that stores all other data.

	// Keys in metaBucket:
	curatorIDKey = []byte("curator_id")
	txnIndexKey  = []byte("txn_index")
	readOnlyKey  = []byte("read_only") // value is empty bytes
	tsidsKey     = []byte("tsids")
)

var (
	mDbSize = promauto.NewGauge(prometheus.GaugeOpts{
		Subsystem: "curator",
		Name:      "db_size",
		Help:      "size of database in bytes",
	})

	// Boltdb performance metrics (comments copied from boltdb):
	mBoltStats = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "curator",
		Name:      "boltdb",
		Help:      "metrics exported by boltdb",
	}, []string{"field"})
	mBoltFreePages     = mBoltStats.WithLabelValues("free_pages")     // total number of free pages on the freelist
	mBoltPendingPages  = mBoltStats.WithLabelValues("pending_pages")  // total number of pending pages on the freelist
	mBoltFreeAlloc     = mBoltStats.WithLabelValues("free_alloc")     // total bytes allocated in free pages
	mBoltFreelistInuse = mBoltStats.WithLabelValues("freelist_inuse") // total bytes used by the freelist
	mBoltOpenTx        = mBoltStats.WithLabelValues("open_tx_count")  // number of currently open read transactions
	// All metrics below here are effectively counters, but maintained by
	// boltdb, so we read them and put them in gauges.
	mBoltTxCount       = mBoltStats.WithLabelValues("tx_count")          // total number of started read transactions
	mBoltTxPageCount   = mBoltStats.WithLabelValues("tx_page_count")     // number of page allocations
	mBoltTxPageAlloc   = mBoltStats.WithLabelValues("tx_page_alloc")     // total bytes allocated
	mBoltTxCursorCount = mBoltStats.WithLabelValues("tx_cursor_count")   // number of cursors created
	mBoltTxNodeCount   = mBoltStats.WithLabelValues("tx_node_count")     // number of node allocations
	mBoltTxNodeDeref   = mBoltStats.WithLabelValues("tx_node_deref")     // number of node dereferences
	mBoltTxRebalance   = mBoltStats.WithLabelValues("tx_rebalance")      // number of node rebalances
	mBoltTxRebalanceT  = mBoltStats.WithLabelValues("tx_rebalance_time") // total time spent rebalancing (s)
	mBoltTxSplit       = mBoltStats.WithLabelValues("tx_split")          // number of nodes split
	mBoltTxSpill       = mBoltStats.WithLabelValues("tx_spill")          // number of nodes spilled
	mBoltTxSpillT      = mBoltStats.WithLabelValues("tx_spill_time")     // total time spent spilling (s)
	mBoltTxWrite       = mBoltStats.WithLabelValues("tx_write")          // number of writes performed
	mBoltTxWriteT      = mBoltStats.WithLabelValues("tx_write_time")     // total time spent writing to disk (s)
)

const (
	mode os.FileMode = 0600

	// FillPercent for different buckets. The boltdb default is 0.5, and we use
	// that when there's no reason to use another. We write blobs in mostly
	// sequential order, so we can use a higher fill percent to save space
	// there. We write RS chunks in almost totally sequential order (except for
	// going back and updating hosts), so we can use an even higher value there.
	defaultFillPct = 0.50
	blobFillPct    = 0.75
	rsChunkFillPct = 0.90
)

// State represents replicated state of a curator.
type State struct {
	db *bolt.DB
}

// Open opens the state database on disk. If the path doesn't exist it will
// create a new one.
func Open(path string) *State {
	db, err := bolt.Open(path, mode, nil)
	if err != nil {
		log.Fatalf("Failed to open DB: %v", err)
	}

	// Creates buckets we need if they have not been created.
	tx, err := db.Begin(true)
	if err != nil {
		log.Fatalf("Failed to start a transaction: %v", err)
	}
	if _, err := tx.CreateBucketIfNotExists(partitionBucket); err != nil {
		log.Fatalf("Failed to create partition bucket: %v", err)
	}
	if _, err := tx.CreateBucketIfNotExists(blobBucket); err != nil {
		log.Fatalf("Failed to create blob bucket: %v", err)
	}
	if _, err := tx.CreateBucketIfNotExists(rschunkBucket); err != nil {
		log.Fatalf("Failed to create rschunk bucket: %v", err)
	}
	if _, err := tx.CreateBucketIfNotExists(metaBucket); err != nil {
		log.Fatalf("Failed to create id bucket: %v", err)
	}
	if err := tx.Commit(); err != nil {
		log.Fatalf("Failed to commit creation of buckets: %v", err)
	}

	s := &State{db: db}
	failures.Register("corrupt_curator_state", s.corrupt)
	return s
}

// Close closes current State. Caller must guarantee that all transactions have
// been closed before calling "Close".
func (s *State) Close() {
	if err := s.db.Close(); err != nil {
		log.Fatalf("Failed to close State: %v", err)
	}
}

// WriteTxn returns a Txn that supports read-write accesses to State.
// All mutations to State from a Txn object will not be visible to
// other transactions until 'Commit' is called, though the uncommitted
// mutations can be seen by reads from the same Txn object. After you
// are done with the Txn you should call 'Commit' to make the mutations
// effective. It's OK to start multiple transactions from multiple
// threads, all write transactions will be serialized underneath.
func (s *State) WriteTxn(index uint64) *Txn {
	var txn *bolt.Tx
	var err error
	if txn, err = s.db.Begin(true); err != nil {
		log.Fatalf("Failed to create RW txn: %v", err)
	}

	t := &Txn{txn: txn, readOnly: false}

	// Set the transaction index first. It will be committed atomically
	// with all other mutations in this transaction.
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], index)
	t.put(metaBucket, txnIndexKey, b[:], defaultFillPct)
	return t
}

// ReadOnlyTxn returns a Txn that supports read-only access to State.
// ReadOnlyTxn will get better concurrency than read-write transactions.
func (s *State) ReadOnlyTxn() *Txn {
	var txn *bolt.Tx
	var err error
	if txn, err = s.db.Begin(false); err != nil {
		log.Fatalf("Failed to create RO txn: %v", err)
	}
	return &Txn{txn: txn, readOnly: true}
}

// corrupt makes an arbitrary change to the state, for testing with failure injection.
func (s *State) corrupt(json.RawMessage) error {
	log.Infof("CORRUPTING STATE")
	txn := s.ReadOnlyTxn()
	idx := txn.GetIndex()
	txn.Commit()
	txn = s.WriteTxn(idx)
	txn.SetCuratorID(98765321)
	txn.Commit()
	return nil
}

// BlobIterator is used to iterate metadata of blobs in state.
type BlobIterator struct {
	cursor *bolt.Cursor
	k, v   []byte
}

// Next advances the iterator to next position. Returns true if it's not ended.
func (i *BlobIterator) Next() (has bool) {
	i.k, i.v = i.cursor.Next()
	return i.k != nil
}

// Blob returns metadata of the blob pointed by iterator. Only call
// this if 'Next' returns true.
func (i *BlobIterator) Blob() (core.BlobID, *pb.Blob) {
	b := new(pb.Blob)
	mustUnmarshal(i.v, b)
	return key2BlobID(i.k), b
}

// RSChunkIterator is used to iterate over RSChunks in state.
type RSChunkIterator struct {
	cursor *bolt.Cursor
	k, v   []byte
}

// Next advances the iterator to next position. Returns true if it's not ended.
func (i *RSChunkIterator) Next() (has bool) {
	i.k, i.v = i.cursor.Next()
	return i.k != nil
}

// RSChunk returns the chunk pointed by iterator. Only call this if 'Next' returns true.
func (i *RSChunkIterator) RSChunk() (core.RSChunkID, *pb.RSChunk) {
	c := new(pb.RSChunk)
	mustUnmarshal(i.v, c)
	return key2rschunkID(i.k), c
}

// Txn is a transaction object that captures current snapshot of the
// state. Operations of Txn are done on the snapshot of the state and
// if there are mutations they will only be visible to other transactions
// after 'Commit' is called.
type Txn struct {
	readOnly bool // Is this txn read-only?
	txn      *bolt.Tx

	// Only for write transactions:
	newTSIDs tsidbitmap
}

// GetReadOnlyMode returns true if the read-only flag is set in the state.
// Note that this has nothing to do with whether the transaction is read-only.
func (t *Txn) GetReadOnlyMode() bool {
	_, ok := t.get(metaBucket, readOnlyKey)
	return ok
}

// SetReadOnlyMode changes the read-only flag state.
// Note that this has nothing to do with whether the transaction is read-only.
func (t *Txn) SetReadOnlyMode(mode bool) {
	if mode {
		t.put(metaBucket, readOnlyKey, nil, defaultFillPct)
	} else {
		t.delete(metaBucket, readOnlyKey)
	}
}

// GetIndex returns the index of the latest transaction.
func (t *Txn) GetIndex() uint64 {
	b, ok := t.get(metaBucket, txnIndexKey)
	if !ok {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}

// SetCuratorID sets curator ID to 'id'.
func (t *Txn) SetCuratorID(id core.CuratorID) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(id))
	t.put(metaBucket, curatorIDKey, b[:], defaultFillPct)
}

// GetCuratorID returns curator ID.
func (t *Txn) GetCuratorID() core.CuratorID {
	v, ok := t.get(metaBucket, curatorIDKey)
	if !ok {
		// 0 means not registered yet.
		return 0
	}
	return core.CuratorID(binary.BigEndian.Uint32(v))
}

// Stat returns stat of a blob.
func (t *Txn) Stat(id core.BlobID) (info core.BlobInfo, err core.Error) {
	blob := t.GetBlob(id)
	if blob == nil {
		err = core.ErrNoSuchBlob
		return
	}
	info = core.BlobInfo{
		NumTracts: len(blob.Tracts),
		Repl:      int(blob.GetRepl()),
		MTime:     time.Unix(0, blob.GetMtime()),
		ATime:     time.Unix(0, blob.GetAtime()),
		Class:     blob.GetStorage(),
		Hint:      blob.GetHint(),
	}
	if blob.GetExpires() != 0 {
		info.Expires = time.Unix(0, blob.GetExpires())
	}
	return
}

// PutBlob puts blob metadata into state.
func (t *Txn) PutBlob(id core.BlobID, blob *pb.Blob) {
	pid := id.Partition()
	if p := t.GetPartition(pid); p == nil {
		// Sanity check that the partition of the blob must exist.
		log.Fatalf("bug: partition of the blob doesn't exist")
	}
	t.put(blobBucket, blobID2Key(id), mustMarshal(blob), blobFillPct)
	for _, tract := range blob.Tracts {
		t.ensureKnownTSIDs(tract.Hosts)
	}
}

// DeleteBlob marks a blob as deleted instead of actually deleting it.
func (t *Txn) DeleteBlob(id core.BlobID, when time.Time) core.Error {
	b := t.GetBlob(id)
	if b == nil {
		return core.ErrNoSuchBlob
	}
	b.Deleted = proto.Int64(when.UnixNano())
	t.PutBlob(id, b)
	return core.NoError
}

// UndeleteBlob undeletes a blob that is marked as deleted.
func (t *Txn) UndeleteBlob(id core.BlobID) core.Error {
	b := t.GetBlobAll(id)
	if b == nil {
		return core.ErrNoSuchBlob
	}
	b.Deleted = nil
	t.PutBlob(id, b)
	return core.NoError
}

// SetBlobMetadata changes metadata for a blob. Only fields Hint, MTime, ATime,
// and Expires are used from md, others are ignored. Zero values for those
// fields mean "don't change this".
func (t *Txn) SetBlobMetadata(id core.BlobID, md core.BlobInfo) core.Error {
	b := t.GetBlob(id)
	if b == nil {
		return core.ErrNoSuchBlob
	}
	if !md.MTime.IsZero() {
		b.Mtime = proto.Int64(md.MTime.UnixNano())
	}
	if !md.ATime.IsZero() {
		b.Atime = proto.Int64(md.ATime.UnixNano())
	}
	// Using the zero value to determine presence here means that we can't set a
	// blob from expiring back to not expiring. As a workaround, set it to
	// expire in the far future.
	if !md.Expires.IsZero() {
		b.Expires = proto.Int64(md.Expires.UnixNano())
	}
	// Using the zero value here means we can't set a blob's hint to "DEFAULT",
	// since that's interpreted as leaving it alone.
	if md.Hint != 0 {
		b.Hint = &md.Hint
	}
	t.PutBlob(id, b)
	return core.NoError
}

// FinishDeleteBlobs deletes the given blobs from the database. This is final
// and the blobs CANNOT be recovered after this. The caller must ensure that the
// given blobs have a deletion time or expiry time in the past.
func (t *Txn) FinishDeleteBlobs(ids []core.BlobID) core.Error {
	for _, id := range ids {
		// Ignore any errors we get removing these tracts from RS chunks, we can
		// continue deleting the blob anyway.
		t.removeTractsFromRSChunks(id)
		t.delete(blobBucket, blobID2Key(id))
	}
	return core.NoError
}

func (t *Txn) removeTractsFromRSChunks(bid core.BlobID) core.Error {
	blob := t.GetBlobAll(bid)
	if blob == nil {
		return core.ErrNoSuchBlob
	}
	for k, tract := range blob.Tracts {
		tid := core.TractIDFromParts(bid, core.TractKey(k))
		for _, cls := range storageclass.AllRS {
			if cid := cls.GetRS(tract); cid != nil {
				t.removeTractFromRSChunk(cid, tid)
			}
		}
	}
	return core.NoError
}

func (t *Txn) removeTractFromRSChunk(cid []byte, tid core.TractID) core.Error {
	v, ok := t.get(rschunkBucket, cid)
	if !ok {
		return core.ErrNoSuchBlob
	}
	chunk := new(pb.RSChunk)
	mustUnmarshal(v, chunk)

	found := false
outer:
	for _, data := range chunk.Data {
		for k, tract := range data.Tracts {
			if tract.Id == tid {
				// Splice this one out of the chunk.
				dt := data.Tracts
				copy(dt[k:], dt[k+1:])
				data.Tracts = dt[:len(dt)-1]
				found = true
				break outer
			}
		}
	}

	if !found {
		return core.ErrNoSuchTract
	}

	t.put(rschunkBucket, cid, mustMarshal(chunk), rsChunkFillPct)
	return core.NoError
}

// PutPartition puts a partition into state.
func (t *Txn) PutPartition(partition *pb.Partition) {
	t.put(partitionBucket, partitionID2Key(core.PartitionID(partition.GetId())), mustMarshal(partition), defaultFillPct)
}

// GetPartitions returns all partitions of this curator.
func (t *Txn) GetPartitions() (partitions []*pb.Partition) {
	t.txn.Bucket(partitionBucket).ForEach(func(_, v []byte) error {
		p := new(pb.Partition)
		mustUnmarshal(v, p)
		partitions = append(partitions, p)
		return nil
	})
	return partitions
}

// GetPartition returns info of a given partition.
func (t *Txn) GetPartition(id core.PartitionID) *pb.Partition {
	b, ok := t.get(partitionBucket, partitionID2Key(id))
	if !ok {
		return nil
	}
	p := new(pb.Partition)
	mustUnmarshal(b, p)
	return p
}

// GetTracts returns tracts of a blob from [start, end), if 'end' is
// past the last tract of the blob, only the tracts from 'start' to
// the last one will be returned.
func (t *Txn) GetTracts(id core.BlobID, start, end int) ([]core.TractInfo, core.StorageClass, core.Error) {
	cls := core.StorageClass_REPLICATED
	// This shouldn't have made it this far but it's worth double-checking
	// instead of crashing.
	if start < 0 || end < 0 || end < start {
		return nil, cls, core.ErrInvalidArgument
	}

	blob := t.GetBlob(id)
	if blob == nil {
		return nil, cls, core.ErrNoSuchBlob
	}
	cls = blob.GetStorage()

	// Special case: If start == end, we're just checking that the blob exists.
	if start == end {
		return nil, cls, core.NoError
	}

	// Make sure that [start, end) is meaningful and safe.
	if len(blob.Tracts) == 0 {
		return nil, cls, core.ErrNoSuchTract
	}
	if start >= len(blob.Tracts) {
		return nil, cls, core.ErrNoSuchTract
	}
	if end > len(blob.Tracts) {
		end = len(blob.Tracts)
	}

	var ret []core.TractInfo
	for i := start; i < end; i++ {
		tid := core.TractID{Blob: id, Index: core.TractKey(i)}
		tt := blob.Tracts[i]
		tract := core.TractInfo{Tract: tid, Version: tt.Version}
		if rsp, ok := t.getRSPointer(tt, tid); ok {
			tract.RS = rsp
		} else {
			tract.TSIDs = tt.Hosts
		}
		ret = append(ret, tract)
	}
	return ret, cls, core.NoError
}

// getRSPointer returns the metadata used by the client to read from an RS-coded tract.
func (t *Txn) getRSPointer(tract *pb.Tract, tid core.TractID) (core.TractPointer, bool) {
	var cid []byte
	var clsid core.StorageClass
	// See if this tract is present in any RS chunks. If it is present in
	// multiple RS chunks, that means it's being transitioned from one RS class
	// to another. In that case it doesn't really matter which one we pick, as
	// long as we pick one.
	for _, cls := range storageclass.AllRS {
		if cid = cls.GetRS(tract); cid != nil {
			clsid = cls.ID()
			break
		}
	}
	if cid == nil {
		return core.TractPointer{}, false
	}
	b, ok := t.get(rschunkBucket, cid)
	if !ok {
		log.Errorf("[curator] missing rschunk key %v", cid)
		return core.TractPointer{}, false
	}
	var c pb.RSChunk
	mustUnmarshal(b, &c)
	return lookupTractInChunk(&c, tid, cid, clsid)
}

func lookupTractInChunk(c *pb.RSChunk, tid core.TractID, cid []byte, clsid core.StorageClass) (core.TractPointer, bool) {
	for i, data := range c.Data {
		for _, tract := range data.Tracts {
			if tract.Id == tid {
				id := key2rschunkID(cid)
				return core.TractPointer{
					Chunk:      id.Add(i),
					TSID:       c.Hosts[i],
					Offset:     tract.GetOffset(),
					Length:     tract.GetLength(),
					Class:      clsid,
					BaseChunk:  id,
					OtherTSIDs: c.Hosts,
				}, true
			}
		}
	}
	log.Errorf("[curator] couldn't find tract %v in rs chunk %v", tid, cid)
	return core.TractPointer{}, false
}

// LookupRSPiece looks up an RS chunk ID (passed as a TractID) in the RS chunk
// bucket and returns the tractserver that it's supposed to be on, or false if
// the chunk piece ID is unknown.
func (t *Txn) LookupRSPiece(id core.TractID) (core.TractserverID, bool) {
	// id is an RSChunkID in the form of a TractID.
	key := tractID2Key(id)
	c := t.txn.Bucket(rschunkBucket).Cursor()
	k, v := c.Seek(key)
	if k == nil {
		// If id is in the last RSChunk in the database and not the first piece,
		// we'll end up with nil here. Find the actual last piece.
		k, v = c.Last()
	} else if bytes.Equal(k, key) {
		// If id is the first piece of an RS chunk, we'll hit it exactly.
	} else {
		// Otherwise, we overshot. Go back one.
		k, v = c.Prev()
	}
	if k == nil || v == nil {
		return 0, false
	}

	var chunk pb.RSChunk
	mustUnmarshal(v, &chunk)

	// Figure out where within the chunk this piece is.
	baseID := key2rschunkID(k)
	myID := key2rschunkID(key)
	index := int64(myID.ID) - int64(baseID.ID)
	if index < 0 || index >= int64(len(chunk.Hosts)) {
		return 0, false
	}
	return chunk.Hosts[index], true
}

// EncodedTract describes one tract in an RSChunk
type EncodedTract struct {
	ID         core.TractID
	Offset     int
	Length     int
	NewVersion int
}

// PutRSChunk adds a new RSChunk and updates all the contained tracts to point to it.
func (t *Txn) PutRSChunk(id core.RSChunkID,
	storage core.StorageClass,
	hosts []core.TractserverID,
	data [][]EncodedTract) core.Error {

	// Check that it doesn't exist already.
	if _, ok := t.get(rschunkBucket, rschunkID2Key(id)); ok {
		return core.ErrConflictingState
	}

	cls := storageclass.Get(storage)
	key := rschunkID2Key(id)

	chunk := pb.RSChunk{
		Data:  make([]*pb.RSChunk_Data, len(data)),
		Hosts: hosts,
	}
	blobUpdates := make(map[core.BlobID]*pb.Blob)
	for i, c := range data {
		ts := make([]*pb.RSChunk_Data_Tract, len(c))
		for j, tract := range c {
			ts[j] = &pb.RSChunk_Data_Tract{
				Id:     tract.ID,
				Length: uint32(tract.Length),
				Offset: uint32(tract.Offset),
			}
			bid := tract.ID.Blob
			blob, ok := blobUpdates[bid]
			if !ok {
				blob = t.GetBlob(bid)
				if blob == nil {
					return core.ErrNoSuchBlob
				}
				blobUpdates[bid] = blob
			}
			if int(tract.ID.Index) >= len(blob.Tracts) {
				return core.ErrNoSuchTract
			}
			st := blob.Tracts[tract.ID.Index]
			if err := cls.Set(st, key); err != core.NoError {
				return err
			}
			st.Version = tract.NewVersion
		}
		chunk.Data[i] = &pb.RSChunk_Data{Tracts: ts}
	}
	// Write chunk.
	t.put(rschunkBucket, key, mustMarshal(&chunk), rsChunkFillPct)
	// Write new blobs.
	for bid, blob := range blobUpdates {
		t.put(blobBucket, blobID2Key(bid), mustMarshal(blob), blobFillPct)
	}
	// Update TSIDs.
	t.ensureKnownTSIDs(hosts)
	return core.NoError
}

// UpdateRSHosts updates the host set for one RS chunk.
func (t *Txn) UpdateRSHosts(id core.RSChunkID, hosts []core.TractserverID) core.Error {
	c := t.GetRSChunk(id)
	if c == nil {
		return core.ErrInvalidArgument
	}
	if len(c.Hosts) != len(hosts) {
		return core.ErrInvalidArgument
	}
	c.Hosts = hosts
	t.put(rschunkBucket, rschunkID2Key(id), mustMarshal(c), rsChunkFillPct)
	t.ensureKnownTSIDs(hosts)
	return core.NoError
}

// UpdateStorageClass changes the storage class of a blob. All the tracts in the
// blob must already support the new class.
func (t *Txn) UpdateStorageClass(id core.BlobID, storage core.StorageClass) core.Error {
	blob := t.GetBlob(id)
	if blob == nil {
		return core.ErrNoSuchBlob
	}

	targetCls := storageclass.Get(storage)

	for _, tract := range blob.Tracts {
		// Double-check that this is valid.
		if !targetCls.Has(tract) {
			return core.ErrInvalidArgument
		}
		// Clear the others.
		for _, cls := range storageclass.All {
			if cls.ID() != storage {
				cls.Clear(tract)
			}
		}
	}

	blob.Storage = &storage
	t.PutBlob(id, blob)
	return core.NoError
}

// GetIterator returns an iterator that can iterate all blobs, and a boolean
// whether the iterator has any blobs left. The iteration will only see the
// current snapshot of the state, it's concurrent with any modifications. The
// iterator is positioned at the given blob to start (or the blob with the next
// higher ID.
func (t *Txn) GetIterator(start core.BlobID) (it *BlobIterator, has bool) {
	cursor := t.txn.Bucket(blobBucket).Cursor()
	k, v := cursor.Seek(blobID2Key(start))
	return &BlobIterator{cursor: cursor, k: k, v: v}, k != nil
}

// GetRSChunkIterator returns an iterator that can iterate over all RS chunks.
func (t *Txn) GetRSChunkIterator(start core.RSChunkID) (it *RSChunkIterator, has bool) {
	cursor := t.txn.Bucket(rschunkBucket).Cursor()
	k, v := cursor.Seek(rschunkID2Key(start))
	return &RSChunkIterator{cursor: cursor, k: k, v: v}, k != nil
}

// GetBlob returns a blob given its blob ID. It will only
// return if the blob exists and is not marked as deleted.
func (t *Txn) GetBlob(id core.BlobID) *pb.Blob {
	b := t.GetBlobAll(id)
	if b == nil || b.GetDeleted() != 0 {
		// Do not return blob which is marked as deleted.
		return nil
	}
	return b
}

// GetBlobAll is like GetBlob, but also returns blob that is marked as
// deleted.
func (t *Txn) GetBlobAll(id core.BlobID) *pb.Blob {
	b, ok := t.get(blobBucket, blobID2Key(id))
	if !ok {
		return nil
	}
	blob := new(pb.Blob)
	mustUnmarshal(b, blob)
	return blob
}

// GetRSChunk returns an RS chunk given its ID.
func (t *Txn) GetRSChunk(id core.RSChunkID) *pb.RSChunk {
	c, ok := t.get(rschunkBucket, rschunkID2Key(id))
	if !ok {
		return nil
	}
	chunk := new(pb.RSChunk)
	mustUnmarshal(c, chunk)
	return chunk
}

// UpdateTime is an instruction to update one blob's mtime/atime.
type UpdateTime struct {
	Blob         core.BlobID
	MTime, ATime int64 // zero means "don't change"
}

// BatchUpdateTimes updates mtime/atime for a bunch of blobs at once.
// Errors are ignored.
func (t *Txn) BatchUpdateTimes(updates []UpdateTime) {
	for _, update := range updates {
		if blob := t.GetBlob(update.Blob); blob != nil {
			if update.MTime != 0 && update.MTime > blob.GetMtime() {
				blob.Mtime = &update.MTime
			}
			if update.ATime != 0 && update.ATime > blob.GetAtime() {
				blob.Atime = &update.ATime
			}
			t.PutBlob(update.Blob, blob)
		}
	}
}

// CreateTSIDCache creates the cached TSID record in the database.
func (t *Txn) CreateTSIDCache() core.Error {
	if _, ok := t.get(metaBucket, tsidsKey); ok {
		// Already exists, don't do anything.
		return core.NoError
	}

	// Scan all blobs and chunks (this may take a while but we only have to do it once).
	for it, has := t.GetIterator(0); has; has = it.Next() {
		_, blob := it.Blob()
		for _, tract := range blob.Tracts {
			t.ensureKnownTSIDs(tract.Hosts)
		}
	}
	for it, has := t.GetRSChunkIterator(core.RSChunkID{}); has; has = it.Next() {
		_, chunk := it.RSChunk()
		t.ensureKnownTSIDs(chunk.Hosts)
	}

	// Write record.
	t.put(metaBucket, tsidsKey, t.newTSIDs.ToBytes(), defaultFillPct)

	return core.NoError
}

// GetKnownTSIDs returns all the known TSIDs in the database as a slice.
// Note that IDs that were added during this transaction may not be returned.
func (t *Txn) GetKnownTSIDs() ([]core.TractserverID, core.Error) {
	if cache, ok := t.get(metaBucket, tsidsKey); ok {
		return tsidbitmapFromBytes(cache).ToSlice(), core.NoError
	}
	return nil, core.ErrInvalidState
}

// ensureKnownTSIDs marks the given TSIDs to be merged into the cached set with
// this transaction.
func (t *Txn) ensureKnownTSIDs(hosts []core.TractserverID) {
	for _, h := range hosts {
		t.newTSIDs = t.newTSIDs.Set(h)
	}
}

// mergeNewTSIDs ensures that all the TSIDs in the new set are written to the cache.
func (t *Txn) mergeNewTSIDs() {
	if t.newTSIDs == nil {
		return
	}
	cache, ok := t.get(metaBucket, tsidsKey)
	if !ok {
		// If no cache present, do nothing.
		return
	}
	bm := tsidbitmapFromBytes(cache)
	if merged, dirty := bm.Merge(t.newTSIDs); dirty {
		t.put(metaBucket, tsidsKey, merged.ToBytes(), defaultFillPct)
	}
}

func (t *Txn) get(bucket, key []byte) ([]byte, bool) {
	v := t.txn.Bucket(bucket).Get(key)
	if v == nil {
		return v, false
	}
	return v, true
}

func (t *Txn) put(bucket, key, val []byte, fillPct float64) {
	if t.readOnly {
		log.Fatalf("Can't do mutation to in a read-only transaction")
	}
	b := t.txn.Bucket(bucket)
	b.FillPercent = fillPct
	if err := b.Put(key, val); err != nil {
		log.Fatalf("Failed to put into db: %v", err)
	}
}

func (t *Txn) delete(bucket, k []byte) {
	if t.readOnly {
		log.Fatalf("delete is not allowed in a read-only transaction!")
	}
	t.txn.Bucket(bucket).Delete(k)
}

// Commit commits all mutations in Txn to State, if there's any.
// Commit never fails.
func (t *Txn) Commit() {
	if !t.readOnly {
		// Write new TSIDs if necessary.
		t.mergeNewTSIDs()

		// Grab size before committing.
		db, size := t.txn.DB(), t.txn.Size()

		if err := t.txn.Commit(); err != nil {
			log.Fatalf("Failed to commit transaction: %v", err)
		}

		// boltdb updates db-wide stats after committing, so update those now.
		updateDbStats(size, db.Stats())

	} else {
		t.txn.Rollback()
	}
}

// Dump dumps the state of the DB to a given writer.
func (t *Txn) Dump(writer io.Writer) (int64, error) {
	return t.txn.WriteTo(writer)
}

// updateDbStats updates our database stats metrics from boltdb. Most of these
// are performance-related counters. The size has to be read from a transaction,
// which is why it gets passed in separately.
func updateDbStats(size int64, stats bolt.Stats) {
	mDbSize.Set(float64(size))
	mBoltFreePages.Set(float64(stats.FreePageN))
	mBoltPendingPages.Set(float64(stats.PendingPageN))
	mBoltFreeAlloc.Set(float64(stats.FreeAlloc))
	mBoltFreelistInuse.Set(float64(stats.FreelistInuse))
	mBoltOpenTx.Set(float64(stats.OpenTxN))
	mBoltTxCount.Set(float64(stats.TxN))
	mBoltTxPageCount.Set(float64(stats.TxStats.PageCount))
	mBoltTxPageAlloc.Set(float64(stats.TxStats.PageAlloc))
	mBoltTxCursorCount.Set(float64(stats.TxStats.CursorCount))
	mBoltTxNodeCount.Set(float64(stats.TxStats.NodeCount))
	mBoltTxNodeDeref.Set(float64(stats.TxStats.NodeDeref))
	mBoltTxRebalance.Set(float64(stats.TxStats.Rebalance))
	mBoltTxRebalanceT.Set(float64(stats.TxStats.RebalanceTime) / 1e9)
	mBoltTxSplit.Set(float64(stats.TxStats.Split))
	mBoltTxSpill.Set(float64(stats.TxStats.Spill))
	mBoltTxSpillT.Set(float64(stats.TxStats.SpillTime) / 1e9)
	mBoltTxWrite.Set(float64(stats.TxStats.Write))
	mBoltTxWriteT.Set(float64(stats.TxStats.WriteTime) / 1e9)
}
