// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package blb

import (
	"context"
	"math/rand"
	"strings"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/westerndigitalcorporation/blb/pkg/retry"

	"github.com/westerndigitalcorporation/blb/internal/core"
	"github.com/westerndigitalcorporation/blb/internal/server"
	"github.com/westerndigitalcorporation/blb/platform/clustersniff"
)

var (
	clientOpLatenciesSet = promauto.NewSummaryVec(prometheus.SummaryOpts{
		Subsystem: "blb_client",
		Name:      "latencies",
	}, []string{"op", "instance"})
	clientOpSizesSet = promauto.NewSummaryVec(prometheus.SummaryOpts{
		Subsystem: "blb_client",
		Name:      "sizes",
	}, []string{"op", "instance"})
	clientOpBytesSet = promauto.NewCounterVec(prometheus.CounterOpts{
		Subsystem: "blb_client",
		Name:      "bytes",
	}, []string{"op", "instance"})
)

const (
	// ParallelRPCs is the number of read/write rpcs to issue in parallel per
	// call to blob.Read or Write. (Note that this is not a global limit. Each
	// call may issue up to this many RPCs in parallel.)
	ParallelRPCs = 12

	// LookupCacheSize is the number of partitions for which we cache the
	// current curator. We don't expect that many partitions so we might as well
	// cache them all.
	LookupCacheSize = 100

	// TractCacheSize is the number of _blobs_ for which we cache tract
	// locations. (Large blobs may use more space in the cache, there's no
	// weight tracking.)
	TractCacheSize = 100

	// TractLength defines the size of a full tract.
	TractLength = core.TractLength
)

// Options contains configurations of a client.
type Options struct {
	// The cluster this client will connect to. Most users can use a simple
	// cluster name here. This may also be a cluster/user/service string, to
	// fully specify a service discovery record, or a comma-separated list of
	// master hostnames. If empty, we will try to connect to the default cluster
	// (but this is not recommended; try to use an explicit cluster name if
	// possible).
	Cluster string

	// Whether client will retry operations failed due to "retriable" erorr.
	DisableRetry bool

	// RetryTimeout bounds the total time of retries if it's greater than zero.
	RetryTimeout time.Duration

	// Whether client will use cache.
	DisableCache bool

	// An optional label to diffrentiate metrics from differen client instances.
	// It will be "default" if it's not specified.
	Instance string

	// How the client decides whether to attempt client-side RS reconstruction.
	ReconstructBehavior ReconstructBehavior

	// How the client decides to send backup reads if the primary read is delayed.
	BackupReadBehavior BackupReadBehavior
}

// Client exposes a simple interface to Blb users for requesting services and
// handles communications with master, curator and tractservers under the hood.
type Client struct {
	// which cluster this client is connecting to
	cluster string

	// Connection to the primary master.
	master MasterConnection

	// How we talk to curators
	curators CuratorTalker

	// How we talk to tractservers
	tractservers TractserverTalker

	// Cache of partition->curator mapping.
	lookupCache *lookupCache

	// Cache of tract metadata.
	tractCache *tractCache

	// Whether to use cache or not.
	cacheDisabled bool

	// Lock for curators, tractservers, cacheDisabled.
	lock sync.Mutex

	// Retrier for retrying client's operations once they failed.
	retrier retry.Retrier

	// Reconstruct behavior.
	reconstructState reconstructState
	// Backup read behavior.
	backupReadState backupReadState

	// Metrics we collect.
	metricOpen           prometheus.Observer
	metricCreate         prometheus.Observer
	metricReadDurations  prometheus.Observer
	metricReadSizes      prometheus.Observer
	metricReadBytes      prometheus.Counter
	metricWriteDurations prometheus.Observer
	metricWriteSizes     prometheus.Observer
	metricWriteBytes     prometheus.Counter
	metricDelete         prometheus.Observer
	metricUndelete       prometheus.Observer
	metricReconDuration  prometheus.Observer
	metricReconBytes     prometheus.Counter
}

// newBaseClient returns a new Client with common fields initialized, but that isn't
// actually useful yet.
func newBaseClient(options *Options) *Client {
	var retrier retry.Retrier

	// Create a retrier.
	if options.DisableRetry {
		retrier = retry.Retrier{MaxNumRetries: 1}
	} else {
		if options.RetryTimeout == 0 {
			// Give it a default timeout: 30 seconds.
			options.RetryTimeout = 30 * time.Second
		}
		retrier = retry.Retrier{
			MinSleep: 500 * time.Millisecond,
			MaxSleep: options.RetryTimeout,
			MaxRetry: options.RetryTimeout,
		}
	}

	if options.Instance == "" {
		options.Instance = "default"
	}
	if options.Cluster == "" {
		options.Cluster = clustersniff.Cluster()
	}

	return &Client{
		lookupCache:          lookupCacheNew(LookupCacheSize),
		tractCache:           tractCacheNew(TractCacheSize),
		cacheDisabled:        options.DisableCache,
		cluster:              options.Cluster,
		retrier:              retrier,
		reconstructState:     makeReconstructState(options.ReconstructBehavior),
		backupReadState:      makeBackupReadState(options.BackupReadBehavior),
		metricOpen:           clientOpLatenciesSet.WithLabelValues("open", options.Instance),
		metricCreate:         clientOpLatenciesSet.WithLabelValues("create", options.Instance),
		metricReadDurations:  clientOpLatenciesSet.WithLabelValues("read", options.Instance),
		metricWriteDurations: clientOpLatenciesSet.WithLabelValues("write", options.Instance),
		metricDelete:         clientOpLatenciesSet.WithLabelValues("delete", options.Instance),
		metricUndelete:       clientOpLatenciesSet.WithLabelValues("undelete", options.Instance),
		metricReconDuration:  clientOpLatenciesSet.WithLabelValues("reconstruct", options.Instance),
		metricReadSizes:      clientOpSizesSet.WithLabelValues("read", options.Instance),
		metricWriteSizes:     clientOpSizesSet.WithLabelValues("write", options.Instance),
		metricReadBytes:      clientOpBytesSet.WithLabelValues("read", options.Instance),
		metricWriteBytes:     clientOpBytesSet.WithLabelValues("write", options.Instance),
		metricReconBytes:     clientOpBytesSet.WithLabelValues("reconstruct", options.Instance),
	}
}

// NewClient returns a new Client that can be used to interact with a Blob cell.
// You must pass an Option object that contains the configuration of the client.
func NewClient(options Options) *Client {
	cli := newBaseClient(&options)
	cli.master = NewRPCMasterConnection(options.Cluster)
	cli.curators = NewRPCCuratorTalker()
	cli.tractservers = NewRPCTractserverTalker()
	return cli
}

// NewMockClient returns a new in-memory mock client that can only be used for
// testing.
func NewMockClient() *Client {
	options := Options{
		DisableRetry: true,
		DisableCache: true,
	}
	cli := newBaseClient(&options)
	cli.master = newMemMasterConnection([]string{"1", "2", "3"})
	cli.curators = newMemCuratorTalker()
	cli.tractservers = newMemTractserverTalker(nil)
	return cli
}

//----------------------------
// Mechanism for serving user
//----------------------------

const (
	dialTimeout = 5 * time.Second  // timeout for dial to a server
	rpcTimeout  = 10 * time.Second // timeout for a rpc invocation
)

// Create creates a blob with the given options. It will retry the operation internally
// according to the retry policy specified by users if the operation failed due to
// "retriable" errors.
func (cli *Client) Create(opts ...createOpt) (*Blob, error) {
	// Construct options from arguments.
	options := defaultCreateOptions
	options.ctx = context.Background()
	for _, o := range opts {
		o(&options)
	}
	options.ctx = context.WithValue(options.ctx, priorityKey, options.pri)

	var blob *Blob
	var berr core.Error

	st := time.Now()

	cli.retrier.Do(options.ctx, func(seq int) bool {
		log.Infof("create blob with opts %v, attempt #%d", options, seq)
		blob, berr = cli.createOnce(options)
		return !core.IsRetriableError(berr)
	})

	cli.metricCreate.Observe(float64(time.Since(st)) / 1e9)

	return blob, berr.Error()
}

// createOnce creates a blob with the given options.
func (cli *Client) createOnce(options createOptions) (*Blob, core.Error) {
	// Contact master for a proper curator.
	addr, err := cli.master.MasterCreateBlob(options.ctx)
	if core.NoError != err {
		return nil, err
	}

	// Contact the curator for creating a BlobID.
	metadata := core.BlobInfo{
		Repl:    options.repl,
		Hint:    options.hint,
		Expires: options.expires,
	}
	id, err := cli.curators.CreateBlob(options.ctx, addr, metadata)
	if core.NoError != err {
		return nil, err
	}

	if cli.useCache() {
		// A write will usually follow a successful create, we should cache
		// partition->curator mapping so a subsequent write doesn't have to
		// contact master again.
		cli.lookupCache.put(id.Partition(), addr)
	}

	// Create and return a Blob.
	return &Blob{
		cli: cli,
		id:  id,
		// Both reading and writing are allowed on a new blob (it wouldn't be
		// very useful otherwise).
		allowRead:  true,
		allowWrite: true,
		ctx:        options.ctx,
	}, core.NoError
}

// ClusterID returns the cluster this client is connecting to
func (cli *Client) ClusterID() string {
	return cli.cluster
}

// EnableCache turns on/off caching.
func (cli *Client) EnableCache(cacheEnabled bool) {
	cli.lock.Lock()
	defer cli.lock.Unlock()
	cli.cacheDisabled = !cacheEnabled
}

// useCache checks whethe we should use cache or not.
func (cli *Client) useCache() bool {
	cli.lock.Lock()
	defer cli.lock.Unlock()
	return !cli.cacheDisabled
}

// Open opens a blob referenced by 'id'. It will retry the operation internally
// according to the retry policy specified by users if the operation failed due
// to "retriable" errors.
// 'mode' should be a combination of "r" for reading, "w" for writing, and "s"
// for statting.
func (cli *Client) Open(id BlobID, mode string, opts ...openOpt) (*Blob, error) {
	// Construct options from arguments.
	options := defaultOpenOptions
	options.ctx = context.Background()
	for _, o := range opts {
		o(&options)
	}
	options.ctx = context.WithValue(options.ctx, priorityKey, options.pri)

	// Check mode.
	if strings.Trim(mode, "rws") != "" {
		return nil, core.ErrInvalidArgument.Error()
	}

	var blob *Blob
	var berr core.Error

	st := time.Now()

	cli.retrier.Do(options.ctx, func(seq int) bool {
		log.Infof("open blob %v, attempt #%d", id, seq)
		blob, berr = cli.openOnce(options.ctx, core.BlobID(id), mode)
		// Return false if we want to retry this operation, true otherwise.
		// Ditto for all the code below.
		return !core.IsRetriableError(berr)
	})

	cli.metricOpen.Observe(float64(time.Since(st)) / 1e9)

	return blob, berr.Error()
}

// openOnce opens a blob referenced by 'id'.
func (cli *Client) openOnce(ctx context.Context, id core.BlobID, mode string) (*Blob, core.Error) {
	allowRead := strings.Contains(mode, "r")
	allowWrite := strings.Contains(mode, "w")

	addr, lookupWasCached, err := cli.lookup(ctx, id.Partition())
	if core.NoError != err {
		return nil, err
	}

	// Do a non-cached read on tract metadata. This isn't technically necessary,
	// but it ensures that we contact the curator directly in the next step so
	// that we really do return an error here if the blob has been deleted or
	// is unreachable.
	tracts, err := cli.curators.GetTracts(ctx, addr, id, 0, 0, allowRead, allowWrite)
	if err != core.NoError {
		if lookupWasCached {
			// Maybe we're talking to the wrong curator.
			cli.lookupCache.invalidate(id.Partition())
			return cli.openOnce(ctx, id, mode)
		}
		return nil, err
	}
	if cli.useCache() {
		// We still want to cache the first tract so a subsequent access on the
		// first tract of the blob doesn't need to talk to curator.
		cli.tractCache.put(id, tracts)
	}

	// Create and return a Blob.
	return &Blob{
		cli:        cli,
		id:         id,
		allowRead:  allowRead,
		allowWrite: allowWrite,
		ctx:        ctx,
	}, core.NoError
}

// Delete deletes 'blob'. It will retry the operation internally according to
// the retry policy specified by users if the operation failed due to
// "retriable" errors.
func (cli *Client) Delete(ctx context.Context, id BlobID) error {
	var berr core.Error

	st := time.Now()

	cli.retrier.Do(ctx, func(seq int) bool {
		log.Infof("delete blob %v, attempt #%d", id, seq)
		berr = cli.deleteOnce(ctx, core.BlobID(id))
		return !core.IsRetriableError(berr)
	})

	cli.metricDelete.Observe(float64(time.Since(st)) / 1e9)

	return berr.Error()
}

// deleteOnce deletes 'blob'.
func (cli *Client) deleteOnce(ctx context.Context, blob core.BlobID) core.Error {
	log.V(1).Infof("delete blob %d", blob)

	addr, lookupWasCached, err := cli.lookup(ctx, blob.Partition())
	if core.NoError != err {
		return err
	}
	err = cli.curators.DeleteBlob(ctx, addr, blob)
	if err != core.NoError && lookupWasCached {
		// Maybe we're talking to the wrong curator.
		cli.lookupCache.invalidate(blob.Partition())
		return cli.deleteOnce(ctx, blob)
	}
	return err
}

// Undelete undeletes 'blob'.
func (cli *Client) Undelete(ctx context.Context, id BlobID) error {
	var berr core.Error

	st := time.Now()

	cli.retrier.Do(ctx, func(seq int) bool {
		log.Infof("undelete blob %v, attempt #%d", id, seq)
		berr = cli.undeleteOnce(ctx, core.BlobID(id))
		return !core.IsRetriableError(berr)
	})

	cli.metricUndelete.Observe(float64(time.Since(st)) / 1e9)

	return berr.Error()
}

// undeleteOnce undeletes 'blob'.
func (cli *Client) undeleteOnce(ctx context.Context, blob core.BlobID) core.Error {
	log.V(1).Infof("undelete blob %d", blob)

	addr, lookupWasCached, err := cli.lookup(ctx, blob.Partition())
	if core.NoError != err {
		return err
	}
	err = cli.curators.UndeleteBlob(ctx, addr, blob)
	if err != core.NoError && lookupWasCached {
		// Maybe we're talking to the wrong curator.
		cli.lookupCache.invalidate(blob.Partition())
		return cli.undeleteOnce(ctx, blob)
	}
	return err
}

// GetTracts returns a subset of the tracts for the blob. It will retry the
// operation internally according to the retry policy specified by users if the
// operation failed due to "retriable" errors.
func (cli *Client) GetTracts(ctx context.Context, id BlobID, start, end int) ([]core.TractInfo, error) {
	var tracts []core.TractInfo
	var berr core.Error

	cli.retrier.Do(ctx, func(seq int) bool {
		log.Infof("get tracts [%d, %d) on blob %v, attempt #%d", start, end, id, seq)
		tracts, berr = cli.getTractsOnce(ctx, core.BlobID(id), start, end)
		return !core.IsRetriableError(berr)
	})

	return tracts, berr.Error()
}

// getTractsOnce returns a subset of the tracts for the blob.
func (cli *Client) getTractsOnce(ctx context.Context, id core.BlobID, start, end int) ([]core.TractInfo, core.Error) {
	log.V(1).Infof("get tracts on blob %d from %d to %d", id, start, end)

	addr, lookupWasCached, err := cli.lookup(ctx, id.Partition())
	if core.NoError != err {
		return nil, err
	}

	tracts, tractsWereCached, err := cli.getTracts(ctx, addr, id, start, end)
	if core.NoError != err {
		if lookupWasCached || tractsWereCached {
			// Maybe we're talking to the wrong curator.
			cli.lookupCache.invalidate(id.Partition())
			// This shouldn't be cached, but it could theoretically happen if
			// two threads call GetTracts at the same time. Invalidate again just to
			// be sure.
			cli.tractCache.invalidate(id)
			return cli.getTractsOnce(ctx, id, start, end)
		}
		return nil, err
	}

	return tracts, err
}

// SetMetadata allows changing various fields of blob metadata. Currently
// changing the storage hint, mtime, atime are supported.
func (cli *Client) SetMetadata(ctx context.Context, id BlobID, metadata core.BlobInfo) error {
	var berr core.Error
	cli.retrier.Do(ctx, func(seq int) bool {
		log.Infof("set metadata on %s: %+v, attempt #%d", id, metadata, seq)
		berr = cli.setMetadataOnce(ctx, core.BlobID(id), metadata)
		return !core.IsRetriableError(berr)
	})
	return berr.Error()
}

func (cli *Client) setMetadataOnce(ctx context.Context, blob core.BlobID, metadata core.BlobInfo) core.Error {
	log.V(1).Infof("setmetadata on %s: %v", blob, metadata)
	addr, lookupWasCached, err := cli.lookup(ctx, blob.Partition())
	if core.NoError != err {
		return err
	}
	err = cli.curators.SetMetadata(ctx, addr, blob, metadata)
	if err != core.NoError && lookupWasCached {
		// Maybe we're talking to the wrong curator.
		cli.lookupCache.invalidate(blob.Partition())
		return cli.setMetadataOnce(ctx, blob, metadata)
	}
	return err
}

// ListBlobs returns an iterator that lists all existing blobs, in batches.
// Clients should keep calling the iterator until it return nil, or an error.
// The iterator should be used in a single-threaded manner.
//
// ListBlobs is not guaranteed to return all blobs if cluster membership or raft
// leadership changes during iteration. It's intended for informative and
// diagnostic use only.
func (cli *Client) ListBlobs(ctx context.Context) BlobIterator {
	bi := &blobIterator{cli: cli, ctx: ctx}
	return bi.next
}

// BlobIterator is a function that returns BlobIDs in batches.
type BlobIterator func() ([]BlobID, error)

//----------------
// Helper methods
//----------------

// getNextRange is a helper function to split a byte slice, conceptually
// positioned at a given offset in a blob, that may cross tract boundaries, into
// ranges that do not cross tract boundaries.
//
// Call it repeatedly with the whole range and offset, and a pointer to an int
// representing the number of bytes returned so far (initialized to zero).
func (cli *Client) getNextRange(b []byte, offset int64, position *int) (thisB []byte, thisOffset int64) {
	pos := *position
	thisOffset = (offset + int64(pos)) % core.TractLength
	thisLen := int(core.TractLength - thisOffset)
	if thisLen > (len(b) - pos) {
		thisLen = (len(b) - pos)
	}
	thisB = b[pos : pos+thisLen]
	*position += thisLen
	return
}

// extendTo extends the given blob, by possibly creating empty tracts, to
// accomodate futher writes upto 'offset'.
//
// NOTE: This can be possibly used to pre-allocate empty tracts so that multiple
// clients can write to different tracts in the same blob simultaneously w/o
// conflicts. A single client should use 'writeAt' which will create empty
// tracts on demand.
func (cli *Client) extendTo(ctx context.Context, id core.BlobID, offset int64) core.Error {
	if offset < 0 {
		return core.ErrInvalidArgument
	}

	// We want to cover tracts in [0, end).
	end := int((offset + core.TractLength - 1) / core.TractLength)

	// Contact the curator for the blob stat. Extend the blob if there are
	// not enough existing tracts.
	curatorAddr, info, err := cli.statBlob(ctx, id)
	if core.NoError != err {
		return err
	}

	if info.NumTracts >= end {
		return core.NoError
	}

	return cli.createEmptyTracts(ctx, id, info.NumTracts, end, curatorAddr)
}

// writeAt writes 'len(b)' bytes from 'b' to 'blob' at 'offset'. It returns the
// number of bytes written from 'b' and any error encountered that caused the
// write to stop early.
func (cli *Client) writeAt(ctx context.Context, id core.BlobID, b []byte, offset int64) (int, core.Error) {
	if offset < 0 {
		return 0, core.ErrInvalidArgument
	}
	if len(b) == 0 {
		return 0, core.NoError
	}

	// We want to write tracts in [start, end).
	start := int(offset / core.TractLength)
	end := int((offset + int64(len(b)) + core.TractLength - 1) / core.TractLength)

	// Contact the curator for the blob stat. Extend the blob if there are
	// not enough existing tracts for the write.
	// TODO: we shouldn't stat every time here: cache info.NumTracts
	curatorAddr, info, err := cli.statBlob(ctx, id)
	if core.NoError != err {
		return 0, err
	}

	// Can only write to replicated blobs. We shouldn't get here, since the
	// curator should return an error to the initial GetTracts call, but this is
	// here for defense in depth.
	if info.Class != core.StorageClass_REPLICATED {
		return 0, core.ErrReadOnlyStorageClass
	}

	// For existing tracts, write directly.
	var writePos int
	if start < info.NumTracts {
		if writePos, err = cli.writeExistingTracts(ctx, id, start, min(end, info.NumTracts), b, offset, curatorAddr); err != core.NoError {
			return 0, err
		}
		start = info.NumTracts
		b = b[writePos:]
		offset += int64(writePos)
	}

	// Create new tracts if necessary.
	var createPos int
	if end > info.NumTracts {
		// Might need to create empty tracts (hole) in [info.NumTracts, start).
		if start > info.NumTracts {
			if err = cli.createEmptyTracts(ctx, id, info.NumTracts, start, curatorAddr); err != core.NoError {
				return 0, err
			}
		}
		// Create and write tracts in [start, end).
		createPos, err = cli.createWriteTracts(ctx, id, start, end, b, offset, curatorAddr)
	}
	return writePos + createPos, err
}

// Write bytes to existing tracts in the range [start, end) from the give blob.
// The caller needs to make sure that these tracts exist in curator's durable
// state and thus can be returned by getTracts call; otherwise error will be
// returned.
func (cli *Client) writeExistingTracts(
	ctx context.Context,
	id core.BlobID,
	start int,
	end int,
	b []byte,
	offset int64,
	curatorAddr string) (int, core.Error) {

	// Contact the curator for the TractInfo's.
	tracts, tractsWereCached, err := cli.getTracts(ctx, curatorAddr, id, start, end)
	if core.NoError != err {
		return 0, err
	}
	// Check that we got as many tracts as we asked for.
	if len(tracts) < end-start {
		log.Errorf("curator didn't return enough tracts: %d < %d", len(tracts), end-start)
		return 0, core.ErrNoSuchTract
	}

	// Check for the same replication factor across all tracts.
	repl := len(tracts[0].Hosts)
	for _, tract := range tracts {
		if len(tract.Hosts) != repl {
			log.Errorf("host set length mismatch for tract %s", tract.Tract)
			return 0, core.ErrInvalidState
		}
		// We also need them all to be present.
		for j, host := range tract.Hosts {
			if host == "" {
				log.Errorf("missing host for tsid %d writing tract %s", tract.TSIDs[j], tract.Tract)
				return 0, core.ErrHostNotExist
			}
		}
	}

	// We set up an array of Errors for results and each goroutine writes into
	// its own slot. This is simpler than using a channel and is closer to how
	// readAt works. The WaitGroup is used to block the caller on the completion
	// of all goroutines and the Semaphore is used to limit the concurrency.
	sem := server.NewSemaphore(ParallelRPCs)
	results := make([]core.Error, repl*len(tracts))
	var wg sync.WaitGroup

	// Fire off a bunch of rpcs in parallel.
	var position, idx int
	for _, tract := range tracts {
		thisB, thisOffset := cli.getNextRange(b, offset, &position)
		for _, host := range tract.Hosts {
			wg.Add(1)
			go cli.writeOneTract(ctx, &results[idx], &wg, sem, tract, host, thisB, thisOffset)
			idx++
		}
	}
	wg.Wait()

	// The write only succeeds if every tract write succeeded.
	for i := 0; i < len(results); i++ {
		// This tract is OK.
		if results[i] == core.NoError {
			continue
		}

		// Something went wrong.  We can take steps to recover.

		// Maybe we got older cached tracts.
		// PL-1153: This will retry the whole write operation after talking to
		// the curator again. Ideally we would only retry the tracts that
		// failed.
		if tractsWereCached {
			cli.tractCache.invalidate(id)
			return cli.writeExistingTracts(ctx, id, start, end, b, offset, curatorAddr)
		}

		// We couldn't talk to one of the tractservers, and we're pretty sure that the
		// metadata was fresh as it wasn't cached.  We can ask the curator to consider
		// re-replicating data.
		// PL-1154: We should collect all these ReportBadTS and FixVersion calls
		// and put them in a queue and send them in batches, at a throttled rate.
		if results[i] == core.ErrRPC {
			tractNo := i / repl
			replicaNo := i % repl
			id := tracts[tractNo].Tract
			host := tracts[tractNo].Hosts[replicaNo]
			log.Errorf("rpc error writing tract %s to replica on tractserver at address %s", id, host)
			cli.curators.ReportBadTS(context.Background(), curatorAddr, id, host, "write", results[i], false)
		} else if results[i] == core.ErrVersionMismatch {
			tract := tracts[i/repl]
			host := tract.Hosts[i%repl]
			log.Errorf("version mismatch when writing tract %v to tractserver at %s", tract, host)
			cli.curators.FixVersion(context.Background(), curatorAddr, tract, host)
		}

		// For now, just assume nothing written if anything failed.
		// PL-1153: Return partial writes if a nonzero prefix succeeded.
		return 0, results[i]
	}
	return position, core.NoError
}

// Create empty tracts in the range [start, end) from the given blob . The
// caller needs to make sure that these tracts don't already exist in curator's
// durable state yet; otherwise error will be returned.
func (cli *Client) createEmptyTracts(
	ctx context.Context,
	id core.BlobID,
	start int,
	end int,
	curatorAddr string) core.Error {

	// Contact curator to allocate tractservers.
	var newTracts []core.TractInfo
	var err core.Error
	if newTracts, err = cli.curators.ExtendBlob(ctx, curatorAddr, id, end); core.NoError != err {
		return err
	}

	// Check that we got as many tracts as we asked for.
	if len(newTracts) < end-start {
		log.Errorf("curator didn't return enough tracts: %d < %d", len(newTracts), end-start)
		return core.ErrNoSuchTract
	}

	// Check for the same replication factor across all tracts.
	repl := len(newTracts[0].Hosts)
	for _, tract := range newTracts {
		if len(tract.Hosts) != repl {
			log.Fatalf("host set length mismatch for tract %s", tract.Tract)
			return core.ErrInvalidState
		}
	}

	// We set up an array of Errors for results and each goroutine writes into
	// its own slot. This is simpler than using a channel and is closer to how
	// readAt works. The WaitGroup is used to block the caller on the completion
	// of all goroutines and the Semaphore is used to limit the concurrency.
	sem := server.NewSemaphore(ParallelRPCs)
	results := make([]core.Error, repl*len(newTracts))
	var wg sync.WaitGroup

	var idx int
	for _, tract := range newTracts {
		for i, host := range tract.Hosts {
			wg.Add(1)
			go cli.createOneTract(ctx, &results[idx], &wg, sem, tract, host, tract.TSIDs[i], nil, 0)
			idx++
		}
	}
	wg.Wait()

	// The creation only succeeds if every tract creation succeeded.
	for _, err := range results {
		if err != core.NoError {
			return err
		}
	}

	// Ack the success of extend to curator.
	if err = cli.curators.AckExtendBlob(ctx, curatorAddr, id, newTracts); core.NoError != err {
		log.Errorf("failed to ack extending blob %s with new tracts %+v: %s", id, newTracts, err)
	}
	return err
}

// Create new tracts in the range [start, end) from the given blob and write
// bytes to them. The caller needs to make sure that these tracts don't already
// exist in curator's durable state yet; otherwise error will be returned.
func (cli *Client) createWriteTracts(
	ctx context.Context,
	id core.BlobID,
	start int,
	end int,
	b []byte,
	offset int64,
	curatorAddr string) (int, core.Error) {

	// Contact curator to allocate tractservers.
	var newTracts []core.TractInfo
	var err core.Error
	if newTracts, err = cli.curators.ExtendBlob(ctx, curatorAddr, id, end); core.NoError != err {
		return 0, err
	}

	// Check that we got as many tracts as we asked for.
	if len(newTracts) < end-start {
		log.Errorf("curator didn't return enough tracts: %d < %d", len(newTracts), end-start)
		return 0, core.ErrNoSuchTract
	}

	// Check for the same replication factor across all tracts.
	repl := len(newTracts[0].Hosts)
	for _, tract := range newTracts {
		if len(tract.Hosts) != repl {
			log.Fatalf("host set length mismatch for tract %s", tract.Tract)
			return 0, core.ErrInvalidState
		}
	}

	// We set up an array of Errors for results and each goroutine writes into
	// its own slot. This is simpler than using a channel and is closer to how
	// readAt works. The WaitGroup is used to block the caller on the completion
	// of all goroutines and the Semaphore is used to limit the concurrency.
	sem := server.NewSemaphore(ParallelRPCs)
	results := make([]core.Error, repl*len(newTracts))
	var wg sync.WaitGroup

	// Fire off a bunch of rpcs in parallel.
	var position, idx int
	for _, tract := range newTracts {
		thisB, thisOffset := cli.getNextRange(b, offset, &position)
		for i, host := range tract.Hosts {
			wg.Add(1)
			go cli.createOneTract(ctx, &results[idx], &wg, sem, tract, host, tract.TSIDs[i], thisB, thisOffset)
			idx++
		}
	}
	wg.Wait()

	// The write only succeeds if every tract write succeeded.
	for _, err := range results {
		if err != core.NoError {
			return 0, err
		}
	}

	// Ack the success of extend to curator.
	if err := cli.curators.AckExtendBlob(ctx, curatorAddr, id, newTracts); core.NoError != err {
		log.Errorf("failed to ack extending blob %s with new tracts %+v: %s", id, newTracts, err)
		return 0, err
	}

	return position, core.NoError
}

func (cli *Client) writeOneTract(
	ctx context.Context,
	result *core.Error,
	wg *sync.WaitGroup,
	sem server.Semaphore,
	tract core.TractInfo,
	host string,
	thisB []byte,
	thisOffset int64) {

	defer wg.Done()

	sem.Acquire()
	defer sem.Release()

	reqID := core.GenRequestID()
	*result = cli.tractservers.Write(ctx, host, reqID, tract.Tract, tract.Version, thisB, thisOffset)

	log.V(1).Infof("write %s to %s: %s", tract.Tract, host, *result)
}

func (cli *Client) createOneTract(
	ctx context.Context,
	result *core.Error,
	wg *sync.WaitGroup,
	sem server.Semaphore,
	tract core.TractInfo,
	host string,
	tsid core.TractserverID,
	thisB []byte,
	thisOffset int64) {

	defer wg.Done()

	sem.Acquire()
	defer sem.Release()

	reqID := core.GenRequestID()
	*result = cli.tractservers.Create(ctx, host, reqID, tsid, tract.Tract, thisB, thisOffset)

	log.V(1).Infof("create %s to %s: %s", tract.Tract, host, *result)
}

type tractResult struct {
	wanted         int        // how much wanted to read
	read           int        // how much was read
	err            core.Error // any error
	badVersionHost string     // last host that we got an ErrVersionMismatch from
	// Note that we can set a badVersionHost for some host even if we return no
	// overall error because another host succeeded.
}

// readAt reads up to 'len(b)' bytes from 'blob' into 'b' at 'offset'. It
// returns the number of bytes read and any error encountered.
func (cli *Client) readAt(ctx context.Context, id core.BlobID, b []byte, offset int64) (int, core.Error) {
	if offset < 0 {
		return 0, core.ErrInvalidArgument
	} else if len(b) == 0 {
		return 0, core.NoError
	}

	// We want to read tracts in [start, end).
	start := int(offset / core.TractLength)
	end := int((offset + int64(len(b)) + core.TractLength - 1) / core.TractLength)

	// Get the curator address.
	addr, curatorWasCached, err := cli.lookup(ctx, id.Partition())
	if core.NoError != err {
		return 0, err
	}

	// Contact the curator for the blob stat.
	// We will try to get tracts in [start, end+1). The extra tract is used
	// to check if tracts in [start, end) include the last one in the blob.
	tracts, tractsWereCached, err := cli.getTracts(ctx, addr, id, start, end+1)
	if core.NoError != err {
		if err == core.ErrNoSuchTract {
			// This means the blob exists but the required tracts don't exist
			// in curator's state. Because we don't allow tracts truncation, this
			// means we must have read past the last tract, return EOF error
			// directly.
			return 0, core.ErrEOF
		}
		if curatorWasCached {
			// Maybe we're talking to the wrong curator.
			cli.lookupCache.invalidate(id.Partition())
			return cli.readAt(ctx, id, b, offset)
		}
		return 0, err
	}

	// If the blob has no tracts in this range, we're at the end.
	if len(tracts) == 0 {
		return 0, core.ErrEOF
	}

	var padAll bool // If we need to pad all short-read tracts.

	// If the number of returned tracts is equal to what we asked for,
	// end+1-start (including the extra tract), it means that all tracts in
	// the range [start, end) are not the last tract in the blob. Therefore,
	// we will need to pad the read results if short read happens to any of
	// them.
	//
	// We will remove the extra tract before actually pulling data from them.
	if len(tracts) == end+1-start {
		padAll = true
		tracts = tracts[:len(tracts)-1]
	} else {
		// The number of returned tract is less than what we asked for
		// (cannot be more than that). It means that the range [start,
		// end) includes the last tract in the blob. We should not pad
		// the last tract in case of short read (still need to pad
		// others in case of short read).
		//
		// The extra tract is not included so we don't need to remove
		// anything from the returned slice.
		padAll = false
	}

	// We create an array of results and let each goroutine write into one of
	// them, in addition to writing directly into the slice of b that it's
	// responsible for. The result here includes the length as well as the error
	// code. We use an array instead of passing the results on a channel because
	// we need to examine them in order to handle EOFs and short reads.
	sem := server.NewSemaphore(ParallelRPCs)
	results := make([]tractResult, len(tracts))
	var wg sync.WaitGroup
	position := 0

	// Fire off a goroutine to position each tract.
	for idx := range tracts {
		thisB, thisOffset := cli.getNextRange(b, offset, &position)
		wg.Add(1)
		go cli.readOneTract(ctx, addr, &results[idx], &wg, sem, &tracts[idx], thisB, thisOffset)
	}

	wg.Wait()

	// Figure out how much succeeded.
	read := 0
	for i, res := range results {
		err = res.err
		if err == core.NoError {
			read += res.read
		} else if err == core.ErrEOF {
			// Any tract can be a short read at the tractserver end.
			// However, the client will pad those that are not the last
			// tract in the blob.
			//
			// For the last tract, the lengths of the bytes returned
			// by the client is equal to that from the tractserver.
			if i == len(results)-1 && !padAll {
				read += res.read
				break
			}
			// For previous tracts, short read means there is a hole
			// in the tract. As the client pads them with 0's (see
			// 'readOneTract'), the lengths of the bytes returned by
			// the client is equal to how much we wanted to read
			// from the tractserver. This is not EOF so we will
			// reset the error here.
			read += res.wanted
			err = core.NoError
		} else {
			break
		}
	}

	if err != core.NoError && err != core.ErrEOF && tractsWereCached {
		// Maybe we got older cached tracts.
		cli.tractCache.invalidate(id)
		return cli.readAt(ctx, id, b, offset)
	}

	// Maybe kick off FixVersion rpcs. Do this after the cache invalidate/retry
	// since we don't want to do extra work if the only reason for version
	// mismatches is that we have out of date cached data.
	wg = sync.WaitGroup{}
	for i, res := range results {
		tract := tracts[i]
		// For replicated tracts, we might want to do a FixVersion if someone reported a wrong version.
		if res.badVersionHost == "" {
			continue
		}
		log.Infof("got version mismatch when reading tract %s from host %s, requesting FixVersion",
			tract.Tract, res.badVersionHost)
		var doneWg *sync.WaitGroup
		if res.err == core.ErrVersionMismatch {
			// If we failed to read this tract because none of the
			// tractservers had the right version, then we should wait on
			// this FixVersion call. Otherwise we can kick it off and forget
			// about it.
			doneWg = &wg
			doneWg.Add(1)
		}
		go func(badHost string, tract core.TractInfo) {
			cli.curators.FixVersion(context.Background(), addr, tract, badHost)
			if doneWg != nil {
				doneWg.Done()
			}
		}(res.badVersionHost, tract)
	}
	wg.Wait()

	return read, err
}

func (cli *Client) readOneTract(
	ctx context.Context,
	curAddr string,
	result *tractResult,
	wg *sync.WaitGroup,
	sem server.Semaphore,
	tract *core.TractInfo,
	thisB []byte,
	thisOffset int64) {

	defer wg.Done()

	sem.Acquire()
	defer sem.Release()

	if len(tract.Hosts) > 0 {
		cli.readOneTractReplicated(ctx, curAddr, result, tract, thisB, thisOffset)
	} else if tract.RS.Present() {
		cli.readOneTractRS(ctx, curAddr, result, tract, thisB, thisOffset)
	} else {
		*result = tractResult{err: core.ErrInvalidState}
	}
}

// errorResult carries state to deterime if a failed tract read should
// report a bad TS to the curator.
type errorResult struct {
	tractID core.TractID
	host    string
	err     core.Error
}

func (cli *Client) readOneTractReplicated(
	ctx context.Context,
	curAddr string,
	result *tractResult,
	tract *core.TractInfo,
	thisB []byte,
	thisOffset int64) {

	var badVersionHost string

	reqID := core.GenRequestID()
	order := rand.Perm(len(tract.Hosts))

	err := core.ErrAllocHost // default error if none present
	if cli.backupReadState.BackupReadBehavior.Enabled {
		ch := make(chan tractResult)

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		maxNumBackups := cli.backupReadState.BackupReadBehavior.MaxNumBackups
		delay := cli.backupReadState.BackupReadBehavior.Delay

		// report bad tractservers
		readFunc := func(n int) {
			host := tract.Hosts[n]
			if host == "" {
				log.V(1).Infof("read %s from tsid %d: no host", tract.Tract, tract.TSIDs[n])
				ch <- tractResult{0, 0, err, badVersionHost}
				return
			}

			// TODO(eric): fill in otherHosts when ts-ts cancellation is done.
			read, err := cli.tractservers.ReadInto(ctx, host, nil, reqID, tract.Tract, tract.Version, thisB, thisOffset)
			if err == core.ErrVersionMismatch {
				badVersionHost = host
			}
			if err != core.NoError && err != core.ErrEOF {
				// TODO(eric): Redo ts reporting.
				ch <- tractResult{0, 0, err, badVersionHost}
				return
			}
			log.V(1).Infof("read %s from tractserver at address %s: %s", tract.Tract, host, err)
			// In case of short read, i.e., read < len(thisB), we should
			// pad the result with 0's. This doesn't need to be done
			// explicitly if 'thisB' given to us starts to be empty.
			// However, just in case it's not, we will ensure that by
			// overwritting untouched bytes with 0's.
			for i := read; i < len(thisB); i++ {
				thisB[i] = 0
			}
			ch <- tractResult{len(thisB), read, err, badVersionHost}
			return
		}

		// Backup request logic involves the following:
		// 1) Send the first read to the first host (item 0 available from the orderCh) in a goroutine.
		// 2) Start another goroutine to issue backup requests. Each subsequent request is spawned after the
		// configured backup time.After delay time expires.
		// 3) Block until the first read is returned from the goroutines servicing the reads. Note that if the
		// the request returned fails, we accept that and continume on to the fallback phase for sequential reads.
		// We could optimize this later and scan for the first successful backup request, but that may not be
		// worth the latency savings.
		go func() {
			nb := min(maxNumBackups+1, len(order))
			for i := 0; i < nb; i++ {
				select {
				case <-cli.backupReadState.backupDelayFunc(time.Duration(i) * delay):
					go readFunc(order[i])
				case <-ctx.Done():
					log.V(1).Infof("backup read %s-%d to tractserver cancelled", tract.Tract, i)
					break
				}
			}
		}()

		// take the first result.
		*result = <-ch
		if result.err == core.NoError {
			return
		}
	}

	// Sequentailly read from tract server replicas. This phase will run if backups are disabled from config state, or
	// the backup read phase fails and we use this as a fallback.
	for n := range order {
		host := tract.Hosts[n]
		if host == "" {
			log.V(1).Infof("read %s from tsid %d: no host", tract.Tract, tract.TSIDs[n])
			continue
		}

		var read int
		read, err = cli.tractservers.ReadInto(ctx, host, nil, reqID, tract.Tract, tract.Version, thisB, thisOffset)
		if err == core.ErrVersionMismatch {
			badVersionHost = host
		}
		if err != core.NoError && err != core.ErrEOF {
			log.V(1).Infof("read %s from tractserver at address %s: %s", tract.Tract, host, err)
			// TODO(eric): redo bad TS reporting mechanism.
			continue // try another host
		}
		log.V(1).Infof("read %s from tractserver at address %s: %s", tract.Tract, host, err)
		// In case of short read, i.e., read < len(thisB), we should
		// pad the result with 0's. This doesn't need to be done
		// explicitly if 'thisB' given to us starts to be empty.
		// However, just in case it's not, we will ensure that by
		// overwritting untouched bytes with 0's.
		for i := read; i < len(thisB); i++ {
			thisB[i] = 0
		}
		*result = tractResult{len(thisB), read, err, badVersionHost}
		return
	}

	log.V(1).Infof("read %s all hosts failed", tract.Tract)
	*result = tractResult{0, 0, err, badVersionHost}
}

func (cli *Client) readOneTractRS(
	ctx context.Context,
	curAddr string,
	result *tractResult,
	tract *core.TractInfo,
	thisB []byte,
	thisOffset int64) {

	rsTract := tract.RS.Chunk.ToTractID()
	length := min(len(thisB), int(tract.RS.Length))
	offset := int64(tract.RS.Offset) + thisOffset
	reqID := core.GenRequestID()
	read, err := cli.tractservers.ReadInto(ctx, tract.RS.Host, nil, reqID, rsTract, core.RSChunkVersion, thisB[:length], offset)

	if err != core.NoError && err != core.ErrEOF {
		log.V(1).Infof("rs read %s from tractserver at address %s: %s", tract.Tract, tract.RS.Host, err)
		// If we failed to read from a TS, report that to the curator. Defer so
		// we can examine result.err at the end, to see if we recovered or not.
		defer func(err core.Error) {
			couldRecover := result.err == core.NoError || result.err == core.ErrEOF
			go cli.curators.ReportBadTS(context.Background(), curAddr, rsTract, tract.RS.Host, "read", err, couldRecover)
		}(err)
		if !cli.shouldReconstruct(tract) {
			*result = tractResult{len(thisB), 0, err, ""}
			return
		}
		// Let's try reconstruction. We're already recording metrics for this as a read, but we
		// should keep separate metrics for client-side recovery, since it's a heavyweight process.
		st := time.Now()
		cli.reconstructOneTract(ctx, result, tract, thisB, offset, length)
		cli.metricReconDuration.Observe(float64(time.Since(st)) / 1e9)
		cli.metricReconBytes.Add(float64(result.read))
		return
	}

	log.V(1).Infof("rs read %s from tractserver at address %s: %s", tract.Tract, tract.RS.Host, err)
	for i := read; i < len(thisB); i++ {
		thisB[i] = 0 // Pad with zeros. See comment in readOneTractReplicated.
	}

	// Even if the tractserver doesn't think this was an EOF (because it was
	// packed with other data), we might have requested less than the caller
	// asked for. That counts as an EOF too.
	if int(tract.RS.Length) < len(thisB) {
		err = core.ErrEOF
	}

	*result = tractResult{len(thisB), read, err, ""}
}

// statBlob returns the corresponding curator and number of tracts for a given blob.
// Error is returned if any.
func (cli *Client) statBlob(ctx context.Context, id core.BlobID) (addr string, info core.BlobInfo, err core.Error) {
	var wasCached bool
	addr, wasCached, err = cli.lookup(ctx, id.Partition())
	if err != core.NoError {
		return
	}
	info, err = cli.curators.StatBlob(ctx, addr, id)
	if err != core.NoError && wasCached {
		// Maybe we're talking to the wrong curator.
		cli.lookupCache.invalidate(id.Partition())
		return cli.statBlob(ctx, id)
	}
	return
}

// statTract returns the length of the given tract. tract should be the result
// of a GetTracts call. Error is returned if any.
func (cli *Client) statTract(ctx context.Context, tract core.TractInfo) (int64, core.Error) {
	if tract.RS.Present() {
		return int64(tract.RS.Length), core.NoError
	}

	order := rand.Perm(len(tract.Hosts))

	err := core.ErrAllocHost // default error if none present
	for _, n := range order {
		host := tract.Hosts[n]
		if host == "" {
			continue
		}
		var res int64
		res, err = cli.tractservers.StatTract(ctx, host, tract.Tract, tract.Version)
		if err != core.NoError {
			log.V(1).Infof("stat %s from %s: %s", tract.Tract, host, err)
			continue // try another host
		}
		return res, core.NoError
	}

	log.V(1).Infof("stat %s: all hosts failed", tract.Tract)
	return 0, err // return last error
}

// ByteLength returns the length of the blob in bytes.
func (cli *Client) byteLength(ctx context.Context, id core.BlobID) (int64, core.Error) {
	curator, info, err := cli.statBlob(ctx, id)
	if err != core.NoError {
		return 0, err
	}
	if info.NumTracts == 0 {
		return 0, core.NoError
	}

	tracts, tractsWereCached, err := cli.getTracts(ctx, curator, id, info.NumTracts-1, info.NumTracts)
	if err != core.NoError || len(tracts) != 1 {
		return 0, err
	}

	lastLen, err := cli.statTract(ctx, tracts[0])
	if err != core.NoError {
		if tractsWereCached {
			// Maybe we're talking to the wrong tractserver.
			cli.tractCache.invalidate(id)
			return cli.byteLength(ctx, id)
		}
		return 0, err
	}

	return int64(info.NumTracts-1)*core.TractLength + lastLen, core.NoError
}

// lookup returns a curator address for the given partition. The blob->curator
// mapping may be cached, and that will be indicated in the return value. If the
// result was cached and the caller gets an error talking to the returned
// curator, the caller should call invalidate and try again.
func (cli *Client) lookup(ctx context.Context, id core.PartitionID) (addr string, wasCached bool, err core.Error) {
	useCache := cli.useCache()
	if useCache {
		// Check cache if allowed.
		addr, wasCached = cli.lookupCache.get(id)
	}

	if !wasCached {
		// Do the rpc.
		addr, err = cli.master.LookupPartition(ctx, id)
		if err != core.NoError {
			return "", false, err
		}
		if useCache {
			// Insert into cache.
			cli.lookupCache.put(id, addr)
		}
	}
	return
}

// getTracts returns tract info for the given tracts. It may return data from a cache.
func (cli *Client) getTracts(ctx context.Context, addr string, id core.BlobID, start, end int) (tracts []core.TractInfo, wasCached bool, err core.Error) {
	useCache := cli.useCache()
	if useCache {
		// Check cache if allowed.
		tracts, wasCached = cli.tractCache.get(id, start, end)
	}

	if !wasCached {
		// Do the rpc.
		// We can use `false, false` here because Open does an uncached
		// GetTracts call with the correct read/write intention.
		tracts, err = cli.curators.GetTracts(ctx, addr, id, start, end, false, false)
		if err != core.NoError {
			return nil, false, err
		}
		if useCache {
			// Insert into cache.
			cli.tractCache.put(id, tracts)
		}
	}
	return
}

type blobIterator struct {
	cli           *Client
	thisPartition core.PartitionID
	partitions    []core.PartitionID
	curator       string
	start         core.BlobKey

	// Conceptually, an iteration is a single "request", so we use one context throughout.
	ctx context.Context
}

func (bi *blobIterator) next() (ids []BlobID, err error) {
	var berr core.Error

	if bi.partitions == nil {
		// Need to get partitions from master.
		bi.cli.retrier.Do(bi.ctx, func(seq int) bool {
			log.Infof("ListPartitions, attempt #%d", seq)
			bi.partitions, berr = bi.cli.master.ListPartitions(bi.ctx)
			return !core.IsRetriableError(berr)
		})
		if berr != core.NoError {
			return nil, berr.Error()
		}
	}

	if len(bi.partitions) == 0 && bi.thisPartition == 0 {
		// No more partitions, return nil to signal end.
		return nil, nil
	}

	curatorWasCached := false
	doLookup := false

	if bi.thisPartition == 0 {
		// Check next partition.
		bi.thisPartition = bi.partitions[0]
		bi.partitions = bi.partitions[1:]
		bi.start = 0
		doLookup = true
	}
RetryLookup:
	if doLookup {
		bi.cli.retrier.Do(bi.ctx, func(seq int) bool {
			log.Infof("LookupPartition(%x), attempt #%d", bi.thisPartition, seq)
			bi.curator, curatorWasCached, berr = bi.cli.lookup(bi.ctx, bi.thisPartition)
			return !core.IsRetriableError(berr)
		})
		if berr != core.NoError {
			return nil, berr.Error()
		}
	}

	// Ask curator for blobs.
	var keys []core.BlobKey
	bi.cli.retrier.Do(bi.ctx, func(seq int) bool {
		log.Infof("ListBlobs(%x), attempt #%d", bi.thisPartition, seq)
		keys, berr = bi.cli.curators.ListBlobs(bi.ctx, bi.curator, bi.thisPartition, bi.start)
		return !core.IsRetriableError(berr)
	})
	if berr != core.NoError {
		if curatorWasCached {
			// Maybe we got the wrong curator from the cache.
			bi.cli.lookupCache.invalidate(bi.thisPartition)
			goto RetryLookup
		}
		return nil, berr.Error()
	}

	if len(keys) == 0 {
		// We're done with this partition.
		bi.thisPartition = 0
		return bi.next()
	}

	// Return some ids.
	ids = make([]BlobID, len(keys))
	for i, key := range keys {
		ids[i] = BlobID(core.BlobIDFromParts(bi.thisPartition, key))
	}

	// start here next time
	bi.start = keys[len(keys)-1] + 1

	return ids, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Priorities in contexts:

type contextKey int

const priorityKey contextKey = iota

func priorityFromContext(ctx context.Context) core.Priority {
	if pri, ok := ctx.Value(priorityKey).(core.Priority); ok {
		return pri
	}
	return core.Priority_TSDEFAULT
}
