
BLB
===

Blb is a distributed object storage system. It's designed for use on bare metal
in cluster computing environments.

It's implemented in Go. The API is a client library, also implemented in Go,
that exposes objects with an interface similar to Go files
(`io.ReadWriteSeeker`).


Design Philosophy
---

Blb was designed with the following ideals in mind, in roughly this priority:

- Simplicity of architecture and implementation
- Scalability
- Ease of operation and administration
- Optimized for large, write-rarely, objects, stored on spinning disks, on
  commodity hardware


Architecture Overview
---

Blb borrows many ideas from other storage systems, most prominently Google's GFS
and Colossus, and Microsoft's FDS.

As in similar systems, blobs are divided into fixed-size "tracts" of 8MB. Tracts
are spread across storage nodes in the cluster. Each storage node runs a
"tractserver" that manages the storage on that node and exposes it to the
network.

Locations of tracts are stored in metadata servers, called "curators". Locations
are stored explicitly, not based on consistent hashing or a similar scheme. This
allows for the most control over data placement and movement.

Curators store their metadata in an embedded database (Bolt DB) and use the Raft
protocol to guarantee consistency of updates across replicas.

Objects (also called blobs) are identified with a fixed-length numeric ID
(currently 64 bits, but will probably move to 128). Objects are assigned to
curators by a mapping from the upper bits of their ID, called the partition.
That mapping is stored in another set of metadata servers, called the masters.
The masters also use Raft to keep that mapping consistent.

Blb clusters should be scalable almost indefinitely by adding more curators.

To manipulate an object, the client library talks with the masters, curators,
and tractservers. Some metadata can be cached to reduce the number of
round-trips required to complete an operation.

### Storage

Tractservers store tracts as files in a regular filesystem on local disks (we've
used ext4). (Future plans include raw disk storage, bypassing the filesystem,
with optional support for host-managed SMR.)

The tractserver is designed to have low memory and CPU requirements, so it can
easily coexist with other services in a "converged" storage configuration. (In
our environment we observed it using 50-60MB per 4TB disk under moderate load.)

Curator and master metadata is stored in a local filesystem as well, in Bolt DB
files, logs, and DB snapshots, and should be on SSDs for good performance.

### Durability

Blb supports two schemes for ensuring durability: plain replication, and
Reed-Solomon erasure coding. All objects are created with replicated storage and
can be optionally transitioned to erasure coded storage, after which they cannot
be written to anymore. (Future plans may allow transitioning back to replicated
storage when writing is requested.)

The replication factor may be controlled by clients. For RS erasure coding,
clients choose from one of several pre-configured sets of parameters.

All data on disk is written with embedded crc32 checksums to detect corruption
and errors in storage devices.

When full or partial disk failures are detected, Blb re-replicates affected data
on new disks in the cluster.

Tractservers slowly scrub all their data to proactively detect failures.

### Consistency

The main consistency guarantee provided is that after a successful write to a
range of an object, all reads from that range will return the data that was
written. If a write returns a failure, future reads of that range may return
anything, though other ranges of the object are unaffected.

### Security

Blb assumes that it's running in a protected network and currently does no
authentication of network requests. Data is not encrypted on disk. (Future plans
include encrypting data on disk and managing encryption keys in the curator.)

### Operation

Because Blb stores explicit locations for all data, adding storage to a cluster
is simple and doesn't require migrating any data. (Future plans include
rebalancing existing data across new disks in a slow controlled way.) Removing
storage from a cluster is similarly simple: an operator can mark disks for
draining, which will migrate data off of them in a slow controlled way.

Blb exports many detailed metrics about performance and other operational
concerns in Prometheus format.

In general, Blb is designed to heal and recover from routine hardware failures
and not bother ops staff. With proper configuration, it can even replace members
of Raft groups automatically.


Building and running
---

Blb uses the new Go module system as a build system, so you'll need Go 1.10 or
later.

```
go get github.com/westerndigitalcorporation/blb/cmd/...
```

### Quick test

To test things out quickly, you can use `blbcli` to start a cluster on your
local machine and interact with it. Here's a sample transcript, lightly edited
to remove irrelevant bits:

```
$ mkdir /tmp/blb
$ export TMPDIR=/tmp/blb
$ echo hello > testfile
$ ./blbcli shell
(blb) start_cluster
cli.go:967] Root directory of the cluster: /tmp/blb/blbcli231626552/blbcli245145911
cli.go:976] Log file path: /tmp/blb/blbcli231626552/blbcli245145911/log.txt
cluster.go:85] m0 has addr localhost:50883
cluster.go:85] m1 has addr localhost:50884
cluster.go:85] m2 has addr localhost:50885
cluster.go:85] c0.0 has addr localhost:50886
cluster.go:85] c0.1 has addr localhost:50887
cluster.go:85] c0.2 has addr localhost:50888
cluster.go:85] c1.0 has addr localhost:50889
cluster.go:85] c1.1 has addr localhost:50890
cluster.go:85] c1.2 has addr localhost:50891
cluster.go:85] t0 has addr localhost:50892
cluster.go:85] t1 has addr localhost:50893
cluster.go:85] t2 has addr localhost:50894
cluster.go:85] t3 has addr localhost:50895
cluster.go:85] t4 has addr localhost:50896
cli.go:982] Local cluster is started!
# wait ~10s little while for raft to set up clusters
(blb) create
client.go:233] create a blob with opts {3 0 {0 0 <nil>} 0 0xc0005c6960}, attempt #0
cli.go:505] New blob id: 0000000100000001
(blb) write -b 0000000100000001 -f testfile
client.go:325] open blob 0000000100000001, attempt #0
(blb) read -b 0000000100000001
client.go:325] open blob 0000000100000001, attempt #0
hello
(blb) stat -b 0000000100000001
client.go:325] open blob 0000000100000001, attempt #0
cli.go:532] blob 0000000100000001 Repl=3 NumTracts=1 ByteLen=6
cli.go:539]                       MTime=2018-11-08T15:09:54-08:00 ATime=2018-11-08T15:10:02-08:00
cli.go:540]                       Expires=---
cli.go:541]                       StorageHint=DEFAULT StorageClass=REPLICATED
(blb) exit
```

### Testing

Run `./testblb` from the bin directory to run the suite of integration tests.
Unit tests can be run with the usual `go test` command.

### Production environment

`master`, `curator`, and `tractserver` are the main binaries that you'll need to
deploy. There are scripts in `scripts` that have examples for running them.
They'll need customization for your environment, of course. A basic cluster
would start with three or more tractservers, one master group with three
replicas, and one or more curator groups with three replicas each.


### Service discovery

Several of the environment-specific dependencies of Blb were stubbed out in this
open source release, including cluster sniffing ("What cluster am I running
in?"), dynamic configuration, and most importantly, service discovery.

The stub service discovery implementation polls DNS, but for best results, you
should replace it with something that uses whichever better mechanism is
available in your environment (e.g. Consul, ZooKeeper, or a fancier DNS-based
approach).

All the platform-specific stubs are in the `platform` directory.


### Disks

The set of disks managed by the tractserver can be dynamic, allowing for hot
swapping without restarting the tractserver and interrupting traffic. The
interface is HTTP over an unix socket. Details are in
`internal/tractserver/disk_controller.go`.


History
---

Blb was originally developed at Upthere (a cloud storage service) as the
intended storage system for most of our bulk data. It ran in production for
several months, although not as the sole storage system. In September 2017,
Upthere was acquired by Western Digital, and it was decided to pause development
on Blb and move data to other storage systems.


Future and contribution
---

Although Blb is not being actively developed as a production system, its authors
plan to continue improving the system in their spare time as an educational
project. If you'd like to contribute in any way, just file pull requests or
issues here on GitHub. If you'd like to use it as a base for production
services, we'd also like to hear from you.

Some projects we'd like to work on:

* Expanding the ID bit sizes.
* Adding object names (flat namespace, not directories).
* Replacing protocol buffers with FlatBuffers in the curator database to reduce
  scan overhead.
* Replacing the Go RPC system with GRPC.
* Encryption at rest with end-to-end checksums.
* Speculatively initiate a read from another replica, or RS reconstruction,
  while still waiting for a read.
* Harden the Raft implementation.
* Rebalance data onto new tractservers.
* Use the FileSets algorithm to choose replica sets.
* Use raw disks (CMR and SMR).
* Using a fancier erasure coding scheme that allows cheaper/faster recovery.

