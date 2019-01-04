# NOTE

**These scripts are several years (and build systems) old and need to be updated before they work again.**


# jepsen.raftkv

[Jepsen](https://github.com/aphyr/jepsen) test for RaftKV.

## Overview

Test the consistency (linearizability) of RaftKV during network partition.

Test includes:

* Linearizable CAS, read and write

The tests are run against nemesises including:

* Partitions with random halves

## Usage

Everything needed to run the Jepsen test against RaftKV are wrapped in a docker
image so you can run it easily if you have docker installed.

First check out the version of the code you would like to test. The version in
your working directory is what will be run.

Run

```
make setup
```

to ensure submodules are initialized.

Now run

```
src/jepsen.raftkv/docker/start.sh
```

The first run will fetch lots of dependencies, build lots of things, and perform
the Jepsen test.

Subsequent runs should go much faster since docker images and build products are
cached.

## Troubleshooting

Things are cached across runs to speed things up. If things get confused, you
can try clearing the cache:

```
rm -rf /var/tmp/jepsen.raftkv-$UID
```

You might want to inspect the output of RaftKV instances and the history/summary
of jepsen test. You can find the output logs of RaftKV instances at
`/var/tmp/jepsen.raftkv-$UID/output/{n1-n5}` and the history/summary of jepsen
test at `/var/tmp/jepsen.raftkv-$UID/output/store`.

## TODO

Explore other Jepsen tests besides paritition.

Occasionally it seems to hang after the test is complete. Fix this.

