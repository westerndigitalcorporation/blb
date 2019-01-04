#!/bin/bash
# Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
# SPDX-License-Identifier: MIT

set -ex

cd $(dirname $0)
base=$(cd ../../../.. && pwd -P)

# We'd like to cache a bunch of things, including the inner docker images, go
# build output, and maven dependencies (as installed by lein).
#
# Note: Even if we don't cache go build products, we should still use temprary
# volumes for those directories to avoid polluting the workspace with binaries
# that may be for a different platform.
cache_base=/var/tmp/jepsen.raftkv-$UID
mkdir -p $cache_base
chmod 700 $cache_base

# Build the image with most dependencies baked in. We can't do some stuff (like
# build raftkv) in this step because we can't bind-mount the code.
docker build -t jepsen.raftkv .

# Note: Because we're sharing /var/lib/docker, we should never run more than one
# of these at once. The container "name" will help enforce that.
docker run \
  -v $cache_base/docker:/var/lib/docker \
  -v $cache_base/m2repo:/root/.m2/repository \
  -v $base:/blb \
  -v $cache_base/gobin:/go/bin \
  -v $cache_base/output:/output \
  --privileged \
  -t \
  -i \
  --rm \
  --name jepsen \
  jepsen.raftkv
