#!/bin/bash
# Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
# SPDX-License-Identifier: MIT

set -ex

# How many raftkv instances
N=5

export GOPATH=$(mktemp -d)

echo "Compiling raftkv binary..."
cd /blb
go install ./raftkv

echo "Building inner docker image..."
# We need to get the raftkv binary in here, so build from a temp directory.
t=$(mktemp -d)
cp -a internal/raftkv/jepsen/docker/inner-Dockerfile $t/Dockerfile
cp -a $GOPATH/bin/raftkv $t/raftkv
docker build -t jepsennode $t
rm -rf $GOPATH $t

echo "Generating ssh keypair..."
test -e /root/.ssh/id_rsa || ssh-keygen -f /root/.ssh/id_rsa -P '' -q

echo "Starting nodes..."
for i in $(seq 1 $N); do
  docker kill n$i || true
  docker rm n$i || true
  docker run \
    -v /output/n$i:/var/log \
    --privileged \
    -d \
    --name n$i \
    -h n$i \
    -e ROOT_PASS="root" \
    -e AUTHORIZED_KEYS="$(cat ~/.ssh/id_rsa.pub)" \
    jepsennode
done

echo "Setting up /etc/hosts..."

{
  echo "127.0.0.1   localhost"
  echo "::1     localhost ip6-localhost ip6-loopback"
  for i in $(seq 1 $N); do
    docker inspect --format "{{ .NetworkSettings.IPAddress }} n$i {{ .Config.Hostname }}" n$i
  done
} > /etc/hosts

# Setup ssh known_hosts so jepsen driver can ssh to each replica.
for i in $(seq 1 $N); do
  ssh-keyscan -t rsa n$i
done > ~/.ssh/known_hosts

# Replicate the hostname to ip address mappings to nodes.
for i in $(seq 1 $N); do
  scp /etc/hosts n$i:/etc/hosts
done

# The actual test writes stuff into the current directory, which in our case
# is the user's actual working git repo, so copy the necessary bits elsewhere
# to run them.
t=$(mktemp -d)
cp -aT /blb/raftkv/jepsen $t
cd $t

echo "Running test..."
lein test || true

# Copy the generated result to 'output' directory which is mapped to host's
# file system so we can inspecte them.
cp -a store /output
