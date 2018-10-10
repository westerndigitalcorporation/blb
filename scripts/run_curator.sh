#!/bin/bash

# This script runs the blb curator
set -e

group=$1 # Which curator group this curator belongs to (0, 1, 2, ...)
port=$((4020 + group))
addr="$(hostname -s):$port"
# Set this to enable Raft autoconfig:
# autoconfig_spec="$cluster/$user/$service=$num_replicas"

base="/disk/a/blb-curator"
mkdir -p $base/{snapshot,state,log,db}

exec ./curator \
  --addr="${addr}" \
  --raftID="${addr}" \
  ${autoconfig_spec:+--raftAC=$autoconfig_spec} \
  --snapshotDir="$base"/snapshot \
  --stateDir="$base"/state \
  --logDir="$base"/log \
  --dbDir="$base"/db
