#!/bin/bash

# This script runs the blb master
set -e

port=4000
addr="$(hostname -s):$port"
# Set this to enable Raft autoconfig:
# autoconfig_spec="$cluster/$user/$service=$num_replicas"

base="/disk/a/blb-master"
mkdir -p "$base"/{snapshot,state,log}

exec ./master \
  --addr="$addr" \
  --raftID="$addr" \
  ${autoconfig_spec:+--raftAC=$autoconfig_spec} \
  --snapshotDir="$base"/snapshot \
  --stateDir="$base"/state \
  --logDir="$base"/log
