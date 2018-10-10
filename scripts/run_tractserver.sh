#!/bin/bash

# This script runs the blb tractserver
set -e

port="4010"
addr="$(hostname -s):$port"
controller_base="/var/tmp/blb-tractserver"
controller_socket="$controller_base/$addr"

# Set this to the list of all your disks. You should have already mounted
# filesystems here. Blb will assume it can use all the remaining space on the
# disk, but won't disturb existing files except in the blb-tractserver
# directory. If you'd like to use directories that aren't mountpoints, for
# testing, just comment out the "findmnt" line below.
disks=$(ls -d /disk/?)

num=0
for d in $disks; do
    # Check that the disk is a mountpoint and mounted read-write
    findmnt "$d" | grep -qw rw || continue
    root="$d/blb-tractserver"
    # Skip failed disks
    mkdir -p "$root/data" "$root/deleted" || continue
    # Poke the tractserver to add this disk. Note that the tractserver hasn't started yet,
    # so we ask curl to retry a bunch of times. We should theoretically url-encode $root
    # below, but it's unlikely to contain & or = characters, so it's ok for now.
    curl -X POST --retry 10 --retry-connrefused --unix-socket "$controller_socket" \
        "http://_/disk?root=$root" &
    let num++
done

# If we don't see any disks, sleep for a while, then exit and let the process
# manager restart us to check again.
if (( $num == 0 )); then
    echo "No mounted disks found!" >&2
    sleep 3600
    exit 1
fi

exec ./tractserver \
    --addr="$addr" \
    --diskControllerBase="$controller_base"
