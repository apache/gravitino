#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

check_gvfs_fuse_ready() {
  local retries=10
  local wait_time=1

  for ((i=1; i<=retries; i++)); do
    # check the $MOUNT_DIR/.gvfs_meta is exist
    if [ -f "$MOUNT_DIR/.gvfs_meta" ]; then
      echo "Gvfs fuse is ready."
      return 0
    else
      echo "Attempt $i/$retries: Gvfs fuse not ready. Retrying in $wait_time seconds..."
      sleep "$wait_time"
    fi
  done

  echo "Error: Gvfs fuse did not become ready after $((retries * wait_time)) seconds."
  tail -n 100 $CLIENT_FUSE_DIR/target/debug/fuse.log
  exit 1
}

start_gvfs_fuse() {
  MOUNT_DIR=$CLIENT_FUSE_DIR/target/gvfs

  umount $MOUNT_DIR > /dev/null 2>&1 || true
  if [ ! -d "$MOUNT_DIR" ]; then
    echo "Create the mount point"
    mkdir -p $MOUNT_DIR
  fi

  MOUNT_FROM_LOCATION=gvfs://fileset/test/c1/s1/fileset1

  # Build the gvfs-fuse
  cd $CLIENT_FUSE_DIR
  make build

  echo "Starting gvfs-fuse-daemon"
  $CLIENT_FUSE_DIR/target/debug/gvfs-fuse $MOUNT_DIR $MOUNT_FROM_LOCATION $TEST_CONF_FILE > $CLIENT_FUSE_DIR/target/debug/fuse.log 2>&1 &
  check_gvfs_fuse_ready
  cd -
}

stop_gvfs_fuse() {
  # Stop the gvfs-fuse process if it's running
  pkill -INT gvfs-fuse || true
  echo "Stopping gvfs-fuse-daemon"
}