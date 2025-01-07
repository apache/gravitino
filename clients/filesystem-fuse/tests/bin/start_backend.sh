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

set -euo pipefail

S3_ACCESS_KEY_ID=${S3_ACCESS_KEY_ID:-}
S3_SECRET_ACCESS=${S3_SECRET_ACCESS:-}
S3_REGION=${S3_REGION:-}
S3_BUCKET=${S3_BUCKET:-}

# Check required environment variables
if [[ -z "$S3_ACCESS_KEY_ID" || -z "$S3_SECRET_ACCESS" || -z "$S3_REGION" || -z "$S3_BUCKET" ]]; then
  echo "Error: One or more required S3 environment variables are not set."
  echo "Please set: S3_ACCESS_KEY_ID, S3_SECRET_ACCESS, S3_REGION, S3_BUCKET."
  exit 1
fi

GRAVITINO_HOME=../../../..
GRAVITINO_HOME=$(cd $GRAVITINO_HOME && pwd)
GRAVITINO_SERVER_DIR=$GRAVITINO_HOME/distribution/package
CLIENT_FUSE_DIR=$GRAVITINO_HOME/clients/filesystem-fuse
GRAVITINO_SERVER_URL=http://localhost:8090

# copy the aws-bundle to the server
if ls $GRAVITINO_SERVER_DIR/catalogs/hadoop/libs/gravitino-aws-bundle-*-incubating-SNAPSHOT.jar 1>/dev/null 2>&1; then
  echo "File exists, skipping copy."
else
  cp $GRAVITINO_HOME/bundles/aws-bundle/build/libs/gravitino-aws-bundle-*-incubating-SNAPSHOT.jar \
    $GRAVITINO_SERVER_DIR/catalogs/hadoop/libs
fi


echo "Start the Gravitino server"
rm -rf $GRAVITINO_SERVER_DIR/data
$GRAVITINO_SERVER_DIR/bin/gravitino.sh restart

check_gravitino_server_ready() {
  local url=$1
  local retries=10  # Number of retries
  local wait_time=1 # Wait time between retries (seconds)

  for ((i=1; i<=retries; i++)); do
    if curl --silent --head --fail "$url" >/dev/null; then
      echo "Gravitino server is ready."
      return 0
    else
      echo "Attempt $i/$retries: Server not ready. Retrying in $wait_time seconds..."
      sleep "$wait_time"
    fi
  done

  echo "Error: Gravitino server did not become ready after $((retries * wait_time)) seconds."
  exit 1
}

check_gravitino_server_ready "$GRAVITINO_SERVER_URL/api/metalakes"

echo "Create the metalake, catalog, schema, and fileset"
# create metalake
curl -s -o /dev/null -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name":"test","comment":"comment","properties":{}
}' $GRAVITINO_SERVER_URL/api/metalakes

# create catalog
curl -s -o /dev/null -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "c1",
  "type": "FILESET",
  "comment": "comment",
  "provider": "hadoop",
  "properties": {
    "location": "s3a://'"$S3_BUCKET"'",
    "s3-access-key-id": "'"$S3_ACCESS_KEY_ID"'",
    "s3-secret-access-key": "'"$S3_SECRET_ACCESS"'",
    "s3-endpoint": "http://s3.'"$S3_REGION"'.amazonaws.com",
    "filesystem-providers": "s3"
  }
}' $GRAVITINO_SERVER_URL/api/metalakes/test/catalogs

# create schema
curl -s -o /dev/null -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name":"s1","comment":"comment","properties":{}
}' $GRAVITINO_SERVER_URL/api/metalakes/test/catalogs/c1/schemas

# create FILESET
curl -s -o /dev/null -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name":"fileset1","comment":"comment","properties":{}
}' $GRAVITINO_SERVER_URL/api/metalakes/test/catalogs/c1/schemas/s1/filesets

echo "Start the Gvfs fuse client"

MOUNT_DIR=$CLIENT_FUSE_DIR/target/gvfs

umount $MOUNT_DIR > /dev/null 2>&1 || true
if [ ! -d "$MOUNT_DIR" ]; then
  echo "Create the mount point"
  mkdir -p $MOUNT_DIR
fi

MOUNT_FROM_LOCATION=gvfs://fileset/test/c1/s1/fileset1

CONF_FILE=$CLIENT_FUSE_DIR/target/debug/gvfs-fuse.toml

awk -v access_key="$S3_ACCESS_KEY_ID" \
    -v secret_key="$S3_SECRET_ACCESS" \
    -v region="$S3_REGION" \
    -v bucket="$S3_BUCKET" \
    'BEGIN { in_extend_config = 0 }
    /^\[extend_config\]/ { in_extend_config = 1 }
    in_extend_config && /s3-access_key_id/ { $0 = "s3-access_key_id = \"" access_key "\"" }
    in_extend_config && /s3-secret_access_key/ { $0 = "s3-secret_access_key = \"" secret_key "\"" }
    in_extend_config && /s3-region/ { $0 = "s3-region = \"" region "\"" }
    in_extend_config && /s3-bucket/ { $0 = "s3-bucket = \"" bucket "\"" }
    { print }' $CLIENT_FUSE_DIR/tests/conf/gvfs_fuse_s3.toml > "$CONF_FILE"

cleanup() {
    # Stop the gvfs-fuse process if it's running
    if [ -n "$FUSE_PID" ] && ps -p $FUSE_PID > /dev/null; then
        echo "Stopping gvfs-fuse..."
        kill -INT $FUSE_PID
    else
        echo "gvfs-fuse process not found or already stopped."
    fi

    # Stop the Gravitino server
    echo "Stopping Gravitino server..."
    $GRAVITINO_SERVER_DIR/bin/gravitino.sh stop || echo "Failed to stop Gravitino server."
}
trap cleanup EXIT

# Start the gvfs-fuse process in the background
$CLIENT_FUSE_DIR/target/debug/gvfs-fuse $MOUNT_DIR $MOUNT_FROM_LOCATION $CONF_FILE > $CLIENT_FUSE_DIR/target/debug/fuse.log 2>&1 &
FUSE_PID=$!
echo "Gvfs fuse started with PID: $FUSE_PID"

#check the gvfs-fuse is ready
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
  exit 1
}

check_gvfs_fuse_ready

# run the integration test
cd $CLIENT_FUSE_DIR
export RUN_TEST_WITH_BACKGROUND=1
cargo test --test fuse_test test_fuse_system_with_manual -- --exact

sleep 3
