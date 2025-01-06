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

S3-ACCESS_KEY_ID=${S3-ACCESS_KEY_ID:-}
S3-SECRET_ACCESS=${S3-SECRET_ACCESS:-}
S3-REGION=${S3-REGION:-}
S3-BUCKET=${S3-BUCKET:-}

# Check required environment variables
if [[ -z "$S3-ACCESS_KEY_ID" || -z "$S3-SECRET_ACCESS" || -z "$S3-REGION" || -z "$S3-BUCKET" ]]; then
  echo "Error: One or more required S3 environment variables are not set."
  echo "Please set: S3-ACCESS_KEY_ID, S3-SECRET_ACCESS, S3-REGION, S3-BUCKET."
  exit 1
fi

GRAVITINO_SERVER_HOME=../../..
GRAVITINO_SERVER_DIR=$GRAVITINO_SERVER_HOME/distribution/package
CLIENT_FUSE_DIR=$GRAVITINO_SERVER_HOME/clients/filesystem-fuse

echo "Start the Gravitino server"
$GRAVITINO_SERVER_DIR/bin/start_gravitino_server.sh

GRAVITINO_SERVER_URL=http://localhost:8090

check_server_ready "$GRAVITINO_SERVER_URL/api/metalakes"

# create metalake
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name":"test","comment":"comment","properties":{}
}' $GRAVITINO_SERVER_URL/api/metalakes

# create catalog
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "c1",
  "type": "FILESET",
  "comment": "comment",
  "provider": "hadoop",
  "properties": {
    "location": "s3a://'"$S3-BUCKET"'",
    "s3-access-key-id": "'"$S3-ACCESS_KEY_ID"'",
    "s3-secret-access-key": "'"$S3-SECRET_ACCESS"'",
    "s3-endpoint": "http://s3.'"$S3-REGION"'.amazonaws.com",
    "filesystem-providers": "s3"
  }
}' $GRAVITINO_SERVER_URL/api/metalakes/test/catalogs

# create schema
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name":"s1","comment":"comment","properties":{}
}' $GRAVITINO_SERVER_URL/api/metalakes/test/catalogs/catalog/schemas

# create FILESET
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name":"fileset1","comment":"comment","properties":{}
}' $GRAVITINO_SERVER_URL/api/metalakes/test/catalogs/c1/schemas/s1/filesets


echo "Start the Gvfs fuse client"

MOUNT_DIR=$CLIENT_FUSE_DIR/target/gvfs
if [ -d "$MOUNT_DIR" ]; then
  echo "Unmount the existing mount point"
  fusermount -u $MOUNT_DIR
else
  echo "Create the mount point"
  mkdir -p $MOUNT_DIR
fi

FILESET=gvfs://fileset/test/c1/s1/fileset1

CONF_FILE=$CLIENT_FUSE_DIR/target/debug/gvfs-fuse.toml
cp $CLIENT_FUSE_DIR/test/conf/gvfs_fuse-s3.toml $CONF_FILE


sed -i 's|S3-ACCESS_KEY_ID = ".*"|S3-ACCESS_KEY_ID = "$S3-ACCESS_KEY_ID"|' "$CONF_FILE"
sed -i 's|S3-SECRET_ACCESS_key = ".*"|S3-SECRET_ACCESS_key = "$S3-SECRET_ACCESS"|' "$CONF_FILE"
sed -i 's|S3-REGION = ".*"|S3-REGION = "$S3-REGION"|' "$CONF_FILE"
sed -i 's|S3-BUCKET = ".*"|S3-BUCKET = "$S3-BUCKET"|' "$CONF_FILE"

$CLIENT_FUSE_DIR/target/debug/gvfs-fuse $MOUNT_DIR $FILESET $CONF_FILE

check_server_ready() {
  local url=$1
  local retries=10  # Number of retries
  local wait_time=3 # Wait time between retries (seconds)

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

