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

check_server_ready() {
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
    "location": "s3a://'"$S3_BUCKET"'",
    "s3-access-key-id": "'"$S3_ACCESS_KEY_ID"'",
    "s3-secret-access-key": "'"$S3_SECRET_ACCESS"'",
    "s3-endpoint": "http://s3.'"$S3_REGION"'.amazonaws.com",
    "filesystem-providers": "s3"
  }
}' $GRAVITINO_SERVER_URL/api/metalakes/test/catalogs

# create schema
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name":"s1","comment":"comment","properties":{}
}' $GRAVITINO_SERVER_URL/api/metalakes/test/catalogs/c1/schemas

# create FILESET
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name":"fileset1","comment":"comment","properties":{}
}' $GRAVITINO_SERVER_URL/api/metalakes/test/catalogs/c1/schemas/s1/filesets


echo "Start the Gvfs fuse client"

MOUNT_DIR=$CLIENT_FUSE_DIR/target/gvfs
if [ -d "$MOUNT_DIR" ]; then
  echo "Unmount the existing mount point"
  umount -l $MOUNT_DIR > /dev/null 2>&1 || true
else
  echo "Create the mount point"
  mkdir -p $MOUNT_DIR
fi

FILESET=gvfs://fileset/test/c1/s1/fileset1

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

$CLIENT_FUSE_DIR/target/debug/gvfs-fuse $MOUNT_DIR $FILESET $CONF_FILE


