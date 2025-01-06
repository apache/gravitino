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

s3-access_key_id=${s3-access_key_id:-}
s3-secret_access=${s3-secret_access:-}
s3-region=${s3-region:-}
s3-bucket=${s3-bucket:-}

# Check required environment variables
if [[ -z "$s3-access_key_id" || -z "$s3-secret_access" || -z "$s3-region" || -z "$s3-bucket" ]]; then
  echo "Error: One or more required S3 environment variables are not set."
  echo "Please set: s3-access_key_id, s3-secret_access, s3-region, s3-bucket."
  exit 1
fi

GRAVITINO_SERVER_HOME=../../..
GRAVITINO_SERVER_DIR=$GRAVITINO_SERVER_HOME/distribution/package
CLIENT_FUSE_DIR=$GRAVITINO_SERVER_HOME/clients/filesystem-fuse

echo "Start the Gravitino server"
$GRAVITINO_SERVER_DIR/bin/start_gravitino_server.sh

GRAVITINO_SERVER_URL=http://localhost:8090

curl $GRAVITINO_SERVER_URL/api/metalakes

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
    "location": "s3a://'"$s3-bucket"'",
    "s3-access-key-id": "'"$s3-access_key_id"'",
    "s3-secret-access-key": "'"$s3-secret_access"'",
    "s3-endpoint": "http://s3.'"$s3-region"'.amazonaws.com",
    "filesystem-providers": "s3"
  }
}' $GRAVITINO_SERVER_URL/api/metalakes/test/catalogs

# create schema
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name":"s1","comment":"comment","properties":{}
}' $GRAVITINO_SERVER_URL/api/metalakes/test/catalogs/catalog/schemas

# create fileset
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name":"fileset1","comment":"comment","properties":{}
}' $GRAVITINO_SERVER_URL/api/metalakes/test/catalogs/c1/schemas/s1/filesets


echo "Start the Gvfs fuse client"

mount_dir=$CLIENT_FUSE_DIR/target/gvfs
if [ -d "$mount_dir" ]; then
  echo "Unmount the existing mount point"
  fusermount -u $mount_dir
else
  echo "Create the mount point"
  mkdir -p $mount_dir
fi

fileset=gvfs://fileset/test/c1/s1/fileset1

config_file=$CLIENT_FUSE_DIR/target/debug/gvfs-fuse.toml
cp $CLIENT_FUSE_DIR/test/conf/gvfs_fuse-s3.toml $config_file


sed -i 's|s3-access_key_id = ".*"|s3-access_key_id = "$s3-access_key_id"|' "$config_file"
sed -i 's|s3-secret_access_key = ".*"|s3-secret_access_key = "$s3-secret_access"|' "$config_file"
sed -i 's|s3-region = ".*"|s3-region = "$s3-region"|' "$config_file"
sed -i 's|s3-bucket = ".*"|s3-bucket = "$s3-bucket"|' "$config_file"

$CLIENT_FUSE_DIR/target/debug/gvfs-fuse $mount_dir $fileset $config_file


