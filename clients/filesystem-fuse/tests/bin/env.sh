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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

S3_ACCESS_KEY_ID=${S3_ACCESS_KEY_ID:-test}
S3_SECRET_ACCESS=${S3_SECRET_ACCESS:-test}
S3_REGION=${S3_REGION:-ap-southeast-2}
S3_BUCKET=${S3_BUCKET:-my-bucket}
S3_ENDPOINT=${S3_ENDPOINT:-http://127.0.0.1:4566}

# Check required environment variables
if [[ -z "$S3_ACCESS_KEY_ID" || -z "$S3_SECRET_ACCESS" || -z "$S3_REGION" || -z "$S3_BUCKET" || -z "$S3_ENDPOINT" ]]; then
  echo "Error: One or more required S3 environment variables are not set."
  echo "Please set: S3_ACCESS_KEY_ID, S3_SECRET_ACCESS, S3_REGION, S3_BUCKET, S3_ENDPOINT."
  exit 1
fi

DISABLE_LOCALSTACK=${DISABLE_LOCALSTACK:-0}
# if S3 endpoint is not default value. disable localstack
if [[ "$S3_ENDPOINT" != "http://127.0.0.1:4566" ]]; then
  echo "AWS S3 endpoint detected, disabling localstack"
  DISABLE_LOCALSTACK=1
fi

GRAVITINO_HOME=../../../..
GRAVITINO_HOME=$(cd $GRAVITINO_HOME && pwd)
GRAVITINO_SERVER_DIR=$GRAVITINO_HOME/distribution/package
CLIENT_FUSE_DIR=$GRAVITINO_HOME/clients/filesystem-fuse

generate_test_config() {
  local config_dir
  config_dir=$(dirname "$TEST_CONFIG_FILE")
  mkdir -p "$config_dir"

  awk -v access_key="$S3_ACCESS_KEY_ID" \
      -v secret_key="$S3_SECRET_ACCESS" \
      -v region="$S3_REGION" \
      -v bucket="$S3_BUCKET" \
      -v endpoint="$S3_ENDPOINT" \
      'BEGIN { in_extend_config = 0 }
      /^\[extend_config\]/ { in_extend_config = 1 }
      in_extend_config && /s3-access_key_id/ { $0 = "s3-access_key_id = \"" access_key "\"" }
      in_extend_config && /s3-secret_access_key/ { $0 = "s3-secret_access_key = \"" secret_key "\"" }
      in_extend_config && /s3-region/ { $0 = "s3-region = \"" region "\"" }
      in_extend_config && /s3-bucket/ { $0 = "s3-bucket = \"" bucket "\"" }
      in_extend_config && /s3-endpoint/ { $0 = "s3-endpoint = \"" endpoint "\"" }
      { print }' $CLIENT_FUSE_DIR/tests/conf/gvfs_fuse_s3.toml > "$TEST_CONFIG_FILE"
}
