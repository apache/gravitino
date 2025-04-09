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

GRAVITINO_SERVER_URL="http://localhost:8090"

check_gravitino_server_ready() {
  local url=$1
  local retries=10  # Number of retries
  local wait_time=1 # Wait time between retries (seconds)

  for ((i=1; i<=retries; i++)); do
    if curl --silent --head --fail "$url/api/metalakes" >/dev/null; then
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

create_resource() {
  local url=$1
  local data=$2

  response=$(curl -s -w "\n%{http_code}" -X POST -H "Accept: application/vnd.gravitino.v1+json" \
    -H "Content-Type: application/json" -d "$data" "$url")

  body=$(echo "$response" | head -n -1)
  response_code=$(echo "$response" | tail -n 1)

  # Check if the response code is not 2xx
  if [[ "$response_code" -lt 200 || "$response_code" -ge 300 ]]; then
    echo "Error: Failed to create resource. Status code: $response_code"
    echo "Response body: $body"
    exit 1
  fi
}



start_gravitino_server() {
  echo "Starting Gravitino Server"
  # copy the aws-bundle to the server
  if ls $GRAVITINO_SERVER_DIR/catalogs/hadoop/libs/gravitino-aws-bundle-*.jar 1>/dev/null 2>&1; then
     echo "File exists, skipping copy."
  else
    echo "Copying the aws-bundle to the server"
    cp $GRAVITINO_HOME/bundles/aws-bundle/build/libs/gravitino-aws-bundle-*.jar \
      $GRAVITINO_SERVER_DIR/catalogs/hadoop/libs
  fi

  rm -rf $GRAVITINO_SERVER_DIR/data
  $GRAVITINO_SERVER_DIR/bin/gravitino.sh restart

  check_gravitino_server_ready $GRAVITINO_SERVER_URL

  # Create metalake
  create_resource "$GRAVITINO_SERVER_URL/api/metalakes" '{
    "name":"test",
    "comment":"comment",
    "properties":{}
  }'

  # Create catalog
  create_resource "$GRAVITINO_SERVER_URL/api/metalakes/test/catalogs" '{
    "name": "c1",
    "type": "FILESET",
    "comment": "comment",
    "provider": "hadoop",
    "properties": {
      "location": "s3a://'"$S3_BUCKET"'",
      "s3-access-key-id": "'"$S3_ACCESS_KEY_ID"'",
      "s3-secret-access-key": "'"$S3_SECRET_ACCESS"'",
      "s3-endpoint": "'"$S3_ENDPOINT"'",
      "filesystem-providers": "s3"
    }
  }'

  # Create schema
  create_resource "$GRAVITINO_SERVER_URL/api/metalakes/test/catalogs/c1/schemas" '{
    "name":"s1",
    "comment":"comment",
    "properties":{}
  }'

  # Create FILESET
  create_resource "$GRAVITINO_SERVER_URL/api/metalakes/test/catalogs/c1/schemas/s1/filesets" '{
    "name":"fileset1",
    "comment":"comment",
    "properties":{}
  }'
}

stop_gravitino_server() {
  $GRAVITINO_SERVER_DIR/bin/gravitino.sh stop
  echo "Gravitino Server stopped"
}
