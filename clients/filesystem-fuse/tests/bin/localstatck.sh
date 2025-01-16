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

start_localstack() {
if [ "$DISABLE_LOCALSTACK" -eq 1 ]; then
  return
fi

  echo "Starting localstack..."
  docker run -d -p 4566:4566 -p 4571:4571 --name localstack localstack/localstack
  echo "Localstack started"

  docker exec localstack sh -c "\
    aws configure set aws_access_key_id $S3_ACCESS_KEY_ID && \
    aws configure set aws_secret_access_key $S3_SECRET_ACCESS && \
    aws configure set region $S3_REGION && \
    aws configure set output json"

  docker exec localstack awslocal s3 mb s3://$S3_BUCKET
}

stop_localstack() {
if [ "$DISABLE_LOCALSTACK" -eq 1 ]; then
  return
fi

  echo "Stopping localstack..."
  docker stop localstack 2>/dev/null || true
  docker rm localstack 2>/dev/null || true
  echo "Localstack stopped"
}