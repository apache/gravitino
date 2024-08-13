#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

#set -ex
cd "$(dirname "${BASH_SOURCE-$0}")"

playground_dir=$(pwd)

if ! command -v docker &>/dev/null; then
  echo "ERROR: No docker service environment found, please install Docker first."
  exit 1
fi

if [ -z "${GRAVITINO_SERVER_HOME}" ]; then
    GRAVITINO_SERVER_HOME=`realpath ../../distribution/package`
fi

if [ ! -d "$GRAVITINO_SERVER_HOME" ]; then
  echo "Error: Gravitino server directory '$GRAVITINO_SERVER_HOME' does not exist."
  exit 1
fi

# create log dir
LOG_DIR=../../integration-test/build/trino-distribution-ci-container-log
rm -fr $LOG_DIR
mkdir -p $LOG_DIR
LOG_FILE=$(realpath $LOG_DIR)/trino-ci-docker-compose.log
echo "The docker compose log is: $LOG_FILE"

docker compose up -d
nohup docker compose logs -f  -t > $LOG_FILE &

max_attempts=300
attempts=0

while true; do
    docker compose exec -T trino-coordinator trino --execute "SELECT 1" >/dev/null 2>&1 && {
        break;
    }

    echo "Check the Trino server startup"

    if [ "$attempts" -ge "$max_attempts" ]; then
        echo "ERROR: Trino service did not start within the $max_attempts time."
        exit 1
    fi

   ((attempts++))
    sleep 1
done

echo "All docker compose service is now available."
