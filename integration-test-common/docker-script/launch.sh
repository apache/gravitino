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

#
#set -ex
cd "$(dirname "$0")"

playground_dir="$(dirname "${BASH_SOURCE-$0}")"
playground_dir="$(cd "${playground_dir}">/dev/null; pwd)"
isExist=`which docker`
if [ ! $isExist ]
then
  echo "ERROR: No docker service environment found, please install docker first."
  exit
fi

cd ${playground_dir}

# create log dir
LOG_DIR=../build/trino-ci-container-log
rm -fr $LOG_DIR
mkdir -p $LOG_DIR
LOG_PATH=$LOG_DIR/trino-ci-docker-compose.log
echo "The docker compose log is: $LOG_PATH"

docker compose up -d

# Stream logs directly to the log file (no console output).
nohup docker compose logs -f -t | tee -a "$LOG_PATH" &
LOG_FOLLOW_PID=$!
cleanup_log_follow() {
  if [ -n "$LOG_FOLLOW_PID" ] && kill -0 "$LOG_FOLLOW_PID" 2>/dev/null; then
    kill "$LOG_FOLLOW_PID" >/dev/null 2>&1
  fi
}
trap cleanup_log_follow EXIT

max_attempts=300
attempts=0

while true; do
    docker compose exec -T trino trino --execute "SELECT 1" >/dev/null 2>&1 && {
        break;
    }

    num_container=$(docker ps --format '{{.Names}}' | grep trino-ci | wc -l)
    if [ "$num_container" -lt 4 ]; then
        echo "ERROR: Trino-ci containers start failed."
        exit 0
    fi

    if [ "$attempts" -ge "$max_attempts" ]; then
        echo "ERROR: Trino service did not start within the $max_attempts time."
        exit 1
    fi

   ((attempts++))
    sleep 1
done


echo "All docker compose service is now available."
