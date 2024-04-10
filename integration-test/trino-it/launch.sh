#!/bin/bash
#
# Copyright 2024 Datastrato Pvt Ltd.
# This software is licensed under the Apache License version 2.
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

docker compose up -d

if [ -n "$GRAVITINO_LOG_PATH" ]; then
    LOG_PATH=$GRAVITINO_LOG_PATH
else
    LOG_PATH=../build/integration-test.log
fi

echo "The docker compose log is: $LOG_PATH"

nohup docker compose logs -f  -t >> $LOG_PATH &

max_attempts=600

for ((i = 0; i < max_attempts; i++)); do
    docker compose exec -T trino trino --execute "SELECT 1" >/dev/null 2>&1 && {
        echo "All docker compose service is now available."
        exit 0
    }
    sleep 1
done

echo "Trino service did not start within the specified time."
exit 1
