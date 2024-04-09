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

# create log dir
mkdir -p ../build/trino-ci-container-log

docker compose up -d

if [ -n "$GRAVITINO_LOG_PATH" ]; then
    LOG_PATH=$GRAVITINO_LOG_PATH
else
    LOG_PATH=../build/integration-test.log
fi

echo "The docker compose log is: $LOG_PATH"

nohup docker compose logs -f  -t >> $LOG_PATH &

max_attempts=0

while true; do
    docker compose exec -T trino trino --execute "SELECT 1" >/dev/null 2>&1 && {
        break;
    }

    num_container=$(docker ps --format '{{.Names}}' | grep trino-ci | wc -l)
    if [ "$num_container" -lt 4 ]; then
        echo "ERROR: Trino-ci containers start failed."
        exit 0
    fi

    sleep 1

    if [ "$max_attempts" -ge 600 ]; then 
        echo "ERROR: Trino service did not start within the specified time."
        exit 1
    fi
    ((count++))
done


echo "All docker compose service is now available."

docker exec trino-ci-hive chown -R `id -u`:`id -g` /tmp/root
docker exec trino-ci-hive chown -R `id -u`:`id -g` /usr/local/hadoop/logs
ls -l ../build/trino-ci-container-log
ls -l ../build/trino-ci-container-log/hive
ls -l ../build/trino-ci-container-log/hdfs

