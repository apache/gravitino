#!/bin/bash
#
# Copyright 2024 Datastrato Pvt Ltd.
# This software is licensed under the Apache License version 2.
#
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
mkdir -p ../../../it-archive/log/python-ci-container-log

docker compose up -d

if [ -n "$GRAVITINO_LOG_PATH" ]; then
    LOG_PATH=$GRAVITINO_LOG_PATH
else
    LOG_PATH=../../../it-archive/log/integration-test.log
fi

echo "The docker compose log is: $LOG_PATH"

nohup docker compose logs -f  -t >> $LOG_PATH &

echo "All docker compose service is now available."

# change the hive container's logs directory permission
docker exec python-ci-hive chown -R `id -u`:`id -g` /tmp/root
docker exec python-ci-hive chown -R `id -u`:`id -g` /usr/local/hadoop/logs

