#!/bin/bash
#
# Copyright 2023 Datastrato Pvt Ltd.
# This software is licensed under the Apache License version 2.
#
#set -ex
cd "$(dirname "$0")"

playground_dir="$(dirname "${BASH_SOURCE-$0}")"
playground_dir="$(cd "${playground_dir}">/dev/null; pwd)"
isExist=`which docker-compose`
if [ $isExist ]
then
  true # Placeholder, do nothing
else
  echo "ERROR: No docker service environment found, please install docker-compose first."
  exit
fi

cd ${playground_dir}

if [ "$1" = "-s" ]; then
  docker-compose up -d > ../../../integration-test/build/trino-it-docker.log 2>&1
else
  docker-compose up -d


nohup docker-compose logs -f  -t > ../../../integration-test/build/trino-it-docker.log &
