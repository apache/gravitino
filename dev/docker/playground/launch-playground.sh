#!/bin/bash
#
# Copyright 2023 Datastrato.
# This software is licensed under the Apache License version 2.
#
#set -ex
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
export VERSION=`grep 'version =' ../../../gradle.properties | awk '{printf $3}'`
docker-compose up

# Clean Docker containers when you quit this script
docker-compose down
