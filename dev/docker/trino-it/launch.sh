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

docker-compose up -d

nohup docker-compose logs -f  -t >> ../../../integration-test/build/integration-test.log &

max_attempts=180

for ((i = 0; i < max_attempts; i++)); do
    docker-compose exec -T trino trino --execute "SELECT 1" >/dev/null 2>&1 && {
        echo "All docker-compse service is now available."
        exit 0
    }
    sleep 1
done

echo "Trino service did not start within the specified time."
exit 1
