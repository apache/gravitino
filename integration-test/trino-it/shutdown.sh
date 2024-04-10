#!/bin/bash
#
# Copyright 2024 Datastrato Pvt Ltd.
# This software is licensed under the Apache License version 2.
#
cd "$(dirname "$0")"

docker exec trino-ci-hive chown -R `id -u`:`id -g` /tmp/root
docker exec trino-ci-hive chown -R `id -u`:`id -g` /usr/local/hadoop/logs

# for trace file permission
ls -l ../build/trino-ci-container-log
ls -l ../build/trino-ci-container-log/hive
ls -l ../build/trino-ci-container-log/hdfs

docker compose down
