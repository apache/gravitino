#!/bin/bash
#
# Copyright 2024 Datastrato Pvt Ltd.
# This software is licensed under the Apache License version 2.
#
cd "$(dirname "$0")"

# change the hive container's logs directory permission
docker exec python-ci-hive chown -R `id -u`:`id -g` /tmp/root
docker exec python-ci-hive chown -R `id -u`:`id -g` /usr/local/hadoop/logs

# for trace file permission
ls -l ../../../it-archive/log/python-ci-container-log
ls -l ../../../it-archive/log/python-ci-container-log/hive
ls -l ../../../it-archive/log/python-ci-container-log/hdfs

docker compose down
