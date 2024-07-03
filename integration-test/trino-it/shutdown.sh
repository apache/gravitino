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
cd "$(dirname "$0")"

# change the hive container's logs directory permission
docker exec trino-ci-hive chown -R `id -u`:`id -g` /tmp/root
docker exec trino-ci-hive chown -R `id -u`:`id -g` /usr/local/hadoop/logs

# for trace file permission
ls -l ../build/trino-ci-container-log
ls -l ../build/trino-ci-container-log/hive
ls -l ../build/trino-ci-container-log/hdfs

docker compose down
