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

DIR=$(cd "$(dirname "$0")" && pwd)/../../../
export GRAVITINO_ROOT_DIR=$(cd "$DIR" && pwd)
export GRAVITINO_HOME=$GRAVITINO_ROOT_DIR
export GRAVITINO_TEST=true
export HADOOP_USER_NAME=anonymous

# When connecting to a Hive metastore version 3.x,
# the Hive connector supports reading from and writing to insert-only and ACID tables,
# with full support for partitioning and bucketing
export HIVE_RUNTIME_VERSION=hive3

# Whether a Trino connector test or not
# Only Trino connector test will create Hive ACID table when deploy Hive with
# `/integration-test-common/docker-script/init/hive/init.sh`
export TRINO_CONNECTOR_TEST=true

echo $GRAVITINO_ROOT_DIR
cd $GRAVITINO_ROOT_DIR

args="\"$@\""

./gradlew :trino-connector:integration-test:TrinoTest -PappArgs="$args"
