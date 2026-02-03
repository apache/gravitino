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

DEFAULT_TRINO_VERSION=435
DEFAULT_TRINO_CONNECTOR_DIR=$GRAVITINO_ROOT_DIR/trino-connector/trino-connector-435-439/build/libs

echo $GRAVITINO_ROOT_DIR
cd $GRAVITINO_ROOT_DIR

has_trino_version=false
has_trino_connector_dir=false
new_args=()

for arg in "$@"; do
  if [[ "$arg" == --trino_version=* ]]; then
    has_trino_version=true
    new_args+=("$arg")
  elif [[ "$arg" == --trino_connector_dir=* ]]; then
    has_trino_connector_dir=true
    path="${arg#*=}"
    if [[ "$path" != /* ]]; then
      path="$GRAVITINO_ROOT_DIR/$path"
    fi
    new_args+=("--trino_connector_dir=$path")
  else
    new_args+=("$arg")
  fi
done

set -- "${new_args[@]}"

if [ "$has_trino_version" = false ]; then
  set -- "$@" --trino_version=$DEFAULT_TRINO_VERSION
fi

if [ "$has_trino_connector_dir" = false ]; then
  set -- "$@" "--trino_connector_dir=$DEFAULT_TRINO_CONNECTOR_DIR"
fi

args="\"$@\""

./gradlew :trino-connector:integration-test:TrinoTest -PappArgs="$args"
