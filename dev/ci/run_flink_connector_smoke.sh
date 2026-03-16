#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "Usage: $0 <1.18|1.19|1.20> <embedded|deploy>" >&2
  exit 1
fi

readonly flink_version="$1"
readonly test_mode="$2"
readonly smoke_test="org.apache.gravitino.flink.connector.integration.test.catalog.GravitinoCatalogManagerIT"

case "$flink_version" in
  1.18)
    readonly test_task=":flink-connector:flink-common:test"
    ;;
  1.19)
    readonly test_task=":flink-connector:flink-1.19:test"
    ;;
  1.20)
    readonly test_task=":flink-connector:flink-1.20:test"
    ;;
  *)
    echo "Unsupported Flink version: ${flink_version}" >&2
    exit 1
    ;;
esac

case "$test_mode" in
  embedded | deploy)
    ;;
  *)
    echo "Unsupported test mode: ${test_mode}" >&2
    exit 1
    ;;
esac

export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:+${JAVA_TOOL_OPTIONS} }-Dfile.encoding=UTF-8"

./gradlew \
  "${test_task}" \
  -PskipTests \
  "-PtestMode=${test_mode}" \
  -PskipDockerTests=false \
  --tests "${smoke_test}" \
  --console=plain
