#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

set -euo pipefail

if [ $# -ne 2 ]; then
  echo "Usage: $0 <flink|access-control> <shard|all>" >&2
  exit 1
fi
suite="$1"
case "${suite}" in
  flink|access-control) ;;
  *) echo "Unknown integration suite: ${suite}" >&2; exit 1 ;;
esac

cd "$(dirname "${BASH_SOURCE[0]}")/../.."
# Resolve and validate the complete selection before starting any Gradle process.
cases="$(dev/ci/test-shards.sh "${suite}" "$2")"
while read -r mode target; do
  case "${suite}" in
    flink)
      ./gradlew -PskipTests -PtestMode="${mode}" -PskipDockerTests=false -PskipWeb=true \
        ":flink-connector:flink-${target}:test" \
        --tests 'org.apache.gravitino.flink.connector.integration.test.**'
      ;;
    access-control)
      ./gradlew -PtestMode="${mode}" -PjdbcBackend="${target}" \
        -PskipDockerTests=false -PskipWeb=true :authorizations:test
      ;;
  esac
done <<< "${cases}"
