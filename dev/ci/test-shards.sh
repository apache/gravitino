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
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Single source of truth for how CI test suites are split into parallel shards.
#
# Usage:
#   dev/ci/test-shards.sh <suite> --list     Print the suite's shard names as a JSON array.
#   dev/ci/test-shards.sh <suite> <shard>    Print the Gradle task arguments of a shard, one per line.
#
# Suites:
#   build       Unit tests run by .github/workflows/build.yml.
#   flink       Flink version/mode pairs, one pair per line.
#   access-control  Authorization mode/backend pairs, one pair per line.
#   backend-it  Integration tests run by .github/workflows/backend-integration-test.yml.
#               The `all` selector preserves the unsharded cron/manual suite.
#
# The task-based build/backend-it suites end with a catch-all `others` shard that excludes the projects of all named
# shards, so a new module is always tested by `others` until it is moved to a named shard.
# To rebalance, move a project between the lists below; the workflows need no change.

set -euo pipefail

# ---- build suite -------------------------------------------------------------------------------
# Core tests run in the dedicated core-test workflow lanes, outside these build shards.
# Projects with `gravitino-docker-test` tests. Gradle runs them one by one under the shared test
# environment lock, so they are kept away from the parallel unit tests in `others`.
# shellcheck disable=SC2034 # Referenced indirectly by print_test_tasks.
BUILD_DOCKER=(
  :authorizations:authorization-chain
  :authorizations:authorization-ranger
  :catalogs:catalog-fileset
  :catalogs:catalog-glue
  :catalogs:catalog-hive
  :catalogs:catalog-jdbc-doris
  :catalogs:catalog-jdbc-mysql
  :catalogs:catalog-jdbc-postgresql
  :catalogs:catalog-jdbc-starrocks
  :catalogs:catalog-kafka
  :catalogs:catalog-lakehouse-hudi
  :catalogs:catalog-lakehouse-iceberg
  :catalogs:catalog-lakehouse-paimon
  :catalogs:hive-metastore-common
  :clients:client-java
  :clients:filesystem-hadoop3
  :flink-connector:flink-common
  :iceberg:iceberg-rest-server
  :maintenance:jobs
  :maintenance:optimizer
  :plugins:idp-basic
  :spark-connector:spark-3.5
)

# ---- backend-it suite --------------------------------------------------------------------------
# client-java, catalog-fileset and filesystem-hadoop3 stay in `others`. Recent task timings
# split roughly 28/28/35 minutes before setup; rebalance these lists as the suites grow.
# shellcheck disable=SC2034 # Referenced indirectly by print_test_tasks.
BACKEND_IT_HIVE=(
  :catalogs:catalog-hive
  :catalogs:catalog-glue
  :catalogs:catalog-lakehouse-hudi
)

# shellcheck disable=SC2034 # Referenced indirectly by print_test_tasks.
BACKEND_IT_LAKEHOUSE=(
  :iceberg:iceberg-rest-server
  :catalogs:catalog-lakehouse-iceberg
  :catalogs:catalog-lakehouse-paimon
  :lance:lance-rest-server
)

usage() {
  sed -n '/^# Usage:/,/^# To rebalance/p' "$0" | sed 's/^# \{0,1\}//' >&2
  exit 1
}

# Prints the shard names of a suite, in matrix order.
shards_of() {
  case "$1" in
    build) echo "docker others" ;;
    backend-it) echo "hive lakehouse others" ;;
    flink) echo "embedded deploy" ;;
    access-control) echo "h2 mysql postgresql" ;;
    *) echo "Unknown suite: $1" >&2; usage ;;
  esac
}

# Prints the variable name holding the projects of a named shard.
projects_var() {
  case "$1/$2" in
    build/docker) echo BUILD_DOCKER ;;
    backend-it/hive) echo BACKEND_IT_HIVE ;;
    backend-it/lakehouse) echo BACKEND_IT_LAKEHOUSE ;;
    *) echo "Unknown shard '$2' for suite '$1'" >&2; usage ;;
  esac
}

# Prints the test tasks for every project in the named array, including suite-specific tasks.
print_test_tasks() {
  local project
  eval 'local projects=("${'"$1"'[@]}")'
  for project in "${projects[@]}"; do
    echo "${project}:test"
    if [ "$2" = "backend-it" ] && [ "${project}" = ":iceberg:iceberg-rest-server" ]; then
      echo "${project}:experimentalTest"
    fi
  done
}

# `others` runs the suite's root task with every named shard's test task excluded.
print_others() {
  local suite="$1" root_task="$2" shard task
  echo "${root_task}"
  for shard in $(shards_of "${suite}"); do
    [ "${shard}" = "others" ] && continue
    for task in $(print_test_tasks "$(projects_var "${suite}" "${shard}")" "${suite}"); do
      printf -- '-x\n%s\n' "${task}"
    done
  done
}

[ $# -eq 2 ] || usage
suite="$1"
shard="$2"
shard_names="$(shards_of "${suite}")"

if [ "${shard}" = "--list" ]; then
  printf '['
  sep=""
  for name in ${shard_names}; do
    printf '%s"%s"' "${sep}" "${name}"
    sep=","
  done
  printf ']\n'
  exit 0
fi

# Keep both modes for every supported connector version; tests compare this list with settings.
if [ "${suite}" = "flink" ]; then
  case "${shard}" in
    embedded|deploy) modes=("${shard}") ;;
    all) modes=(embedded deploy) ;;
    *) echo "Unknown shard '${shard}' for suite '${suite}'" >&2; usage ;;
  esac
  for version in 1.19 1.20 2.1 2.2; do
    for mode in "${modes[@]}"; do
      printf '%s %s\n' "${mode}" "${version}"
    done
  done
  exit 0
fi

if [ "${suite}" = "access-control" ]; then
  case "${shard}" in
    h2|mysql|postgresql) backends=("${shard}") ;;
    all) backends=(h2 mysql postgresql) ;;
    *) echo "Unknown shard '${shard}' for suite '${suite}'" >&2; usage ;;
  esac
  for backend in "${backends[@]}"; do
    mode=deploy
    [ "${backend}" != "h2" ] || mode=embedded
    printf '%s %s\n' "${mode}" "${backend}"
  done
  exit 0
fi

if [ "${suite}/${shard}" = "backend-it/all" ]; then
  printf '%s\n' test :iceberg:iceberg-rest-server:experimentalTest
  exit 0
fi

if [ "${shard}" = "others" ]; then
  case "${suite}" in
    build) print_others build build ;;
    backend-it) print_others backend-it test ;;
    *) echo "Unknown suite: ${suite}" >&2; usage ;;
  esac
else
  projects="$(projects_var "${suite}" "${shard}")"
  print_test_tasks "${projects}" "${suite}"
fi
