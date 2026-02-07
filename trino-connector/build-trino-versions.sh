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

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

START_VERSION=""
if [ "$#" -ge 1 ]; then
  START_VERSION="$1"
  shift
elif [ -n "${TRINO_VERSION_START:-}" ]; then
  START_VERSION="${TRINO_VERSION_START}"
else
  START_VERSION=435
fi

END_VERSION=""
if [ "$#" -ge 1 ]; then
  END_VERSION="$1"
  shift
elif [ -n "${TRINO_VERSION_END:-}" ]; then
  END_VERSION="${TRINO_VERSION_END}"
fi

if [[ ! ${START_VERSION} =~ ^[0-9]+$ ]]; then
  echo "Invalid starting Trino version: ${START_VERSION}" >&2
  exit 1
fi

if [ -n "${END_VERSION}" ] && [[ ! ${END_VERSION} =~ ^[0-9]+$ ]]; then
  echo "Invalid ending Trino version: ${END_VERSION}" >&2
  exit 1
fi

if [ "$#" -gt 0 ]; then
  echo "Unexpected arguments: $*" >&2
  echo "Usage: $0 [startVersion] [endVersion]" >&2
  exit 1
fi

EXTRA_ARGS=()
if [ -n "${GRADLE_ARGS:-}" ]; then
  # shellcheck disable=SC2206
  EXTRA_ARGS=(${GRADLE_ARGS})
fi

CURRENT_VERSION=${START_VERSION}
while true; do
  if [ -n "${END_VERSION}" ] && [ "${CURRENT_VERSION}" -gt "${END_VERSION}" ]; then
    echo "Completed builds through version ${END_VERSION}."
    break
  fi
  echo "=============================="
  echo "Building Trino connector for version ${CURRENT_VERSION}"
  echo "=============================="
  if [ ${#EXTRA_ARGS[@]} -gt 0 ]; then
    if ! ./gradlew :trino-connector:trino-connector:clean :trino-connector:trino-connector:build -PtrinoVersion="${CURRENT_VERSION}" "${EXTRA_ARGS[@]}"; then
      echo "Build failed for version ${CURRENT_VERSION}." >&2
      exit 1
    fi
  else
    if ! ./gradlew :trino-connector:trino-connector:clean :trino-connector:trino-connector:build -PtrinoVersion="${CURRENT_VERSION}"; then
      echo "Build failed for version ${CURRENT_VERSION}." >&2
      exit 1
    fi
  fi
  if [ -n "${END_VERSION}" ] && [ "${CURRENT_VERSION}" -ge "${END_VERSION}" ]; then
    echo "Completed builds through version ${CURRENT_VERSION}."
    break
  fi
  CURRENT_VERSION=$((CURRENT_VERSION + 1))
done
