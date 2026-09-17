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

# Build every Apache Gravitino Flink connector runtime shadow jar present in
# the source tree and prepare the layout consumed by the Docker image.
# Flink only supports Scala 2.12.
#
# The set of Flink runtime modules is DISCOVERED from the Gradle project, so
# this script does not hard-code which versions exist. Whatever the checked-out
# branch supports (for example 1.18/1.19/1.20) is built automatically.
#
# Output layout:
#   packages/connectors/flink-<ver>/gravitino-flink-connector-runtime-<ver>_2.12-*.jar

set -euo pipefail

conn_dir="$(dirname "${BASH_SOURCE-$0}")"
conn_dir="$(cd "${conn_dir}" >/dev/null; pwd)"
gravitino_home="$(cd "${conn_dir}/../../.." >/dev/null; pwd)"

cd "${gravitino_home}"

# Discover all Flink runtime modules from the Gradle project,
# e.g. "flink-runtime-1.20" -> version "1.20". Keep stderr so a Gradle failure
# is visible instead of being misreported as "no modules found".
runtime_modules="$(./gradlew -q projects | grep -oE "flink-runtime-[0-9]+\.[0-9]+" | sort -u)"

if [ -z "${runtime_modules}" ]; then
  echo "ERROR: no flink-runtime modules found in the Gradle project." >&2
  exit 1
fi

versions="$(echo "${runtime_modules}" | sed 's/flink-runtime-//' | sort -u)"

echo "Discovered Flink connector versions:"
echo "${versions}" | sed 's/^/  - /'

# Assemble each discovered runtime shadow jar (Scala 2.12 only).
tasks=""
for m in ${runtime_modules}; do
  tasks="${tasks} :flink-connector:${m}:shadowJar"
done

# shellcheck disable=SC2086
./gradlew ${tasks} -x test

# Clean old packages
rm -rf "${conn_dir}/packages"
mkdir -p "${conn_dir}/packages/connectors"

# Copy shadow jars (exclude *-empty.jar artifacts) from each version's build output.
copied=0
for ver in ${versions}; do
  libs_dir="flink-connector/v${ver}/flink-runtime/build/libs"
  dest="${conn_dir}/packages/connectors/flink-${ver}"
  if [ -d "${libs_dir}" ]; then
    mkdir -p "${dest}"
    found=0
    for jar in "${libs_dir}"/*.jar; do
      [ -e "$jar" ] || continue
      case "$jar" in
        *-empty*) continue ;;
      esac
      cp "$jar" "${dest}/"
      found=1
    done
    if [ "${found}" -eq 1 ]; then
      copied=$((copied + 1))
    else
      echo "ERROR: no runtime jar found under ${libs_dir}" >&2
      exit 1
    fi
  else
    echo "ERROR: expected build output not found: ${libs_dir}" >&2
    exit 1
  fi
done

if [ "${copied}" -eq 0 ]; then
  echo "ERROR: no Flink runtime jars were staged." >&2
  exit 1
fi

# Stage the canonical Apache-2.0 LICENSE and NOTICE from the repository root so
# the image ships the real texts (not drifting copies committed in-tree).
cp "${gravitino_home}/LICENSE" "${conn_dir}/licenses/LICENSE"
cp "${gravitino_home}/NOTICE" "${conn_dir}/licenses/NOTICE"

echo ""
echo "=== Flink connectors prepared (${copied} version(s)) ==="
echo "Output: ${conn_dir}/packages/connectors/"
find "${conn_dir}/packages/connectors/" -name "*.jar" | sort
