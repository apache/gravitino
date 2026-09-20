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

# Build every Apache Gravitino Trino connector version range present in the
# source tree and prepare the layout consumed by the Docker image.
#
# The set of version-range modules is DISCOVERED from the Gradle project, so
# this script does not hard-code which ranges exist. Whatever the checked-out
# branch supports (for example 440-478 on main, or 435-478 on branch-1.3) is
# built automatically.
#
# Output layout:
#   packages/connectors/trino-<range>/   (plugin dir: jars + LICENSE + NOTICE + README)
#
# Trino ships its own MySQL and PostgreSQL drivers inside its mysql and
# postgresql plugins, so no JDBC drivers are bundled here.

set -euo pipefail

conn_dir="$(dirname "${BASH_SOURCE-$0}")"
conn_dir="$(cd "${conn_dir}" >/dev/null; pwd)"
gravitino_home="$(cd "${conn_dir}/../../.." >/dev/null; pwd)"

cd "${gravitino_home}"

# Discover all Trino connector version-range modules from the Gradle project,
# e.g. "trino-connector-440-445". Keep stderr so a Gradle failure is visible
# instead of being misreported as "no modules found".
modules="$(./gradlew -q projects | grep -oE "trino-connector-[0-9]+-[0-9]+" | sort -u)"

if [ -z "${modules}" ]; then
  echo "ERROR: no trino-connector version-range modules found in the Gradle project." >&2
  exit 1
fi

echo "Discovered Trino connector modules:"
echo "${modules}" | sed 's/^/  - /'

# Assemble each discovered version range.
tasks=""
for m in ${modules}; do
  tasks="${tasks} :trino-connector:${m}:assembleTrinoConnector"
done

# shellcheck disable=SC2086
./gradlew ${tasks} -x test

# Clean old packages
rm -rf "${conn_dir}/packages"
mkdir -p "${conn_dir}/packages/connectors"

# The assembleTrinoConnector task produces, per module, a plugin directory at
# distribution/gravitino-trino-connector-<range>/ (jars + LICENSE + NOTICE + README).
# Copy ONLY the ranges built in this run (derived from the discovered modules),
# so leftover distribution/ directories from previous builds are never picked up.
copied=0
for m in ${modules}; do
  range="${m#trino-connector-}"
  dir="distribution/gravitino-trino-connector-${range}"
  if [ -d "$dir" ]; then
    mkdir -p "${conn_dir}/packages/connectors/trino-${range}"
    cp -r "$dir"/* "${conn_dir}/packages/connectors/trino-${range}/"
    copied=$((copied + 1))
  else
    echo "ERROR: expected distribution directory not found: ${dir}" >&2
    exit 1
  fi
done

if [ "${copied}" -eq 0 ]; then
  echo "ERROR: no Trino connector bands were staged." >&2
  exit 1
fi

# Stage the canonical Apache-2.0 LICENSE and NOTICE from the repository root so
# the image ships the real texts (not drifting copies committed in-tree).
cp "${gravitino_home}/LICENSE" "${conn_dir}/licenses/LICENSE"
cp "${gravitino_home}/NOTICE" "${conn_dir}/licenses/NOTICE"

echo ""
echo "=== Trino connectors prepared (${copied} version range(s)) ==="
echo "Output: ${conn_dir}/packages/connectors/"
find "${conn_dir}/packages/connectors/" -maxdepth 1 -type d | sort
