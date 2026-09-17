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

set -ex

script_dir="$(dirname "${BASH_SOURCE-$0}")"
script_dir="$(cd "${script_dir}" >/dev/null; pwd)"
gravitino_home="$(cd "${script_dir}/../../.." >/dev/null; pwd)"

cd "${gravitino_home}"

# Discover all Trino connector version-range modules from the Gradle project,
# e.g. "trino-connector-440-445". This tracks the branch without edits here.
modules="$(./gradlew -q projects 2>/dev/null \
  | grep -oE "trino-connector-[0-9]+-[0-9]+" \
  | sort -u)"

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
rm -rf "${script_dir}/packages"
mkdir -p "${script_dir}/packages/connectors"

# The assembleTrinoConnector task produces, per module, a plugin directory at
# distribution/gravitino-trino-connector-<range>/ (jars + LICENSE + NOTICE + README).
# Copy each into the image layout as trino-<range>/.
copied=0
for dir in distribution/gravitino-trino-connector-*; do
  if [ -d "$dir" ]; then
    version="$(basename "$dir" | sed 's/gravitino-trino-connector-//')"
    mkdir -p "${script_dir}/packages/connectors/trino-${version}"
    cp -r "$dir"/* "${script_dir}/packages/connectors/trino-${version}/"
    copied=$((copied + 1))
  fi
done

if [ "${copied}" -eq 0 ]; then
  echo "ERROR: no distribution/gravitino-trino-connector-* directories were produced." >&2
  exit 1
fi

echo ""
echo "=== Trino connectors prepared (${copied} version range(s)) ==="
echo "Output: ${script_dir}/packages/connectors/"
find "${script_dir}/packages/connectors/" -maxdepth 1 -type d | sort
