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

# Copies the Apache Gravitino Trino connector jars matching TRINO_VERSION to
# /target/.
#
# Environment variables:
#   TRINO_VERSION   - Specific Trino version number (default: 478), or an exact
#                     range directory name such as "473-478".
#   LIST_VERSIONS   - When "true", only list the available version ranges and
#                     exit. Useful for `docker run --rm <image>`.
#
# The supported version ranges are DISCOVERED at runtime from the directories
# baked into /connectors (trino-<lo>-<hi>), so this script needs no edits when
# the set of ranges changes between branches.
#
# As an init container, a missing /target volume is treated as an error so a
# misconfigured pod fails fast instead of letting the engine start with an
# empty plugin directory. Set LIST_VERSIONS=true to only inspect the image.

set -euo pipefail

TRINO_VERSION="${TRINO_VERSION:-478}"
LIST_VERSIONS="${LIST_VERSIONS:-false}"

list_available_ranges() {
  ls -1 /connectors/ 2>/dev/null | grep "^trino-" | sed 's/trino-/  - /'
}

# Resolve a specific Trino version number to the matching connector range
# directory by inspecting the ranges actually present under /connectors.
# Prints the resolved range (empty if unsupported) and always returns 0, so it
# is safe under `set -e` in a command substitution.
resolve_version_range() {
  local version="$1"
  local dir range lo hi

  # Exact range directory name passed directly, e.g. "473-478".
  if [ -d "/connectors/trino-${version}" ]; then
    echo "${version}"
    return 0
  fi

  # A specific numeric version: find the range [lo,hi] that contains it.
  if echo "${version}" | grep -qE '^[0-9]+$'; then
    for dir in /connectors/trino-*; do
      [ -d "$dir" ] || continue
      range="$(basename "$dir" | sed 's/^trino-//')"
      lo="${range%-*}"
      hi="${range#*-}"
      case "${lo}${hi}" in
        *[!0-9]*) continue ;;
      esac
      if [ "$version" -ge "$lo" ] && [ "$version" -le "$hi" ]; then
        echo "${range}"
        return 0
      fi
    done
  fi

  echo ""
  return 0
}

if [ "${LIST_VERSIONS}" = "true" ]; then
  echo "Apache Gravitino Trino/Starburst connector jars available at /connectors/"
  echo ""
  echo "Available connector ranges:"
  list_available_ranges
  echo ""
  echo "Usage: mount a /target volume and set TRINO_VERSION (e.g. TRINO_VERSION=478)."
  exit 0
fi

if [ ! -d "/target" ]; then
  echo "ERROR: /target volume is not mounted." >&2
  echo "Mount an empty volume at /target (the engine plugin directory) so the" >&2
  echo "connector can be installed. To only list versions, run with LIST_VERSIONS=true." >&2
  exit 1
fi

VERSION_RANGE="$(resolve_version_range "$TRINO_VERSION")"

if [ -z "$VERSION_RANGE" ]; then
  echo "ERROR: Trino version ${TRINO_VERSION} is not supported by this image." >&2
  echo "" >&2
  echo "Available connector ranges:" >&2
  list_available_ranges >&2
  exit 1
fi

SOURCE_DIR="/connectors/trino-${VERSION_RANGE}"

if [ ! -d "$SOURCE_DIR" ]; then
  echo "ERROR: Connector directory not found: ${SOURCE_DIR}" >&2
  exit 1
fi

echo "Trino version ${TRINO_VERSION} resolved to connector range: ${VERSION_RANGE}"
echo "Copying connector jars to /target/..."
cp -r "${SOURCE_DIR}"/* /target/

echo ""
echo "Done. Files in /target/:"
find /target -name "*.jar" | sort
