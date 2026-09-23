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

# Copies the Apache Gravitino Flink connector jar matching FLINK_VERSION to
# /target/.
#
# Environment variables:
#   FLINK_VERSION   - Flink major version (default: 1.20)
#   LIST_VERSIONS   - When "true", only list the available versions and exit.
#                     Useful for `docker run --rm <image>`.
#
# The available versions are DISCOVERED at runtime from the directories baked
# into /connectors (flink-<ver>). All versions are Scala 2.12 only (Flink does
# not support Scala 2.13).
#
# As an init container, a missing /target volume is treated as an error so a
# misconfigured pod fails fast instead of letting the engine start without the
# connector. Set LIST_VERSIONS=true to only inspect the image.

set -euo pipefail

FLINK_VERSION="${FLINK_VERSION:-1.20}"
LIST_VERSIONS="${LIST_VERSIONS:-false}"

list_available_versions() {
  ls -1 /connectors/ 2>/dev/null | grep "^flink-" | sed 's/flink-/  - /'
}

if [ "${LIST_VERSIONS}" = "true" ]; then
  echo "Apache Gravitino Flink connector jars available at /connectors/ (all Scala 2.12):"
  echo ""
  list_available_versions
  echo ""
  echo "Usage: mount a /target volume and set FLINK_VERSION (e.g. FLINK_VERSION=1.20)."
  exit 0
fi

if [ ! -d "/target" ]; then
  echo "ERROR: /target volume is not mounted." >&2
  echo "Mount an empty volume at /target so the connector jar can be installed." >&2
  echo "To only list versions, run with LIST_VERSIONS=true." >&2
  exit 1
fi

SOURCE_DIR="/connectors/flink-${FLINK_VERSION}"

if [ ! -d "$SOURCE_DIR" ]; then
  echo "ERROR: Flink version ${FLINK_VERSION} is not supported by this image." >&2
  echo "" >&2
  echo "Available versions (all Scala 2.12):" >&2
  list_available_versions >&2
  exit 1
fi

echo "Copying Flink ${FLINK_VERSION} connector (Scala 2.12) to /target/..."
cp "${SOURCE_DIR}"/*.jar /target/
echo "Done. Jars copied to /target/:"
ls -1 /target/*.jar 2>/dev/null
