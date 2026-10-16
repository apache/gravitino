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

# Copies the Apache Gravitino Spark connector jar matching SPARK_VERSION and
# SCALA_VERSION to /target/.
#
# Environment variables:
#   SPARK_VERSION   - Spark major version (default: 3.5)
#   SCALA_VERSION   - Scala version. If unset, defaults to 2.13 for Spark 4.x
#                     (which is Scala 2.13 only) and 2.12 otherwise.
#   LIST_VERSIONS   - When "true", only list the available combinations and
#                     exit. Useful for `docker run --rm <image>`.
#
# The available combinations are DISCOVERED at runtime from the directories
# baked into /connectors (spark-<major>_<scala>), so this script needs no edits
# when the matrix changes between branches.
#
# As an init container, a missing /target volume is treated as an error so a
# misconfigured pod fails fast instead of letting the engine start without the
# connector. Set LIST_VERSIONS=true to only inspect the image.

set -euo pipefail

SPARK_VERSION="${SPARK_VERSION:-3.5}"
LIST_VERSIONS="${LIST_VERSIONS:-false}"

# Default Scala per Spark major: Spark 4.x is Scala 2.13 only.
if [ -z "${SCALA_VERSION:-}" ]; then
  case "${SPARK_VERSION}" in
    4.*) SCALA_VERSION="2.13" ;;
    *)   SCALA_VERSION="2.12" ;;
  esac
fi

SOURCE_DIR="/connectors/spark-${SPARK_VERSION}_${SCALA_VERSION}"

list_available_combos() {
  ls -1 /connectors/ 2>/dev/null | grep "^spark-" \
    | sed 's/spark-/  Spark /' | sed 's/_/ + Scala /'
}

if [ "${LIST_VERSIONS}" = "true" ]; then
  echo "Apache Gravitino Spark connector jars available at /connectors/:"
  echo ""
  list_available_combos
  echo ""
  echo "Usage: mount a /target volume and set SPARK_VERSION / SCALA_VERSION"
  echo "  (e.g. SPARK_VERSION=3.5 SCALA_VERSION=2.12)."
  exit 0
fi

if [ ! -d "/target" ]; then
  echo "ERROR: /target volume is not mounted." >&2
  echo "Mount an empty volume at /target so the connector jar can be installed." >&2
  echo "To only list versions, run with LIST_VERSIONS=true." >&2
  exit 1
fi

if [ ! -d "$SOURCE_DIR" ]; then
  echo "ERROR: Spark ${SPARK_VERSION} with Scala ${SCALA_VERSION} is not supported by this image." >&2
  echo "" >&2
  echo "Available combinations:" >&2
  list_available_combos >&2
  exit 1
fi

echo "Copying Spark ${SPARK_VERSION} connector (Scala ${SCALA_VERSION}) to /target/..."
cp "${SOURCE_DIR}"/*.jar /target/
echo "Done. Jars copied to /target/:"
ls -1 /target/*.jar 2>/dev/null
