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
#   FLINK_VERSION  - Flink major version (default: 1.20)
#
# The available versions are DISCOVERED at runtime from the directories baked
# into /connectors (flink-<ver>). All versions are Scala 2.12 only (Flink does
# not support Scala 2.13).

set -e

FLINK_VERSION="${FLINK_VERSION:-1.20}"
SOURCE_DIR="/connectors/flink-${FLINK_VERSION}"

list_available_versions() {
  ls -1 /connectors/ 2>/dev/null | grep "^flink-" | sed 's/flink-/  - /'
}

if [ ! -d "$SOURCE_DIR" ]; then
  echo "ERROR: Flink version ${FLINK_VERSION} is not supported by this image."
  echo ""
  echo "Available versions (all Scala 2.12):"
  list_available_versions
  exit 1
fi

if [ -d "/target" ]; then
  echo "Copying Flink ${FLINK_VERSION} connector (Scala 2.12) to /target/..."
  cp "${SOURCE_DIR}"/*.jar /target/
  echo "Done. Jars copied to /target/:"
  ls -1 /target/*.jar 2>/dev/null
else
  echo "No /target volume mounted."
  echo ""
  echo "Usage: Mount /target volume and set FLINK_VERSION env var."
  echo "  docker run -e FLINK_VERSION=1.20 -v /path:/target <image>"
  echo ""
  echo "Available versions (all Scala 2.12):"
  list_available_versions
fi
