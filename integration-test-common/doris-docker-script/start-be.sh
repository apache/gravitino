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

# BE-only startup script for docker-compose multi-container deployment.
# This overrides the image's default start.sh which starts both FE and BE.

DORIS_HOME="$(dirname "${BASH_SOURCE-$0}")"
DORIS_HOME="$(cd "${DORIS_HOME}" >/dev/null; pwd)"
DORIS_BE_HOME="${DORIS_HOME}/be/"

# Skip vm.max_map_count and ulimit checks via env var (supported by Doris 3.0+ images).
# For older CI images that don't support this env var, fall back to sed patching.
export SKIP_CHECK_ULIMIT=true

DORIS_BE_SCRIPT="${DORIS_BE_HOME}/bin/start_be.sh"
# Fallback sed patches for CI images without SKIP_CHECK_ULIMIT support
sed -i '/Please set vm.max_map_count/,/exit 1/{s/exit 1/#exit 1\n        echo "skip this"/}' ${DORIS_BE_SCRIPT} 2>/dev/null || true
sed -i '/Set kernel parameter.*vm.max_map_count/,/exit 1/{s/exit 1/#exit 1\n        echo "skip this"/}' ${DORIS_BE_SCRIPT} 2>/dev/null || true
sed -i '/Please set the maximum number of open file descriptors/,/exit 1/{s/exit 1/#exit 1\n        echo "skip this"/}' ${DORIS_BE_SCRIPT} 2>/dev/null || true
sed -i 's/chmod 755 "${DORIS_HOME}\/lib\/doris_be"/#&/' ${DORIS_BE_SCRIPT} 2>/dev/null || true

# Configure priority_networks and report interval in be.conf
CONTAINER_IP=$(hostname -i)
PRIORITY_NETWORKS=$(echo "${CONTAINER_IP}" | awk -F '.' '{print$1"."$2"."$3".0/24"}')
echo "add priority_networks = ${PRIORITY_NETWORKS} to be.conf"
echo "priority_networks = ${PRIORITY_NETWORKS}" >> ${DORIS_BE_HOME}/conf/be.conf
echo "report_disk_state_interval_seconds = 10" >> ${DORIS_BE_HOME}/conf/be.conf

# Start only BE in daemon mode
${DORIS_BE_HOME}/bin/start_be.sh --daemon

# Wait for BE to be ready
be_started=false
for i in {1..20}; do
  sleep 5
  url="localhost:8040/api/health"
  echo "check be for the $i times"
  response=$(curl --silent --request GET $url)

  if echo "$response" | grep -q '"status": "OK"'; then
    be_started=true
    echo "Doris BE started successfully, response: $response"
    break
  else
    echo "check failed, response: $response"
  fi
done

if [ "$be_started" = false ]; then
  echo "ERROR: Doris BE failed to start"
  exit 1
fi

# Register this BE to the central FE
be_added=false
for i in {1..10}; do
  echo "add Doris BE to FE for the $i times"

  mysql -h doris-fe -P9030 -uroot -e "ALTER SYSTEM ADD BACKEND '${CONTAINER_IP}:9050'"
  if [ $? -ne 0 ]; then
    echo "Failed to add the BE to the FE"
  else
    be_added=true
    echo "Doris BE added to FE successfully"
    break
  fi

  sleep 1
done

if [ "$be_added" = false ]; then
  echo "ERROR: Doris BE failed to add to FE"
  exit 1
fi

# Keep container alive
tail -f /dev/null
