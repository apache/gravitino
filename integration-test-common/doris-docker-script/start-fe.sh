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

# FE-only startup script for docker-compose multi-container deployment.
# This overrides the image's default start.sh which starts both FE and BE.

DORIS_HOME="$(dirname "${BASH_SOURCE-$0}")"
DORIS_HOME="$(cd "${DORIS_HOME}" >/dev/null; pwd)"
DORIS_FE_HOME="${DORIS_HOME}/fe/"

# Configure priority_networks based on container IP
CONTAINER_IP=$(hostname -i)
PRIORITY_NETWORKS=$(echo "${CONTAINER_IP}" | awk -F '.' '{print$1"."$2"."$3".0/24"}')
echo "add priority_networks = ${PRIORITY_NETWORKS} to fe.conf"
echo "priority_networks = ${PRIORITY_NETWORKS}" >> ${DORIS_FE_HOME}/conf/fe.conf

# Start only FE in daemon mode
${DORIS_FE_HOME}/bin/start_fe.sh --daemon

# Wait for FE to be ready
fe_started=false
for i in {1..20}; do
  sleep 5
  url="http://localhost:8030/api/bootstrap"
  echo "check fe for the $i times"
  response=$(curl --silent --request GET $url)

  if echo "$response" | grep -q '"msg":"success"'; then
    fe_started=true
    echo "Doris FE started successfully, response: $response"
    break
  else
    echo "check failed, response: $response"
  fi
done

if [ "$fe_started" = false ]; then
  echo "ERROR: Doris FE failed to start"
fi

# Keep container alive
tail -f /dev/null
