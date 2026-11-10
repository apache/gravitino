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
# Tear down the demo environment started by start_inspector_demo.sh:
# stops the MCP server, stops Gravitino, and restores the original config.

set -uo pipefail

MCP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "${MCP_DIR}/.." && pwd)"

MCP_PORT="${MCP_PORT:-8000}"
MCP_PID_FILE="${MCP_DIR}/.inspector-demo-mcp.pid"
INSPECTOR_UI_PORT="${INSPECTOR_UI_PORT:-6274}"
INSPECTOR_PROXY_PORT="${INSPECTOR_PROXY_PORT:-6277}"
INSPECTOR_PID_FILE="${MCP_DIR}/.inspector-demo-inspector.pid"

log() { echo "[demo] $*"; }

if [[ -z "${GRAVITINO_HOME:-}" ]]; then
  GRAVITINO_HOME="${REPO_ROOT}/distribution/package"
fi

# 1. Stop the MCP Inspector (npx spawns child processes; free its ports too).
if [[ -f "${INSPECTOR_PID_FILE}" ]]; then
  INSPECTOR_PID="$(cat "${INSPECTOR_PID_FILE}")"
  kill "${INSPECTOR_PID}" 2>/dev/null && \
    log "Stopped Inspector (pid ${INSPECTOR_PID})" || true
  rm -f "${INSPECTOR_PID_FILE}"
fi
for port in "${INSPECTOR_UI_PORT}" "${INSPECTOR_PROXY_PORT}"; do
  lsof -ti :"${port}" 2>/dev/null | xargs kill -9 2>/dev/null && \
    log "Freed Inspector port ${port}" || true
done

# 2. Stop the MCP server.
if [[ -f "${MCP_PID_FILE}" ]]; then
  MCP_PID="$(cat "${MCP_PID_FILE}")"
  if kill "${MCP_PID}" 2>/dev/null; then
    log "Stopped MCP server (pid ${MCP_PID})"
  fi
  rm -f "${MCP_PID_FILE}"
fi
# Belt and suspenders: free the port if anything is still bound.
lsof -ti :"${MCP_PORT}" 2>/dev/null | xargs kill -9 2>/dev/null && \
  log "Freed port ${MCP_PORT}" || true

# 3. Stop Gravitino.
if [[ -x "${GRAVITINO_HOME}/bin/gravitino.sh" ]]; then
  "${GRAVITINO_HOME}/bin/gravitino.sh" stop || true
  log "Stopped Gravitino"
fi

# 4. Restore the original config.
CONF="${GRAVITINO_HOME}/conf/gravitino.conf"
if [[ -f "${CONF}.inspector-demo.bak" ]]; then
  mv "${CONF}.inspector-demo.bak" "${CONF}"
  log "Restored original gravitino.conf"
fi

log "Teardown complete."
