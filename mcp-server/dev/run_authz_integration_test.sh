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
# End-to-end authorization integration test for the Gravitino MCP server.
#
# This script:
#   1. Builds the Gravitino distribution (unless GRAVITINO_HOME is provided).
#   2. Enables simple authentication + authorization (serviceAdmins=admin).
#   3. Starts the Gravitino server.
#   4. Starts the MCP server in HTTP transport mode.
#   5. Runs the pytest integration suite (which provisions metadata as admin and
#      verifies per-user authorization through MCP).
#   6. Tears everything down and restores the original config.
#
# Usage:
#   ./dev/run_authz_integration_test.sh
#   GRAVITINO_HOME=/path/to/distribution ./dev/run_authz_integration_test.sh

set -euo pipefail

# All services run on localhost; never route them through an HTTP proxy.
# httpx (MCP server -> Gravitino, test client) and curl both honour NO_PROXY.
export NO_PROXY="localhost,127.0.0.1"
export no_proxy="localhost,127.0.0.1"

MCP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "${MCP_DIR}/.." && pwd)"

# Use the loopback literal (not "localhost") so the MCP server binds to a
# guaranteed-assignable address; on some hosts "localhost" resolves to a LAN IP.
GRAVITINO_PORT="${GRAVITINO_PORT:-8090}"
GRAVITINO_URI="http://127.0.0.1:${GRAVITINO_PORT}"
MCP_PORT="${MCP_PORT:-8000}"
MCP_URL="http://127.0.0.1:${MCP_PORT}/mcp"
MCP_METALAKE="${MCP_METALAKE:-mcp_authz_it}"
MCP_AUDIT_LOG="${MCP_DIR}/gravitino-mcp-audit.log"

MCP_PID=""
GRAVITINO_STARTED="false"
CONF_BACKUP=""

log() { echo "[authz-it] $*"; }

cleanup() {
  log "Tearing down..."
  if [[ -n "${MCP_PID}" ]] && kill -0 "${MCP_PID}" 2>/dev/null; then
    kill "${MCP_PID}" 2>/dev/null || true
    wait "${MCP_PID}" 2>/dev/null || true
  fi
  if [[ "${GRAVITINO_STARTED}" == "true" ]]; then
    "${GRAVITINO_HOME}/bin/gravitino.sh" stop || true
  fi
  if [[ -n "${CONF_BACKUP}" && -f "${CONF_BACKUP}" ]]; then
    mv "${CONF_BACKUP}" "${GRAVITINO_HOME}/conf/gravitino.conf"
    log "Restored original gravitino.conf"
  fi
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# 1. Resolve / build the Gravitino distribution
# ---------------------------------------------------------------------------
if [[ -z "${GRAVITINO_HOME:-}" ]]; then
  log "GRAVITINO_HOME not set; building distribution..."
  (cd "${REPO_ROOT}" && ./gradlew compileDistribution -x test)
  GRAVITINO_HOME="${REPO_ROOT}/distribution/package"
fi
log "Using GRAVITINO_HOME=${GRAVITINO_HOME}"

if [[ ! -x "${GRAVITINO_HOME}/bin/gravitino.sh" ]]; then
  log "ERROR: ${GRAVITINO_HOME}/bin/gravitino.sh not found"
  exit 1
fi

# ---------------------------------------------------------------------------
# 2. Enable simple auth + authorization
# ---------------------------------------------------------------------------
CONF="${GRAVITINO_HOME}/conf/gravitino.conf"
CONF_BACKUP="${CONF}.authz-it.bak"
cp "${CONF}" "${CONF_BACKUP}"

# Remove any pre-existing values for the keys we manage, then append ours.
sed -i.tmp \
  -e '/^gravitino.authenticators/d' \
  -e '/^gravitino.authorization.enable/d' \
  -e '/^gravitino.authorization.serviceAdmins/d' \
  "${CONF}"
rm -f "${CONF}.tmp"
cat >> "${CONF}" <<EOF

# --- injected by run_authz_integration_test.sh ---
gravitino.authenticators = simple
gravitino.authorization.enable = true
gravitino.authorization.serviceAdmins = admin
EOF
log "Configured simple auth + authorization (serviceAdmins=admin)"

# ---------------------------------------------------------------------------
# 3. Start Gravitino
# ---------------------------------------------------------------------------
log "Starting Gravitino server..."
"${GRAVITINO_HOME}/bin/gravitino.sh" start
GRAVITINO_STARTED="true"

# With simple auth enabled every request needs an Authorization header.
ADMIN_AUTH="Basic $(printf '%s' 'admin:dummy' | base64)"

log "Waiting for Gravitino to become healthy..."
for i in $(seq 1 60); do
  if curl -sf --noproxy '*' -H "Authorization: ${ADMIN_AUTH}" \
       "${GRAVITINO_URI}/api/version" >/dev/null 2>&1; then
    log "Gravitino is up."
    break
  fi
  if [[ "${i}" == "60" ]]; then
    log "ERROR: Gravitino did not become healthy in time"
    exit 1
  fi
  sleep 2
done

# ---------------------------------------------------------------------------
# 4. Start the MCP server in HTTP transport mode
# ---------------------------------------------------------------------------
log "Starting MCP server (HTTP) on ${MCP_URL}..."
rm -f "${MCP_AUDIT_LOG}"
(
  cd "${MCP_DIR}"
  uv run python -m mcp_server \
    --metalake "${MCP_METALAKE}" \
    --gravitino-uri "${GRAVITINO_URI}" \
    --transport http \
    --mcp-url "${MCP_URL}"
) &
MCP_PID=$!

log "Waiting for MCP server to become reachable..."
for i in $(seq 1 30); do
  if nc -z localhost "${MCP_PORT}" 2>/dev/null; then
    log "MCP server is up."
    break
  fi
  if [[ "${i}" == "30" ]]; then
    log "ERROR: MCP server did not start in time"
    exit 1
  fi
  sleep 1
done

# ---------------------------------------------------------------------------
# 5. Run the pytest integration suite
# ---------------------------------------------------------------------------
log "Running integration tests..."
(
  cd "${MCP_DIR}"
  GRAVITINO_URI="${GRAVITINO_URI}" \
  MCP_URL="${MCP_URL}" \
  MCP_METALAKE="${MCP_METALAKE}" \
  MCP_AUDIT_LOG="${MCP_AUDIT_LOG}" \
  uv run pytest tests/integration -v
)

log "Integration tests passed."
