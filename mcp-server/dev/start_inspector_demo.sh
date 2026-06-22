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
# Bring up a persistent local demo environment for hands-on MCP authorization
# testing with the MCP Inspector:
#
#   1. Gravitino server with simple auth + authorization enabled.
#   2. Demo data: metalake + two catalogs, user "bob", a role granting bob
#      access to only one catalog (so admin and bob see different slices).
#   3. MCP server in HTTP transport mode.
#
# Unlike run_authz_integration_test.sh, this script LEAVES everything running so
# you can drive it from the Inspector. Run stop_inspector_demo.sh to tear down.
#
# Usage:
#   ./dev/start_inspector_demo.sh
#   GRAVITINO_HOME=/path/to/distribution ./dev/start_inspector_demo.sh

set -euo pipefail

# Everything is local; never route through an HTTP proxy.
export NO_PROXY="localhost,127.0.0.1"
export no_proxy="localhost,127.0.0.1"

MCP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "${MCP_DIR}/.." && pwd)"

GRAVITINO_PORT="${GRAVITINO_PORT:-8090}"
GRAVITINO_URI="http://127.0.0.1:${GRAVITINO_PORT}"
MCP_PORT="${MCP_PORT:-8000}"
MCP_URL="http://127.0.0.1:${MCP_PORT}/mcp"
MCP_METALAKE="${MCP_METALAKE:-mcp_authz_it}"
MCP_PID_FILE="${MCP_DIR}/.inspector-demo-mcp.pid"
ADMIN_AUTH="Basic $(printf '%s' 'admin:dummy' | base64)"

# MCP Inspector ports (defaults baked into @modelcontextprotocol/inspector).
INSPECTOR_UI_PORT="${INSPECTOR_UI_PORT:-6274}"
INSPECTOR_PROXY_PORT="${INSPECTOR_PROXY_PORT:-6277}"
INSPECTOR_PID_FILE="${MCP_DIR}/.inspector-demo-inspector.pid"

log() { echo "[demo] $*"; }

# ---------------------------------------------------------------------------
# 1. Resolve the Gravitino distribution
# ---------------------------------------------------------------------------
if [[ -z "${GRAVITINO_HOME:-}" ]]; then
  GRAVITINO_HOME="${REPO_ROOT}/distribution/package"
fi
if [[ ! -x "${GRAVITINO_HOME}/bin/gravitino.sh" ]]; then
  log "Distribution not found at ${GRAVITINO_HOME}."
  log "Build it first:  ./gradlew compileDistribution -x test -PskipWeb=true"
  log "Or set GRAVITINO_HOME to an existing distribution."
  exit 1
fi
log "Using GRAVITINO_HOME=${GRAVITINO_HOME}"

# ---------------------------------------------------------------------------
# 2. Enable simple auth + authorization (idempotent)
# ---------------------------------------------------------------------------
CONF="${GRAVITINO_HOME}/conf/gravitino.conf"
if ! grep -q "gravitino.authorization.enable = true" "${CONF}"; then
  cp "${CONF}" "${CONF}.inspector-demo.bak"
  sed -i.tmp \
    -e '/^gravitino.authenticators/d' \
    -e '/^gravitino.authorization.enable/d' \
    -e '/^gravitino.authorization.serviceAdmins/d' \
    "${CONF}"
  rm -f "${CONF}.tmp"
  cat >> "${CONF}" <<EOF

# --- injected by start_inspector_demo.sh (restored on stop) ---
gravitino.authenticators = simple
gravitino.authorization.enable = true
gravitino.authorization.serviceAdmins = admin
EOF
  log "Configured simple auth + authorization (serviceAdmins=admin)"
else
  log "Authorization already enabled in config"
fi

# ---------------------------------------------------------------------------
# 3. Start Gravitino (if not already up)
# ---------------------------------------------------------------------------
if curl -sf --noproxy '*' -H "Authorization: ${ADMIN_AUTH}" \
     "${GRAVITINO_URI}/api/version" >/dev/null 2>&1; then
  log "Gravitino already running on ${GRAVITINO_PORT}"
else
  log "Starting Gravitino server..."
  "${GRAVITINO_HOME}/bin/gravitino.sh" start
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
fi

# ---------------------------------------------------------------------------
# 4. Provision demo data (idempotent: drop + recreate the metalake)
# ---------------------------------------------------------------------------
log "Provisioning demo data into metalake '${MCP_METALAKE}'..."
(
  cd "${MCP_DIR}"
  GRAVITINO_URI="${GRAVITINO_URI}" MCP_METALAKE="${MCP_METALAKE}" \
  uv run python <<'PY'
import base64
import os

import httpx

from tests.integration.gravitino_setup import GravitinoFixture

uri = os.environ["GRAVITINO_URI"]
ml = os.environ["MCP_METALAKE"]


def hdr(user):
    return "Basic " + base64.b64encode(f"{user}:dummy".encode()).decode()


# Drop an existing metalake so the script can be re-run cleanly.
client = httpx.Client(
    base_url=uri, headers={"Authorization": hdr("admin")}, timeout=30
)
if client.get(f"/api/metalakes/{ml}").status_code == 200:
    client.put(
        f"/api/metalakes/{ml}",
        json={"updates": [{"@type": "setProperty", "property": "in-use", "value": "false"}]},
    )
    client.delete(f"/api/metalakes/{ml}?force=true")
    print(f"[demo] removed existing metalake '{ml}'")
client.close()

GravitinoFixture(uri, ml).provision()
print(f"[demo] provisioned metalake '{ml}': cat_allowed, cat_denied, user bob, reader role")

# Show the resulting authorization slice.
for user in ("admin", "bob"):
    r = httpx.get(
        f"{uri}/api/metalakes/{ml}/catalogs?details=true",
        headers={"Authorization": hdr(user)},
    )
    names = [c["name"] for c in r.json().get("catalogs", [])]
    print(f"[demo]   {user} sees catalogs: {names}")
PY
)

# ---------------------------------------------------------------------------
# 5. Start the MCP server in HTTP mode (if not already up)
# ---------------------------------------------------------------------------
if nc -z 127.0.0.1 "${MCP_PORT}" 2>/dev/null; then
  log "An MCP server is already listening on ${MCP_PORT}; leaving it as-is."
else
  log "Starting MCP server (HTTP) on ${MCP_URL}..."
  (
    cd "${MCP_DIR}"
    # Truncate (not delete) the audit log so the running process keeps its handle.
    : > gravitino-mcp-audit.log
    nohup uv run python -m mcp_server \
      --metalake "${MCP_METALAKE}" \
      --gravitino-uri "${GRAVITINO_URI}" \
      --transport http \
      --mcp-url "${MCP_URL}" > "${MCP_DIR}/.inspector-demo-mcp.out" 2>&1 &
    echo $! > "${MCP_PID_FILE}"
  )
  for i in $(seq 1 30); do
    nc -z 127.0.0.1 "${MCP_PORT}" 2>/dev/null && break
    sleep 1
  done
  if nc -z 127.0.0.1 "${MCP_PORT}" 2>/dev/null; then
    log "MCP server is up (pid $(cat "${MCP_PID_FILE}"))."
  else
    log "ERROR: MCP server did not start; see ${MCP_DIR}/.inspector-demo-mcp.out"
    exit 1
  fi
fi

# ---------------------------------------------------------------------------
# 6. Start the MCP Inspector (UI on :6274, proxy on :6277)
# ---------------------------------------------------------------------------
# DANGEROUSLY_OMIT_AUTH disables the session-token requirement so the plain
# http://localhost:6274/ URL works without a token (fine for a local demo).
# MCP_AUTO_OPEN_ENABLED=false keeps it from popping a browser when backgrounded.
# The Inspector binds to "localhost" (may be IPv6 ::1), so probe via localhost.
if nc -z localhost "${INSPECTOR_UI_PORT}" 2>/dev/null; then
  log "An Inspector is already listening on ${INSPECTOR_UI_PORT}; leaving it as-is."
else
  log "Starting MCP Inspector on http://localhost:${INSPECTOR_UI_PORT} ..."
  (
    cd "${MCP_DIR}"
    DANGEROUSLY_OMIT_AUTH=true \
    MCP_AUTO_OPEN_ENABLED=false \
    nohup npx @modelcontextprotocol/inspector \
      > "${MCP_DIR}/.inspector-demo-inspector.out" 2>&1 &
    echo $! > "${INSPECTOR_PID_FILE}"
  )
  for i in $(seq 1 60); do
    nc -z localhost "${INSPECTOR_UI_PORT}" 2>/dev/null && break
    sleep 1
  done
  if nc -z localhost "${INSPECTOR_UI_PORT}" 2>/dev/null; then
    log "Inspector is up (pid $(cat "${INSPECTOR_PID_FILE}"))."
  else
    log "WARN: Inspector did not come up; see ${MCP_DIR}/.inspector-demo-inspector.out"
    log "      (You can still start it manually: npx @modelcontextprotocol/inspector)"
  fi
fi

# ---------------------------------------------------------------------------
# 7. Print connection details
# ---------------------------------------------------------------------------
cat <<EOF

============================================================
 Demo environment is ready.
============================================================
 Gravitino : ${GRAVITINO_URI}   (simple auth + authorization)
 MCP server: ${MCP_URL}   (Streamable HTTP)
 Metalake  : ${MCP_METALAKE}
 Audit log : ${MCP_DIR}/gravitino-mcp-audit.log

 >>> Open the Inspector:  http://localhost:${INSPECTOR_UI_PORT}/

 In the Inspector, connect with:
   Transport Type : Streamable HTTP
   URL            : ${MCP_URL}
   Header Name    : Authorization
   Header Value   : ${ADMIN_AUTH}     <- admin
                    Basic $(printf '%s' 'bob:dummy' | base64)     <- bob

 Watch audit records live:
   tail -f ${MCP_DIR}/gravitino-mcp-audit.log

 Tear everything down (includes the Inspector):
   ./dev/stop_inspector_demo.sh
============================================================
EOF
