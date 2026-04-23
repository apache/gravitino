#!/usr/bin/env bash
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

set -euo pipefail

SERVER="${SERVER:-http://localhost:8090}"
METALAKE="${METALAKE:-metalake}"
METASTORE_URIS="${METASTORE_URIS:-}"
ITERATIONS="${ITERATIONS:-1000}"
CATALOG_PREFIX="${CATALOG_PREFIX:-hive_oom}"
SLEEP_MS="${SLEEP_MS:-0}"
AUTH_HEADER="${AUTH_HEADER:-}"
CONTINUE_ON_ERROR="${CONTINUE_ON_ERROR:-false}"

usage() {
  cat <<EOF
Usage: $0 --metastore-uris <thrift://host:9083> [options]

Required:
  --metastore-uris <uri>           Hive metastore URI, e.g. thrift://localhost:9083

Options:
  --server <url>                   Gravitino server URL (default: ${SERVER})
  --metalake <name>                Metalake name (default: ${METALAKE})
  --iterations <n>                 Loop iterations (default: ${ITERATIONS})
  --catalog-prefix <prefix>        Catalog prefix (default: ${CATALOG_PREFIX})
  --sleep-ms <ms>                  Sleep between iterations (default: ${SLEEP_MS})
  --auth-header <header>           Extra HTTP header, e.g. "Authorization: Bearer xxx"
  --continue-on-error <true|false> Continue after failures (default: ${CONTINUE_ON_ERROR})
  -h, --help                       Show this help

Examples:
  $0 --metastore-uris thrift://localhost:9083 --iterations 20000
  $0 --metastore-uris thrift://hive:9083 --auth-header "Authorization: Bearer <token>"
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --server)
      SERVER="$2"
      shift 2
      ;;
    --metalake)
      METALAKE="$2"
      shift 2
      ;;
    --metastore-uris)
      METASTORE_URIS="$2"
      shift 2
      ;;
    --iterations)
      ITERATIONS="$2"
      shift 2
      ;;
    --catalog-prefix)
      CATALOG_PREFIX="$2"
      shift 2
      ;;
    --sleep-ms)
      SLEEP_MS="$2"
      shift 2
      ;;
    --auth-header)
      AUTH_HEADER="$2"
      shift 2
      ;;
    --continue-on-error)
      CONTINUE_ON_ERROR="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -z "${METASTORE_URIS}" ]]; then
  echo "--metastore-uris is required" >&2
  usage
  exit 1
fi

if ! [[ "${ITERATIONS}" =~ ^[0-9]+$ && "${ITERATIONS}" -gt 0 ]]; then
  echo "--iterations must be a positive integer" >&2
  exit 1
fi

if ! [[ "${SLEEP_MS}" =~ ^[0-9]+$ ]]; then
  echo "--sleep-ms must be a non-negative integer" >&2
  exit 1
fi

if [[ "${CONTINUE_ON_ERROR}" != "true" && "${CONTINUE_ON_ERROR}" != "false" ]]; then
  echo "--continue-on-error must be true or false" >&2
  exit 1
fi

HEADERS=(
  -H "Accept: application/vnd.gravitino.v1+json"
  -H "Content-Type: application/json"
)
if [[ -n "${AUTH_HEADER}" ]]; then
  HEADERS+=(-H "${AUTH_HEADER}")
fi

request() {
  local method="$1"
  local path="$2"
  local body="${3:-}"
  if [[ -n "${body}" ]]; then
    curl -sS --fail-with-body -X "${method}" "${HEADERS[@]}" --data "${body}" \
      "${SERVER}/api${path}" >/dev/null
  else
    curl -sS --fail-with-body -X "${method}" "${HEADERS[@]}" \
      "${SERVER}/api${path}" >/dev/null
  fi
}

drop_catalog_force() {
  local catalog="$1"
  curl -sS --fail-with-body -X DELETE "${HEADERS[@]}" \
    "${SERVER}/api/metalakes/${METALAKE}/catalogs/${catalog}?force=true" >/dev/null || true
}

run_id="$(date +%s)"
success=0
failed=0
current_catalog=""

trap 'if [[ -n "${current_catalog}" ]]; then drop_catalog_force "${current_catalog}"; fi' EXIT

echo "Start hive catalog OOM repro: server=${SERVER}, metalake=${METALAKE}, iterations=${ITERATIONS}"
for ((i = 1; i <= ITERATIONS; i++)); do
  catalog="${CATALOG_PREFIX}_${run_id}_${i}"
  schema="schema_${i}"
  table="table_${i}"
  current_catalog="${catalog}"

  catalog_body=$(cat <<EOF
{"name":"${catalog}","type":"RELATIONAL","provider":"hive","comment":"oom repro ${i}","properties":{"metastore.uris":"${METASTORE_URIS}"}}
EOF
)
  schema_body=$(cat <<EOF
{"name":"${schema}","comment":"oom repro schema ${i}","properties":{}}
EOF
)
  table_body=$(cat <<EOF
{"name":"${table}","comment":"oom repro table ${i}","columns":[{"name":"id","type":"integer","nullable":true},{"name":"name","type":"string","nullable":true}]}
EOF
)

  set +e
  err=0
  request POST "/metalakes/${METALAKE}/catalogs" "${catalog_body}" || err=1
  if [[ "${err}" -eq 0 ]]; then
    request POST "/metalakes/${METALAKE}/catalogs/${catalog}/schemas" "${schema_body}" || err=1
  fi
  if [[ "${err}" -eq 0 ]]; then
    request POST "/metalakes/${METALAKE}/catalogs/${catalog}/schemas/${schema}/tables" "${table_body}" \
      || err=1
  fi
  if [[ "${err}" -eq 0 ]]; then
    request GET "/metalakes/${METALAKE}/catalogs/${catalog}/schemas/${schema}/tables/${table}" || err=1
  fi
  if [[ "${err}" -eq 0 ]]; then
    request DELETE "/metalakes/${METALAKE}/catalogs/${catalog}?force=true" || err=1
  else
    drop_catalog_force "${catalog}"
  fi
  set -e

  if [[ "${err}" -eq 0 ]]; then
    success=$((success + 1))
    current_catalog=""
    echo "[OK] iteration=${i}, catalog=${catalog}"
  else
    failed=$((failed + 1))
    current_catalog=""
    echo "[FAIL] iteration=${i}, catalog=${catalog}" >&2
    if [[ "${CONTINUE_ON_ERROR}" != "true" ]]; then
      break
    fi
  fi

  if [[ "${SLEEP_MS}" -gt 0 ]]; then
    sleep "$(awk "BEGIN { printf \"%.3f\", ${SLEEP_MS}/1000 }")"
  fi
done

echo "Done. success=${success}, failed=${failed}, total=$((success + failed))"
