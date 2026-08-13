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
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Real-process entity-cache consistency test. Both Gravitino instances use the
# same relational entity store, but each owns an independent local cache.
# Every consistency case follows the same sequence:
#
#   1. Load the old entity through the target instance to warm its cache.
#   2. Alter, rename, disable, or drop it through the other instance.
#   3. Wait one poll interval plus a scheduling buffer.
#   4. Load it through the target and assert that the stale value is gone.
#
# Covered cache entity types and mutations:
#   METALAKE  alter, rename, disable/enable, drop
#   CATALOG   alter, rename, disable/enable, drop
#   SCHEMA    alter, rapid alters, drop, cascade drop, multi-level hierarchy
#   TABLE     alter, rename, drop
#   TOPIC     alter, drop
#   VIEW      alter, rename, drop
#   FILESET   alter, rename, drop
#   TAG       alter, rename, drop
#   POLICY    alter, rename, disable/enable, drop
#   JOB       status alter through cancellation
#
# Dedicated edge cases cover A-to-B and B-to-A propagation, drop plus recreate
# with the same name inside one poll window, several updates inside one poll
# window, cross-type batched changes, recursive invalidation, and restart at a
# change-log high-water mark.
#
# Kafka supplies the external topic backend. Iceberg's JDBC backend supplies
# schemas, tables, views, and hierarchical schemas; the managed fileset catalog
# supplies schema-level cascade behavior.

set -uo pipefail

INSTANCE_A="${INSTANCE_A:-http://localhost:8090}"
INSTANCE_B="${INSTANCE_B:-http://localhost:8190}"
INSTANCE_B_HOME="${INSTANCE_B_HOME:-distribution/package-b}"
ADMIN_USER="${ADMIN_USER:-admin}"
KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
POLL_WAIT_SECS="${POLL_WAIT_SECS:-1.5}"
RUN_RESTART_TEST="${RUN_RESTART_TEST:-false}"

SUFFIX="$(date +%s)_$$"
MAIN_METALAKE="entity_cache_${SUFFIX}"
LIFECYCLE_METALAKE="metalake_cache_${SUFFIX}"
RENAMED_METALAKE="metalake_cache_renamed_${SUFFIX}"
RECREATE_METALAKE="metalake_recreate_${SUFFIX}"
LIFECYCLE_CATALOG="catalog_cache_${SUFFIX}"
RENAMED_CATALOG="catalog_cache_renamed_${SUFFIX}"
RECREATE_CATALOG="catalog_recreate_${SUFFIX}"
ICEBERG_CATALOG="iceberg_cache_${SUFFIX}"
FILESET_CATALOG="fileset_cache_${SUFFIX}"
KAFKA_CATALOG="kafka_cache_${SUFFIX}"
SCHEMA_NAME="schema_cache_${SUFFIX}"
FILESET_SCHEMA="fileset_schema_${SUFFIX}"
TABLE_NAME="table_cache_${SUFFIX}"
RENAMED_TABLE="table_cache_renamed_${SUFFIX}"
RECREATE_TABLE="table_recreate_${SUFFIX}"
VIEW_NAME="view_cache_${SUFFIX}"
RENAMED_VIEW="view_cache_renamed_${SUFFIX}"
FILESET_NAME="fileset_entity_${SUFFIX}"
RENAMED_FILESET="fileset_entity_renamed_${SUFFIX}"
RECREATE_FILESET="fileset_recreate_${SUFFIX}"
TOPIC_NAME="topic_cache_${SUFFIX}"
TAG_NAME="tag_cache_${SUFFIX}"
RENAMED_TAG="tag_cache_renamed_${SUFFIX}"
RECREATE_TAG="tag_recreate_${SUFFIX}"
LATEST_TAG="tag_latest_${SUFFIX}"
POLICY_NAME="policy_cache_${SUFFIX}"
RENAMED_POLICY="policy_cache_renamed_${SUFFIX}"
JOB_TEMPLATE="job_cache_${SUFFIX}"
CASCADE_SCHEMA="cascade_schema_${SUFFIX}"
CASCADE_FILESET="cascade_fileset_${SUFFIX}"
HIER_PARENT="parent_cache_${SUFFIX}"
HIER_CHILD="${HIER_PARENT}:child"
HIER_GRANDCHILD="${HIER_CHILD}:grandchild"
HIER_SIBLING="${HIER_PARENT}:sibling"
HIER_TABLE="hier_grandchild_table_${SUFFIX}"
HIER_VIEW="hier_sibling_view_${SUFFIX}"
REVERSE_TAG="reverse_tag_${SUFFIX}"
RENAMED_REVERSE_TAG="reverse_tag_renamed_${SUFFIX}"
REVERSE_FILESET="reverse_fileset_${SUFFIX}"
RENAMED_REVERSE_FILESET="reverse_fileset_renamed_${SUFFIX}"
REVERSE_POLICY="reverse_policy_${SUFFIX}"
BURST_TAG="burst_tag_${SUFFIX}"
BURST_POLICY="burst_policy_${SUFFIX}"
BURST_FILESET="burst_fileset_${SUFFIX}"
RESTART_TAG="restart_tag_${SUFFIX}"

PASS=0
FAIL=0
CONSISTENCY_CASES=0
FAILED_TESTS=()
HTTP_CODE=""
RESPONSE_BODY=""
JOB_ID=""
JOB_SCRIPT=""

auth_header() {
  printf 'Basic %s' "$(jq -rn --arg credentials "$1:" '$credentials | @base64')"
}

# api <base_url> <method> <path> [body_json]
# Writes the response status and body to HTTP_CODE and RESPONSE_BODY.
api() {
  local base="$1" method="$2" path="$3" body="${4:-}"
  local tmp
  tmp="$(mktemp -t gravitino_entity_cache_api.XXXX)"
  if [[ -n "$body" ]]; then
    HTTP_CODE=$(curl -sS --connect-timeout 10 --max-time 60 \
      -o "$tmp" -w '%{http_code}' \
      -H "Authorization: $(auth_header "$ADMIN_USER")" \
      -H 'Accept: application/vnd.gravitino.v1+json' \
      -H 'Content-Type: application/json' \
      -X "$method" --data "$body" "${base}${path}" || echo 000)
  else
    HTTP_CODE=$(curl -sS --connect-timeout 10 --max-time 60 \
      -o "$tmp" -w '%{http_code}' \
      -H "Authorization: $(auth_header "$ADMIN_USER")" \
      -H 'Accept: application/vnd.gravitino.v1+json' \
      -X "$method" "${base}${path}" || echo 000)
  fi
  RESPONSE_BODY="$(<"$tmp")"
  rm -f "$tmp"
}

pass() {
  PASS=$((PASS + 1))
  printf '  \033[32m✓\033[0m %s\n' "$1"
}

fail() {
  FAIL=$((FAIL + 1))
  FAILED_TESTS+=("$1")
  printf '  \033[31m✗\033[0m %s\n' "$1"
}

body_snippet() {
  printf '%s' "$RESPONSE_BODY" | tr '\n' ' ' | cut -c1-240
}

expect_http() {
  local desc="$1" expected="$2"
  if [[ "$HTTP_CODE" == "$expected" ]]; then
    pass "$desc"
  else
    fail "$desc — expected HTTP $expected, got $HTTP_CODE. Body: $(body_snippet)"
  fi
}

expect_value() {
  local desc="$1" filter="$2" expected="$3"
  local actual
  if [[ "$HTTP_CODE" != "200" ]]; then
    fail "$desc — expected HTTP 200, got $HTTP_CODE. Body: $(body_snippet)"
    return
  fi
  if ! actual=$(printf '%s' "$RESPONSE_BODY" | jq -r "$filter" 2>/dev/null); then
    fail "$desc — response does not contain $filter. Body: $(body_snippet)"
    return
  fi
  if [[ "$actual" == "null" ]]; then
    fail "$desc — response does not contain $filter. Body: $(body_snippet)"
    return
  fi
  if [[ "$actual" == "$expected" ]]; then
    pass "$desc"
  else
    fail "$desc — expected '$expected', got '$actual'. Body: $(body_snippet)"
  fi
}

expect_one_of() {
  local desc="$1" filter="$2"
  shift 2
  local actual expected
  if [[ "$HTTP_CODE" != "200" ]]; then
    fail "$desc — expected HTTP 200, got $HTTP_CODE. Body: $(body_snippet)"
    return
  fi
  if ! actual=$(printf '%s' "$RESPONSE_BODY" | jq -r "$filter" 2>/dev/null); then
    fail "$desc — response does not contain $filter. Body: $(body_snippet)"
    return
  fi
  if [[ "$actual" == "null" ]]; then
    fail "$desc — response does not contain $filter. Body: $(body_snippet)"
    return
  fi
  for expected in "$@"; do
    if [[ "$actual" == "$expected" ]]; then
      pass "$desc"
      return
    fi
  done
  fail "$desc — got unexpected value '$actual'. Body: $(body_snippet)"
}

section() {
  printf '\n\033[1m== %s ==\033[0m\n' "$1"
}

consistency_case() {
  CONSISTENCY_CASES=$((CONSISTENCY_CASES + 1))
  printf '\n  \033[1m[consistency case %d]\033[0m %s\n' "$CONSISTENCY_CASES" "$1"
}

mutate_a() {
  local desc="$1" method="$2" path="$3" body="${4:-}"
  api "$INSTANCE_A" "$method" "$path" "$body"
  expect_http "$desc" 200
}

mutate_b() {
  local desc="$1" method="$2" path="$3" body="${4:-}"
  api "$INSTANCE_B" "$method" "$path" "$body"
  expect_http "$desc" 200
}

wait_for_invalidation() {
  local target="${1:-instance B}"
  printf '  ...waiting %ss for %s change-log polling\n' "$POLL_WAIT_SECS" "$target"
  sleep "$POLL_WAIT_SECS"
}

wait_ready() {
  local url="$1"
  local attempt
  for attempt in $(seq 1 60); do
    if curl -fsS --connect-timeout 2 --max-time 5 "$url/api/version" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  return 1
}

cleanup() {
  if [[ -n "$JOB_ID" ]]; then
    api "$INSTANCE_A" POST "/api/metalakes/${MAIN_METALAKE}/jobs/runs/${JOB_ID}" >/dev/null 2>&1 || true
  fi
  api "$INSTANCE_A" DELETE "/api/metalakes/${MAIN_METALAKE}?force=true" >/dev/null 2>&1 || true
  api "$INSTANCE_A" DELETE "/api/metalakes/${LIFECYCLE_METALAKE}?force=true" >/dev/null 2>&1 || true
  api "$INSTANCE_A" DELETE "/api/metalakes/${RENAMED_METALAKE}?force=true" >/dev/null 2>&1 || true
  api "$INSTANCE_A" DELETE "/api/metalakes/${RECREATE_METALAKE}?force=true" >/dev/null 2>&1 || true
  if [[ -n "$JOB_SCRIPT" ]]; then
    rm -f "$JOB_SCRIPT"
  fi
}
trap cleanup EXIT

section "Preflight"
for command_name in curl jq; do
  if command -v "$command_name" >/dev/null 2>&1; then
    pass "$command_name is available"
  else
    fail "$command_name is required"
  fi
done

api "$INSTANCE_A" GET /api/version
expect_http "instance A is reachable" 200
api "$INSTANCE_B" GET /api/version
expect_http "instance B is reachable" 200
if ((FAIL > 0)); then
  printf '\nPreflight failed; entity-cache scenarios were not executed.\n'
  exit 1
fi

section "METALAKE cache: alter, rename, disable/enable, and drop"
mutate_a "create lifecycle metalake on A" POST /api/metalakes \
  "{\"name\":\"${LIFECYCLE_METALAKE}\",\"comment\":\"metalake-old\",\"properties\":{}}"
api "$INSTANCE_B" GET "/api/metalakes/${LIFECYCLE_METALAKE}"
expect_value "warm metalake cache on B" '.metalake.comment' metalake-old

consistency_case "METALAKE alter propagates A -> B"
mutate_a "alter metalake comment on A" PUT "/api/metalakes/${LIFECYCLE_METALAKE}" \
  '{"updates":[{"@type":"updateComment","newComment":"metalake-new"}]}'
wait_for_invalidation
api "$INSTANCE_B" GET "/api/metalakes/${LIFECYCLE_METALAKE}"
expect_value "B sees altered metalake" '.metalake.comment' metalake-new

consistency_case "METALAKE rename invalidates the old name on B"
mutate_a "rename metalake on A" PUT "/api/metalakes/${LIFECYCLE_METALAKE}" \
  "{\"updates\":[{\"@type\":\"rename\",\"newName\":\"${RENAMED_METALAKE}\"}]}"
wait_for_invalidation
api "$INSTANCE_B" GET "/api/metalakes/${LIFECYCLE_METALAKE}"
# Authorization resolves the missing metalake before dispatching the load, so
# a missing top-level name is surfaced as forbidden rather than not found.
expect_http "B no longer serves the old metalake name" 403
api "$INSTANCE_B" GET "/api/metalakes/${RENAMED_METALAKE}"
expect_value "B loads the renamed metalake" '.metalake.name' "$RENAMED_METALAKE"

consistency_case "METALAKE disable blocks operations through B"
mutate_a "disable metalake on A" PATCH "/api/metalakes/${RENAMED_METALAKE}" '{"inUse":false}'
wait_for_invalidation
api "$INSTANCE_B" GET "/api/metalakes/${RENAMED_METALAKE}"
expect_value "B sees disabled metalake state" '.metalake.properties["in-use"]' false
api "$INSTANCE_B" GET "/api/metalakes/${RENAMED_METALAKE}/catalogs"
expect_http "B rejects an operation under the disabled metalake" 409

consistency_case "METALAKE enable restores operations through B"
mutate_a "enable metalake on A" PATCH "/api/metalakes/${RENAMED_METALAKE}" '{"inUse":true}'
wait_for_invalidation
api "$INSTANCE_B" GET "/api/metalakes/${RENAMED_METALAKE}"
expect_value "B sees enabled metalake state" '.metalake.properties["in-use"]' true
api "$INSTANCE_B" GET "/api/metalakes/${RENAMED_METALAKE}/catalogs"
expect_http "B accepts an operation under the re-enabled metalake" 200

consistency_case "METALAKE drop invalidates B"
mutate_a "drop metalake on A" DELETE "/api/metalakes/${RENAMED_METALAKE}?force=true"
wait_for_invalidation
api "$INSTANCE_B" GET "/api/metalakes/${RENAMED_METALAKE}"
expect_http "B no longer serves the dropped metalake" 403

section "METALAKE edge case: drop and recreate the same name in one poll window"
mutate_a "create recreate-probe metalake on A" POST /api/metalakes \
  "{\"name\":\"${RECREATE_METALAKE}\",\"comment\":\"metalake-generation-1\",\"properties\":{}}"
api "$INSTANCE_B" GET "/api/metalakes/${RECREATE_METALAKE}"
expect_value "warm first metalake generation on B" '.metalake.comment' metalake-generation-1

consistency_case "METALAKE drop plus same-name recreate evicts the old generation"
mutate_a "drop first metalake generation on A" DELETE "/api/metalakes/${RECREATE_METALAKE}?force=true"
mutate_a "recreate the same metalake name on A" POST /api/metalakes \
  "{\"name\":\"${RECREATE_METALAKE}\",\"comment\":\"metalake-generation-2\",\"properties\":{}}"
wait_for_invalidation
api "$INSTANCE_B" GET "/api/metalakes/${RECREATE_METALAKE}"
expect_value "B loads the second metalake generation" '.metalake.comment' metalake-generation-2

section "Shared test metalake"
mutate_a "create shared test metalake" POST /api/metalakes \
  "{\"name\":\"${MAIN_METALAKE}\",\"comment\":\"entity cache test\",\"properties\":{}}"
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}"
expect_value "B loads shared test metalake" '.metalake.name' "$MAIN_METALAKE"

section "CATALOG cache: alter, rename, disable/enable, rebuild, and drop"
mutate_a "create lifecycle catalog on A" POST "/api/metalakes/${MAIN_METALAKE}/catalogs" \
  "{\"name\":\"${LIFECYCLE_CATALOG}\",\"type\":\"FILESET\",\"comment\":\"catalog-old\",\"properties\":{\"disable-filesystem-ops\":\"true\"}}"
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${LIFECYCLE_CATALOG}"
expect_value "warm catalog cache on B" '.catalog.comment' catalog-old

consistency_case "CATALOG alter propagates A -> B"
mutate_a "alter catalog comment on A" PUT "/api/metalakes/${MAIN_METALAKE}/catalogs/${LIFECYCLE_CATALOG}" \
  '{"updates":[{"@type":"updateComment","newComment":"catalog-new"}]}'
wait_for_invalidation
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${LIFECYCLE_CATALOG}"
expect_value "B sees altered catalog" '.catalog.comment' catalog-new

consistency_case "CATALOG rename invalidates the old name and connector on B"
mutate_a "rename catalog on A" PUT "/api/metalakes/${MAIN_METALAKE}/catalogs/${LIFECYCLE_CATALOG}" \
  "{\"updates\":[{\"@type\":\"rename\",\"newName\":\"${RENAMED_CATALOG}\"}]}"
wait_for_invalidation
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${LIFECYCLE_CATALOG}"
expect_http "B invalidates the old catalog name" 404
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${RENAMED_CATALOG}"
expect_value "B loads the renamed catalog" '.catalog.name' "$RENAMED_CATALOG"

consistency_case "CATALOG disable rebuilds B's connector as unavailable"
mutate_a "disable catalog on A" PATCH "/api/metalakes/${MAIN_METALAKE}/catalogs/${RENAMED_CATALOG}" '{"inUse":false}'
wait_for_invalidation
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${RENAMED_CATALOG}"
expect_value "B sees disabled catalog state" '.catalog.properties["in-use"]' false
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${RENAMED_CATALOG}/schemas"
expect_http "B rebuilds the disabled catalog instance" 409

consistency_case "CATALOG enable rebuilds B's connector as available"
mutate_a "enable catalog on A" PATCH "/api/metalakes/${MAIN_METALAKE}/catalogs/${RENAMED_CATALOG}" '{"inUse":true}'
wait_for_invalidation
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${RENAMED_CATALOG}"
expect_value "B sees enabled catalog state" '.catalog.properties["in-use"]' true
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${RENAMED_CATALOG}/schemas"
expect_http "B rebuilds the re-enabled catalog instance" 200

consistency_case "CATALOG drop invalidates B's entity and connector caches"
mutate_a "drop catalog on A" DELETE "/api/metalakes/${MAIN_METALAKE}/catalogs/${RENAMED_CATALOG}?force=true"
wait_for_invalidation
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${RENAMED_CATALOG}"
expect_http "B invalidates dropped catalog" 404

section "CATALOG edge case: drop and recreate the same name in one poll window"
mutate_a "create recreate-probe catalog on A" POST "/api/metalakes/${MAIN_METALAKE}/catalogs" \
  "{\"name\":\"${RECREATE_CATALOG}\",\"type\":\"FILESET\",\"comment\":\"catalog-generation-1\",\"properties\":{\"disable-filesystem-ops\":\"true\"}}"
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${RECREATE_CATALOG}"
expect_value "warm first catalog generation on B" '.catalog.comment' catalog-generation-1

consistency_case "CATALOG drop plus same-name recreate evicts the old generation"
mutate_a "drop first catalog generation on A" DELETE "/api/metalakes/${MAIN_METALAKE}/catalogs/${RECREATE_CATALOG}?force=true"
mutate_a "recreate the same catalog name on A" POST "/api/metalakes/${MAIN_METALAKE}/catalogs" \
  "{\"name\":\"${RECREATE_CATALOG}\",\"type\":\"FILESET\",\"comment\":\"catalog-generation-2\",\"properties\":{\"disable-filesystem-ops\":\"true\"}}"
wait_for_invalidation
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${RECREATE_CATALOG}"
expect_value "B loads the second catalog generation" '.catalog.comment' catalog-generation-2

section "Create external catalogs used by entity tests"
ICEBERG_BODY=$(jq -nc \
  --arg name "$ICEBERG_CATALOG" \
  --arg backend_name "multi_instance_${SUFFIX}" \
  --arg warehouse "file:///tmp/gravitino-multi-instance-iceberg-${SUFFIX}" \
  '{name:$name,type:"RELATIONAL",comment:"Iceberg cache test",provider:"lakehouse-iceberg",properties:{"catalog-backend":"jdbc",uri:"jdbc:mysql://127.0.0.1:3306/gravitino?useSSL=false&allowPublicKeyRetrieval=true&nullCatalogMeansCurrent=true",warehouse:$warehouse,"catalog-backend-name":$backend_name,"jdbc-driver":"com.mysql.cj.jdbc.Driver","jdbc-user":"gravitino","jdbc-password":"gravitino","jdbc-initialize":"true"}}')
mutate_a "create Iceberg JDBC catalog" POST "/api/metalakes/${MAIN_METALAKE}/catalogs" "$ICEBERG_BODY"

mutate_a "create fileset catalog" POST "/api/metalakes/${MAIN_METALAKE}/catalogs" \
  "{\"name\":\"${FILESET_CATALOG}\",\"type\":\"FILESET\",\"comment\":\"Fileset cache test\",\"properties\":{\"disable-filesystem-ops\":\"true\"}}"

KAFKA_BODY=$(jq -nc --arg name "$KAFKA_CATALOG" --arg servers "$KAFKA_BOOTSTRAP_SERVERS" \
  '{name:$name,type:"MESSAGING",comment:"Kafka cache test",provider:"kafka",properties:{"bootstrap.servers":$servers}}')
mutate_a "create Kafka catalog" POST "/api/metalakes/${MAIN_METALAKE}/catalogs" "$KAFKA_BODY"

section "SCHEMA cache: alter and drop"
mutate_a "create Iceberg schema on A" POST "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas" \
  "{\"name\":\"${SCHEMA_NAME}\",\"comment\":\"schema-old\",\"properties\":{}}"
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}"
expect_value "warm schema cache on B" '.schema.name' "$SCHEMA_NAME"

consistency_case "SCHEMA alter propagates A -> B"
mutate_a "alter schema property on A" PUT "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}" \
  '{"updates":[{"@type":"setProperty","property":"cache-key","value":"schema-new"}]}'
wait_for_invalidation
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}"
expect_value "B sees altered schema" '.schema.properties["cache-key"]' schema-new

consistency_case "SCHEMA keeps the latest of several alters inside one poll window"
mutate_a "set schema rapid value 1 on A" PUT "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}" \
  '{"updates":[{"@type":"setProperty","property":"rapid-key","value":"schema-v1"}]}'
mutate_a "set schema rapid value 2 on A" PUT "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}" \
  '{"updates":[{"@type":"setProperty","property":"rapid-key","value":"schema-v2"}]}'
mutate_a "set schema rapid value 3 on A" PUT "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}" \
  '{"updates":[{"@type":"setProperty","property":"rapid-key","value":"schema-v3"}]}'
wait_for_invalidation
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}"
expect_value "B sees the latest schema value" '.schema.properties["rapid-key"]' schema-v3

section "TABLE cache: alter, rename, and drop"
mutate_a "create Iceberg table on A" POST "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/tables" \
  "{\"name\":\"${TABLE_NAME}\",\"comment\":\"table-old\",\"columns\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false}],\"properties\":{}}"
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/tables/${TABLE_NAME}"
expect_value "warm table cache on B" '.table.comment' table-old

consistency_case "TABLE alter propagates A -> B"
mutate_a "alter table comment on A" PUT "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/tables/${TABLE_NAME}" \
  '{"updates":[{"@type":"updateComment","newComment":"table-new"}]}'
wait_for_invalidation
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/tables/${TABLE_NAME}"
expect_value "B sees altered table" '.table.comment' table-new

consistency_case "TABLE rename invalidates the old name on B"
mutate_a "rename table on A" PUT "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/tables/${TABLE_NAME}" \
  "{\"updates\":[{\"@type\":\"rename\",\"newName\":\"${RENAMED_TABLE}\"}]}"
wait_for_invalidation
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/tables/${TABLE_NAME}"
expect_http "B invalidates the old table name" 404
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/tables/${RENAMED_TABLE}"
expect_value "B loads the renamed table" '.table.name' "$RENAMED_TABLE"

consistency_case "TABLE drop invalidates B"
mutate_a "drop table on A" DELETE "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/tables/${RENAMED_TABLE}?purge=true"
wait_for_invalidation
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/tables/${RENAMED_TABLE}"
expect_http "B invalidates dropped table" 404

section "TABLE edge case: drop and recreate the same name in one poll window"
mutate_a "create first recreate-probe table generation on A" POST "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/tables" \
  "{\"name\":\"${RECREATE_TABLE}\",\"comment\":\"table-generation-1\",\"columns\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false}],\"properties\":{}}"
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/tables/${RECREATE_TABLE}"
expect_value "warm first table generation on B" '.table.comment' table-generation-1

consistency_case "TABLE drop plus same-name recreate evicts the old generation"
mutate_a "drop first table generation on A" DELETE "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/tables/${RECREATE_TABLE}?purge=true"
mutate_a "recreate the same table name on A" POST "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/tables" \
  "{\"name\":\"${RECREATE_TABLE}\",\"comment\":\"table-generation-2\",\"columns\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false}],\"properties\":{}}"
wait_for_invalidation
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/tables/${RECREATE_TABLE}"
expect_value "B loads the second table generation" '.table.comment' table-generation-2
mutate_a "remove recreate-probe table" DELETE "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/tables/${RECREATE_TABLE}?purge=true"

section "VIEW cache: alter, rename, and drop"
mutate_a "create Iceberg view on A" POST "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/views" \
  "{\"name\":\"${VIEW_NAME}\",\"comment\":\"view-old\",\"columns\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false}],\"representations\":[{\"type\":\"sql\",\"dialect\":\"spark\",\"sql\":\"SELECT 1 AS id\"}],\"properties\":{}}"
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/views/${VIEW_NAME}"
expect_value "warm view cache on B" '.view.comment' view-old

consistency_case "VIEW alter propagates A -> B"
mutate_a "alter view property on A" PUT "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/views/${VIEW_NAME}" \
  '{"updates":[{"@type":"setProperty","property":"cache-key","value":"view-new"}]}'
wait_for_invalidation
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/views/${VIEW_NAME}"
expect_value "B sees altered view" '.view.properties["cache-key"]' view-new

consistency_case "VIEW rename invalidates the old name on B"
mutate_a "rename view on A" PUT "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/views/${VIEW_NAME}" \
  "{\"updates\":[{\"@type\":\"rename\",\"newName\":\"${RENAMED_VIEW}\"}]}"
wait_for_invalidation
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/views/${VIEW_NAME}"
expect_http "B invalidates the old view name" 404
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/views/${RENAMED_VIEW}"
expect_value "B loads the renamed view" '.view.name' "$RENAMED_VIEW"

consistency_case "VIEW drop invalidates B"
mutate_a "drop view on A" DELETE "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/views/${RENAMED_VIEW}"
wait_for_invalidation
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/views/${RENAMED_VIEW}"
expect_http "B invalidates dropped view" 404

consistency_case "SCHEMA drop invalidates B"
mutate_a "drop the warmed schema on A" DELETE "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}"
wait_for_invalidation
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}"
expect_http "B invalidates dropped schema" 404

section "SCHEMA cascade: invalidate descendant entity caches"
mutate_a "create cascade schema on A" POST "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas" \
  "{\"name\":\"${CASCADE_SCHEMA}\",\"comment\":\"cascade schema\",\"properties\":{}}"
mutate_a "create cascade descendant fileset on A" POST "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${CASCADE_SCHEMA}/filesets" \
  "{\"name\":\"${CASCADE_FILESET}\",\"type\":\"EXTERNAL\",\"comment\":\"cascade fileset\",\"storageLocation\":\"file:///tmp/gravitino-cascade-fileset-${SUFFIX}\",\"properties\":{}}"
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${CASCADE_SCHEMA}"
expect_value "warm cascade schema cache on B" '.schema.name' "$CASCADE_SCHEMA"
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${CASCADE_SCHEMA}/filesets/${CASCADE_FILESET}"
expect_value "warm cascade descendant cache on B" '.fileset.name' "$CASCADE_FILESET"

consistency_case "SCHEMA cascade drop invalidates its cached descendant"
mutate_a "cascade-drop schema on A" DELETE "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${CASCADE_SCHEMA}?cascade=true"
wait_for_invalidation
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${CASCADE_SCHEMA}"
expect_http "B invalidates cascade-dropped schema" 404
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${CASCADE_SCHEMA}/filesets/${CASCADE_FILESET}"
expect_http "B invalidates cascade-dropped descendant fileset" 404

# Iceberg is the only built-in catalog that accepts hierarchical schema names,
# but its schema endpoint intentionally rejects cascade=true. Exercise four
# cached schema nodes (parent, child, grandchild, and sibling), then use forced
# catalog drop as the public recursive operation for the nested hierarchy.
section "Multi-level SCHEMA cache: parent, child, grandchild, and sibling"
mutate_a "create hierarchical parent schema on A" POST "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas" \
  "{\"name\":\"${HIER_PARENT}\",\"comment\":\"parent\",\"properties\":{}}"
mutate_a "create hierarchical child schema on A" POST "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas" \
  "{\"name\":\"${HIER_CHILD}\",\"comment\":\"child\",\"properties\":{}}"
mutate_a "create hierarchical grandchild schema on A" POST "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas" \
  "{\"name\":\"${HIER_GRANDCHILD}\",\"comment\":\"grandchild\",\"properties\":{}}"
mutate_a "create hierarchical sibling schema on A" POST "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas" \
  "{\"name\":\"${HIER_SIBLING}\",\"comment\":\"sibling\",\"properties\":{}}"
mutate_a "create grandchild table on A" POST "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_GRANDCHILD}/tables" \
  "{\"name\":\"${HIER_TABLE}\",\"comment\":\"hierarchical table\",\"columns\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false}],\"properties\":{}}"
mutate_a "create sibling view on A" POST "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_SIBLING}/views" \
  "{\"name\":\"${HIER_VIEW}\",\"comment\":\"hierarchical view\",\"columns\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false}],\"representations\":[{\"type\":\"sql\",\"dialect\":\"spark\",\"sql\":\"SELECT 1 AS id\"}],\"properties\":{}}"

api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}"
expect_value "warm hierarchical catalog cache on B" '.catalog.name' "$ICEBERG_CATALOG"
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_PARENT}"
expect_value "warm parent schema cache on B" '.schema.name' "$HIER_PARENT"
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_CHILD}"
expect_value "warm child schema cache on B" '.schema.name' "$HIER_CHILD"
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_GRANDCHILD}"
expect_value "warm grandchild schema cache on B" '.schema.name' "$HIER_GRANDCHILD"
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_SIBLING}"
expect_value "warm sibling schema cache on B" '.schema.name' "$HIER_SIBLING"
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_GRANDCHILD}/tables/${HIER_TABLE}"
expect_value "warm grandchild table cache on B" '.table.name' "$HIER_TABLE"
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_SIBLING}/views/${HIER_VIEW}"
expect_value "warm sibling view cache on B" '.view.name' "$HIER_VIEW"

consistency_case "SCHEMA alter invalidates a cached top-level parent"
mutate_a "alter hierarchical parent on A" PUT "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_PARENT}" \
  '{"updates":[{"@type":"setProperty","property":"level-key","value":"parent-new"}]}'
wait_for_invalidation
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_PARENT}"
expect_value "B sees altered parent schema" '.schema.properties["level-key"]' parent-new

consistency_case "SCHEMA alter invalidates a cached middle-level child"
mutate_a "alter hierarchical child on A" PUT "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_CHILD}" \
  '{"updates":[{"@type":"setProperty","property":"level-key","value":"child-new"}]}'
wait_for_invalidation
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_CHILD}"
expect_value "B sees altered child schema" '.schema.properties["level-key"]' child-new

consistency_case "SCHEMA alter invalidates a cached grandchild"
mutate_a "alter hierarchical grandchild on A" PUT "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_GRANDCHILD}" \
  '{"updates":[{"@type":"setProperty","property":"level-key","value":"grandchild-new"}]}'
wait_for_invalidation
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_GRANDCHILD}"
expect_value "B sees altered grandchild schema" '.schema.properties["level-key"]' grandchild-new

consistency_case "SCHEMA alter invalidates a cached sibling branch"
mutate_a "alter hierarchical sibling on A" PUT "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_SIBLING}" \
  '{"updates":[{"@type":"setProperty","property":"level-key","value":"sibling-new"}]}'
wait_for_invalidation
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_SIBLING}"
expect_value "B sees altered sibling schema" '.schema.properties["level-key"]' sibling-new

consistency_case "CATALOG force drop recursively invalidates the full schema tree"
mutate_a "force-drop hierarchical catalog on A" DELETE "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}?force=true"
wait_for_invalidation
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}"
expect_http "B invalidates force-dropped hierarchical catalog" 404
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_PARENT}"
expect_http "B invalidates force-dropped parent schema" 404
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_CHILD}"
expect_http "B invalidates force-dropped child schema" 404
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_GRANDCHILD}"
expect_http "B invalidates force-dropped grandchild schema" 404
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_SIBLING}"
expect_http "B invalidates force-dropped sibling schema" 404
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_GRANDCHILD}/tables/${HIER_TABLE}"
expect_http "B invalidates force-dropped grandchild table" 404
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_SIBLING}/views/${HIER_VIEW}"
expect_http "B invalidates force-dropped sibling view" 404

# Recreate an empty catalog with the same name so requests can pass the parent
# lookup. This proves the nested entity caches themselves were evicted instead
# of merely being hidden by the missing catalog.
HIER_RECREATE_BODY=$(jq -nc \
  --arg name "$ICEBERG_CATALOG" \
  --arg backend_name "multi_instance_recreated_${SUFFIX}" \
  --arg warehouse "file:///tmp/gravitino-multi-instance-iceberg-recreated-${SUFFIX}" \
  '{name:$name,type:"RELATIONAL",comment:"Iceberg empty generation",provider:"lakehouse-iceberg",properties:{"catalog-backend":"jdbc",uri:"jdbc:mysql://127.0.0.1:3306/gravitino?useSSL=false&allowPublicKeyRetrieval=true&nullCatalogMeansCurrent=true",warehouse:$warehouse,"catalog-backend-name":$backend_name,"jdbc-driver":"com.mysql.cj.jdbc.Driver","jdbc-user":"gravitino","jdbc-password":"gravitino","jdbc-initialize":"true"}}')
mutate_a "recreate an empty hierarchical catalog with the same name" POST "/api/metalakes/${MAIN_METALAKE}/catalogs" "$HIER_RECREATE_BODY"
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}"
expect_value "B loads the empty replacement catalog" '.catalog.comment' "Iceberg empty generation"
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_PARENT}"
expect_http "B does not leak the old parent through the replacement catalog" 404
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_GRANDCHILD}"
expect_http "B does not leak the old grandchild through the replacement catalog" 404
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_SIBLING}"
expect_http "B does not leak the old sibling through the replacement catalog" 404
mutate_a "remove the empty replacement catalog" DELETE "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}?force=true"

section "FILESET cache: alter, rename, and drop"
mutate_a "create fileset schema on A" POST "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas" \
  "{\"name\":\"${FILESET_SCHEMA}\",\"comment\":\"fileset schema\",\"properties\":{}}"
mutate_a "create fileset on A" POST "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets" \
  "{\"name\":\"${FILESET_NAME}\",\"type\":\"EXTERNAL\",\"comment\":\"fileset-old\",\"storageLocation\":\"file:///tmp/gravitino-fileset-${SUFFIX}\",\"properties\":{}}"
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets/${FILESET_NAME}"
expect_value "warm fileset cache on B" '.fileset.comment' fileset-old

consistency_case "FILESET alter propagates A -> B"
mutate_a "alter fileset comment on A" PUT "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets/${FILESET_NAME}" \
  '{"updates":[{"@type":"updateComment","newComment":"fileset-new"}]}'
wait_for_invalidation
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets/${FILESET_NAME}"
expect_value "B sees altered fileset" '.fileset.comment' fileset-new

consistency_case "FILESET rename invalidates the old name on B"
mutate_a "rename fileset on A" PUT "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets/${FILESET_NAME}" \
  "{\"updates\":[{\"@type\":\"rename\",\"newName\":\"${RENAMED_FILESET}\"}]}"
wait_for_invalidation
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets/${FILESET_NAME}"
expect_http "B invalidates the old fileset name" 404
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets/${RENAMED_FILESET}"
expect_value "B loads the renamed fileset" '.fileset.name' "$RENAMED_FILESET"

consistency_case "FILESET drop invalidates B"
mutate_a "drop fileset on A" DELETE "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets/${RENAMED_FILESET}"
wait_for_invalidation
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets/${RENAMED_FILESET}"
expect_http "B invalidates dropped fileset" 404

section "FILESET edge case: drop and recreate the same name in one poll window"
mutate_a "create first recreate-probe fileset generation on A" POST "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets" \
  "{\"name\":\"${RECREATE_FILESET}\",\"type\":\"EXTERNAL\",\"comment\":\"fileset-generation-1\",\"storageLocation\":\"file:///tmp/gravitino-fileset-recreate-1-${SUFFIX}\",\"properties\":{}}"
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets/${RECREATE_FILESET}"
expect_value "warm first fileset generation on B" '.fileset.comment' fileset-generation-1

consistency_case "FILESET drop plus same-name recreate evicts the old generation"
mutate_a "drop first fileset generation on A" DELETE "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets/${RECREATE_FILESET}"
mutate_a "recreate the same fileset name on A" POST "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets" \
  "{\"name\":\"${RECREATE_FILESET}\",\"type\":\"EXTERNAL\",\"comment\":\"fileset-generation-2\",\"storageLocation\":\"file:///tmp/gravitino-fileset-recreate-2-${SUFFIX}\",\"properties\":{}}"
wait_for_invalidation
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets/${RECREATE_FILESET}"
expect_value "B loads the second fileset generation" '.fileset.comment' fileset-generation-2

section "TOPIC cache: alter and drop"
mutate_a "create Kafka topic on A" POST "/api/metalakes/${MAIN_METALAKE}/catalogs/${KAFKA_CATALOG}/schemas/default/topics" \
  "{\"name\":\"${TOPIC_NAME}\",\"comment\":\"topic-old\",\"properties\":{\"partition-count\":\"1\",\"replication-factor\":\"1\"}}"
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${KAFKA_CATALOG}/schemas/default/topics/${TOPIC_NAME}"
expect_value "warm topic cache on B" '.topic.comment' topic-old

consistency_case "TOPIC alter propagates A -> B"
mutate_a "alter topic comment on A" PUT "/api/metalakes/${MAIN_METALAKE}/catalogs/${KAFKA_CATALOG}/schemas/default/topics/${TOPIC_NAME}" \
  '{"updates":[{"@type":"updateComment","newComment":"topic-new"}]}'
wait_for_invalidation
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${KAFKA_CATALOG}/schemas/default/topics/${TOPIC_NAME}"
expect_value "B sees altered topic" '.topic.comment' topic-new

consistency_case "TOPIC drop invalidates B"
mutate_a "drop topic on A" DELETE "/api/metalakes/${MAIN_METALAKE}/catalogs/${KAFKA_CATALOG}/schemas/default/topics/${TOPIC_NAME}"
wait_for_invalidation
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${KAFKA_CATALOG}/schemas/default/topics/${TOPIC_NAME}"
expect_http "B invalidates dropped topic" 404

section "TAG cache: alter, rename, and drop"
mutate_a "create tag on A" POST "/api/metalakes/${MAIN_METALAKE}/tags" \
  "{\"name\":\"${TAG_NAME}\",\"comment\":\"tag-old\",\"properties\":{}}"
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/tags/${TAG_NAME}"
expect_value "warm tag cache on B" '.tag.comment' tag-old

consistency_case "TAG alter propagates A -> B"
mutate_a "alter tag comment on A" PUT "/api/metalakes/${MAIN_METALAKE}/tags/${TAG_NAME}" \
  '{"updates":[{"@type":"updateComment","newComment":"tag-new"}]}'
wait_for_invalidation
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/tags/${TAG_NAME}"
expect_value "B sees altered tag" '.tag.comment' tag-new

consistency_case "TAG rename invalidates the old name on B"
mutate_a "rename tag on A" PUT "/api/metalakes/${MAIN_METALAKE}/tags/${TAG_NAME}" \
  "{\"updates\":[{\"@type\":\"rename\",\"newName\":\"${RENAMED_TAG}\"}]}"
wait_for_invalidation
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/tags/${TAG_NAME}"
expect_http "B invalidates the old tag name" 404
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/tags/${RENAMED_TAG}"
expect_value "B loads the renamed tag" '.tag.name' "$RENAMED_TAG"

consistency_case "TAG drop invalidates B"
mutate_a "drop tag on A" DELETE "/api/metalakes/${MAIN_METALAKE}/tags/${RENAMED_TAG}"
wait_for_invalidation
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/tags/${RENAMED_TAG}"
expect_http "B invalidates dropped tag" 404

section "TAG edge cases: same-name recreate and rapid updates"
mutate_a "create first recreate-probe tag generation on A" POST "/api/metalakes/${MAIN_METALAKE}/tags" \
  "{\"name\":\"${RECREATE_TAG}\",\"comment\":\"tag-generation-1\",\"properties\":{}}"
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/tags/${RECREATE_TAG}"
expect_value "warm first tag generation on B" '.tag.comment' tag-generation-1

consistency_case "TAG drop plus same-name recreate evicts the old generation"
mutate_a "drop first tag generation on A" DELETE "/api/metalakes/${MAIN_METALAKE}/tags/${RECREATE_TAG}"
mutate_a "recreate the same tag name on A" POST "/api/metalakes/${MAIN_METALAKE}/tags" \
  "{\"name\":\"${RECREATE_TAG}\",\"comment\":\"tag-generation-2\",\"properties\":{}}"
wait_for_invalidation
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/tags/${RECREATE_TAG}"
expect_value "B loads the second tag generation" '.tag.comment' tag-generation-2

mutate_a "create rapid-update tag on A" POST "/api/metalakes/${MAIN_METALAKE}/tags" \
  "{\"name\":\"${LATEST_TAG}\",\"comment\":\"tag-v0\",\"properties\":{}}"
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/tags/${LATEST_TAG}"
expect_value "warm rapid-update tag on B" '.tag.comment' tag-v0

consistency_case "TAG keeps the latest of several alters inside one poll window"
mutate_a "set rapid tag value 1 on A" PUT "/api/metalakes/${MAIN_METALAKE}/tags/${LATEST_TAG}" \
  '{"updates":[{"@type":"updateComment","newComment":"tag-v1"}]}'
mutate_a "set rapid tag value 2 on A" PUT "/api/metalakes/${MAIN_METALAKE}/tags/${LATEST_TAG}" \
  '{"updates":[{"@type":"updateComment","newComment":"tag-v2"}]}'
mutate_a "set rapid tag value 3 on A" PUT "/api/metalakes/${MAIN_METALAKE}/tags/${LATEST_TAG}" \
  '{"updates":[{"@type":"updateComment","newComment":"tag-v3"}]}'
wait_for_invalidation
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/tags/${LATEST_TAG}"
expect_value "B sees the latest rapid tag value" '.tag.comment' tag-v3

section "POLICY cache: alter, rename, disable/enable, and drop"
POLICY_BODY=$(jq -nc --arg name "$POLICY_NAME" \
  '{name:$name,comment:"policy-old",policyType:"custom",enabled:true,content:{customRules:{retentionDays:30},supportedObjectTypes:["CATALOG","SCHEMA","TABLE"],properties:{owner:"platform"}}}')
mutate_a "create policy on A" POST "/api/metalakes/${MAIN_METALAKE}/policies" "$POLICY_BODY"
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/policies/${POLICY_NAME}"
expect_value "warm policy cache on B" '.policy.comment' policy-old

consistency_case "POLICY alter propagates A -> B"
mutate_a "alter policy comment on A" PUT "/api/metalakes/${MAIN_METALAKE}/policies/${POLICY_NAME}" \
  '{"updates":[{"@type":"updateComment","newComment":"policy-new"}]}'
wait_for_invalidation
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/policies/${POLICY_NAME}"
expect_value "B sees altered policy" '.policy.comment' policy-new

consistency_case "POLICY rename invalidates the old name on B"
mutate_a "rename policy on A" PUT "/api/metalakes/${MAIN_METALAKE}/policies/${POLICY_NAME}" \
  "{\"updates\":[{\"@type\":\"rename\",\"newName\":\"${RENAMED_POLICY}\"}]}"
wait_for_invalidation
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/policies/${POLICY_NAME}"
expect_http "B invalidates the old policy name" 404
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/policies/${RENAMED_POLICY}"
expect_value "B loads the renamed policy" '.policy.name' "$RENAMED_POLICY"

consistency_case "POLICY disable propagates A -> B"
mutate_a "disable policy on A" PATCH "/api/metalakes/${MAIN_METALAKE}/policies/${RENAMED_POLICY}" '{"enable":false}'
wait_for_invalidation
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/policies/${RENAMED_POLICY}"
expect_value "B sees disabled policy state" '.policy.enabled' false

consistency_case "POLICY enable propagates A -> B"
mutate_a "enable policy on A" PATCH "/api/metalakes/${MAIN_METALAKE}/policies/${RENAMED_POLICY}" '{"enable":true}'
wait_for_invalidation
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/policies/${RENAMED_POLICY}"
expect_value "B sees enabled policy state" '.policy.enabled' true

consistency_case "POLICY drop invalidates B"
mutate_a "drop policy on A" DELETE "/api/metalakes/${MAIN_METALAKE}/policies/${RENAMED_POLICY}"
wait_for_invalidation
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/policies/${RENAMED_POLICY}"
expect_http "B invalidates dropped policy" 404

section "Reverse direction: mutate B and invalidate warmed caches on A"
mutate_a "create reverse-direction tag" POST "/api/metalakes/${MAIN_METALAKE}/tags" \
  "{\"name\":\"${REVERSE_TAG}\",\"comment\":\"reverse-tag-old\",\"properties\":{}}"
api "$INSTANCE_A" GET "/api/metalakes/${MAIN_METALAKE}/tags/${REVERSE_TAG}"
expect_value "warm reverse-direction tag cache on A" '.tag.comment' reverse-tag-old

consistency_case "TAG alter propagates B -> A"
mutate_b "alter reverse-direction tag on B" PUT "/api/metalakes/${MAIN_METALAKE}/tags/${REVERSE_TAG}" \
  '{"updates":[{"@type":"updateComment","newComment":"reverse-tag-new"}]}'
wait_for_invalidation "instance A"
api "$INSTANCE_A" GET "/api/metalakes/${MAIN_METALAKE}/tags/${REVERSE_TAG}"
expect_value "A sees the tag altered through B" '.tag.comment' reverse-tag-new

consistency_case "TAG rename propagates B -> A"
mutate_b "rename reverse-direction tag on B" PUT "/api/metalakes/${MAIN_METALAKE}/tags/${REVERSE_TAG}" \
  "{\"updates\":[{\"@type\":\"rename\",\"newName\":\"${RENAMED_REVERSE_TAG}\"}]}"
wait_for_invalidation "instance A"
api "$INSTANCE_A" GET "/api/metalakes/${MAIN_METALAKE}/tags/${REVERSE_TAG}"
expect_http "A invalidates the tag's old name" 404
api "$INSTANCE_A" GET "/api/metalakes/${MAIN_METALAKE}/tags/${RENAMED_REVERSE_TAG}"
expect_value "A loads the tag renamed through B" '.tag.name' "$RENAMED_REVERSE_TAG"

mutate_a "create reverse-direction fileset" POST "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets" \
  "{\"name\":\"${REVERSE_FILESET}\",\"type\":\"EXTERNAL\",\"comment\":\"reverse-fileset-old\",\"storageLocation\":\"file:///tmp/gravitino-reverse-fileset-${SUFFIX}\",\"properties\":{}}"
api "$INSTANCE_A" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets/${REVERSE_FILESET}"
expect_value "warm reverse-direction fileset cache on A" '.fileset.comment' reverse-fileset-old

consistency_case "FILESET alter propagates B -> A"
mutate_b "alter reverse-direction fileset on B" PUT "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets/${REVERSE_FILESET}" \
  '{"updates":[{"@type":"updateComment","newComment":"reverse-fileset-new"}]}'
wait_for_invalidation "instance A"
api "$INSTANCE_A" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets/${REVERSE_FILESET}"
expect_value "A sees the fileset altered through B" '.fileset.comment' reverse-fileset-new

consistency_case "FILESET rename propagates B -> A"
mutate_b "rename reverse-direction fileset on B" PUT "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets/${REVERSE_FILESET}" \
  "{\"updates\":[{\"@type\":\"rename\",\"newName\":\"${RENAMED_REVERSE_FILESET}\"}]}"
wait_for_invalidation "instance A"
api "$INSTANCE_A" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets/${REVERSE_FILESET}"
expect_http "A invalidates the fileset's old name" 404
api "$INSTANCE_A" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets/${RENAMED_REVERSE_FILESET}"
expect_value "A loads the fileset renamed through B" '.fileset.name' "$RENAMED_REVERSE_FILESET"

REVERSE_POLICY_BODY=$(jq -nc --arg name "$REVERSE_POLICY" \
  '{name:$name,comment:"reverse-policy",policyType:"custom",enabled:true,content:{customRules:{retentionDays:30},supportedObjectTypes:["CATALOG","SCHEMA","TABLE"],properties:{owner:"platform"}}}')
mutate_a "create reverse-direction policy" POST "/api/metalakes/${MAIN_METALAKE}/policies" "$REVERSE_POLICY_BODY"
api "$INSTANCE_A" GET "/api/metalakes/${MAIN_METALAKE}/policies/${REVERSE_POLICY}"
expect_value "warm reverse-direction policy cache on A" '.policy.enabled' true

consistency_case "POLICY disable propagates B -> A"
mutate_b "disable reverse-direction policy on B" PATCH "/api/metalakes/${MAIN_METALAKE}/policies/${REVERSE_POLICY}" '{"enable":false}'
wait_for_invalidation "instance A"
api "$INSTANCE_A" GET "/api/metalakes/${MAIN_METALAKE}/policies/${REVERSE_POLICY}"
expect_value "A sees the policy disabled through B" '.policy.enabled' false

consistency_case "POLICY enable propagates B -> A"
mutate_b "enable reverse-direction policy on B" PATCH "/api/metalakes/${MAIN_METALAKE}/policies/${REVERSE_POLICY}" '{"enable":true}'
wait_for_invalidation "instance A"
api "$INSTANCE_A" GET "/api/metalakes/${MAIN_METALAKE}/policies/${REVERSE_POLICY}"
expect_value "A sees the policy enabled through B" '.policy.enabled' true

section "Cross-type batch: invalidate parent and child entities in one poll"
mutate_a "create burst tag" POST "/api/metalakes/${MAIN_METALAKE}/tags" \
  "{\"name\":\"${BURST_TAG}\",\"comment\":\"burst-tag-old\",\"properties\":{}}"
BURST_POLICY_BODY=$(jq -nc --arg name "$BURST_POLICY" \
  '{name:$name,comment:"burst-policy-old",policyType:"custom",enabled:true,content:{customRules:{retentionDays:30},supportedObjectTypes:["CATALOG","SCHEMA","TABLE"],properties:{owner:"platform"}}}')
mutate_a "create burst policy" POST "/api/metalakes/${MAIN_METALAKE}/policies" "$BURST_POLICY_BODY"
mutate_a "create burst fileset" POST "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets" \
  "{\"name\":\"${BURST_FILESET}\",\"type\":\"EXTERNAL\",\"comment\":\"burst-fileset-old\",\"storageLocation\":\"file:///tmp/gravitino-burst-fileset-${SUFFIX}\",\"properties\":{}}"
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}"
expect_value "warm burst catalog cache on B" '.catalog.comment' "Fileset cache test"
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}"
expect_value "warm burst schema cache on B" '.schema.name' "$FILESET_SCHEMA"
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets/${BURST_FILESET}"
expect_value "warm burst fileset cache on B" '.fileset.comment' burst-fileset-old
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/tags/${BURST_TAG}"
expect_value "warm burst tag cache on B" '.tag.comment' burst-tag-old
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/policies/${BURST_POLICY}"
expect_value "warm burst policy cache on B" '.policy.comment' burst-policy-old

consistency_case "CATALOG, SCHEMA, FILESET, TAG, and POLICY invalidate in one batch"
mutate_a "alter burst catalog on A" PUT "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}" \
  '{"updates":[{"@type":"updateComment","newComment":"burst-catalog-new"}]}'
mutate_a "alter burst schema on A" PUT "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}" \
  '{"updates":[{"@type":"setProperty","property":"burst-key","value":"burst-schema-new"}]}'
mutate_a "alter burst fileset on A" PUT "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets/${BURST_FILESET}" \
  '{"updates":[{"@type":"updateComment","newComment":"burst-fileset-new"}]}'
mutate_a "alter burst tag on A" PUT "/api/metalakes/${MAIN_METALAKE}/tags/${BURST_TAG}" \
  '{"updates":[{"@type":"updateComment","newComment":"burst-tag-new"}]}'
mutate_a "alter burst policy on A" PUT "/api/metalakes/${MAIN_METALAKE}/policies/${BURST_POLICY}" \
  '{"updates":[{"@type":"updateComment","newComment":"burst-policy-new"}]}'
wait_for_invalidation
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}"
expect_value "B sees the catalog from the batch" '.catalog.comment' burst-catalog-new
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}"
expect_value "B sees the schema from the batch" '.schema.properties["burst-key"]' burst-schema-new
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets/${BURST_FILESET}"
expect_value "B sees the fileset from the batch" '.fileset.comment' burst-fileset-new
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/tags/${BURST_TAG}"
expect_value "B sees the tag from the batch" '.tag.comment' burst-tag-new
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/policies/${BURST_POLICY}"
expect_value "B sees the policy from the batch" '.policy.comment' burst-policy-new

section "JOB cache: cancellation alters the cached status"
JOB_SCRIPT="$(mktemp -t gravitino-entity-cache-job.XXXX.sh)"
printf '#!/usr/bin/env bash\nsleep 60\n' >"$JOB_SCRIPT"
chmod +x "$JOB_SCRIPT"
JOB_TEMPLATE_BODY=$(jq -nc --arg name "$JOB_TEMPLATE" --arg executable "$JOB_SCRIPT" \
  '{jobTemplate:{name:$name,jobType:"shell",comment:"Job cache test",executable:$executable,arguments:[],environments:{},customFields:{},scripts:[]}}')
mutate_a "register job template on A" POST "/api/metalakes/${MAIN_METALAKE}/jobs/templates" "$JOB_TEMPLATE_BODY"

api "$INSTANCE_A" POST "/api/metalakes/${MAIN_METALAKE}/jobs/runs" \
  "{\"jobTemplateName\":\"${JOB_TEMPLATE}\",\"jobConf\":{}}"
expect_http "run job on A" 200
JOB_ID=$(printf '%s' "$RESPONSE_BODY" | jq -r '.job.jobId // empty')
if [[ -n "$JOB_ID" ]]; then
  pass "job id returned"
else
  fail "job id returned — Body: $(body_snippet)"
fi

if [[ -n "$JOB_ID" ]]; then
  api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/jobs/runs/${JOB_ID}"
  expect_one_of "warm job cache on B" '.job.status' queued started

  consistency_case "JOB cancellation status propagates A -> B"
  mutate_a "cancel job on A" POST "/api/metalakes/${MAIN_METALAKE}/jobs/runs/${JOB_ID}"
  wait_for_invalidation
  api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/jobs/runs/${JOB_ID}"
  expect_one_of "B sees altered job status" '.job.status' cancelling canceled
fi

if [[ "$RUN_RESTART_TEST" == "true" ]]; then
  section "Instance B restart: initialize at high-water, then process new changes"
  mutate_a "create restart probe tag on A" POST "/api/metalakes/${MAIN_METALAKE}/tags" \
    "{\"name\":\"${RESTART_TAG}\",\"comment\":\"restart-old\",\"properties\":{}}"
  api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/tags/${RESTART_TAG}"
  expect_value "warm restart probe tag on B" '.tag.comment' restart-old

  if [[ -x "${INSTANCE_B_HOME}/bin/gravitino.sh" ]]; then
    "${INSTANCE_B_HOME}/bin/gravitino.sh" stop
    consistency_case "Restarted B initializes at the current change-log high-water mark"
    mutate_a "alter probe while B is stopped" PUT "/api/metalakes/${MAIN_METALAKE}/tags/${RESTART_TAG}" \
      '{"updates":[{"@type":"updateComment","newComment":"restart-current"}]}'
    GRAVITINO_DEBUG_OPTS= "${INSTANCE_B_HOME}/bin/gravitino.sh" start
    if wait_ready "$INSTANCE_B"; then
      pass "instance B restarted"
    else
      fail "instance B did not restart in time"
    fi

    api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/tags/${RESTART_TAG}"
    expect_value "restarted B loads current state without history replay" '.tag.comment' restart-current

    consistency_case "Restarted B resumes polling new changes"
    mutate_a "alter warmed probe after B restart" PUT "/api/metalakes/${MAIN_METALAKE}/tags/${RESTART_TAG}" \
      '{"updates":[{"@type":"updateComment","newComment":"restart-future"}]}'
    wait_for_invalidation
    api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/tags/${RESTART_TAG}"
    expect_value "restarted B polls and invalidates future changes" '.tag.comment' restart-future
  else
    fail "INSTANCE_B_HOME does not contain an executable bin/gravitino.sh"
  fi
fi

section "Summary"
printf 'Consistency cases executed: %d\n' "$CONSISTENCY_CASES"
printf 'Assertions passed: %d\nAssertions failed: %d\n' "$PASS" "$FAIL"
if ((FAIL > 0)); then
  printf '\nFailed assertions:\n'
  printf '  - %s\n' "${FAILED_TESTS[@]}"
  exit 1
fi

printf '\nAll multi-instance entity-cache consistency assertions passed.\n'
