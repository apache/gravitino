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
#   3. Re-read through the target until it converges, or until the deadline.
#   4. Assert that the stale value is gone.
#
# Step 3 retries instead of sleeping for a fixed interval. Every consistency
# assertion here is positive ("the target eventually converges"), so retrying
# can only remove timing flakiness, never mask a regression: a cache that is
# never invalidated still fails when the deadline expires. The exceptions are
# the stale-read controls, which assert a *negative* and therefore read at a
# fixed instant across several rounds.
#
# The stale-read controls are what make the rest of the suite meaningful.
# Without them, a run with the entity cache switched off for one entity type
# would pass that type's convergence cases while proving nothing, because "B
# serves the new value" is also what an uncached B does. Every cacheable type
# therefore has a repeated negative control that proves the target really held
# a stale copy first; a reverse TAG control proves the same for node A.
#
# Assertions on externally backed entities need care. For Iceberg tables/views/
# schemas and Kafka topics, a load merges live catalog state with the stored
# entity (see TableOperationDispatcher#internalLoadTable), and name, comment and
# properties all come from the *catalog* side, which no Gravitino cache serves.
# Those fields would converge even with the cache disabled and the poller
# stopped. The audit block is the opposite: EntityCombined*#auditInfo() merges
# the entity's audit over the catalog's with overwrite=true, so
# audit.lastModifiedTime is served from the entity cache alone. Externally
# backed cases use it whenever the operation preserves a reliable audit block,
# and treat name/comment/properties as an end-to-end check only.
#
# Two properties of that field drive how the cases are written. It is null until
# the first alter (creation sets only creator and createTime), so each such
# section primes it once before the case starts. And it is unreliable across a
# same-name recreate: tables, views, and schemas can reload with an empty audit
# block. Those recreate scenarios are external-backend E2E checks, not cache
# assertions; the type-level alter controls prove their entity caches instead.
#
# Covered cache entity types and mutations:
#   METALAKE  alter, rename, disable/enable, drop
#   CATALOG   alter, rename, disable/enable, drop
#   SCHEMA    alter, rapid alters, cascade drop, multi-level hierarchy
#   TABLE     alter (rename/drop/recreate are external-backend E2E only; see their notes)
#   TOPIC     alter (drop is external-backend E2E only)
#   VIEW      alter (rename/drop/recreate are external-backend E2E only)
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
# Upper bound for convergence. Generous on purpose: a correct node converges in
# about one poll interval and never reaches this, while a broken one has to burn
# the whole budget before failing. Raising it costs nothing on a healthy run.
AWAIT_TIMEOUT_SECS="${AWAIT_TIMEOUT_SECS:-20}"
AWAIT_INTERVAL_SECS="${AWAIT_INTERVAL_SECS:-0.2}"
# Rounds of the stale-read control. Each round reads B within milliseconds of
# the mutation on A, so a warm cache is observed with probability ~(1 - rtt/poll
# interval); several rounds make a false alarm negligible while still failing
# hard when no round ever sees a stale value.
STALE_CONTROL_ROUNDS="${STALE_CONTROL_ROUNDS:-5}"
# Each cacheable entity type gets its own warm-cache proof. Repeating the
# immediate stale read makes the proof tolerant of the poller occasionally
# winning the mutation/read race without allowing a cold or bypassed cache to
# pass: a cold target observes the new value in every round.
PER_TYPE_STALE_CONTROL_ROUNDS="${PER_TYPE_STALE_CONTROL_ROUNDS:-3}"
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
CACHE_PROOF_CASES=0
EXPECTED_CACHE_PROOF_CASES=11
FAILED_TESTS=()
CACHE_PROOFS=()
HTTP_CODE=""
RESPONSE_BODY=""
JOB_ID=""
JOB_SCRIPT=""
CASE_PREWARMED=true
CASE_MUTATION_STARTED=true

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

# Matchers used by the await_* helpers. Each inspects the HTTP_CODE and
# RESPONSE_BODY left behind by the most recent api() call and returns success
# when the target has converged. They deliberately mirror the expect_* rules
# (non-200 and a null filter result both count as "not yet") so that a matcher
# never reports converged where the matching expect_* would fail.
matches_http() {
  [[ "$HTTP_CODE" == "$1" ]]
}

matches_one_of() {
  [[ "$HTTP_CODE" == "200" ]] || return 1
  local filter="$1" actual expected
  shift
  actual=$(printf '%s' "$RESPONSE_BODY" | jq -r "$filter" 2>/dev/null) || return 1
  [[ "$actual" == "null" ]] && return 1
  for expected in "$@"; do
    [[ "$actual" == "$expected" ]] && return 0
  done
  return 1
}

matches_value() {
  matches_one_of "$1" "$2"
}

matches_changed() {
  [[ "$HTTP_CODE" == "200" ]] || return 1
  local actual
  actual=$(printf '%s' "$RESPONSE_BODY" | jq -r "$1" 2>/dev/null) || return 1
  [[ "$actual" != "null" && "$actual" != "$2" ]]
}

# await_loop <base> <path> <matcher> [matcher args...]
# Re-issues the GET until <matcher> succeeds or the deadline expires. The final
# response is left in HTTP_CODE/RESPONSE_BODY either way, so the caller's
# expect_* reports a pass on convergence and a real failure message — with the
# actual body — on timeout. Reporting stays entirely in the expect_* helpers.
await_loop() {
  local base="$1" path="$2" matcher="$3"
  shift 3
  local attempts attempt
  attempts=$(awk -v timeout="$AWAIT_TIMEOUT_SECS" -v interval="$AWAIT_INTERVAL_SECS" \
    'BEGIN { n = int(timeout / interval); if (n < 1) n = 1; print n }')
  for ((attempt = 0; attempt < attempts; attempt++)); do
    api "$base" GET "$path"
    if "$matcher" "$@"; then
      return 0
    fi
    sleep "$AWAIT_INTERVAL_SECS"
  done
  return 1
}

await_value() {
  local desc="$1" base="$2" path="$3" filter="$4" expected="$5"
  await_loop "$base" "$path" matches_value "$filter" "$expected"
  expect_value "$desc" "$filter" "$expected"
}

await_http() {
  local desc="$1" base="$2" path="$3" expected="$4"
  await_loop "$base" "$path" matches_http "$expected"
  expect_http "$desc" "$expected"
}

await_one_of() {
  local desc="$1" base="$2" path="$3" filter="$4"
  shift 4
  await_loop "$base" "$path" matches_one_of "$filter" "$@"
  expect_one_of "$desc" "$filter" "$@"
}

# Asserts that a field moved away from a previously captured value. Used for
# audit timestamps, where the point is that the target stopped serving the
# cached entity rather than that it reached one specific new value.
await_changed() {
  local desc="$1" base="$2" path="$3" filter="$4" stale="$5"
  local actual
  # An empty or null baseline would make "the value moved" trivially true, which
  # would turn this assertion into a silent pass. Fail loudly instead: it means
  # the probe field is missing from the response, not that the cache is correct.
  if [[ -z "$stale" || "$stale" == "null" ]]; then
    fail "$desc — no baseline captured for $filter, cannot prove the cached entity was dropped"
    return
  fi
  if await_loop "$base" "$path" matches_changed "$filter" "$stale"; then
    pass "$desc"
    return
  fi
  if [[ "$HTTP_CODE" != "200" ]]; then
    fail "$desc — expected HTTP 200, got $HTTP_CODE. Body: $(body_snippet)"
    return
  fi
  actual=$(printf '%s' "$RESPONSE_BODY" | jq -r "$filter" 2>/dev/null)
  fail "$desc — $filter still serves the cached value '$stale' (got '$actual'). Body: $(body_snippet)"
}

# Reads a single field, for capturing a baseline rather than asserting on it.
read_field() {
  local base="$1" path="$2" filter="$3"
  api "$base" GET "$path"
  printf '%s' "$RESPONSE_BODY" | jq -r "$filter" 2>/dev/null
}

record_cache_proof() {
  CACHE_PROOF_CASES=$((CACHE_PROOF_CASES + 1))
  CACHE_PROOFS+=("$1")
  pass "$1: target served a warm stale entry before change-log invalidation"
}

# stale_value_control <description> <direction> <source> <target> <method>
#   <mutation_path> <read_path> <filter> <original_value> <body_template>
#   <probe_prefix>
#
# Proves that a native entity type is really cached on the target node. The
# body template must contain __VALUE__; the last round restores original_value
# so the regular lifecycle cases can retain their simple, readable baselines.
stale_value_control() {
  local desc="$1" direction="$2" source="$3" target="$4" method="$5"
  local mutation_path="$6" read_path="$7" filter="$8" original_value="$9"
  local body_template="${10}" probe_prefix="${11}"
  local current_value="$original_value" new_value body observed round
  local stale_observed=0

  consistency_case "$desc"
  for round in $(seq 1 "$PER_TYPE_STALE_CONTROL_ROUNDS"); do
    if ((round == PER_TYPE_STALE_CONTROL_ROUNDS)); then
      new_value="$original_value"
    else
      new_value="${probe_prefix}-v${round}"
    fi

    prewarm_value "${direction} round ${round}: target caches ${current_value}" \
      "$target" "$read_path" "$filter" "$current_value"
    body="${body_template//__VALUE__/$new_value}"
    api "$source" "$method" "$mutation_path" "$body"
    expect_http "${direction} round ${round}: source changes value to ${new_value}" 200
    CASE_MUTATION_STARTED=true

    observed=$(read_field "$target" "$read_path" "$filter")
    if [[ "$observed" == "$current_value" ]]; then
      stale_observed=$((stale_observed + 1))
      printf '    %s round %d: target still served cached %s\n' \
        "$direction" "$round" "$current_value"
    else
      printf '    %s round %d: target already served %s (poller won the race)\n' \
        "$direction" "$round" "$observed"
    fi

    await_value "${direction} round ${round}: target converges on ${new_value}" \
      "$target" "$read_path" "$filter" "$new_value"
    current_value="$new_value"
  done

  if ((stale_observed > 0)); then
    record_cache_proof "$direction $desc"
  else
    fail "$direction $desc never served a stale value in ${PER_TYPE_STALE_CONTROL_ROUNDS} rounds — the target may be cold or bypassing this entity cache"
  fi
}

# stale_changed_control <description> <direction> <source> <target> <method>
#   <mutation_path> <read_path> <audit_filter> <body_template> <probe_prefix>
#   <final_value>
#
# Externally backed fields come from the catalog and can change while the
# Gravitino entity cache is cold. This variant observes an entity-store audit
# field instead: the immediate read must retain the old audit in at least one
# round, and every round must later move after the poller invalidates the key.
stale_changed_control() {
  local desc="$1" direction="$2" source="$3" target="$4" method="$5"
  local mutation_path="$6" read_path="$7" audit_filter="$8"
  local body_template="$9" probe_prefix="${10}" final_value="${11}"
  local baseline new_value body observed round
  local stale_observed=0

  consistency_case "$desc"
  for round in $(seq 1 "$PER_TYPE_STALE_CONTROL_ROUNDS"); do
    prewarm_http "${direction} round ${round}: target loads the entity before mutation" \
      "$target" "$read_path" 200
    baseline=$(read_field "$target" "$read_path" "$audit_filter")
    if [[ -z "$baseline" || "$baseline" == "null" ]]; then
      fail "${direction} $desc round ${round}: no audit baseline at ${audit_filter}"
      CASE_MUTATION_STARTED=true
      continue
    fi

    if ((round == PER_TYPE_STALE_CONTROL_ROUNDS)); then
      new_value="$final_value"
    else
      new_value="${probe_prefix}-v${round}"
    fi
    body="${body_template//__VALUE__/$new_value}"
    api "$source" "$method" "$mutation_path" "$body"
    expect_http "${direction} round ${round}: source changes the external entity" 200
    CASE_MUTATION_STARTED=true

    observed=$(read_field "$target" "$read_path" "$audit_filter")
    if [[ "$observed" == "$baseline" ]]; then
      stale_observed=$((stale_observed + 1))
      printf '    %s round %d: target still served cached audit %s\n' \
        "$direction" "$round" "$baseline"
    else
      printf '    %s round %d: target audit already moved to %s (poller won the race)\n' \
        "$direction" "$round" "$observed"
    fi

    await_changed "${direction} round ${round}: target reloads the changed entity" \
      "$target" "$read_path" "$audit_filter" "$baseline"
  done

  if ((stale_observed > 0)); then
    record_cache_proof "$direction $desc"
  else
    fail "$direction $desc never served a stale audit in ${PER_TYPE_STALE_CONTROL_ROUNDS} rounds — the target may be cold or bypassing this entity cache"
  fi
}

consistency_case() {
  CONSISTENCY_CASES=$((CONSISTENCY_CASES + 1))
  CASE_PREWARMED=false
  CASE_MUTATION_STARTED=false
  printf '\n  \033[1m[consistency case %d]\033[0m %s\n' "$CONSISTENCY_CASES" "$1"
}

restart_case() {
  CONSISTENCY_CASES=$((CONSISTENCY_CASES + 1))
  CASE_PREWARMED=true
  CASE_MUTATION_STARTED=false
  printf '\n  \033[1m[restart case %d]\033[0m %s\n' "$CONSISTENCY_CASES" "$1"
}

prewarm_value() {
  local desc="$1" base="$2" path="$3" filter="$4" expected="$5"
  api "$base" GET "$path"
  expect_value "$desc" "$filter" "$expected"
  CASE_PREWARMED=true
}

prewarm_http() {
  local desc="$1" base="$2" path="$3" expected="$4"
  api "$base" GET "$path"
  expect_http "$desc" "$expected"
  CASE_PREWARMED=true
}

prewarm_one_of() {
  local desc="$1" base="$2" path="$3" filter="$4"
  shift 4
  api "$base" GET "$path"
  expect_one_of "$desc" "$filter" "$@"
  CASE_PREWARMED=true
}

ensure_case_prewarmed() {
  if [[ "$CASE_MUTATION_STARTED" == "false" ]]; then
    if [[ "$CASE_PREWARMED" != "true" ]]; then
      fail "consistency case mutated its source before explicitly prewarming the target cache"
      return 1
    fi
    CASE_MUTATION_STARTED=true
  fi
}

mutate_a() {
  local desc="$1" method="$2" path="$3" body="${4:-}"
  ensure_case_prewarmed || return
  api "$INSTANCE_A" "$method" "$path" "$body"
  expect_http "$desc" 200
}

mutate_b() {
  local desc="$1" method="$2" path="$3" body="${4:-}"
  ensure_case_prewarmed || return
  api "$INSTANCE_B" "$method" "$path" "$body"
  expect_http "$desc" 200
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

if [[ "$PER_TYPE_STALE_CONTROL_ROUNDS" =~ ^[0-9]+$ ]] \
  && ((PER_TYPE_STALE_CONTROL_ROUNDS >= 2)); then
  pass "per-type stale control runs at least twice"
else
  fail "PER_TYPE_STALE_CONTROL_ROUNDS must be an integer >= 2"
fi

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

stale_value_control "METALAKE cache residency" "METALAKE A -> B" \
  "$INSTANCE_A" "$INSTANCE_B" PUT \
  "/api/metalakes/${LIFECYCLE_METALAKE}" \
  "/api/metalakes/${LIFECYCLE_METALAKE}" \
  '.metalake.comment' metalake-old \
  '{"updates":[{"@type":"updateComment","newComment":"__VALUE__"}]}' \
  metalake-proof

consistency_case "METALAKE alter propagates A -> B"
prewarm_value "B caches the old metalake before A alters it" "$INSTANCE_B" \
  "/api/metalakes/${LIFECYCLE_METALAKE}" '.metalake.comment' metalake-old
mutate_a "alter metalake comment on A" PUT "/api/metalakes/${LIFECYCLE_METALAKE}" \
  '{"updates":[{"@type":"updateComment","newComment":"metalake-new"}]}'
await_value "B sees altered metalake" "$INSTANCE_B" \
  "/api/metalakes/${LIFECYCLE_METALAKE}" '.metalake.comment' metalake-new

consistency_case "METALAKE rename invalidates the old name on B"
prewarm_value "B caches the pre-rename metalake" "$INSTANCE_B" \
  "/api/metalakes/${LIFECYCLE_METALAKE}" '.metalake.comment' metalake-new
mutate_a "rename metalake on A" PUT "/api/metalakes/${LIFECYCLE_METALAKE}" \
  "{\"updates\":[{\"@type\":\"rename\",\"newName\":\"${RENAMED_METALAKE}\"}]}"
# Authorization resolves the missing metalake before dispatching the load, so
# a missing top-level name is surfaced as forbidden rather than not found.
await_http "B no longer serves the old metalake name" "$INSTANCE_B" \
  "/api/metalakes/${LIFECYCLE_METALAKE}" 403
await_value "B loads the renamed metalake" "$INSTANCE_B" \
  "/api/metalakes/${RENAMED_METALAKE}" '.metalake.name' "$RENAMED_METALAKE"

consistency_case "METALAKE disable blocks operations through B"
prewarm_value "B caches the enabled metalake before A disables it" "$INSTANCE_B" \
  "/api/metalakes/${RENAMED_METALAKE}" '.metalake.properties["in-use"]' true
prewarm_http "B warms an operation under the enabled metalake" "$INSTANCE_B" \
  "/api/metalakes/${RENAMED_METALAKE}/catalogs" 200
mutate_a "disable metalake on A" PATCH "/api/metalakes/${RENAMED_METALAKE}" '{"inUse":false}'
await_value "B sees disabled metalake state" "$INSTANCE_B" \
  "/api/metalakes/${RENAMED_METALAKE}" '.metalake.properties["in-use"]' false
await_http "B rejects an operation under the disabled metalake" "$INSTANCE_B" \
  "/api/metalakes/${RENAMED_METALAKE}/catalogs" 409

consistency_case "METALAKE enable restores operations through B"
prewarm_value "B caches the disabled metalake before A enables it" "$INSTANCE_B" \
  "/api/metalakes/${RENAMED_METALAKE}" '.metalake.properties["in-use"]' false
prewarm_http "B warms the disabled operation state" "$INSTANCE_B" \
  "/api/metalakes/${RENAMED_METALAKE}/catalogs" 409
mutate_a "enable metalake on A" PATCH "/api/metalakes/${RENAMED_METALAKE}" '{"inUse":true}'
await_value "B sees enabled metalake state" "$INSTANCE_B" \
  "/api/metalakes/${RENAMED_METALAKE}" '.metalake.properties["in-use"]' true
await_http "B accepts an operation under the re-enabled metalake" "$INSTANCE_B" \
  "/api/metalakes/${RENAMED_METALAKE}/catalogs" 200

consistency_case "METALAKE drop invalidates B"
prewarm_value "B caches the enabled metalake before A drops it" "$INSTANCE_B" \
  "/api/metalakes/${RENAMED_METALAKE}" '.metalake.properties["in-use"]' true
mutate_a "drop metalake on A" DELETE "/api/metalakes/${RENAMED_METALAKE}?force=true"
await_http "B no longer serves the dropped metalake" "$INSTANCE_B" \
  "/api/metalakes/${RENAMED_METALAKE}" 403

section "METALAKE edge case: drop and recreate the same name in one poll window"
mutate_a "create recreate-probe metalake on A" POST /api/metalakes \
  "{\"name\":\"${RECREATE_METALAKE}\",\"comment\":\"metalake-generation-1\",\"properties\":{}}"

consistency_case "METALAKE drop plus same-name recreate evicts the old generation"
prewarm_value "B caches the first metalake generation" "$INSTANCE_B" \
  "/api/metalakes/${RECREATE_METALAKE}" '.metalake.comment' metalake-generation-1
mutate_a "drop first metalake generation on A" DELETE "/api/metalakes/${RECREATE_METALAKE}?force=true"
mutate_a "recreate the same metalake name on A" POST /api/metalakes \
  "{\"name\":\"${RECREATE_METALAKE}\",\"comment\":\"metalake-generation-2\",\"properties\":{}}"
await_value "B loads the second metalake generation" "$INSTANCE_B" \
  "/api/metalakes/${RECREATE_METALAKE}" '.metalake.comment' metalake-generation-2

section "Shared test metalake"
mutate_a "create shared test metalake" POST /api/metalakes \
  "{\"name\":\"${MAIN_METALAKE}\",\"comment\":\"entity cache test\",\"properties\":{}}"
api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}"
expect_value "B loads shared test metalake" '.metalake.name' "$MAIN_METALAKE"

# ---------------------------------------------------------------------------
# Stale-read control.
#
# Every other case asserts that B eventually serves the new value. That is also
# what B does with no cache at all, so on its own the suite cannot tell "the
# poller invalidated a warm entry" from "there was never an entry to
# invalidate". This section closes that gap by asserting the negative: B must
# serve the *old* value when read immediately after the mutation on A.
#
# A single round would be racy, because the poller can fire between the mutation
# and the read. Instead each round records whether it observed the stale value
# and the section requires at least one hit across STALE_CONTROL_ROUNDS. With a
# one-second poll interval and a read issued milliseconds after the mutation,
# missing every round is vanishingly unlikely when the cache works — while a
# disabled or non-caching B misses every round by construction and fails here.
#
# This runs before the rest of the suite so a misconfigured deployment (cache
# off, wrong poll interval) fails immediately rather than passing 50-odd cases
# that prove nothing.
# ---------------------------------------------------------------------------
section "Stale-read control: B must serve a cached value before the poller runs"
CONTROL_TAG="control_tag_${SUFFIX}"
mutate_a "create stale-read control tag on A" POST "/api/metalakes/${MAIN_METALAKE}/tags" \
  "{\"name\":\"${CONTROL_TAG}\",\"comment\":\"control-v0\",\"properties\":{}}"

STALE_OBSERVED=0
for round in $(seq 1 "$STALE_CONTROL_ROUNDS"); do
  old_value="control-v$((round - 1))"
  new_value="control-v${round}"

  # Warm B with the current value, so B holds a cache entry to invalidate.
  api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/tags/${CONTROL_TAG}"
  expect_value "round ${round}: B caches ${old_value}" '.tag.comment' "$old_value"

  # Mutate on A, then read B as fast as possible.
  api "$INSTANCE_A" PUT "/api/metalakes/${MAIN_METALAKE}/tags/${CONTROL_TAG}" \
    "{\"updates\":[{\"@type\":\"updateComment\",\"newComment\":\"${new_value}\"}]}"
  expect_http "round ${round}: A updates the control tag to ${new_value}" 200

  observed=$(read_field "$INSTANCE_B" \
    "/api/metalakes/${MAIN_METALAKE}/tags/${CONTROL_TAG}" '.tag.comment')
  if [[ "$observed" == "$old_value" ]]; then
    STALE_OBSERVED=$((STALE_OBSERVED + 1))
    printf '    round %d: B still served the cached %s\n' "$round" "$old_value"
  else
    printf '    round %d: B already served %s (poller won the race)\n' "$round" "$observed"
  fi

  # Let the round converge before warming the next one.
  await_value "round ${round}: B converges on ${new_value}" "$INSTANCE_B" \
    "/api/metalakes/${MAIN_METALAKE}/tags/${CONTROL_TAG}" '.tag.comment' "$new_value"
done

if ((STALE_OBSERVED > 0)); then
  record_cache_proof "TAG A -> B cache residency (${STALE_OBSERVED}/${STALE_CONTROL_ROUNDS} stale rounds)"
else
  fail "B never served a stale value in ${STALE_CONTROL_ROUNDS} rounds — the entity cache is not caching, so every convergence assertion in this suite is vacuous"
fi

mutate_a "drop stale-read control tag" DELETE "/api/metalakes/${MAIN_METALAKE}/tags/${CONTROL_TAG}"

section "CATALOG cache: alter, rename, disable/enable, rebuild, and drop"
mutate_a "create lifecycle catalog on A" POST "/api/metalakes/${MAIN_METALAKE}/catalogs" \
  "{\"name\":\"${LIFECYCLE_CATALOG}\",\"type\":\"FILESET\",\"comment\":\"catalog-old\",\"properties\":{\"disable-filesystem-ops\":\"true\"}}"

stale_value_control "CATALOG cache residency" "CATALOG A -> B" \
  "$INSTANCE_A" "$INSTANCE_B" PUT \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${LIFECYCLE_CATALOG}" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${LIFECYCLE_CATALOG}" \
  '.catalog.comment' catalog-old \
  '{"updates":[{"@type":"updateComment","newComment":"__VALUE__"}]}' \
  catalog-proof

consistency_case "CATALOG alter propagates A -> B"
prewarm_value "B caches the old catalog before A alters it" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${LIFECYCLE_CATALOG}" '.catalog.comment' catalog-old
mutate_a "alter catalog comment on A" PUT "/api/metalakes/${MAIN_METALAKE}/catalogs/${LIFECYCLE_CATALOG}" \
  '{"updates":[{"@type":"updateComment","newComment":"catalog-new"}]}'
await_value "B sees altered catalog" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${LIFECYCLE_CATALOG}" '.catalog.comment' catalog-new

consistency_case "CATALOG rename invalidates the old name and connector on B"
prewarm_value "B caches the pre-rename catalog" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${LIFECYCLE_CATALOG}" '.catalog.comment' catalog-new
prewarm_http "B initializes the pre-rename catalog connector" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${LIFECYCLE_CATALOG}/schemas" 200
mutate_a "rename catalog on A" PUT "/api/metalakes/${MAIN_METALAKE}/catalogs/${LIFECYCLE_CATALOG}" \
  "{\"updates\":[{\"@type\":\"rename\",\"newName\":\"${RENAMED_CATALOG}\"}]}"
await_http "B invalidates the old catalog name" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${LIFECYCLE_CATALOG}" 404
await_value "B loads the renamed catalog" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${RENAMED_CATALOG}" '.catalog.name' "$RENAMED_CATALOG"

consistency_case "CATALOG disable rebuilds B's connector as unavailable"
prewarm_value "B caches the enabled catalog before A disables it" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${RENAMED_CATALOG}" '.catalog.properties["in-use"]' true
prewarm_http "B initializes the enabled catalog connector" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${RENAMED_CATALOG}/schemas" 200
mutate_a "disable catalog on A" PATCH "/api/metalakes/${MAIN_METALAKE}/catalogs/${RENAMED_CATALOG}" '{"inUse":false}'
await_value "B sees disabled catalog state" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${RENAMED_CATALOG}" '.catalog.properties["in-use"]' false
await_http "B rebuilds the disabled catalog instance" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${RENAMED_CATALOG}/schemas" 409

consistency_case "CATALOG enable rebuilds B's connector as available"
prewarm_value "B caches the disabled catalog before A enables it" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${RENAMED_CATALOG}" '.catalog.properties["in-use"]' false
prewarm_http "B caches the disabled catalog connector state" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${RENAMED_CATALOG}/schemas" 409
mutate_a "enable catalog on A" PATCH "/api/metalakes/${MAIN_METALAKE}/catalogs/${RENAMED_CATALOG}" '{"inUse":true}'
await_value "B sees enabled catalog state" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${RENAMED_CATALOG}" '.catalog.properties["in-use"]' true
await_http "B rebuilds the re-enabled catalog instance" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${RENAMED_CATALOG}/schemas" 200

consistency_case "CATALOG drop invalidates B's entity and connector caches"
prewarm_value "B caches the enabled catalog before A drops it" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${RENAMED_CATALOG}" '.catalog.properties["in-use"]' true
prewarm_http "B initializes the catalog connector before A drops it" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${RENAMED_CATALOG}/schemas" 200
mutate_a "drop catalog on A" DELETE "/api/metalakes/${MAIN_METALAKE}/catalogs/${RENAMED_CATALOG}?force=true"
await_http "B invalidates dropped catalog" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${RENAMED_CATALOG}" 404

section "CATALOG edge case: drop and recreate the same name in one poll window"
mutate_a "create recreate-probe catalog on A" POST "/api/metalakes/${MAIN_METALAKE}/catalogs" \
  "{\"name\":\"${RECREATE_CATALOG}\",\"type\":\"FILESET\",\"comment\":\"catalog-generation-1\",\"properties\":{\"disable-filesystem-ops\":\"true\"}}"

consistency_case "CATALOG drop plus same-name recreate evicts the old generation"
prewarm_value "B caches the first catalog generation" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${RECREATE_CATALOG}" '.catalog.comment' catalog-generation-1
mutate_a "drop first catalog generation on A" DELETE "/api/metalakes/${MAIN_METALAKE}/catalogs/${RECREATE_CATALOG}?force=true"
mutate_a "recreate the same catalog name on A" POST "/api/metalakes/${MAIN_METALAKE}/catalogs" \
  "{\"name\":\"${RECREATE_CATALOG}\",\"type\":\"FILESET\",\"comment\":\"catalog-generation-2\",\"properties\":{\"disable-filesystem-ops\":\"true\"}}"
await_value "B loads the second catalog generation" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${RECREATE_CATALOG}" '.catalog.comment' catalog-generation-2

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

# ---------------------------------------------------------------------------
# From here on the entities live in an external backend (Iceberg or Kafka).
# A load merges live catalog state with the stored entity, and name, comment and
# properties are served by the catalog, not by any Gravitino cache — those
# assertions would hold even with the cache disabled. The audit block is served
# by the entity store (EntityCombined*#auditInfo() lets the entity's audit
# overwrite the catalog's), so it is the only field here that can prove B
# stopped serving a cached entity. Alters bump audit.lastModifiedTime, so cache
# consistency cases pair their catalog-side assertion with an audit-side one.
# Same-name recreates can return an empty audit block and are therefore labeled
# as external-backend E2E checks rather than cache assertions.
# ---------------------------------------------------------------------------

section "SCHEMA cache: alter; external-backend recreate E2E"
mutate_a "create Iceberg schema on A" POST "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas" \
  "{\"name\":\"${SCHEMA_NAME}\",\"comment\":\"schema-old\",\"properties\":{}}"
# Creating an entity sets only creator and createTime, so audit.lastModifiedTime
# stays null until the first alter. The audit probes below need a non-null
# baseline to mean anything, so prime the field here, outside the case.
mutate_a "prime schema audit on A" PUT "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}" \
  '{"updates":[{"@type":"setProperty","property":"audit-prime","value":"1"}]}'

stale_changed_control "SCHEMA cache residency" "SCHEMA A -> B" \
  "$INSTANCE_A" "$INSTANCE_B" PUT \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}" \
  '.schema.audit.lastModifiedTime' \
  '{"updates":[{"@type":"setProperty","property":"cache-proof","value":"__VALUE__"}]}' \
  schema-proof schema-proof-final

consistency_case "SCHEMA alter propagates A -> B"
prewarm_value "B caches the old schema before A alters it" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}" \
  '.schema.name' "$SCHEMA_NAME"
STALE_SCHEMA_MODIFIED=$(read_field "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}" \
  '.schema.audit.lastModifiedTime')
mutate_a "alter schema property on A" PUT "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}" \
  '{"updates":[{"@type":"setProperty","property":"cache-key","value":"schema-new"}]}'
await_value "B sees altered schema" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}" '.schema.properties["cache-key"]' schema-new
await_changed "B drops the cached schema entity (audit moved on)" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}" \
  '.schema.audit.lastModifiedTime' "$STALE_SCHEMA_MODIFIED"

consistency_case "SCHEMA keeps the latest of several alters inside one poll window"
prewarm_value "B caches the schema before A performs rapid alters" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}" \
  '.schema.properties["cache-key"]' schema-new
STALE_SCHEMA_RAPID_MODIFIED=$(read_field "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}" \
  '.schema.audit.lastModifiedTime')
mutate_a "set schema rapid value 1 on A" PUT "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}" \
  '{"updates":[{"@type":"setProperty","property":"rapid-key","value":"schema-v1"}]}'
mutate_a "set schema rapid value 2 on A" PUT "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}" \
  '{"updates":[{"@type":"setProperty","property":"rapid-key","value":"schema-v2"}]}'
mutate_a "set schema rapid value 3 on A" PUT "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}" \
  '{"updates":[{"@type":"setProperty","property":"rapid-key","value":"schema-v3"}]}'
await_value "B sees the latest schema value" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}" '.schema.properties["rapid-key"]' schema-v3
await_changed "B drops the schema entity cached before the rapid batch" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}" \
  '.schema.audit.lastModifiedTime' "$STALE_SCHEMA_RAPID_MODIFIED"

section "TABLE cache: alter"
mutate_a "create Iceberg table on A" POST "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/tables" \
  "{\"name\":\"${TABLE_NAME}\",\"comment\":\"table-old\",\"columns\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false}],\"properties\":{}}"
# Prime audit.lastModifiedTime; see the note in the SCHEMA section.
mutate_a "prime table audit on A" PUT "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/tables/${TABLE_NAME}" \
  '{"updates":[{"@type":"setProperty","property":"audit-prime","value":"1"}]}'

stale_changed_control "TABLE cache residency" "TABLE A -> B" \
  "$INSTANCE_A" "$INSTANCE_B" PUT \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/tables/${TABLE_NAME}" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/tables/${TABLE_NAME}" \
  '.table.audit.lastModifiedTime' \
  '{"updates":[{"@type":"setProperty","property":"cache-proof","value":"__VALUE__"}]}' \
  table-proof table-proof-final

consistency_case "TABLE alter propagates A -> B"
prewarm_value "B caches the old table before A alters it" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/tables/${TABLE_NAME}" \
  '.table.comment' table-old
STALE_TABLE_MODIFIED=$(read_field "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/tables/${TABLE_NAME}" \
  '.table.audit.lastModifiedTime')
mutate_a "alter table comment on A" PUT "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/tables/${TABLE_NAME}" \
  '{"updates":[{"@type":"updateComment","newComment":"table-new"}]}'
await_value "B sees altered table" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/tables/${TABLE_NAME}" '.table.comment' table-new
await_changed "B drops the cached table entity (audit moved on)" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/tables/${TABLE_NAME}" \
  '.table.audit.lastModifiedTime' "$STALE_TABLE_MODIFIED"

section "TABLE external backend E2E: rename visibility"
prewarm_value "B caches the pre-rename table" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/tables/${TABLE_NAME}" \
  '.table.comment' table-new
mutate_a "rename table on A" PUT "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/tables/${TABLE_NAME}" \
  "{\"updates\":[{\"@type\":\"rename\",\"newName\":\"${RENAMED_TABLE}\"}]}"
await_http "B invalidates the old table name" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/tables/${TABLE_NAME}" 404
await_value "B loads the renamed table" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/tables/${RENAMED_TABLE}" '.table.name' "$RENAMED_TABLE"

section "TABLE external backend E2E: drop visibility"
prewarm_value "B caches the renamed table before A drops it" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/tables/${RENAMED_TABLE}" \
  '.table.name' "$RENAMED_TABLE"
mutate_a "drop table on A" DELETE "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/tables/${RENAMED_TABLE}?purge=true"
await_http "B observes the table removed from Iceberg" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/tables/${RENAMED_TABLE}" 404

section "TABLE external backend E2E: same-name recreation setup"
mutate_a "create first recreate-probe table generation on A" POST "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/tables" \
  "{\"name\":\"${RECREATE_TABLE}\",\"comment\":\"table-generation-1\",\"columns\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false}],\"properties\":{}}"

section "TABLE external backend E2E: drop plus same-name recreate"
prewarm_value "B caches the first table generation" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/tables/${RECREATE_TABLE}" \
  '.table.comment' table-generation-1
# No audit probe here, unlike the other externally backed cases. A same-name
# recreate of an Iceberg table comes back from B with a null audit block, so
# audit.createTime cannot distinguish "the stale entity was evicted" from "no
# entity was resolved at all". AuditInfo#merge runs with overwrite=true, so a
# null entity audit blanks the catalog-side values rather than falling back to
# them; that is worth chasing separately before relying on audit here.
mutate_a "drop first table generation on A" DELETE "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/tables/${RECREATE_TABLE}?purge=true"
mutate_a "recreate the same table name on A" POST "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/tables" \
  "{\"name\":\"${RECREATE_TABLE}\",\"comment\":\"table-generation-2\",\"columns\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false}],\"properties\":{}}"
await_value "B loads the second table generation" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/tables/${RECREATE_TABLE}" '.table.comment' table-generation-2
mutate_a "remove recreate-probe table" DELETE "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/tables/${RECREATE_TABLE}?purge=true"

section "VIEW cache: alter; external-backend recreate E2E"
mutate_a "create Iceberg view on A" POST "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/views" \
  "{\"name\":\"${VIEW_NAME}\",\"comment\":\"view-old\",\"columns\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false}],\"representations\":[{\"type\":\"sql\",\"dialect\":\"spark\",\"sql\":\"SELECT 1 AS id\"}],\"properties\":{}}"
# Prime audit.lastModifiedTime; see the note in the SCHEMA section.
mutate_a "prime view audit on A" PUT "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/views/${VIEW_NAME}" \
  '{"updates":[{"@type":"setProperty","property":"audit-prime","value":"1"}]}'

stale_changed_control "VIEW cache residency" "VIEW A -> B" \
  "$INSTANCE_A" "$INSTANCE_B" PUT \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/views/${VIEW_NAME}" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/views/${VIEW_NAME}" \
  '.view.audit.lastModifiedTime' \
  '{"updates":[{"@type":"setProperty","property":"cache-proof","value":"__VALUE__"}]}' \
  view-proof view-proof-final

consistency_case "VIEW alter propagates A -> B"
prewarm_value "B caches the old view before A alters it" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/views/${VIEW_NAME}" \
  '.view.comment' view-old
STALE_VIEW_MODIFIED=$(read_field "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/views/${VIEW_NAME}" \
  '.view.audit.lastModifiedTime')
mutate_a "alter view property on A" PUT "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/views/${VIEW_NAME}" \
  '{"updates":[{"@type":"setProperty","property":"cache-key","value":"view-new"}]}'
await_value "B sees altered view" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/views/${VIEW_NAME}" '.view.properties["cache-key"]' view-new
await_changed "B drops the cached view entity (audit moved on)" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/views/${VIEW_NAME}" \
  '.view.audit.lastModifiedTime' "$STALE_VIEW_MODIFIED"

section "VIEW external backend E2E: rename visibility"
prewarm_value "B caches the pre-rename view" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/views/${VIEW_NAME}" \
  '.view.properties["cache-key"]' view-new
mutate_a "rename view on A" PUT "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/views/${VIEW_NAME}" \
  "{\"updates\":[{\"@type\":\"rename\",\"newName\":\"${RENAMED_VIEW}\"}]}"
await_http "B invalidates the old view name" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/views/${VIEW_NAME}" 404
await_value "B loads the renamed view" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/views/${RENAMED_VIEW}" '.view.name' "$RENAMED_VIEW"

section "VIEW external backend E2E: drop plus same-name recreate"
prewarm_value "B caches the renamed view before A drops it" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/views/${RENAMED_VIEW}" \
  '.view.name' "$RENAMED_VIEW"
mutate_a "drop view on A" DELETE "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/views/${RENAMED_VIEW}"
await_http "B invalidates dropped view" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/views/${RENAMED_VIEW}" 404
mutate_a "recreate the dropped view name on A" POST "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/views" \
  "{\"name\":\"${RENAMED_VIEW}\",\"comment\":\"view-generation-2\",\"columns\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false}],\"representations\":[{\"type\":\"sql\",\"dialect\":\"spark\",\"sql\":\"SELECT 2 AS id\"}],\"properties\":{}}"
await_value "B loads the replacement view" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/views/${RENAMED_VIEW}" \
  '.view.comment' view-generation-2
mutate_a "remove the replacement view" DELETE \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}/views/${RENAMED_VIEW}"

section "SCHEMA external backend E2E: drop plus same-name recreate"
prewarm_value "B caches the schema immediately before A drops it" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}" \
  '.schema.properties["rapid-key"]' schema-v3
mutate_a "drop the warmed schema on A" DELETE "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}"
await_http "B invalidates dropped schema" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}" 404
mutate_a "recreate the dropped schema name on A" POST "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas" \
  "{\"name\":\"${SCHEMA_NAME}\",\"comment\":\"schema-generation-2\",\"properties\":{}}"
await_value "B loads the replacement schema" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${SCHEMA_NAME}" \
  '.schema.comment' schema-generation-2

section "SCHEMA cascade: invalidate descendant entity caches"
mutate_a "create cascade schema on A" POST "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas" \
  "{\"name\":\"${CASCADE_SCHEMA}\",\"comment\":\"cascade schema\",\"properties\":{}}"
mutate_a "create cascade descendant fileset on A" POST "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${CASCADE_SCHEMA}/filesets" \
  "{\"name\":\"${CASCADE_FILESET}\",\"type\":\"EXTERNAL\",\"comment\":\"cascade fileset\",\"storageLocation\":\"file:///tmp/gravitino-cascade-fileset-${SUFFIX}\",\"properties\":{}}"

consistency_case "SCHEMA cascade drop invalidates its cached descendant"
prewarm_value "B caches the cascade schema before A drops it" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${CASCADE_SCHEMA}" \
  '.schema.name' "$CASCADE_SCHEMA"
prewarm_value "B caches the descendant fileset before A drops its schema" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${CASCADE_SCHEMA}/filesets/${CASCADE_FILESET}" \
  '.fileset.name' "$CASCADE_FILESET"
mutate_a "cascade-drop schema on A" DELETE "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${CASCADE_SCHEMA}?cascade=true"
await_http "B invalidates cascade-dropped schema" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${CASCADE_SCHEMA}" 404
await_http "B invalidates cascade-dropped descendant fileset" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${CASCADE_SCHEMA}/filesets/${CASCADE_FILESET}" 404

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
# Prime audit.lastModifiedTime for every externally backed schema. Their
# catalog-side properties are useful E2E signals, but only the audit proves
# that B discarded its cached Gravitino entity.
mutate_a "prime hierarchical parent audit on A" PUT "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_PARENT}" \
  '{"updates":[{"@type":"setProperty","property":"audit-prime","value":"1"}]}'
mutate_a "prime hierarchical child audit on A" PUT "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_CHILD}" \
  '{"updates":[{"@type":"setProperty","property":"audit-prime","value":"1"}]}'
mutate_a "prime hierarchical grandchild audit on A" PUT "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_GRANDCHILD}" \
  '{"updates":[{"@type":"setProperty","property":"audit-prime","value":"1"}]}'
mutate_a "prime hierarchical sibling audit on A" PUT "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_SIBLING}" \
  '{"updates":[{"@type":"setProperty","property":"audit-prime","value":"1"}]}'
mutate_a "create grandchild table on A" POST "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_GRANDCHILD}/tables" \
  "{\"name\":\"${HIER_TABLE}\",\"comment\":\"hierarchical table\",\"columns\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false}],\"properties\":{}}"
mutate_a "create sibling view on A" POST "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_SIBLING}/views" \
  "{\"name\":\"${HIER_VIEW}\",\"comment\":\"hierarchical view\",\"columns\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false}],\"representations\":[{\"type\":\"sql\",\"dialect\":\"spark\",\"sql\":\"SELECT 1 AS id\"}],\"properties\":{}}"

consistency_case "SCHEMA alter invalidates a cached top-level parent"
prewarm_value "B caches the top-level parent before A alters it" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_PARENT}" \
  '.schema.name' "$HIER_PARENT"
STALE_HIER_PARENT_MODIFIED=$(read_field "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_PARENT}" \
  '.schema.audit.lastModifiedTime')
mutate_a "alter hierarchical parent on A" PUT "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_PARENT}" \
  '{"updates":[{"@type":"setProperty","property":"level-key","value":"parent-new"}]}'
await_value "B sees altered parent schema" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_PARENT}" '.schema.properties["level-key"]' parent-new
await_changed "B reloads the altered parent entity" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_PARENT}" \
  '.schema.audit.lastModifiedTime' "$STALE_HIER_PARENT_MODIFIED"

consistency_case "SCHEMA alter invalidates a cached middle-level child"
prewarm_value "B caches the middle-level child before A alters it" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_CHILD}" \
  '.schema.name' "$HIER_CHILD"
STALE_HIER_CHILD_MODIFIED=$(read_field "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_CHILD}" \
  '.schema.audit.lastModifiedTime')
mutate_a "alter hierarchical child on A" PUT "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_CHILD}" \
  '{"updates":[{"@type":"setProperty","property":"level-key","value":"child-new"}]}'
await_value "B sees altered child schema" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_CHILD}" '.schema.properties["level-key"]' child-new
await_changed "B reloads the altered child entity" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_CHILD}" \
  '.schema.audit.lastModifiedTime' "$STALE_HIER_CHILD_MODIFIED"

consistency_case "SCHEMA alter invalidates a cached grandchild"
prewarm_value "B caches the grandchild before A alters it" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_GRANDCHILD}" \
  '.schema.name' "$HIER_GRANDCHILD"
STALE_HIER_GRANDCHILD_MODIFIED=$(read_field "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_GRANDCHILD}" \
  '.schema.audit.lastModifiedTime')
mutate_a "alter hierarchical grandchild on A" PUT "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_GRANDCHILD}" \
  '{"updates":[{"@type":"setProperty","property":"level-key","value":"grandchild-new"}]}'
await_value "B sees altered grandchild schema" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_GRANDCHILD}" '.schema.properties["level-key"]' grandchild-new
await_changed "B reloads the altered grandchild entity" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_GRANDCHILD}" \
  '.schema.audit.lastModifiedTime' "$STALE_HIER_GRANDCHILD_MODIFIED"

consistency_case "SCHEMA alter invalidates a cached sibling branch"
prewarm_value "B caches the sibling branch before A alters it" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_SIBLING}" \
  '.schema.name' "$HIER_SIBLING"
STALE_HIER_SIBLING_MODIFIED=$(read_field "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_SIBLING}" \
  '.schema.audit.lastModifiedTime')
mutate_a "alter hierarchical sibling on A" PUT "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_SIBLING}" \
  '{"updates":[{"@type":"setProperty","property":"level-key","value":"sibling-new"}]}'
await_value "B sees altered sibling schema" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_SIBLING}" '.schema.properties["level-key"]' sibling-new
await_changed "B reloads the altered sibling entity" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_SIBLING}" \
  '.schema.audit.lastModifiedTime' "$STALE_HIER_SIBLING_MODIFIED"

consistency_case "CATALOG force drop recursively invalidates the full schema tree"
prewarm_value "B caches the hierarchical catalog before A force-drops it" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}" \
  '.catalog.name' "$ICEBERG_CATALOG"
prewarm_value "B re-caches the altered parent before the recursive drop" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_PARENT}" \
  '.schema.properties["level-key"]' parent-new
prewarm_value "B re-caches the altered child before the recursive drop" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_CHILD}" \
  '.schema.properties["level-key"]' child-new
prewarm_value "B re-caches the altered grandchild before the recursive drop" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_GRANDCHILD}" \
  '.schema.properties["level-key"]' grandchild-new
prewarm_value "B re-caches the altered sibling before the recursive drop" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_SIBLING}" \
  '.schema.properties["level-key"]' sibling-new
prewarm_value "B caches the grandchild table before the recursive drop" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_GRANDCHILD}/tables/${HIER_TABLE}" \
  '.table.name' "$HIER_TABLE"
prewarm_value "B caches the sibling view before the recursive drop" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_SIBLING}/views/${HIER_VIEW}" \
  '.view.name' "$HIER_VIEW"
STALE_HIER_PARENT_CREATED=$(read_field "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_PARENT}" \
  '.schema.audit.createTime')
STALE_HIER_CHILD_CREATED=$(read_field "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_CHILD}" \
  '.schema.audit.createTime')
STALE_HIER_GRANDCHILD_CREATED=$(read_field "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_GRANDCHILD}" \
  '.schema.audit.createTime')
STALE_HIER_SIBLING_CREATED=$(read_field "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_SIBLING}" \
  '.schema.audit.createTime')
mutate_a "force-drop hierarchical catalog on A" DELETE "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}?force=true"
await_http "B invalidates force-dropped hierarchical catalog" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}" 404
await_http "B observes the parent removed from Iceberg" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_PARENT}" 404
await_http "B observes the child removed from Iceberg" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_CHILD}" 404
await_http "B observes the grandchild removed from Iceberg" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_GRANDCHILD}" 404
await_http "B observes the sibling removed from Iceberg" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_SIBLING}" 404
await_http "B observes the grandchild table removed from Iceberg" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_GRANDCHILD}/tables/${HIER_TABLE}" 404
await_http "B observes the sibling view removed from Iceberg" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_SIBLING}/views/${HIER_VIEW}" 404

# Recreate the catalog first to show that the old external tree is absent, then
# recreate the same schema names and compare their entity audit timestamps.
# The latter is the cache assertion: external 404s alone cannot prove that the
# Gravitino schema entities were evicted.
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
mutate_a "recreate hierarchical parent schema" POST "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas" \
  "{\"name\":\"${HIER_PARENT}\",\"comment\":\"parent-generation-2\",\"properties\":{}}"
mutate_a "recreate hierarchical child schema" POST "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas" \
  "{\"name\":\"${HIER_CHILD}\",\"comment\":\"child-generation-2\",\"properties\":{}}"
mutate_a "recreate hierarchical grandchild schema" POST "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas" \
  "{\"name\":\"${HIER_GRANDCHILD}\",\"comment\":\"grandchild-generation-2\",\"properties\":{}}"
mutate_a "recreate hierarchical sibling schema" POST "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas" \
  "{\"name\":\"${HIER_SIBLING}\",\"comment\":\"sibling-generation-2\",\"properties\":{}}"
await_changed "B does not reuse the dropped parent schema entity" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_PARENT}" \
  '.schema.audit.createTime' "$STALE_HIER_PARENT_CREATED"
await_changed "B does not reuse the dropped child schema entity" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_CHILD}" \
  '.schema.audit.createTime' "$STALE_HIER_CHILD_CREATED"
await_changed "B does not reuse the dropped grandchild schema entity" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_GRANDCHILD}" \
  '.schema.audit.createTime' "$STALE_HIER_GRANDCHILD_CREATED"
await_changed "B does not reuse the dropped sibling schema entity" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}/schemas/${HIER_SIBLING}" \
  '.schema.audit.createTime' "$STALE_HIER_SIBLING_CREATED"
mutate_a "remove the replacement hierarchical catalog" DELETE "/api/metalakes/${MAIN_METALAKE}/catalogs/${ICEBERG_CATALOG}?force=true"

section "FILESET cache: alter, rename, and drop"
mutate_a "create fileset schema on A" POST "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas" \
  "{\"name\":\"${FILESET_SCHEMA}\",\"comment\":\"fileset schema\",\"properties\":{}}"
mutate_a "create fileset on A" POST "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets" \
  "{\"name\":\"${FILESET_NAME}\",\"type\":\"EXTERNAL\",\"comment\":\"fileset-old\",\"storageLocation\":\"file:///tmp/gravitino-fileset-${SUFFIX}\",\"properties\":{}}"

stale_value_control "FILESET cache residency" "FILESET A -> B" \
  "$INSTANCE_A" "$INSTANCE_B" PUT \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets/${FILESET_NAME}" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets/${FILESET_NAME}" \
  '.fileset.comment' fileset-old \
  '{"updates":[{"@type":"updateComment","newComment":"__VALUE__"}]}' \
  fileset-proof

consistency_case "FILESET alter propagates A -> B"
prewarm_value "B caches the old fileset before A alters it" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets/${FILESET_NAME}" \
  '.fileset.comment' fileset-old
mutate_a "alter fileset comment on A" PUT "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets/${FILESET_NAME}" \
  '{"updates":[{"@type":"updateComment","newComment":"fileset-new"}]}'
await_value "B sees altered fileset" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets/${FILESET_NAME}" '.fileset.comment' fileset-new

consistency_case "FILESET rename invalidates the old name on B"
prewarm_value "B caches the pre-rename fileset" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets/${FILESET_NAME}" \
  '.fileset.comment' fileset-new
mutate_a "rename fileset on A" PUT "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets/${FILESET_NAME}" \
  "{\"updates\":[{\"@type\":\"rename\",\"newName\":\"${RENAMED_FILESET}\"}]}"
await_http "B invalidates the old fileset name" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets/${FILESET_NAME}" 404
await_value "B loads the renamed fileset" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets/${RENAMED_FILESET}" '.fileset.name' "$RENAMED_FILESET"

consistency_case "FILESET drop invalidates B"
prewarm_value "B caches the renamed fileset before A drops it" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets/${RENAMED_FILESET}" \
  '.fileset.name' "$RENAMED_FILESET"
mutate_a "drop fileset on A" DELETE "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets/${RENAMED_FILESET}"
await_http "B invalidates dropped fileset" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets/${RENAMED_FILESET}" 404

section "FILESET edge case: drop and recreate the same name in one poll window"
mutate_a "create first recreate-probe fileset generation on A" POST "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets" \
  "{\"name\":\"${RECREATE_FILESET}\",\"type\":\"EXTERNAL\",\"comment\":\"fileset-generation-1\",\"storageLocation\":\"file:///tmp/gravitino-fileset-recreate-1-${SUFFIX}\",\"properties\":{}}"

consistency_case "FILESET drop plus same-name recreate evicts the old generation"
prewarm_value "B caches the first fileset generation" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets/${RECREATE_FILESET}" \
  '.fileset.comment' fileset-generation-1
mutate_a "drop first fileset generation on A" DELETE "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets/${RECREATE_FILESET}"
mutate_a "recreate the same fileset name on A" POST "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets" \
  "{\"name\":\"${RECREATE_FILESET}\",\"type\":\"EXTERNAL\",\"comment\":\"fileset-generation-2\",\"storageLocation\":\"file:///tmp/gravitino-fileset-recreate-2-${SUFFIX}\",\"properties\":{}}"
await_value "B loads the second fileset generation" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets/${RECREATE_FILESET}" '.fileset.comment' fileset-generation-2

section "TOPIC cache: alter"
mutate_a "create Kafka topic on A" POST "/api/metalakes/${MAIN_METALAKE}/catalogs/${KAFKA_CATALOG}/schemas/default/topics" \
  "{\"name\":\"${TOPIC_NAME}\",\"comment\":\"topic-priming\",\"properties\":{\"partition-count\":\"1\",\"replication-factor\":\"1\"}}"
# Prime audit.lastModifiedTime; see the note in the SCHEMA section. Kafka
# validates topic properties against its own config keys, so prime through the
# comment rather than an arbitrary property.
mutate_a "prime topic audit on A" PUT "/api/metalakes/${MAIN_METALAKE}/catalogs/${KAFKA_CATALOG}/schemas/default/topics/${TOPIC_NAME}" \
  '{"updates":[{"@type":"updateComment","newComment":"topic-old"}]}'

stale_changed_control "TOPIC cache residency" "TOPIC A -> B" \
  "$INSTANCE_A" "$INSTANCE_B" PUT \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${KAFKA_CATALOG}/schemas/default/topics/${TOPIC_NAME}" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${KAFKA_CATALOG}/schemas/default/topics/${TOPIC_NAME}" \
  '.topic.audit.lastModifiedTime' \
  '{"updates":[{"@type":"updateComment","newComment":"__VALUE__"}]}' \
  topic-proof topic-old

consistency_case "TOPIC alter propagates A -> B"
prewarm_value "B caches the old topic before A alters it" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${KAFKA_CATALOG}/schemas/default/topics/${TOPIC_NAME}" \
  '.topic.comment' topic-old
STALE_TOPIC_MODIFIED=$(read_field "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${KAFKA_CATALOG}/schemas/default/topics/${TOPIC_NAME}" \
  '.topic.audit.lastModifiedTime')
mutate_a "alter topic comment on A" PUT "/api/metalakes/${MAIN_METALAKE}/catalogs/${KAFKA_CATALOG}/schemas/default/topics/${TOPIC_NAME}" \
  '{"updates":[{"@type":"updateComment","newComment":"topic-new"}]}'
await_value "B sees altered topic" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${KAFKA_CATALOG}/schemas/default/topics/${TOPIC_NAME}" '.topic.comment' topic-new
await_changed "B drops the cached topic entity (audit moved on)" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${KAFKA_CATALOG}/schemas/default/topics/${TOPIC_NAME}" \
  '.topic.audit.lastModifiedTime' "$STALE_TOPIC_MODIFIED"

section "TOPIC external backend E2E: drop visibility"
prewarm_value "B caches the altered topic before A drops it" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${KAFKA_CATALOG}/schemas/default/topics/${TOPIC_NAME}" \
  '.topic.comment' topic-new
mutate_a "drop topic on A" DELETE "/api/metalakes/${MAIN_METALAKE}/catalogs/${KAFKA_CATALOG}/schemas/default/topics/${TOPIC_NAME}"
await_http "B observes the topic removed from Kafka" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${KAFKA_CATALOG}/schemas/default/topics/${TOPIC_NAME}" 404

section "TAG cache: alter, rename, and drop"
mutate_a "create tag on A" POST "/api/metalakes/${MAIN_METALAKE}/tags" \
  "{\"name\":\"${TAG_NAME}\",\"comment\":\"tag-old\",\"properties\":{}}"

consistency_case "TAG alter propagates A -> B"
prewarm_value "B caches the old tag before A alters it" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/tags/${TAG_NAME}" '.tag.comment' tag-old
mutate_a "alter tag comment on A" PUT "/api/metalakes/${MAIN_METALAKE}/tags/${TAG_NAME}" \
  '{"updates":[{"@type":"updateComment","newComment":"tag-new"}]}'
await_value "B sees altered tag" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/tags/${TAG_NAME}" '.tag.comment' tag-new

consistency_case "TAG rename invalidates the old name on B"
prewarm_value "B caches the pre-rename tag" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/tags/${TAG_NAME}" '.tag.comment' tag-new
mutate_a "rename tag on A" PUT "/api/metalakes/${MAIN_METALAKE}/tags/${TAG_NAME}" \
  "{\"updates\":[{\"@type\":\"rename\",\"newName\":\"${RENAMED_TAG}\"}]}"
await_http "B invalidates the old tag name" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/tags/${TAG_NAME}" 404
await_value "B loads the renamed tag" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/tags/${RENAMED_TAG}" '.tag.name' "$RENAMED_TAG"

consistency_case "TAG drop invalidates B"
prewarm_value "B caches the renamed tag before A drops it" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/tags/${RENAMED_TAG}" '.tag.name' "$RENAMED_TAG"
mutate_a "drop tag on A" DELETE "/api/metalakes/${MAIN_METALAKE}/tags/${RENAMED_TAG}"
await_http "B invalidates dropped tag" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/tags/${RENAMED_TAG}" 404

section "TAG edge cases: same-name recreate and rapid updates"
mutate_a "create first recreate-probe tag generation on A" POST "/api/metalakes/${MAIN_METALAKE}/tags" \
  "{\"name\":\"${RECREATE_TAG}\",\"comment\":\"tag-generation-1\",\"properties\":{}}"

consistency_case "TAG drop plus same-name recreate evicts the old generation"
prewarm_value "B caches the first tag generation" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/tags/${RECREATE_TAG}" '.tag.comment' tag-generation-1
mutate_a "drop first tag generation on A" DELETE "/api/metalakes/${MAIN_METALAKE}/tags/${RECREATE_TAG}"
mutate_a "recreate the same tag name on A" POST "/api/metalakes/${MAIN_METALAKE}/tags" \
  "{\"name\":\"${RECREATE_TAG}\",\"comment\":\"tag-generation-2\",\"properties\":{}}"
await_value "B loads the second tag generation" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/tags/${RECREATE_TAG}" '.tag.comment' tag-generation-2

mutate_a "create rapid-update tag on A" POST "/api/metalakes/${MAIN_METALAKE}/tags" \
  "{\"name\":\"${LATEST_TAG}\",\"comment\":\"tag-v0\",\"properties\":{}}"

consistency_case "TAG keeps the latest of several alters inside one poll window"
prewarm_value "B caches the tag before A performs rapid alters" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/tags/${LATEST_TAG}" '.tag.comment' tag-v0
mutate_a "set rapid tag value 1 on A" PUT "/api/metalakes/${MAIN_METALAKE}/tags/${LATEST_TAG}" \
  '{"updates":[{"@type":"updateComment","newComment":"tag-v1"}]}'
mutate_a "set rapid tag value 2 on A" PUT "/api/metalakes/${MAIN_METALAKE}/tags/${LATEST_TAG}" \
  '{"updates":[{"@type":"updateComment","newComment":"tag-v2"}]}'
mutate_a "set rapid tag value 3 on A" PUT "/api/metalakes/${MAIN_METALAKE}/tags/${LATEST_TAG}" \
  '{"updates":[{"@type":"updateComment","newComment":"tag-v3"}]}'
await_value "B sees the latest rapid tag value" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/tags/${LATEST_TAG}" '.tag.comment' tag-v3

section "POLICY cache: alter, rename, disable/enable, and drop"
POLICY_BODY=$(jq -nc --arg name "$POLICY_NAME" \
  '{name:$name,comment:"policy-old",policyType:"custom",enabled:true,content:{customRules:{retentionDays:30},supportedObjectTypes:["CATALOG","SCHEMA","TABLE"],properties:{owner:"platform"}}}')
mutate_a "create policy on A" POST "/api/metalakes/${MAIN_METALAKE}/policies" "$POLICY_BODY"

stale_value_control "POLICY cache residency" "POLICY A -> B" \
  "$INSTANCE_A" "$INSTANCE_B" PUT \
  "/api/metalakes/${MAIN_METALAKE}/policies/${POLICY_NAME}" \
  "/api/metalakes/${MAIN_METALAKE}/policies/${POLICY_NAME}" \
  '.policy.comment' policy-old \
  '{"updates":[{"@type":"updateComment","newComment":"__VALUE__"}]}' \
  policy-proof

consistency_case "POLICY alter propagates A -> B"
prewarm_value "B caches the old policy before A alters it" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/policies/${POLICY_NAME}" '.policy.comment' policy-old
mutate_a "alter policy comment on A" PUT "/api/metalakes/${MAIN_METALAKE}/policies/${POLICY_NAME}" \
  '{"updates":[{"@type":"updateComment","newComment":"policy-new"}]}'
await_value "B sees altered policy" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/policies/${POLICY_NAME}" '.policy.comment' policy-new

consistency_case "POLICY rename invalidates the old name on B"
prewarm_value "B caches the pre-rename policy" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/policies/${POLICY_NAME}" '.policy.comment' policy-new
mutate_a "rename policy on A" PUT "/api/metalakes/${MAIN_METALAKE}/policies/${POLICY_NAME}" \
  "{\"updates\":[{\"@type\":\"rename\",\"newName\":\"${RENAMED_POLICY}\"}]}"
await_http "B invalidates the old policy name" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/policies/${POLICY_NAME}" 404
await_value "B loads the renamed policy" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/policies/${RENAMED_POLICY}" '.policy.name' "$RENAMED_POLICY"

consistency_case "POLICY disable propagates A -> B"
prewarm_value "B caches the enabled policy before A disables it" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/policies/${RENAMED_POLICY}" '.policy.enabled' true
mutate_a "disable policy on A" PATCH "/api/metalakes/${MAIN_METALAKE}/policies/${RENAMED_POLICY}" '{"enable":false}'
await_value "B sees disabled policy state" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/policies/${RENAMED_POLICY}" '.policy.enabled' false

consistency_case "POLICY enable propagates A -> B"
prewarm_value "B caches the disabled policy before A enables it" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/policies/${RENAMED_POLICY}" '.policy.enabled' false
mutate_a "enable policy on A" PATCH "/api/metalakes/${MAIN_METALAKE}/policies/${RENAMED_POLICY}" '{"enable":true}'
await_value "B sees enabled policy state" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/policies/${RENAMED_POLICY}" '.policy.enabled' true

consistency_case "POLICY drop invalidates B"
prewarm_value "B caches the enabled policy before A drops it" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/policies/${RENAMED_POLICY}" '.policy.enabled' true
mutate_a "drop policy on A" DELETE "/api/metalakes/${MAIN_METALAKE}/policies/${RENAMED_POLICY}"
await_http "B invalidates dropped policy" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/policies/${RENAMED_POLICY}" 404

section "Reverse direction: mutate B and invalidate warmed caches on A"
mutate_a "create reverse-direction tag" POST "/api/metalakes/${MAIN_METALAKE}/tags" \
  "{\"name\":\"${REVERSE_TAG}\",\"comment\":\"reverse-tag-old\",\"properties\":{}}"

stale_value_control "TAG reverse-direction cache residency" "TAG B -> A" \
  "$INSTANCE_B" "$INSTANCE_A" PUT \
  "/api/metalakes/${MAIN_METALAKE}/tags/${REVERSE_TAG}" \
  "/api/metalakes/${MAIN_METALAKE}/tags/${REVERSE_TAG}" \
  '.tag.comment' reverse-tag-old \
  '{"updates":[{"@type":"updateComment","newComment":"__VALUE__"}]}' \
  reverse-tag-proof

consistency_case "TAG alter propagates B -> A"
prewarm_value "A caches the old tag before B alters it" "$INSTANCE_A" \
  "/api/metalakes/${MAIN_METALAKE}/tags/${REVERSE_TAG}" '.tag.comment' reverse-tag-old
mutate_b "alter reverse-direction tag on B" PUT "/api/metalakes/${MAIN_METALAKE}/tags/${REVERSE_TAG}" \
  '{"updates":[{"@type":"updateComment","newComment":"reverse-tag-new"}]}'
await_value "A sees the tag altered through B" "$INSTANCE_A" \
  "/api/metalakes/${MAIN_METALAKE}/tags/${REVERSE_TAG}" '.tag.comment' reverse-tag-new

consistency_case "TAG rename propagates B -> A"
prewarm_value "A caches the pre-rename tag" "$INSTANCE_A" \
  "/api/metalakes/${MAIN_METALAKE}/tags/${REVERSE_TAG}" '.tag.comment' reverse-tag-new
mutate_b "rename reverse-direction tag on B" PUT "/api/metalakes/${MAIN_METALAKE}/tags/${REVERSE_TAG}" \
  "{\"updates\":[{\"@type\":\"rename\",\"newName\":\"${RENAMED_REVERSE_TAG}\"}]}"
await_http "A invalidates the tag's old name" "$INSTANCE_A" \
  "/api/metalakes/${MAIN_METALAKE}/tags/${REVERSE_TAG}" 404
await_value "A loads the tag renamed through B" "$INSTANCE_A" \
  "/api/metalakes/${MAIN_METALAKE}/tags/${RENAMED_REVERSE_TAG}" '.tag.name' "$RENAMED_REVERSE_TAG"

mutate_a "create reverse-direction fileset" POST "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets" \
  "{\"name\":\"${REVERSE_FILESET}\",\"type\":\"EXTERNAL\",\"comment\":\"reverse-fileset-old\",\"storageLocation\":\"file:///tmp/gravitino-reverse-fileset-${SUFFIX}\",\"properties\":{}}"

consistency_case "FILESET alter propagates B -> A"
prewarm_value "A caches the old fileset before B alters it" "$INSTANCE_A" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets/${REVERSE_FILESET}" \
  '.fileset.comment' reverse-fileset-old
mutate_b "alter reverse-direction fileset on B" PUT "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets/${REVERSE_FILESET}" \
  '{"updates":[{"@type":"updateComment","newComment":"reverse-fileset-new"}]}'
await_value "A sees the fileset altered through B" "$INSTANCE_A" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets/${REVERSE_FILESET}" '.fileset.comment' reverse-fileset-new

consistency_case "FILESET rename propagates B -> A"
prewarm_value "A caches the pre-rename fileset" "$INSTANCE_A" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets/${REVERSE_FILESET}" \
  '.fileset.comment' reverse-fileset-new
mutate_b "rename reverse-direction fileset on B" PUT "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets/${REVERSE_FILESET}" \
  "{\"updates\":[{\"@type\":\"rename\",\"newName\":\"${RENAMED_REVERSE_FILESET}\"}]}"
await_http "A invalidates the fileset's old name" "$INSTANCE_A" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets/${REVERSE_FILESET}" 404
await_value "A loads the fileset renamed through B" "$INSTANCE_A" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets/${RENAMED_REVERSE_FILESET}" '.fileset.name' "$RENAMED_REVERSE_FILESET"

REVERSE_POLICY_BODY=$(jq -nc --arg name "$REVERSE_POLICY" \
  '{name:$name,comment:"reverse-policy",policyType:"custom",enabled:true,content:{customRules:{retentionDays:30},supportedObjectTypes:["CATALOG","SCHEMA","TABLE"],properties:{owner:"platform"}}}')
mutate_a "create reverse-direction policy" POST "/api/metalakes/${MAIN_METALAKE}/policies" "$REVERSE_POLICY_BODY"

consistency_case "POLICY disable propagates B -> A"
prewarm_value "A caches the enabled policy before B disables it" "$INSTANCE_A" \
  "/api/metalakes/${MAIN_METALAKE}/policies/${REVERSE_POLICY}" '.policy.enabled' true
mutate_b "disable reverse-direction policy on B" PATCH "/api/metalakes/${MAIN_METALAKE}/policies/${REVERSE_POLICY}" '{"enable":false}'
await_value "A sees the policy disabled through B" "$INSTANCE_A" \
  "/api/metalakes/${MAIN_METALAKE}/policies/${REVERSE_POLICY}" '.policy.enabled' false

consistency_case "POLICY enable propagates B -> A"
prewarm_value "A caches the disabled policy before B enables it" "$INSTANCE_A" \
  "/api/metalakes/${MAIN_METALAKE}/policies/${REVERSE_POLICY}" '.policy.enabled' false
mutate_b "enable reverse-direction policy on B" PATCH "/api/metalakes/${MAIN_METALAKE}/policies/${REVERSE_POLICY}" '{"enable":true}'
await_value "A sees the policy enabled through B" "$INSTANCE_A" \
  "/api/metalakes/${MAIN_METALAKE}/policies/${REVERSE_POLICY}" '.policy.enabled' true

section "Cross-type batch: invalidate parent and child entities in one poll"
mutate_a "create burst tag" POST "/api/metalakes/${MAIN_METALAKE}/tags" \
  "{\"name\":\"${BURST_TAG}\",\"comment\":\"burst-tag-old\",\"properties\":{}}"
BURST_POLICY_BODY=$(jq -nc --arg name "$BURST_POLICY" \
  '{name:$name,comment:"burst-policy-old",policyType:"custom",enabled:true,content:{customRules:{retentionDays:30},supportedObjectTypes:["CATALOG","SCHEMA","TABLE"],properties:{owner:"platform"}}}')
mutate_a "create burst policy" POST "/api/metalakes/${MAIN_METALAKE}/policies" "$BURST_POLICY_BODY"
mutate_a "create burst fileset" POST "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets" \
  "{\"name\":\"${BURST_FILESET}\",\"type\":\"EXTERNAL\",\"comment\":\"burst-fileset-old\",\"storageLocation\":\"file:///tmp/gravitino-burst-fileset-${SUFFIX}\",\"properties\":{}}"

consistency_case "CATALOG, SCHEMA, FILESET, TAG, and POLICY invalidate in one batch"
prewarm_value "B caches the catalog before the cross-type batch" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}" \
  '.catalog.comment' "Fileset cache test"
prewarm_value "B caches the schema before the cross-type batch" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}" \
  '.schema.name' "$FILESET_SCHEMA"
prewarm_value "B caches the fileset before the cross-type batch" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets/${BURST_FILESET}" \
  '.fileset.comment' burst-fileset-old
prewarm_value "B caches the tag before the cross-type batch" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/tags/${BURST_TAG}" '.tag.comment' burst-tag-old
prewarm_value "B caches the policy before the cross-type batch" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/policies/${BURST_POLICY}" '.policy.comment' burst-policy-old
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
await_value "B sees the catalog from the batch" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}" '.catalog.comment' burst-catalog-new
await_value "B sees the schema from the batch" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}" '.schema.properties["burst-key"]' burst-schema-new
await_value "B sees the fileset from the batch" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/catalogs/${FILESET_CATALOG}/schemas/${FILESET_SCHEMA}/filesets/${BURST_FILESET}" '.fileset.comment' burst-fileset-new
await_value "B sees the tag from the batch" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/tags/${BURST_TAG}" '.tag.comment' burst-tag-new
await_value "B sees the policy from the batch" "$INSTANCE_B" \
  "/api/metalakes/${MAIN_METALAKE}/policies/${BURST_POLICY}" '.policy.comment' burst-policy-new

section "JOB cache: cancellation alters the cached status"
JOB_SCRIPT="$(mktemp -t gravitino-entity-cache-job.XXXX.sh)"
printf '#!/usr/bin/env bash\nsleep 60\n' >"$JOB_SCRIPT"
chmod +x "$JOB_SCRIPT"
JOB_TEMPLATE_BODY=$(jq -nc --arg name "$JOB_TEMPLATE" --arg executable "$JOB_SCRIPT" \
  '{jobTemplate:{name:$name,jobType:"shell",comment:"Job cache test",executable:$executable,arguments:[],environments:{},customFields:{},scripts:[]}}')
mutate_a "register job template on A" POST "/api/metalakes/${MAIN_METALAKE}/jobs/templates" "$JOB_TEMPLATE_BODY"

consistency_case "JOB cancellation invalidates a warm target cache"
JOB_STALE_OBSERVED=0
for round in $(seq 1 "$PER_TYPE_STALE_CONTROL_ROUNDS"); do
  api "$INSTANCE_A" POST "/api/metalakes/${MAIN_METALAKE}/jobs/runs" \
    "{\"jobTemplateName\":\"${JOB_TEMPLATE}\",\"jobConf\":{}}"
  expect_http "JOB A -> B round ${round}: run job on A" 200
  JOB_ID=$(printf '%s' "$RESPONSE_BODY" | jq -r '.job.jobId // empty')
  if [[ -z "$JOB_ID" ]]; then
    fail "JOB A -> B round ${round}: job id returned — Body: $(body_snippet)"
    CASE_MUTATION_STARTED=true
    continue
  fi

  prewarm_one_of "JOB A -> B round ${round}: B caches the running job" "$INSTANCE_B" \
    "/api/metalakes/${MAIN_METALAKE}/jobs/runs/${JOB_ID}" '.job.status' queued started
  STALE_JOB_STATUS=$(printf '%s' "$RESPONSE_BODY" | jq -r '.job.status // empty')

  api "$INSTANCE_A" POST "/api/metalakes/${MAIN_METALAKE}/jobs/runs/${JOB_ID}"
  expect_http "JOB A -> B round ${round}: cancel job on A" 200
  CASE_MUTATION_STARTED=true

  OBSERVED_JOB_STATUS=$(read_field "$INSTANCE_B" \
    "/api/metalakes/${MAIN_METALAKE}/jobs/runs/${JOB_ID}" '.job.status')
  if [[ -n "$STALE_JOB_STATUS" && "$OBSERVED_JOB_STATUS" == "$STALE_JOB_STATUS" ]]; then
    JOB_STALE_OBSERVED=$((JOB_STALE_OBSERVED + 1))
    printf '    JOB A -> B round %d: B still served cached status %s\n' \
      "$round" "$STALE_JOB_STATUS"
  else
    printf '    JOB A -> B round %d: B already served %s (poller won the race)\n' \
      "$round" "$OBSERVED_JOB_STATUS"
  fi

  await_one_of "JOB A -> B round ${round}: B sees canceled job status" "$INSTANCE_B" \
    "/api/metalakes/${MAIN_METALAKE}/jobs/runs/${JOB_ID}" '.job.status' cancelling canceled
done

if ((JOB_STALE_OBSERVED > 0)); then
  record_cache_proof "JOB A -> B cache residency (${JOB_STALE_OBSERVED}/${PER_TYPE_STALE_CONTROL_ROUNDS} stale rounds)"
else
  fail "JOB A -> B never served a stale status in ${PER_TYPE_STALE_CONTROL_ROUNDS} rounds — B may be cold or bypassing the JOB cache"
fi

if [[ "$RUN_RESTART_TEST" == "true" ]]; then
  section "Instance B restart: initialize at high-water, then process new changes"
  mutate_a "create restart probe tag on A" POST "/api/metalakes/${MAIN_METALAKE}/tags" \
    "{\"name\":\"${RESTART_TAG}\",\"comment\":\"restart-old\",\"properties\":{}}"
  api "$INSTANCE_B" GET "/api/metalakes/${MAIN_METALAKE}/tags/${RESTART_TAG}"
  expect_value "warm restart probe tag on B" '.tag.comment' restart-old

  if [[ -x "${INSTANCE_B_HOME}/bin/gravitino.sh" ]]; then
    "${INSTANCE_B_HOME}/bin/gravitino.sh" stop
    # This is intentionally a cold-start check: stopping B discards its local
    # cache, so the scenario validates change-log high-water initialization.
    restart_case "Restarted B initializes at the current change-log high-water mark"
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
    prewarm_value "restarted B caches the current tag before A alters it" "$INSTANCE_B" \
      "/api/metalakes/${MAIN_METALAKE}/tags/${RESTART_TAG}" '.tag.comment' restart-current
    mutate_a "alter warmed probe after B restart" PUT "/api/metalakes/${MAIN_METALAKE}/tags/${RESTART_TAG}" \
      '{"updates":[{"@type":"updateComment","newComment":"restart-future"}]}'
    await_value "restarted B polls and invalidates future changes" "$INSTANCE_B" \
      "/api/metalakes/${MAIN_METALAKE}/tags/${RESTART_TAG}" '.tag.comment' restart-future
  else
    fail "INSTANCE_B_HOME does not contain an executable bin/gravitino.sh"
  fi
fi

section "Summary"
if ((CACHE_PROOF_CASES == EXPECTED_CACHE_PROOF_CASES)); then
  pass "all ${EXPECTED_CACHE_PROOF_CASES} required warm-cache proofs executed"
else
  fail "expected ${EXPECTED_CACHE_PROOF_CASES} warm-cache proofs, executed ${CACHE_PROOF_CASES}"
fi
printf 'Consistency cases executed: %d\n' "$CONSISTENCY_CASES"
printf 'Warm-cache proofs executed: %d\n' "$CACHE_PROOF_CASES"
printf '  - %s\n' "${CACHE_PROOFS[@]}"
printf 'Assertions passed: %d\nAssertions failed: %d\n' "$PASS" "$FAIL"
if ((FAIL > 0)); then
  printf '\nFailed assertions:\n'
  printf '  - %s\n' "${FAILED_TESTS[@]}"
  exit 1
fi

printf '\nAll multi-instance entity-cache consistency assertions passed.\n'
