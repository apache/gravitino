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
#
# Multi-instance consistency test for the JcasbinAuthorizer cache propagation.
#
# What this verifies (one cluster, two Gravitino instances backed by the same DB):
#
#   A. metadataIdCache / ownerRelCache are eventually consistent across instances
#      via the JcasbinChangePoller (default poll interval = 3s).
#      → set owner on A, read it back on B.
#
#   B. The new owner can actually exercise owner privileges on the *other* instance
#      after the poll cycle, and the old owner is rejected.
#      → A sets owner=lisi; on B lisi can re-setOwner, zhangsan gets 403.
#
#   C. userRoleCache (per-request, version-validated) reflects role grants
#      *immediately* on the other instance — no poll wait needed.
#      → grant role on A; on B a user with no prior privileges immediately
#        succeeds at an action gated by that role.
#
#   D. Same path, reverse direction: revoke role on A is immediately effective on B.
#
#   E. loadedRoles cache (per-request, version-validated) reflects role-privilege
#      changes immediately on the other instance.
#      → revoke MANAGE_USERS from role on A; user is denied on B even though
#        the role binding itself didn't change.
#      → grant CREATE_ROLE on the same role on A; user can immediately use it on B.
#
# All authz-enforcement assertions exercise metalake-internal APIs (addUser,
# createRole, setOwner) so the test runs without any external catalog backend.
# The propagation guarantees being tested apply identically to schemas, tables,
# filesets, etc. — only the path you'd PUT/GET differs.
#
# -----------------------------------------------------------------------------
# Server preconditions (configure both instances BEFORE running):
#   gravitino.authorization.enable                          = true
#   gravitino.authenticators                                = simple
#   gravitino.authorization.serviceAdmins                   = admin   # required for metalake create
#   gravitino.entity-store                                  = relational
#   gravitino.entity-store.relational.jdbcUrl               = <shared MySQL/PostgreSQL>
#   gravitino.authorization.jcasbin.changePollIntervalSecs  = 3   (default; lower = faster A↔B)
#
# H2 is NOT supported here — both instances must share one real SQL backend so
# entity_change_log is visible to both pollers.
#
# Note on the "admin" role: per JcasbinAuthorizer.hasSetOwnerPermission, service-admin
# does NOT bypass ownership checks — only the current owner can setOwner. The script
# therefore does ownership chains as the *current* owner (admin → zhangsan → lisi)
# and uses a separate user (wangwu) for role-based tests so role grants and
# ownership grants don't interfere.
# -----------------------------------------------------------------------------
#
# Usage:
#   bash dev/test_multi_instance_consistency.sh
#   INSTANCE_A=http://host1:8090 INSTANCE_B=http://host2:8090 bash dev/test_multi_instance_consistency.sh
#
# Exit code = number of failed assertions (0 = all pass).

set -uo pipefail

# ---- config ----------------------------------------------------------------

INSTANCE_A="${INSTANCE_A:-http://localhost:8090}"
INSTANCE_B="${INSTANCE_B:-http://localhost:8190}"
ADMIN_USER="${ADMIN_USER:-admin}"
POLL_WAIT_SECS="${POLL_WAIT_SECS:-6}"   # default poll = 3s + buffer

SUFFIX="$(date +%s)_$$"
METALAKE="cons_test_${SUFFIX}"
METALAKE2="cons_test2_${SUFFIX}"  # for cross-metalake isolation check (Phase J)
USER_ZHANGSAN="zhangsan_${SUFFIX}"
USER_LISI="lisi_${SUFFIX}"
USER_WANGWU="wangwu_${SUFFIX}"   # separate test user, never an owner
USER_DELME="userdel_${SUFFIX}"   # for delete-user / re-create tests (Phases G, H)
ROLE_NAME="role_${SUFFIX}"
ROLE_FRESH="role_g_${SUFFIX}"     # for Phase G (delete user test)
ROLE_ALLOW="role_allow_${SUFFIX}" # for Phase K (DENY override)
ROLE_DENY="role_deny_${SUFFIX}"   # for Phase K
GROUP_NAME="group_${SUFFIX}"      # for Phase L
TAG_NAME="tag_${SUFFIX}"          # for Phase N

PASS=0
FAIL=0
FAILED_TESTS=()

# ---- helpers ---------------------------------------------------------------

# Simple authenticator wants: Authorization: Basic base64(user:)
auth_header() { printf 'Basic %s' "$(printf '%s:' "$1" | base64 | tr -d '\n')"; }

# api <base_url> <as_user> <method> <path> [body_json]
# Writes status code to HTTP_CODE, body to RESPONSE_BODY.
api() {
  local base="$1" user="$2" method="$3" path="$4" body="${5:-}"
  local tmp
  tmp="$(mktemp -t graviton_api.XXXX)"
  if [[ -n "$body" ]]; then
    HTTP_CODE=$(curl -sS -o "$tmp" -w '%{http_code}' \
      -H "Authorization: $(auth_header "$user")" \
      -H 'Content-Type: application/json' \
      -X "$method" --data "$body" "${base}${path}" || echo 000)
  else
    HTTP_CODE=$(curl -sS -o "$tmp" -w '%{http_code}' \
      -H "Authorization: $(auth_header "$user")" \
      -X "$method" "${base}${path}" || echo 000)
  fi
  RESPONSE_BODY="$(cat "$tmp")"
  rm -f "$tmp"
}

pass() { PASS=$((PASS+1)); printf '  \033[32m✓\033[0m %s\n' "$1"; }
fail() {
  FAIL=$((FAIL+1)); FAILED_TESTS+=("$1")
  printf '  \033[31m✗\033[0m %s\n' "$1"
}

# expect <desc> <expected_http_code> [body_grep_pattern]
expect() {
  local desc="$1" want="$2" pattern="${3:-}"
  local body_snip
  body_snip=$(printf '%s' "$RESPONSE_BODY" | tr '\n' ' ' | cut -c1-180)
  if [[ "$HTTP_CODE" != "$want" ]]; then
    fail "$desc — expected HTTP $want, got $HTTP_CODE. Body: $body_snip"
    return
  fi
  if [[ -n "$pattern" ]] && ! printf '%s' "$RESPONSE_BODY" | grep -q -E "$pattern"; then
    fail "$desc — HTTP $want OK but body missing /$pattern/. Body: $body_snip"
    return
  fi
  pass "$desc"
}

section() { printf '\n\033[1m== %s ==\033[0m\n' "$1"; }

# ---- preflight -------------------------------------------------------------

section "0. Preflight"
api "$INSTANCE_A" "$ADMIN_USER" GET '/api/version'
expect "instance A reachable + admin auth works" 200 'gitCommit'
api "$INSTANCE_B" "$ADMIN_USER" GET '/api/version'
expect "instance B reachable + admin auth works" 200 'gitCommit'

# Confirm 'admin' is configured as a service admin on both instances — required to
# create metalakes (the only service-admin-gated step we need).
SCRATCH="scratch_${SUFFIX}"
api "$INSTANCE_A" "$ADMIN_USER" POST '/api/metalakes' \
  "{\"name\":\"$SCRATCH\",\"comment\":\"scratch\",\"properties\":{}}"
if [[ "$HTTP_CODE" == "403" ]]; then
  fail "admin lacks service-admin rights on A — add '$ADMIN_USER' to gravitino.authorization.serviceAdmins on both instances and restart"
elif [[ "$HTTP_CODE" == "200" ]]; then
  pass "admin is a service admin on A (can create metalake)"
  api "$INSTANCE_A" "$ADMIN_USER" DELETE "/api/metalakes/$SCRATCH?force=true" >/dev/null
fi

if [[ $FAIL -gt 0 ]]; then
  echo
  echo "Preflight failed — aborting. Fix the above before re-running."
  exit 99
fi

# ---- cleanup hook ----------------------------------------------------------

cleanup_metalake() {
  local ml="$1"
  for who in "$USER_LISI" "$USER_ZHANGSAN" "$ADMIN_USER"; do
    api "$INSTANCE_A" "$who" DELETE "/api/metalakes/$ml?force=true"
    if [[ "$HTTP_CODE" =~ ^(200|404)$ ]]; then
      echo "  deleted $ml (as $who, HTTP $HTTP_CODE)"
      return
    fi
  done
  echo "  WARNING: failed to delete $ml — last HTTP $HTTP_CODE"
}

cleanup() {
  section "Cleanup"
  # Force-delete cascades through users/roles/tags/groups inside the metalake.
  # The metalake may be owned by any of (admin / zhangsan / lisi) depending on
  # how far the test got. Try each in turn until DELETE returns 200/404.
  cleanup_metalake "$METALAKE"
  cleanup_metalake "$METALAKE2"
}
trap cleanup EXIT

# ---- setup (on instance A) -------------------------------------------------

section "1. Setup on A"
api "$INSTANCE_A" "$ADMIN_USER" POST '/api/metalakes' \
  "{\"name\":\"$METALAKE\",\"comment\":\"consistency test\",\"properties\":{}}"
expect "create metalake $METALAKE (admin = service admin, becomes initial owner)" 200

api "$INSTANCE_A" "$ADMIN_USER" POST "/api/metalakes/$METALAKE/users" \
  "{\"name\":\"$USER_ZHANGSAN\"}"
expect "add user $USER_ZHANGSAN" 200

api "$INSTANCE_A" "$ADMIN_USER" POST "/api/metalakes/$METALAKE/users" \
  "{\"name\":\"$USER_LISI\"}"
expect "add user $USER_LISI" 200

api "$INSTANCE_A" "$ADMIN_USER" POST "/api/metalakes/$METALAKE/users" \
  "{\"name\":\"$USER_WANGWU\"}"
expect "add user $USER_WANGWU (used for role tests, never an owner)" 200

# Role with MANAGE_USERS at metalake scope — holder can addUser / removeUser.
api "$INSTANCE_A" "$ADMIN_USER" POST "/api/metalakes/$METALAKE/roles" "$(cat <<EOF
{
  "name": "$ROLE_NAME",
  "properties": {},
  "securableObjects": [
    {
      "fullName": "$METALAKE",
      "type": "METALAKE",
      "privileges": [{"name": "MANAGE_USERS", "condition": "ALLOW"}]
    }
  ]
}
EOF
)"
expect "create role $ROLE_NAME with MANAGE_USERS on metalake" 200

# ---- A. owner read-back propagates (eventual ~3s) --------------------------

section "A. Owner read-back propagates from A → B (eventual, ~${POLL_WAIT_SECS}s)"
# admin is current owner → admin transfers to zhangsan
api "$INSTANCE_A" "$ADMIN_USER" PUT "/api/metalakes/$METALAKE/owners/METALAKE/$METALAKE" \
  "{\"name\":\"$USER_ZHANGSAN\",\"type\":\"USER\"}"
expect "A: admin (owner) → setOwner = $USER_ZHANGSAN" 200

echo "  ...sleeping ${POLL_WAIT_SECS}s for the change poller on B"
sleep "$POLL_WAIT_SECS"

api "$INSTANCE_B" "$ADMIN_USER" GET "/api/metalakes/$METALAKE/owners/METALAKE/$METALAKE"
expect "B: GET owner now reports $USER_ZHANGSAN" 200 "\"name\"[^}]*\"$USER_ZHANGSAN\""

# zhangsan is now owner → zhangsan transfers to lisi
api "$INSTANCE_A" "$USER_ZHANGSAN" PUT "/api/metalakes/$METALAKE/owners/METALAKE/$METALAKE" \
  "{\"name\":\"$USER_LISI\",\"type\":\"USER\"}"
expect "A: $USER_ZHANGSAN (owner) → setOwner = $USER_LISI" 200

echo "  ...sleeping ${POLL_WAIT_SECS}s for the change poller on B"
sleep "$POLL_WAIT_SECS"

api "$INSTANCE_B" "$ADMIN_USER" GET "/api/metalakes/$METALAKE/owners/METALAKE/$METALAKE"
expect "B: GET owner now reports $USER_LISI" 200 "\"name\"[^}]*\"$USER_LISI\""

# ---- B. enforcement on B reflects the owner change -------------------------

section "B. Authz enforcement on B reflects the new owner (no extra wait)"
# lisi is the current owner — re-setting (lisi → lisi) is a no-op write that requires
# the caller to be the current owner. If B's ownerRelCache is invalidated, this works.
# Note: this is also B's *first* isOwner check for $METALAKE, so its ownerRelCache
# entry is being populated cold from the DB here.
api "$INSTANCE_B" "$USER_LISI" PUT "/api/metalakes/$METALAKE/owners/METALAKE/$METALAKE" \
  "{\"name\":\"$USER_LISI\",\"type\":\"USER\"}"
expect "B: $USER_LISI (current owner) re-asserts ownership" 200

# zhangsan was owner two transfers ago but is not anymore.
api "$INSTANCE_B" "$USER_ZHANGSAN" PUT "/api/metalakes/$METALAKE/owners/METALAKE/$METALAKE" \
  "{\"name\":\"$USER_ZHANGSAN\",\"type\":\"USER\"}"
expect "B: $USER_ZHANGSAN (former owner) is rejected with 403" 403

# ---- B'. WARM-cache invalidation: critical scenario ------------------------
#
# Phases A/B above only exercise *cold* cache fills on B: when B's first
# isOwner check happens, the new owner is already in the DB, so the cache
# is correctly populated. They do NOT prove that an already-cached entry on
# B (with the stale owner) gets invalidated when A changes the owner.
#
# This phase forces that scenario:
#   1. B's ownerRelCache currently holds lisi (warmed by Phase B above).
#   2. Change owner on A back to zhangsan.
#   3. Wait one poll cycle.
#   4. On B, do an isOwner-gated operation as zhangsan — must succeed if and
#      only if the poller invalidated B's lisi entry. As lisi — must fail.
#
section "B'. Warm-cache invalidation on owner change (eventual, ~${POLL_WAIT_SECS}s)"
api "$INSTANCE_A" "$USER_LISI" PUT "/api/metalakes/$METALAKE/owners/METALAKE/$METALAKE" \
  "{\"name\":\"$USER_ZHANGSAN\",\"type\":\"USER\"}"
expect "A: $USER_LISI (owner) transfers owner back to $USER_ZHANGSAN" 200

echo "  ...sleeping ${POLL_WAIT_SECS}s for poller to invalidate B's warm cache"
sleep "$POLL_WAIT_SECS"

# B's cache previously held lisi. If the poller invalidated correctly, the
# next isOwner check reloads zhangsan from DB.
api "$INSTANCE_B" "$USER_ZHANGSAN" PUT "/api/metalakes/$METALAKE/owners/METALAKE/$METALAKE" \
  "{\"name\":\"$USER_ZHANGSAN\",\"type\":\"USER\"}"
expect "B: $USER_ZHANGSAN (new owner) re-asserts ownership — proves B's cache invalidated" 200

api "$INSTANCE_B" "$USER_LISI" PUT "/api/metalakes/$METALAKE/owners/METALAKE/$METALAKE" \
  "{\"name\":\"$USER_LISI\",\"type\":\"USER\"}"
expect "B: $USER_LISI (former owner, was cached) now rejected" 403

# Transfer back to lisi for the remainder of the test (which assumes lisi is owner).
api "$INSTANCE_A" "$USER_ZHANGSAN" PUT "/api/metalakes/$METALAKE/owners/METALAKE/$METALAKE" \
  "{\"name\":\"$USER_LISI\",\"type\":\"USER\"}"
expect "A: $USER_ZHANGSAN restores owner = $USER_LISI for subsequent phases" 200
echo "  ...sleeping ${POLL_WAIT_SECS}s so B sees lisi-as-owner again before role tests"
sleep "$POLL_WAIT_SECS"

# ---- C. grant role on A → user can act on B IMMEDIATELY --------------------

section "C. Grant role on A — effective on B without poll wait (version-validated)"
# Baseline: wangwu has no role, no ownership, so addUser is forbidden.
api "$INSTANCE_B" "$USER_WANGWU" POST "/api/metalakes/$METALAKE/users" \
  "{\"name\":\"baseline_denied_${SUFFIX}\"}"
expect "B: $USER_WANGWU denied addUser before role grant (baseline)" 403

# As lisi (owner): grant the MANAGE_USERS role to wangwu on A.
api "$INSTANCE_A" "$USER_LISI" PUT \
  "/api/metalakes/$METALAKE/permissions/users/$USER_WANGWU/grant/" \
  "{\"roleNames\":[\"$ROLE_NAME\"]}"
expect "A: $USER_LISI grants $ROLE_NAME to $USER_WANGWU" 200

# No sleep — userRoleCache is version-validated per request.
api "$INSTANCE_B" "$USER_WANGWU" POST "/api/metalakes/$METALAKE/users" \
  "{\"name\":\"granted_ok_${SUFFIX}\"}"
expect "B: $USER_WANGWU addUser succeeds immediately after grant" 200

# ---- D. revoke role on A → user denied on B IMMEDIATELY --------------------

section "D. Revoke role on A — effective on B without poll wait"
api "$INSTANCE_A" "$USER_LISI" PUT \
  "/api/metalakes/$METALAKE/permissions/users/$USER_WANGWU/revoke/" \
  "{\"roleNames\":[\"$ROLE_NAME\"]}"
expect "A: $USER_LISI revokes $ROLE_NAME from $USER_WANGWU" 200

api "$INSTANCE_B" "$USER_WANGWU" POST "/api/metalakes/$METALAKE/users" \
  "{\"name\":\"revoked_denied_${SUFFIX}\"}"
expect "B: $USER_WANGWU denied addUser immediately after revoke" 403

# ---- E. modify role's privileges on A → effective on B IMMEDIATELY ---------

section "E. Modify role privileges on A — effective on B without poll wait"
# Re-grant the role to wangwu so the test isolates a *role-privilege* change
# rather than a *role-membership* change.
api "$INSTANCE_A" "$USER_LISI" PUT \
  "/api/metalakes/$METALAKE/permissions/users/$USER_WANGWU/grant/" \
  "{\"roleNames\":[\"$ROLE_NAME\"]}"
expect "A: $USER_LISI re-grants $ROLE_NAME to $USER_WANGWU for privilege test" 200

# Sanity: wangwu can still addUser at this point.
api "$INSTANCE_B" "$USER_WANGWU" POST "/api/metalakes/$METALAKE/users" \
  "{\"name\":\"priv_baseline_${SUFFIX}\"}"
expect "B: $USER_WANGWU addUser works (role re-granted, sanity)" 200

# Strip MANAGE_USERS from the role itself on A.
api "$INSTANCE_A" "$USER_LISI" PUT \
  "/api/metalakes/$METALAKE/permissions/roles/$ROLE_NAME/METALAKE/$METALAKE/revoke/" \
  '{"privileges":[{"name":"MANAGE_USERS","condition":"ALLOW"}]}'
expect "A: $USER_LISI revokes MANAGE_USERS from $ROLE_NAME" 200

# wangwu still holds the role, but the role no longer grants MANAGE_USERS.
# loadedRoles must be invalidated on B for this to fail.
api "$INSTANCE_B" "$USER_WANGWU" POST "/api/metalakes/$METALAKE/users" \
  "{\"name\":\"priv_revoked_${SUFFIX}\"}"
expect "B: $USER_WANGWU denied after role's MANAGE_USERS privilege revoked" 403

# Now add CREATE_ROLE to the same role on A.
api "$INSTANCE_A" "$USER_LISI" PUT \
  "/api/metalakes/$METALAKE/permissions/roles/$ROLE_NAME/METALAKE/$METALAKE/grant/" \
  '{"privileges":[{"name":"CREATE_ROLE","condition":"ALLOW"}]}'
expect "A: $USER_LISI grants CREATE_ROLE to $ROLE_NAME" 200

# wangwu still can't addUser (role doesn't have MANAGE_USERS anymore)
api "$INSTANCE_B" "$USER_WANGWU" POST "/api/metalakes/$METALAKE/users" \
  "{\"name\":\"still_no_manage_users_${SUFFIX}\"}"
expect "B: $USER_WANGWU still denied addUser (role has only CREATE_ROLE)" 403

# …but he CAN createRole now, immediately.
api "$INSTANCE_B" "$USER_WANGWU" POST "/api/metalakes/$METALAKE/roles" "$(cat <<EOF
{
  "name": "wangwu_role_${SUFFIX}",
  "properties": {},
  "securableObjects": [
    {
      "fullName": "$METALAKE",
      "type": "METALAKE",
      "privileges": [{"name": "MANAGE_USERS","condition":"ALLOW"}]
    }
  ]
}
EOF
)"
expect "B: $USER_WANGWU createRole succeeds immediately after CREATE_ROLE granted on A" 200

# ============================================================================
# Phase F-N: extended coverage — lifecycle operations, isolation, DENY, groups,
# non-METALAKE setOwner.
# State at this point: $METALAKE owner = $USER_LISI; $USER_WANGWU holds
# $ROLE_NAME which currently has only CREATE_ROLE (MANAGE_USERS revoked in E).
# ============================================================================

# ---- F. delete role on A → users with that role lose its privileges --------

section "F. Delete role on A — holders lose its privileges on B (immediate)"
# NOTE: this phase exposes a known cache-invalidation gap when running against
# the *current* JcasbinAuthorizer implementation:
#   - userRoleCache on B is version-validated against user_meta.updated_at, but
#     role deletion does NOT bump user_meta.updated_at for users still holding
#     that role. B's cache therefore considers wangwu's role list still valid.
#   - versionCheckAndLoadRoles only iterates the rows returned by
#     batchGetRoleUpdatedAt; a deleted role simply doesn't appear and is
#     silently skipped, so its cached policies in loadedRoles are never evicted.
# The fix would be either (a) bump user_meta.updated_at on every user that
# held the deleted role, or (b) compare the requested roleIds against the
# returned rows in versionCheckAndLoadRoles and evict the missing ones.
# The assertion below is left strict on purpose: it will turn green once the
# implementation is fixed.

# Sanity: wangwu currently holds ROLE_NAME which has CREATE_ROLE.
api "$INSTANCE_B" "$USER_WANGWU" POST "/api/metalakes/$METALAKE/roles" "$(cat <<EOF
{"name":"sanity_role_${SUFFIX}","properties":{},"securableObjects":[
  {"fullName":"$METALAKE","type":"METALAKE",
   "privileges":[{"name":"MANAGE_USERS","condition":"ALLOW"}]}]}
EOF
)"
expect "B: $USER_WANGWU createRole still works (sanity)" 200

api "$INSTANCE_A" "$USER_LISI" DELETE "/api/metalakes/$METALAKE/roles/$ROLE_NAME"
expect "A: $USER_LISI deletes role $ROLE_NAME" 200

api "$INSTANCE_B" "$USER_WANGWU" POST "/api/metalakes/$METALAKE/roles" "$(cat <<EOF
{"name":"post_delete_${SUFFIX}","properties":{},"securableObjects":[
  {"fullName":"$METALAKE","type":"METALAKE",
   "privileges":[{"name":"MANAGE_USERS","condition":"ALLOW"}]}]}
EOF
)"
expect "B: $USER_WANGWU createRole now denied (role deleted on A)" 403

# ---- G. delete user (caller) on A → that user's requests rejected on B -----

section "G. Delete user on A — calls from that user are rejected on B immediately"
api "$INSTANCE_A" "$USER_LISI" POST "/api/metalakes/$METALAKE/users" \
  "{\"name\":\"$USER_DELME\"}"
expect "A: create user $USER_DELME" 200

# Create a fresh role and grant MANAGE_USERS to the new user.
api "$INSTANCE_A" "$USER_LISI" POST "/api/metalakes/$METALAKE/roles" "$(cat <<EOF
{"name":"$ROLE_FRESH","properties":{},"securableObjects":[
  {"fullName":"$METALAKE","type":"METALAKE",
   "privileges":[{"name":"MANAGE_USERS","condition":"ALLOW"}]}]}
EOF
)"
expect "A: create role $ROLE_FRESH (MANAGE_USERS)" 200

api "$INSTANCE_A" "$USER_LISI" PUT \
  "/api/metalakes/$METALAKE/permissions/users/$USER_DELME/grant/" \
  "{\"roleNames\":[\"$ROLE_FRESH\"]}"
expect "A: grant $ROLE_FRESH to $USER_DELME" 200

api "$INSTANCE_B" "$USER_DELME" POST "/api/metalakes/$METALAKE/users" \
  "{\"name\":\"delme_baseline_${SUFFIX}\"}"
expect "B: $USER_DELME addUser works before deletion (sanity)" 200

api "$INSTANCE_A" "$USER_LISI" DELETE "/api/metalakes/$METALAKE/users/$USER_DELME"
expect "A: delete user $USER_DELME" 200

api "$INSTANCE_B" "$USER_DELME" POST "/api/metalakes/$METALAKE/users" \
  "{\"name\":\"delme_after_${SUFFIX}\"}"
expect "B: $USER_DELME addUser denied immediately after user deletion" 403

# ---- H. re-create same-named user → no stale role bindings -----------------

section "H. Re-create same-named user — no stale role inheritance"
api "$INSTANCE_A" "$USER_LISI" POST "/api/metalakes/$METALAKE/users" \
  "{\"name\":\"$USER_DELME\"}"
expect "A: re-create user $USER_DELME (no roles granted this time)" 200

api "$INSTANCE_B" "$USER_DELME" POST "/api/metalakes/$METALAKE/users" \
  "{\"name\":\"new_delme_${SUFFIX}\"}"
expect "B: re-created $USER_DELME still denied addUser (no stale role leaked from old user)" 403

# ---- I. re-create same-named role → fresh privileges only ------------------

section "I. Re-create same-named role — only fresh privileges apply"
# ROLE_NAME was deleted in F. Re-create it with CREATE_TAG instead of MANAGE_USERS.
api "$INSTANCE_A" "$USER_LISI" POST "/api/metalakes/$METALAKE/roles" "$(cat <<EOF
{"name":"$ROLE_NAME","properties":{},"securableObjects":[
  {"fullName":"$METALAKE","type":"METALAKE",
   "privileges":[{"name":"CREATE_TAG","condition":"ALLOW"}]}]}
EOF
)"
expect "A: re-create $ROLE_NAME with only CREATE_TAG" 200

api "$INSTANCE_A" "$USER_LISI" PUT \
  "/api/metalakes/$METALAKE/permissions/users/$USER_WANGWU/grant/" \
  "{\"roleNames\":[\"$ROLE_NAME\"]}"
expect "A: grant the new $ROLE_NAME to $USER_WANGWU" 200

# Fresh privilege works.
api "$INSTANCE_B" "$USER_WANGWU" POST "/api/metalakes/$METALAKE/tags" \
  "{\"name\":\"i_tag_${SUFFIX}\",\"comment\":\"phase I tag\",\"properties\":{}}"
expect "B: $USER_WANGWU createTag works (fresh CREATE_TAG via re-created role)" 200

# Stale privileges from previous incarnations (MANAGE_USERS, CREATE_ROLE) do NOT leak.
api "$INSTANCE_B" "$USER_WANGWU" POST "/api/metalakes/$METALAKE/users" \
  "{\"name\":\"stale_check_${SUFFIX}\"}"
expect "B: $USER_WANGWU denied addUser (no stale MANAGE_USERS from old role)" 403

api "$INSTANCE_B" "$USER_WANGWU" POST "/api/metalakes/$METALAKE/roles" "$(cat <<EOF
{"name":"stale_role_check_${SUFFIX}","properties":{},"securableObjects":[
  {"fullName":"$METALAKE","type":"METALAKE",
   "privileges":[{"name":"MANAGE_USERS","condition":"ALLOW"}]}]}
EOF
)"
expect "B: $USER_WANGWU denied createRole (no stale CREATE_ROLE from old role)" 403

# ---- J. cross-metalake isolation -------------------------------------------

section "J. Cross-metalake isolation — role/owner in M1 doesn't grant access in M2"
api "$INSTANCE_A" "$ADMIN_USER" POST '/api/metalakes' \
  "{\"name\":\"$METALAKE2\",\"comment\":\"isolation test\",\"properties\":{}}"
expect "A: create second metalake $METALAKE2" 200

api "$INSTANCE_A" "$ADMIN_USER" POST "/api/metalakes/$METALAKE2/users" \
  "{\"name\":\"$USER_WANGWU\"}"
expect "A: add $USER_WANGWU to $METALAKE2 (with no roles)" 200

# wangwu has CREATE_TAG in M1 (via $ROLE_NAME), but should have nothing in M2.
api "$INSTANCE_B" "$USER_WANGWU" POST "/api/metalakes/$METALAKE2/tags" \
  "{\"name\":\"isolation_tag_${SUFFIX}\",\"comment\":\"\",\"properties\":{}}"
expect "B: $USER_WANGWU denied createTag in $METALAKE2 (M1 role doesn't apply)" 403

api "$INSTANCE_B" "$USER_WANGWU" POST "/api/metalakes/$METALAKE2/users" \
  "{\"name\":\"isolation_user_${SUFFIX}\"}"
expect "B: $USER_WANGWU denied addUser in $METALAKE2" 403

# ---- K. DENY condition overrides ALLOW -------------------------------------

section "K. DENY condition overrides ALLOW on a different role (immediate)"
api "$INSTANCE_A" "$USER_LISI" POST "/api/metalakes/$METALAKE/roles" "$(cat <<EOF
{"name":"$ROLE_ALLOW","properties":{},"securableObjects":[
  {"fullName":"$METALAKE","type":"METALAKE",
   "privileges":[{"name":"MANAGE_USERS","condition":"ALLOW"}]}]}
EOF
)"
expect "A: create role $ROLE_ALLOW (MANAGE_USERS ALLOW)" 200

api "$INSTANCE_A" "$USER_LISI" PUT \
  "/api/metalakes/$METALAKE/permissions/users/$USER_WANGWU/grant/" \
  "{\"roleNames\":[\"$ROLE_ALLOW\"]}"
expect "A: grant $ROLE_ALLOW to $USER_WANGWU" 200

api "$INSTANCE_B" "$USER_WANGWU" POST "/api/metalakes/$METALAKE/users" \
  "{\"name\":\"allow_only_${SUFFIX}\"}"
expect "B: $USER_WANGWU addUser works (only ALLOW role granted)" 200

# Now add a DENY role for the same privilege — DENY should override.
api "$INSTANCE_A" "$USER_LISI" POST "/api/metalakes/$METALAKE/roles" "$(cat <<EOF
{"name":"$ROLE_DENY","properties":{},"securableObjects":[
  {"fullName":"$METALAKE","type":"METALAKE",
   "privileges":[{"name":"MANAGE_USERS","condition":"DENY"}]}]}
EOF
)"
expect "A: create role $ROLE_DENY (MANAGE_USERS DENY)" 200

api "$INSTANCE_A" "$USER_LISI" PUT \
  "/api/metalakes/$METALAKE/permissions/users/$USER_WANGWU/grant/" \
  "{\"roleNames\":[\"$ROLE_DENY\"]}"
expect "A: grant $ROLE_DENY to $USER_WANGWU (now has ALLOW + DENY)" 200

api "$INSTANCE_B" "$USER_WANGWU" POST "/api/metalakes/$METALAKE/users" \
  "{\"name\":\"deny_wins_${SUFFIX}\"}"
expect "B: $USER_WANGWU denied addUser (DENY overrides ALLOW)" 403

# Remove DENY — ALLOW should resume working.
api "$INSTANCE_A" "$USER_LISI" PUT \
  "/api/metalakes/$METALAKE/permissions/users/$USER_WANGWU/revoke/" \
  "{\"roleNames\":[\"$ROLE_DENY\"]}"
expect "A: revoke $ROLE_DENY from $USER_WANGWU" 200

api "$INSTANCE_B" "$USER_WANGWU" POST "/api/metalakes/$METALAKE/users" \
  "{\"name\":\"deny_removed_${SUFFIX}\"}"
expect "B: $USER_WANGWU addUser works again after DENY revoked" 200

# ---- L. group lifecycle (metadata-layer read-back) -------------------------

section "L. Group lifecycle — create/grant/delete visible on B"
# NOTE: simple authenticator does NOT carry group membership; we only verify
# that group-role bindings are visible across instances at the metadata layer.
# Runtime enforcement of group-derived privileges needs an IdP that pushes
# UserPrincipal.getGroups(), which is out of scope here.

api "$INSTANCE_A" "$USER_LISI" POST "/api/metalakes/$METALAKE/groups" \
  "{\"name\":\"$GROUP_NAME\"}"
expect "A: create group $GROUP_NAME" 200

api "$INSTANCE_B" "$ADMIN_USER" GET "/api/metalakes/$METALAKE/groups/$GROUP_NAME"
expect "B: GET group $GROUP_NAME visible immediately" 200 "\"name\"[^}]*\"$GROUP_NAME\""

api "$INSTANCE_A" "$USER_LISI" PUT \
  "/api/metalakes/$METALAKE/permissions/groups/$GROUP_NAME/grant/" \
  "{\"roleNames\":[\"$ROLE_ALLOW\"]}"
expect "A: grant $ROLE_ALLOW to group $GROUP_NAME" 200

api "$INSTANCE_B" "$ADMIN_USER" GET "/api/metalakes/$METALAKE/groups/$GROUP_NAME"
expect "B: GET group shows $ROLE_ALLOW in roles" 200 "\"roles\"[^]]*\"$ROLE_ALLOW\""

api "$INSTANCE_A" "$USER_LISI" PUT \
  "/api/metalakes/$METALAKE/permissions/groups/$GROUP_NAME/revoke" \
  "{\"roleNames\":[\"$ROLE_ALLOW\"]}"
expect "A: revoke $ROLE_ALLOW from group $GROUP_NAME" 200

api "$INSTANCE_A" "$USER_LISI" DELETE "/api/metalakes/$METALAKE/groups/$GROUP_NAME"
expect "A: delete group $GROUP_NAME" 200

api "$INSTANCE_B" "$ADMIN_USER" GET "/api/metalakes/$METALAKE/groups/$GROUP_NAME"
expect "B: GET group returns 404 after deletion" 404

# ---- O. multi-role partial revoke ------------------------------------------

section "O. Multi-role partial revoke — only revoked role's privileges go away"
# After K, wangwu holds $ROLE_ALLOW (MANAGE_USERS ALLOW) — and also still holds
# the re-created $ROLE_NAME from Phase I (which grants CREATE_TAG). For the
# partial-revoke check to be meaningful, $ROLE_TAG_ONLY must be wangwu's *only*
# source of CREATE_TAG; otherwise revoking it leaves the privilege intact via
# the other role and the test fails for a setup reason, not a propagation bug.
# Revoke $ROLE_NAME first so wangwu's pre-test state is just [$ROLE_ALLOW].
api "$INSTANCE_A" "$USER_LISI" PUT \
  "/api/metalakes/$METALAKE/permissions/users/$USER_WANGWU/revoke/" \
  "{\"roleNames\":[\"$ROLE_NAME\"]}"
expect "A: clean up — revoke $ROLE_NAME from $USER_WANGWU (isolates CREATE_TAG to one role)" 200

ROLE_TAG_ONLY="role_tag_${SUFFIX}"
api "$INSTANCE_A" "$USER_LISI" POST "/api/metalakes/$METALAKE/roles" "$(cat <<EOF
{"name":"$ROLE_TAG_ONLY","properties":{},"securableObjects":[
  {"fullName":"$METALAKE","type":"METALAKE",
   "privileges":[{"name":"CREATE_TAG","condition":"ALLOW"}]}]}
EOF
)"
expect "A: create role $ROLE_TAG_ONLY (CREATE_TAG only)" 200

api "$INSTANCE_A" "$USER_LISI" PUT \
  "/api/metalakes/$METALAKE/permissions/users/$USER_WANGWU/grant/" \
  "{\"roleNames\":[\"$ROLE_TAG_ONLY\"]}"
expect "A: grant $ROLE_TAG_ONLY to $USER_WANGWU (now holds 2 roles)" 200

# Sanity: both privileges work.
api "$INSTANCE_B" "$USER_WANGWU" POST "/api/metalakes/$METALAKE/users" \
  "{\"name\":\"o_both_user_${SUFFIX}\"}"
expect "B: addUser works (MANAGE_USERS from $ROLE_ALLOW)" 200
api "$INSTANCE_B" "$USER_WANGWU" POST "/api/metalakes/$METALAKE/tags" \
  "{\"name\":\"o_both_tag_${SUFFIX}\",\"comment\":\"\",\"properties\":{}}"
expect "B: createTag works (CREATE_TAG from $ROLE_TAG_ONLY)" 200

# Revoke only the tag role.
api "$INSTANCE_A" "$USER_LISI" PUT \
  "/api/metalakes/$METALAKE/permissions/users/$USER_WANGWU/revoke/" \
  "{\"roleNames\":[\"$ROLE_TAG_ONLY\"]}"
expect "A: revoke $ROLE_TAG_ONLY from $USER_WANGWU ($ROLE_ALLOW kept)" 200

api "$INSTANCE_B" "$USER_WANGWU" POST "/api/metalakes/$METALAKE/users" \
  "{\"name\":\"o_after_user_${SUFFIX}\"}"
expect "B: addUser still works after partial revoke ($ROLE_ALLOW intact)" 200

api "$INSTANCE_B" "$USER_WANGWU" POST "/api/metalakes/$METALAKE/tags" \
  "{\"name\":\"o_after_tag_${SUFFIX}\",\"comment\":\"\",\"properties\":{}}"
expect "B: createTag denied after $ROLE_TAG_ONLY revoked" 403

# ---- P. owner-per-object: TAG owner change with warm cache on B ------------

section "P. Owner change on TAG with warm cache on B (eventual, ~${POLL_WAIT_SECS}s)"
# Mirrors Phase B' but for a non-METALAKE object — confirms ownerRelCache
# invalidation works per-id, not just for the metalake row.
TAG_P="tag_p_${SUFFIX}"
api "$INSTANCE_A" "$USER_LISI" POST "/api/metalakes/$METALAKE/tags" \
  "{\"name\":\"$TAG_P\",\"comment\":\"phase P\",\"properties\":{}}"
expect "A: create tag $TAG_P (initial owner = creator $USER_LISI)" 200

api "$INSTANCE_A" "$USER_LISI" PUT "/api/metalakes/$METALAKE/owners/TAG/$TAG_P" \
  "{\"name\":\"$USER_WANGWU\",\"type\":\"USER\"}"
expect "A: setOwner($TAG_P) = $USER_WANGWU" 200

echo "  ...sleeping ${POLL_WAIT_SECS}s so poller propagates to B"
sleep "$POLL_WAIT_SECS"

# Warm B's ownerRelCache for $TAG_P: wangwu does an owner-gated op on B that
# resolves $TAG_P's owner = wangwu into cache.
api "$INSTANCE_B" "$USER_WANGWU" PUT "/api/metalakes/$METALAKE/owners/TAG/$TAG_P" \
  "{\"name\":\"$USER_WANGWU\",\"type\":\"USER\"}"
expect "B: $USER_WANGWU re-asserts $TAG_P ownership (warms B's cache)" 200

# Change owner on A. lisi can do this as metalake owner.
api "$INSTANCE_A" "$USER_LISI" PUT "/api/metalakes/$METALAKE/owners/TAG/$TAG_P" \
  "{\"name\":\"$USER_ZHANGSAN\",\"type\":\"USER\"}"
expect "A: setOwner($TAG_P) = $USER_ZHANGSAN (was $USER_WANGWU on B's cache)" 200

echo "  ...sleeping ${POLL_WAIT_SECS}s for poller to invalidate B's warm cache"
sleep "$POLL_WAIT_SECS"

# wangwu's cached owner-of-T_P is stale. Setting owner should fail without the
# metalake-owner fallback — but wangwu is NOT metalake owner, so 403.
api "$INSTANCE_B" "$USER_WANGWU" PUT "/api/metalakes/$METALAKE/owners/TAG/$TAG_P" \
  "{\"name\":\"$USER_WANGWU\",\"type\":\"USER\"}"
expect "B: $USER_WANGWU (former tag owner, cached) now rejected on $TAG_P" 403

api "$INSTANCE_B" "$USER_ZHANGSAN" PUT "/api/metalakes/$METALAKE/owners/TAG/$TAG_P" \
  "{\"name\":\"$USER_ZHANGSAN\",\"type\":\"USER\"}"
expect "B: $USER_ZHANGSAN (new tag owner) accepted on $TAG_P" 200

# Metalake-level owner cache must NOT have been invalidated by the tag change.
# lisi (still metalake owner) should still be able to do metalake-scoped ops.
api "$INSTANCE_B" "$USER_LISI" POST "/api/metalakes/$METALAKE/tags" \
  "{\"name\":\"p_sanity_tag_${SUFFIX}\",\"comment\":\"\",\"properties\":{}}"
expect "B: metalake owner $USER_LISI still has METALAKE-scope privileges (cache surgical)" 200

# ---- Q. several setOwner calls within one poll window — convergence --------

section "Q. Burst of setOwner calls on A — B converges to the final state"
# Sequence (no inter-step wait so the poller batches them in one cycle):
#   lisi → wangwu → zhangsan → lisi
api "$INSTANCE_A" "$USER_LISI" PUT "/api/metalakes/$METALAKE/owners/METALAKE/$METALAKE" \
  "{\"name\":\"$USER_WANGWU\",\"type\":\"USER\"}"
expect "A: setOwner(M) = wangwu (intermediate)" 200
api "$INSTANCE_A" "$USER_WANGWU" PUT "/api/metalakes/$METALAKE/owners/METALAKE/$METALAKE" \
  "{\"name\":\"$USER_ZHANGSAN\",\"type\":\"USER\"}"
expect "A: setOwner(M) = zhangsan (intermediate)" 200
api "$INSTANCE_A" "$USER_ZHANGSAN" PUT "/api/metalakes/$METALAKE/owners/METALAKE/$METALAKE" \
  "{\"name\":\"$USER_LISI\",\"type\":\"USER\"}"
expect "A: setOwner(M) = lisi (final)" 200

echo "  ...sleeping ${POLL_WAIT_SECS}s for poller to process the burst"
sleep "$POLL_WAIT_SECS"

# Final state is lisi. Confirm no intermediate state leaked.
api "$INSTANCE_B" "$USER_LISI" PUT "/api/metalakes/$METALAKE/owners/METALAKE/$METALAKE" \
  "{\"name\":\"$USER_LISI\",\"type\":\"USER\"}"
expect "B: $USER_LISI (final owner of burst) re-asserts" 200

api "$INSTANCE_B" "$USER_WANGWU" PUT "/api/metalakes/$METALAKE/owners/METALAKE/$METALAKE" \
  "{\"name\":\"$USER_WANGWU\",\"type\":\"USER\"}"
expect "B: $USER_WANGWU (intermediate owner during burst) rejected" 403

api "$INSTANCE_B" "$USER_ZHANGSAN" PUT "/api/metalakes/$METALAKE/owners/METALAKE/$METALAKE" \
  "{\"name\":\"$USER_ZHANGSAN\",\"type\":\"USER\"}"
expect "B: $USER_ZHANGSAN (intermediate owner during burst) rejected" 403

# ---- R. idempotent re-grant of an existing role assignment -----------------

section "R. Re-granting an already-held role is idempotent"
# wangwu still holds $ROLE_ALLOW from K (we revoked the other role in O).
api "$INSTANCE_A" "$USER_LISI" PUT \
  "/api/metalakes/$METALAKE/permissions/users/$USER_WANGWU/grant/" \
  "{\"roleNames\":[\"$ROLE_ALLOW\"]}"
expect "A: re-grant $ROLE_ALLOW to $USER_WANGWU (already held)" 200

api "$INSTANCE_B" "$USER_WANGWU" POST "/api/metalakes/$METALAKE/users" \
  "{\"name\":\"r_user_${SUFFIX}\"}"
expect "B: $USER_WANGWU addUser still works after idempotent re-grant" 200

# Single revoke should fully strip the privilege (no duplicate g-row leftover).
api "$INSTANCE_A" "$USER_LISI" PUT \
  "/api/metalakes/$METALAKE/permissions/users/$USER_WANGWU/revoke/" \
  "{\"roleNames\":[\"$ROLE_ALLOW\"]}"
expect "A: revoke $ROLE_ALLOW from $USER_WANGWU" 200

api "$INSTANCE_B" "$USER_WANGWU" POST "/api/metalakes/$METALAKE/users" \
  "{\"name\":\"r_after_revoke_${SUFFIX}\"}"
expect "B: $USER_WANGWU addUser denied after single revoke (no duplicate binding leftover)" 403

# ---- N. setOwner propagation for non-METALAKE entities (TAG) ---------------

section "N. setOwner on TAG — same propagation guarantee as METALAKE (eventual, ~${POLL_WAIT_SECS}s)"
# The schema/table case in the original ask works identically — substitute SCHEMA/TABLE
# for TAG once a catalog is configured.
api "$INSTANCE_A" "$USER_LISI" POST "/api/metalakes/$METALAKE/tags" \
  "{\"name\":\"$TAG_NAME\",\"comment\":\"phase N tag\",\"properties\":{}}"
expect "A: $USER_LISI creates tag $TAG_NAME (lisi = metalake owner)" 200

# Transfer tag ownership to wangwu on A. lisi is metalake owner so this is allowed.
api "$INSTANCE_A" "$USER_LISI" PUT \
  "/api/metalakes/$METALAKE/owners/TAG/$TAG_NAME" \
  "{\"name\":\"$USER_WANGWU\",\"type\":\"USER\"}"
expect "A: setOwner(TAG $TAG_NAME) = $USER_WANGWU" 200

echo "  ...sleeping ${POLL_WAIT_SECS}s for the change poller"
sleep "$POLL_WAIT_SECS"

# GET owner on a TAG requires LOAD_TAG (METALAKE::OWNER || TAG::OWNER || ANY_APPLY_TAG),
# so the GET is done as $USER_LISI (metalake owner) rather than admin.
api "$INSTANCE_B" "$USER_LISI" GET "/api/metalakes/$METALAKE/owners/TAG/$TAG_NAME"
expect "B: GET owner(TAG $TAG_NAME) = $USER_WANGWU" 200 "\"name\"[^}]*\"$USER_WANGWU\""

# Enforcement: wangwu (now TAG owner) can DELETE the tag on B (TAG::OWNER required).
api "$INSTANCE_B" "$USER_WANGWU" DELETE "/api/metalakes/$METALAKE/tags/$TAG_NAME"
expect "B: $USER_WANGWU (tag owner) can DELETE the tag on B" 200

# ---- summary ---------------------------------------------------------------

section "Summary"
printf "Passed: %d\n" "$PASS"
printf "Failed: %d\n" "$FAIL"
if [[ $FAIL -gt 0 ]]; then
  printf "\nFailed assertions:\n"
  for t in "${FAILED_TESTS[@]}"; do printf '  - %s\n' "$t"; done
  exit "$FAIL"
fi
echo "All consistency assertions passed."
