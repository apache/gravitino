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
#      → A sets owner=alice; on B alice can re-setOwner, jack gets 403.
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
#   S. ownerRelCache handles GROUP-type owners, not just USER-type.
#      → setOwner(TAG, type=GROUP) on A; B reads back the GROUP owner after poll.
#
#   T. metadataIdCache is invalidated when an entity is deleted and re-created
#      under the same name (new entity ID in the DB).
#      → set owner of tag T to bob; delete T on A; re-create T on A (new ID,
#        owner = alice); B must NOT return bob as the owner of the new T.
#
#   U. User role-list metadata read-back (not just enforcement).
#      → grant/revoke a role on A; GET /users/{user} on B immediately shows the
#        updated role list (userRoleCache is version-validated per request).
#
#   V. Role privilege metadata read-back (not just enforcement).
#      → swap privileges on a role on A; GET /roles/{role} on B shows the new
#        set and no longer shows the removed privilege.
#
#   W. Bidirectional propagation: changes written to B are immediately visible
#      on A (same per-request version-validation, reverse direction).
#      → grant on B; addUser on A succeeds immediately; revoke on B; denied on A.
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
# therefore does ownership chains as the *current* owner (admin → jack → alice)
# and uses a separate user (bob) for role-based tests so role grants and
# ownership grants don't interfere.
# -----------------------------------------------------------------------------
#
# Usage:
#   bash dev/ci/test_multi_instance_consistency.sh
#   INSTANCE_A=http://host1:8090 INSTANCE_B=http://host2:8090 bash dev/ci/test_multi_instance_consistency.sh
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
USER_JACK="jack_${SUFFIX}"
USER_ALICE="alice_${SUFFIX}"
USER_BOB="bob_${SUFFIX}"   # separate test user, never an owner
USER_DELME="userdel_${SUFFIX}"   # for delete-user / re-create tests (Phases G, H)
ROLE_NAME="role_${SUFFIX}"
ROLE_FRESH="role_g_${SUFFIX}"     # for Phase G (delete user test)
ROLE_ALLOW="role_allow_${SUFFIX}" # for Phase K (DENY override)
ROLE_DENY="role_deny_${SUFFIX}"   # for Phase K
GROUP_NAME="group_${SUFFIX}"      # for Phase L
TAG_NAME="tag_${SUFFIX}"          # for Phase N
GROUP_S="grp_s_${SUFFIX}"         # for Phase S (GROUP-type owner)
TAG_S="tag_s_${SUFFIX}"           # for Phase S
TAG_T="tag_t_${SUFFIX}"           # for Phase T (metadataIdCache re-create)
ROLE_U="role_u_${SUFFIX}"         # for Phase U (user role read-back)
ROLE_V="role_v_${SUFFIX}"         # for Phase V (role privilege read-back)
ROLE_W="role_w_${SUFFIX}"         # for Phase W (bidirectional B→A)

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
    HTTP_CODE=$(curl -sS --connect-timeout 10 --max-time 30 \
      -o "$tmp" -w '%{http_code}' \
      -H "Authorization: $(auth_header "$user")" \
      -H 'Content-Type: application/json' \
      -X "$method" --data "$body" "${base}${path}" || echo 000)
  else
    HTTP_CODE=$(curl -sS --connect-timeout 10 --max-time 30 \
      -o "$tmp" -w '%{http_code}' \
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

# expect_absent <desc> <expected_http_code> <body_grep_pattern>
# Passes when the HTTP code matches AND the pattern is NOT found in the body.
expect_absent() {
  local desc="$1" want="$2" pattern="$3"
  local body_snip
  body_snip=$(printf '%s' "$RESPONSE_BODY" | tr '\n' ' ' | cut -c1-180)
  if [[ "$HTTP_CODE" != "$want" ]]; then
    fail "$desc — expected HTTP $want, got $HTTP_CODE. Body: $body_snip"
    return
  fi
  if printf '%s' "$RESPONSE_BODY" | grep -q -E "$pattern"; then
    fail "$desc — HTTP $want OK but body unexpectedly matches /$pattern/. Body: $body_snip"
    return
  fi
  pass "$desc"
}

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
if [[ "$HTTP_CODE" == "200" ]]; then
  pass "admin is a service admin on A (can create metalake)"
  api "$INSTANCE_A" "$ADMIN_USER" DELETE "/api/metalakes/$SCRATCH?force=true"
elif [[ "$HTTP_CODE" == "403" ]]; then
  fail "admin lacks service-admin rights on A — add '$ADMIN_USER' to gravitino.authorization.serviceAdmins on both instances and restart"
else
  fail "preflight metalake-create returned unexpected HTTP $HTTP_CODE (expected 200 or 403) — instance A may not be ready or the entity store is not configured"
fi

if [[ $FAIL -gt 0 ]]; then
  echo
  echo "Preflight failed — aborting. Fix the above before re-running."
  exit 99
fi

# ---- cleanup hook ----------------------------------------------------------

cleanup_metalake() {
  local ml="$1"
  for who in "$USER_ALICE" "$USER_JACK" "$ADMIN_USER"; do
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
  # The metalake may be owned by any of (admin / jack / alice) depending on
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
  "{\"name\":\"$USER_JACK\"}"
expect "add user $USER_JACK" 200

api "$INSTANCE_A" "$ADMIN_USER" POST "/api/metalakes/$METALAKE/users" \
  "{\"name\":\"$USER_ALICE\"}"
expect "add user $USER_ALICE" 200

api "$INSTANCE_A" "$ADMIN_USER" POST "/api/metalakes/$METALAKE/users" \
  "{\"name\":\"$USER_BOB\"}"
expect "add user $USER_BOB (used for role tests, never an owner)" 200

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
# admin is current owner → admin transfers to jack
api "$INSTANCE_A" "$ADMIN_USER" PUT "/api/metalakes/$METALAKE/owners/METALAKE/$METALAKE" \
  "{\"name\":\"$USER_JACK\",\"type\":\"USER\"}"
expect "A: admin (owner) → setOwner = $USER_JACK" 200

echo "  ...sleeping ${POLL_WAIT_SECS}s for the change poller on B"
sleep "$POLL_WAIT_SECS"

api "$INSTANCE_B" "$ADMIN_USER" GET "/api/metalakes/$METALAKE/owners/METALAKE/$METALAKE"
expect "B: GET owner now reports $USER_JACK" 200 "\"name\"[^}]*\"$USER_JACK\""

# jack is now owner → jack transfers to alice
api "$INSTANCE_A" "$USER_JACK" PUT "/api/metalakes/$METALAKE/owners/METALAKE/$METALAKE" \
  "{\"name\":\"$USER_ALICE\",\"type\":\"USER\"}"
expect "A: $USER_JACK (owner) → setOwner = $USER_ALICE" 200

echo "  ...sleeping ${POLL_WAIT_SECS}s for the change poller on B"
sleep "$POLL_WAIT_SECS"

api "$INSTANCE_B" "$ADMIN_USER" GET "/api/metalakes/$METALAKE/owners/METALAKE/$METALAKE"
expect "B: GET owner now reports $USER_ALICE" 200 "\"name\"[^}]*\"$USER_ALICE\""

# ---- B. enforcement on B reflects the owner change -------------------------

section "B. Authz enforcement on B reflects the new owner (no extra wait)"
# alice is the current owner — re-setting (alice → alice) is a no-op write that requires
# the caller to be the current owner. If B's ownerRelCache is invalidated, this works.
# Note: this is also B's *first* isOwner check for $METALAKE, so its ownerRelCache
# entry is being populated cold from the DB here.
api "$INSTANCE_B" "$USER_ALICE" PUT "/api/metalakes/$METALAKE/owners/METALAKE/$METALAKE" \
  "{\"name\":\"$USER_ALICE\",\"type\":\"USER\"}"
expect "B: $USER_ALICE (current owner) re-asserts ownership" 200

# jack was owner two transfers ago but is not anymore.
api "$INSTANCE_B" "$USER_JACK" PUT "/api/metalakes/$METALAKE/owners/METALAKE/$METALAKE" \
  "{\"name\":\"$USER_JACK\",\"type\":\"USER\"}"
expect "B: $USER_JACK (former owner) is rejected with 403" 403

# ---- B'. WARM-cache invalidation: critical scenario ------------------------
#
# Phases A/B above only exercise *cold* cache fills on B: when B's first
# isOwner check happens, the new owner is already in the DB, so the cache
# is correctly populated. They do NOT prove that an already-cached entry on
# B (with the stale owner) gets invalidated when A changes the owner.
#
# This phase forces that scenario:
#   1. B's ownerRelCache currently holds alice (warmed by Phase B above).
#   2. Change owner on A back to jack.
#   3. Wait one poll cycle.
#   4. On B, do an isOwner-gated operation as jack — must succeed if and
#      only if the poller invalidated B's alice entry. As alice — must fail.
#
section "B'. Warm-cache invalidation on owner change (eventual, ~${POLL_WAIT_SECS}s)"
api "$INSTANCE_A" "$USER_ALICE" PUT "/api/metalakes/$METALAKE/owners/METALAKE/$METALAKE" \
  "{\"name\":\"$USER_JACK\",\"type\":\"USER\"}"
expect "A: $USER_ALICE (owner) transfers owner back to $USER_JACK" 200

echo "  ...sleeping ${POLL_WAIT_SECS}s for poller to invalidate B's warm cache"
sleep "$POLL_WAIT_SECS"

# B's cache previously held alice. If the poller invalidated correctly, the
# next isOwner check reloads jack from DB.
api "$INSTANCE_B" "$USER_JACK" PUT "/api/metalakes/$METALAKE/owners/METALAKE/$METALAKE" \
  "{\"name\":\"$USER_JACK\",\"type\":\"USER\"}"
expect "B: $USER_JACK (new owner) re-asserts ownership — proves B's cache invalidated" 200

api "$INSTANCE_B" "$USER_ALICE" PUT "/api/metalakes/$METALAKE/owners/METALAKE/$METALAKE" \
  "{\"name\":\"$USER_ALICE\",\"type\":\"USER\"}"
expect "B: $USER_ALICE (former owner, was cached) now rejected" 403

# Transfer back to alice for the remainder of the test (which assumes alice is owner).
api "$INSTANCE_A" "$USER_JACK" PUT "/api/metalakes/$METALAKE/owners/METALAKE/$METALAKE" \
  "{\"name\":\"$USER_ALICE\",\"type\":\"USER\"}"
expect "A: $USER_JACK restores owner = $USER_ALICE for subsequent phases" 200
echo "  ...sleeping ${POLL_WAIT_SECS}s so B sees alice-as-owner again before role tests"
sleep "$POLL_WAIT_SECS"

# ---- C. grant role on A → user can act on B IMMEDIATELY --------------------

section "C. Grant role on A — effective on B without poll wait (version-validated)"
# Baseline: bob has no role, no ownership, so addUser is forbidden.
api "$INSTANCE_B" "$USER_BOB" POST "/api/metalakes/$METALAKE/users" \
  "{\"name\":\"baseline_denied_${SUFFIX}\"}"
expect "B: $USER_BOB denied addUser before role grant (baseline)" 403

# As alice (owner): grant the MANAGE_USERS role to bob on A.
api "$INSTANCE_A" "$USER_ALICE" PUT \
  "/api/metalakes/$METALAKE/permissions/users/$USER_BOB/grant/" \
  "{\"roleNames\":[\"$ROLE_NAME\"]}"
expect "A: $USER_ALICE grants $ROLE_NAME to $USER_BOB" 200

# No sleep — userRoleCache is version-validated per request.
api "$INSTANCE_B" "$USER_BOB" POST "/api/metalakes/$METALAKE/users" \
  "{\"name\":\"granted_ok_${SUFFIX}\"}"
expect "B: $USER_BOB addUser succeeds immediately after grant" 200

# ---- D. revoke role on A → user denied on B IMMEDIATELY --------------------

section "D. Revoke role on A — effective on B without poll wait"
api "$INSTANCE_A" "$USER_ALICE" PUT \
  "/api/metalakes/$METALAKE/permissions/users/$USER_BOB/revoke/" \
  "{\"roleNames\":[\"$ROLE_NAME\"]}"
expect "A: $USER_ALICE revokes $ROLE_NAME from $USER_BOB" 200

api "$INSTANCE_B" "$USER_BOB" POST "/api/metalakes/$METALAKE/users" \
  "{\"name\":\"revoked_denied_${SUFFIX}\"}"
expect "B: $USER_BOB denied addUser immediately after revoke" 403

# ---- E. modify role's privileges on A → effective on B IMMEDIATELY ---------

section "E. Modify role privileges on A — effective on B without poll wait"
# Re-grant the role to bob so the test isolates a *role-privilege* change
# rather than a *role-membership* change.
api "$INSTANCE_A" "$USER_ALICE" PUT \
  "/api/metalakes/$METALAKE/permissions/users/$USER_BOB/grant/" \
  "{\"roleNames\":[\"$ROLE_NAME\"]}"
expect "A: $USER_ALICE re-grants $ROLE_NAME to $USER_BOB for privilege test" 200

# Sanity: bob can still addUser at this point.
api "$INSTANCE_B" "$USER_BOB" POST "/api/metalakes/$METALAKE/users" \
  "{\"name\":\"priv_baseline_${SUFFIX}\"}"
expect "B: $USER_BOB addUser works (role re-granted, sanity)" 200

# Strip MANAGE_USERS from the role itself on A.
api "$INSTANCE_A" "$USER_ALICE" PUT \
  "/api/metalakes/$METALAKE/permissions/roles/$ROLE_NAME/METALAKE/$METALAKE/revoke/" \
  '{"privileges":[{"name":"MANAGE_USERS","condition":"ALLOW"}]}'
expect "A: $USER_ALICE revokes MANAGE_USERS from $ROLE_NAME" 200

# bob still holds the role, but the role no longer grants MANAGE_USERS.
# loadedRoles must be invalidated on B for this to fail.
api "$INSTANCE_B" "$USER_BOB" POST "/api/metalakes/$METALAKE/users" \
  "{\"name\":\"priv_revoked_${SUFFIX}\"}"
expect "B: $USER_BOB denied after role's MANAGE_USERS privilege revoked" 403

# Now add CREATE_ROLE to the same role on A.
api "$INSTANCE_A" "$USER_ALICE" PUT \
  "/api/metalakes/$METALAKE/permissions/roles/$ROLE_NAME/METALAKE/$METALAKE/grant/" \
  '{"privileges":[{"name":"CREATE_ROLE","condition":"ALLOW"}]}'
expect "A: $USER_ALICE grants CREATE_ROLE to $ROLE_NAME" 200

# bob still can't addUser (role doesn't have MANAGE_USERS anymore)
api "$INSTANCE_B" "$USER_BOB" POST "/api/metalakes/$METALAKE/users" \
  "{\"name\":\"still_no_manage_users_${SUFFIX}\"}"
expect "B: $USER_BOB still denied addUser (role has only CREATE_ROLE)" 403

# …but he CAN createRole now, immediately.
api "$INSTANCE_B" "$USER_BOB" POST "/api/metalakes/$METALAKE/roles" "$(cat <<EOF
{
  "name": "bob_role_${SUFFIX}",
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
expect "B: $USER_BOB createRole succeeds immediately after CREATE_ROLE granted on A" 200

# ============================================================================
# Phase F-N: extended coverage — lifecycle operations, isolation, DENY, groups,
# non-METALAKE setOwner.
# State at this point: $METALAKE owner = $USER_ALICE; $USER_BOB holds
# $ROLE_NAME which currently has only CREATE_ROLE (MANAGE_USERS revoked in E).
# ============================================================================

# ---- F. delete role on A → users with that role lose its privileges --------

section "F. Delete role on A — holders lose its privileges on B (immediate)"
# versionCheckAndLoadRoles compares the caller's cached roleIds against the
# rows returned by batchGetRoleUpdatedAt; any roleId absent from the DB
# (i.e., the deleted role) is evicted from loadedRoles. The per-request
# userRoleCache is also revalidated on the next request so the deleted
# role's policies are never applied.

# Sanity: bob currently holds ROLE_NAME which has CREATE_ROLE.
api "$INSTANCE_B" "$USER_BOB" POST "/api/metalakes/$METALAKE/roles" "$(cat <<EOF
{"name":"sanity_role_${SUFFIX}","properties":{},"securableObjects":[
  {"fullName":"$METALAKE","type":"METALAKE",
   "privileges":[{"name":"MANAGE_USERS","condition":"ALLOW"}]}]}
EOF
)"
expect "B: $USER_BOB createRole still works (sanity)" 200

api "$INSTANCE_A" "$USER_ALICE" DELETE "/api/metalakes/$METALAKE/roles/$ROLE_NAME"
expect "A: $USER_ALICE deletes role $ROLE_NAME" 200

api "$INSTANCE_B" "$USER_BOB" POST "/api/metalakes/$METALAKE/roles" "$(cat <<EOF
{"name":"post_delete_${SUFFIX}","properties":{},"securableObjects":[
  {"fullName":"$METALAKE","type":"METALAKE",
   "privileges":[{"name":"MANAGE_USERS","condition":"ALLOW"}]}]}
EOF
)"
expect "B: $USER_BOB createRole now denied (role deleted on A)" 403

# ---- G. delete user (caller) on A → that user's requests rejected on B -----

section "G. Delete user on A — calls from that user are rejected on B immediately"
api "$INSTANCE_A" "$USER_ALICE" POST "/api/metalakes/$METALAKE/users" \
  "{\"name\":\"$USER_DELME\"}"
expect "A: create user $USER_DELME" 200

# Create a fresh role and grant MANAGE_USERS to the new user.
api "$INSTANCE_A" "$USER_ALICE" POST "/api/metalakes/$METALAKE/roles" "$(cat <<EOF
{"name":"$ROLE_FRESH","properties":{},"securableObjects":[
  {"fullName":"$METALAKE","type":"METALAKE",
   "privileges":[{"name":"MANAGE_USERS","condition":"ALLOW"}]}]}
EOF
)"
expect "A: create role $ROLE_FRESH (MANAGE_USERS)" 200

api "$INSTANCE_A" "$USER_ALICE" PUT \
  "/api/metalakes/$METALAKE/permissions/users/$USER_DELME/grant/" \
  "{\"roleNames\":[\"$ROLE_FRESH\"]}"
expect "A: grant $ROLE_FRESH to $USER_DELME" 200

api "$INSTANCE_B" "$USER_DELME" POST "/api/metalakes/$METALAKE/users" \
  "{\"name\":\"delme_baseline_${SUFFIX}\"}"
expect "B: $USER_DELME addUser works before deletion (sanity)" 200

api "$INSTANCE_A" "$USER_ALICE" DELETE "/api/metalakes/$METALAKE/users/$USER_DELME"
expect "A: delete user $USER_DELME" 200

api "$INSTANCE_B" "$USER_DELME" POST "/api/metalakes/$METALAKE/users" \
  "{\"name\":\"delme_after_${SUFFIX}\"}"
expect "B: $USER_DELME addUser denied immediately after user deletion" 403

# ---- H. re-create same-named user → no stale role bindings -----------------

section "H. Re-create same-named user — no stale role inheritance"
api "$INSTANCE_A" "$USER_ALICE" POST "/api/metalakes/$METALAKE/users" \
  "{\"name\":\"$USER_DELME\"}"
expect "A: re-create user $USER_DELME (no roles granted this time)" 200

api "$INSTANCE_B" "$USER_DELME" POST "/api/metalakes/$METALAKE/users" \
  "{\"name\":\"new_delme_${SUFFIX}\"}"
expect "B: re-created $USER_DELME still denied addUser (no stale role leaked from old user)" 403

# ---- I. re-create same-named role → fresh privileges only ------------------

section "I. Re-create same-named role — only fresh privileges apply"
# ROLE_NAME was deleted in F. Re-create it with CREATE_TAG instead of MANAGE_USERS.
api "$INSTANCE_A" "$USER_ALICE" POST "/api/metalakes/$METALAKE/roles" "$(cat <<EOF
{"name":"$ROLE_NAME","properties":{},"securableObjects":[
  {"fullName":"$METALAKE","type":"METALAKE",
   "privileges":[{"name":"CREATE_TAG","condition":"ALLOW"}]}]}
EOF
)"
expect "A: re-create $ROLE_NAME with only CREATE_TAG" 200

api "$INSTANCE_A" "$USER_ALICE" PUT \
  "/api/metalakes/$METALAKE/permissions/users/$USER_BOB/grant/" \
  "{\"roleNames\":[\"$ROLE_NAME\"]}"
expect "A: grant the new $ROLE_NAME to $USER_BOB" 200

# Fresh privilege works.
api "$INSTANCE_B" "$USER_BOB" POST "/api/metalakes/$METALAKE/tags" \
  "{\"name\":\"i_tag_${SUFFIX}\",\"comment\":\"phase I tag\",\"properties\":{}}"
expect "B: $USER_BOB createTag works (fresh CREATE_TAG via re-created role)" 200

# Stale privileges from previous incarnations (MANAGE_USERS, CREATE_ROLE) do NOT leak.
api "$INSTANCE_B" "$USER_BOB" POST "/api/metalakes/$METALAKE/users" \
  "{\"name\":\"stale_check_${SUFFIX}\"}"
expect "B: $USER_BOB denied addUser (no stale MANAGE_USERS from old role)" 403

api "$INSTANCE_B" "$USER_BOB" POST "/api/metalakes/$METALAKE/roles" "$(cat <<EOF
{"name":"stale_role_check_${SUFFIX}","properties":{},"securableObjects":[
  {"fullName":"$METALAKE","type":"METALAKE",
   "privileges":[{"name":"MANAGE_USERS","condition":"ALLOW"}]}]}
EOF
)"
expect "B: $USER_BOB denied createRole (no stale CREATE_ROLE from old role)" 403

# ---- J. cross-metalake isolation -------------------------------------------

section "J. Cross-metalake isolation — role/owner in M1 doesn't grant access in M2"
api "$INSTANCE_A" "$ADMIN_USER" POST '/api/metalakes' \
  "{\"name\":\"$METALAKE2\",\"comment\":\"isolation test\",\"properties\":{}}"
expect "A: create second metalake $METALAKE2" 200

api "$INSTANCE_A" "$ADMIN_USER" POST "/api/metalakes/$METALAKE2/users" \
  "{\"name\":\"$USER_BOB\"}"
expect "A: add $USER_BOB to $METALAKE2 (with no roles)" 200

# bob has CREATE_TAG in M1 (via $ROLE_NAME), but should have nothing in M2.
api "$INSTANCE_B" "$USER_BOB" POST "/api/metalakes/$METALAKE2/tags" \
  "{\"name\":\"isolation_tag_${SUFFIX}\",\"comment\":\"\",\"properties\":{}}"
expect "B: $USER_BOB denied createTag in $METALAKE2 (M1 role doesn't apply)" 403

api "$INSTANCE_B" "$USER_BOB" POST "/api/metalakes/$METALAKE2/users" \
  "{\"name\":\"isolation_user_${SUFFIX}\"}"
expect "B: $USER_BOB denied addUser in $METALAKE2" 403

# ---- K. DENY condition overrides ALLOW -------------------------------------

section "K. DENY condition overrides ALLOW on a different role (immediate)"
api "$INSTANCE_A" "$USER_ALICE" POST "/api/metalakes/$METALAKE/roles" "$(cat <<EOF
{"name":"$ROLE_ALLOW","properties":{},"securableObjects":[
  {"fullName":"$METALAKE","type":"METALAKE",
   "privileges":[{"name":"MANAGE_USERS","condition":"ALLOW"}]}]}
EOF
)"
expect "A: create role $ROLE_ALLOW (MANAGE_USERS ALLOW)" 200

api "$INSTANCE_A" "$USER_ALICE" PUT \
  "/api/metalakes/$METALAKE/permissions/users/$USER_BOB/grant/" \
  "{\"roleNames\":[\"$ROLE_ALLOW\"]}"
expect "A: grant $ROLE_ALLOW to $USER_BOB" 200

api "$INSTANCE_B" "$USER_BOB" POST "/api/metalakes/$METALAKE/users" \
  "{\"name\":\"allow_only_${SUFFIX}\"}"
expect "B: $USER_BOB addUser works (only ALLOW role granted)" 200

# Now add a DENY role for the same privilege — DENY should override.
api "$INSTANCE_A" "$USER_ALICE" POST "/api/metalakes/$METALAKE/roles" "$(cat <<EOF
{"name":"$ROLE_DENY","properties":{},"securableObjects":[
  {"fullName":"$METALAKE","type":"METALAKE",
   "privileges":[{"name":"MANAGE_USERS","condition":"DENY"}]}]}
EOF
)"
expect "A: create role $ROLE_DENY (MANAGE_USERS DENY)" 200

api "$INSTANCE_A" "$USER_ALICE" PUT \
  "/api/metalakes/$METALAKE/permissions/users/$USER_BOB/grant/" \
  "{\"roleNames\":[\"$ROLE_DENY\"]}"
expect "A: grant $ROLE_DENY to $USER_BOB (now has ALLOW + DENY)" 200

api "$INSTANCE_B" "$USER_BOB" POST "/api/metalakes/$METALAKE/users" \
  "{\"name\":\"deny_wins_${SUFFIX}\"}"
expect "B: $USER_BOB denied addUser (DENY overrides ALLOW)" 403

# Remove DENY — ALLOW should resume working.
api "$INSTANCE_A" "$USER_ALICE" PUT \
  "/api/metalakes/$METALAKE/permissions/users/$USER_BOB/revoke/" \
  "{\"roleNames\":[\"$ROLE_DENY\"]}"
expect "A: revoke $ROLE_DENY from $USER_BOB" 200

api "$INSTANCE_B" "$USER_BOB" POST "/api/metalakes/$METALAKE/users" \
  "{\"name\":\"deny_removed_${SUFFIX}\"}"
expect "B: $USER_BOB addUser works again after DENY revoked" 200

# ---- L. group lifecycle (metadata-layer read-back) -------------------------

section "L. Group lifecycle — create/grant/delete visible on B"
# NOTE: simple authenticator does NOT carry group membership; we only verify
# that group-role bindings are visible across instances at the metadata layer.
# Runtime enforcement of group-derived privileges needs an IdP that pushes
# UserPrincipal.getGroups(), which is out of scope here.

api "$INSTANCE_A" "$USER_ALICE" POST "/api/metalakes/$METALAKE/groups" \
  "{\"name\":\"$GROUP_NAME\"}"
expect "A: create group $GROUP_NAME" 200

api "$INSTANCE_B" "$ADMIN_USER" GET "/api/metalakes/$METALAKE/groups/$GROUP_NAME"
expect "B: GET group $GROUP_NAME visible immediately" 200 "\"name\"[^}]*\"$GROUP_NAME\""

api "$INSTANCE_A" "$USER_ALICE" PUT \
  "/api/metalakes/$METALAKE/permissions/groups/$GROUP_NAME/grant/" \
  "{\"roleNames\":[\"$ROLE_ALLOW\"]}"
expect "A: grant $ROLE_ALLOW to group $GROUP_NAME" 200

api "$INSTANCE_B" "$ADMIN_USER" GET "/api/metalakes/$METALAKE/groups/$GROUP_NAME"
expect "B: GET group shows $ROLE_ALLOW in roles" 200 "\"roles\"[^]]*\"$ROLE_ALLOW\""

api "$INSTANCE_A" "$USER_ALICE" PUT \
  "/api/metalakes/$METALAKE/permissions/groups/$GROUP_NAME/revoke" \
  "{\"roleNames\":[\"$ROLE_ALLOW\"]}"
expect "A: revoke $ROLE_ALLOW from group $GROUP_NAME" 200

api "$INSTANCE_A" "$USER_ALICE" DELETE "/api/metalakes/$METALAKE/groups/$GROUP_NAME"
expect "A: delete group $GROUP_NAME" 200

api "$INSTANCE_B" "$ADMIN_USER" GET "/api/metalakes/$METALAKE/groups/$GROUP_NAME"
expect "B: GET group returns 404 after deletion" 404

# ---- N. setOwner propagation for non-METALAKE entities (TAG) ---------------

section "N. setOwner on TAG — same propagation guarantee as METALAKE (eventual, ~${POLL_WAIT_SECS}s)"
# The schema/table case in the original ask works identically — substitute SCHEMA/TABLE
# for TAG once a catalog is configured.
api "$INSTANCE_A" "$USER_ALICE" POST "/api/metalakes/$METALAKE/tags" \
  "{\"name\":\"$TAG_NAME\",\"comment\":\"phase N tag\",\"properties\":{}}"
expect "A: $USER_ALICE creates tag $TAG_NAME (alice = metalake owner)" 200

# Transfer tag ownership to bob on A. alice is metalake owner so this is allowed.
api "$INSTANCE_A" "$USER_ALICE" PUT \
  "/api/metalakes/$METALAKE/owners/TAG/$TAG_NAME" \
  "{\"name\":\"$USER_BOB\",\"type\":\"USER\"}"
expect "A: setOwner(TAG $TAG_NAME) = $USER_BOB" 200

echo "  ...sleeping ${POLL_WAIT_SECS}s for the change poller"
sleep "$POLL_WAIT_SECS"

# GET owner on a TAG requires LOAD_TAG (METALAKE::OWNER || TAG::OWNER || ANY_APPLY_TAG),
# so the GET is done as $USER_ALICE (metalake owner) rather than admin.
api "$INSTANCE_B" "$USER_ALICE" GET "/api/metalakes/$METALAKE/owners/TAG/$TAG_NAME"
expect "B: GET owner(TAG $TAG_NAME) = $USER_BOB" 200 "\"name\"[^}]*\"$USER_BOB\""

# Enforcement: bob (now TAG owner) can DELETE the tag on B (TAG::OWNER required).
api "$INSTANCE_B" "$USER_BOB" DELETE "/api/metalakes/$METALAKE/tags/$TAG_NAME"
expect "B: $USER_BOB (tag owner) can DELETE the tag on B" 200

# ---- O. multi-role partial revoke ------------------------------------------

section "O. Multi-role partial revoke — only revoked role's privileges go away"
# After K, bob holds $ROLE_ALLOW (MANAGE_USERS ALLOW) — and also still holds
# the re-created $ROLE_NAME from Phase I (which grants CREATE_TAG). For the
# partial-revoke check to be meaningful, $ROLE_TAG_ONLY must be bob's *only*
# source of CREATE_TAG; otherwise revoking it leaves the privilege intact via
# the other role and the test fails for a setup reason, not a propagation bug.
# Revoke $ROLE_NAME first so bob's pre-test state is just [$ROLE_ALLOW].
api "$INSTANCE_A" "$USER_ALICE" PUT \
  "/api/metalakes/$METALAKE/permissions/users/$USER_BOB/revoke/" \
  "{\"roleNames\":[\"$ROLE_NAME\"]}"
expect "A: clean up — revoke $ROLE_NAME from $USER_BOB (isolates CREATE_TAG to one role)" 200

ROLE_TAG_ONLY="role_tag_${SUFFIX}"
api "$INSTANCE_A" "$USER_ALICE" POST "/api/metalakes/$METALAKE/roles" "$(cat <<EOF
{"name":"$ROLE_TAG_ONLY","properties":{},"securableObjects":[
  {"fullName":"$METALAKE","type":"METALAKE",
   "privileges":[{"name":"CREATE_TAG","condition":"ALLOW"}]}]}
EOF
)"
expect "A: create role $ROLE_TAG_ONLY (CREATE_TAG only)" 200

api "$INSTANCE_A" "$USER_ALICE" PUT \
  "/api/metalakes/$METALAKE/permissions/users/$USER_BOB/grant/" \
  "{\"roleNames\":[\"$ROLE_TAG_ONLY\"]}"
expect "A: grant $ROLE_TAG_ONLY to $USER_BOB (now holds 2 roles)" 200

# Sanity: both privileges work.
api "$INSTANCE_B" "$USER_BOB" POST "/api/metalakes/$METALAKE/users" \
  "{\"name\":\"o_both_user_${SUFFIX}\"}"
expect "B: addUser works (MANAGE_USERS from $ROLE_ALLOW)" 200
api "$INSTANCE_B" "$USER_BOB" POST "/api/metalakes/$METALAKE/tags" \
  "{\"name\":\"o_both_tag_${SUFFIX}\",\"comment\":\"\",\"properties\":{}}"
expect "B: createTag works (CREATE_TAG from $ROLE_TAG_ONLY)" 200

# Revoke only the tag role.
api "$INSTANCE_A" "$USER_ALICE" PUT \
  "/api/metalakes/$METALAKE/permissions/users/$USER_BOB/revoke/" \
  "{\"roleNames\":[\"$ROLE_TAG_ONLY\"]}"
expect "A: revoke $ROLE_TAG_ONLY from $USER_BOB ($ROLE_ALLOW kept)" 200

api "$INSTANCE_B" "$USER_BOB" POST "/api/metalakes/$METALAKE/users" \
  "{\"name\":\"o_after_user_${SUFFIX}\"}"
expect "B: addUser still works after partial revoke ($ROLE_ALLOW intact)" 200

api "$INSTANCE_B" "$USER_BOB" POST "/api/metalakes/$METALAKE/tags" \
  "{\"name\":\"o_after_tag_${SUFFIX}\",\"comment\":\"\",\"properties\":{}}"
expect "B: createTag denied after $ROLE_TAG_ONLY revoked" 403

# ---- P. owner-per-object: TAG owner change with warm cache on B ------------

section "P. Owner change on TAG with warm cache on B (eventual, ~${POLL_WAIT_SECS}s)"
# Mirrors Phase B' but for a non-METALAKE object — confirms ownerRelCache
# invalidation works per-id, not just for the metalake row.
TAG_P="tag_p_${SUFFIX}"
api "$INSTANCE_A" "$USER_ALICE" POST "/api/metalakes/$METALAKE/tags" \
  "{\"name\":\"$TAG_P\",\"comment\":\"phase P\",\"properties\":{}}"
expect "A: create tag $TAG_P (initial owner = creator $USER_ALICE)" 200

api "$INSTANCE_A" "$USER_ALICE" PUT "/api/metalakes/$METALAKE/owners/TAG/$TAG_P" \
  "{\"name\":\"$USER_BOB\",\"type\":\"USER\"}"
expect "A: setOwner($TAG_P) = $USER_BOB" 200

echo "  ...sleeping ${POLL_WAIT_SECS}s so poller propagates to B"
sleep "$POLL_WAIT_SECS"

# Warm B's ownerRelCache for $TAG_P: bob does an owner-gated op on B that
# resolves $TAG_P's owner = bob into cache.
api "$INSTANCE_B" "$USER_BOB" PUT "/api/metalakes/$METALAKE/owners/TAG/$TAG_P" \
  "{\"name\":\"$USER_BOB\",\"type\":\"USER\"}"
expect "B: $USER_BOB re-asserts $TAG_P ownership (warms B's cache)" 200

# Change owner on A. alice can do this as metalake owner.
api "$INSTANCE_A" "$USER_ALICE" PUT "/api/metalakes/$METALAKE/owners/TAG/$TAG_P" \
  "{\"name\":\"$USER_JACK\",\"type\":\"USER\"}"
expect "A: setOwner($TAG_P) = $USER_JACK (was $USER_BOB on B's cache)" 200

echo "  ...sleeping ${POLL_WAIT_SECS}s for poller to invalidate B's warm cache"
sleep "$POLL_WAIT_SECS"

# bob's cached owner-of-T_P is stale. Setting owner should fail without the
# metalake-owner fallback — but bob is NOT metalake owner, so 403.
api "$INSTANCE_B" "$USER_BOB" PUT "/api/metalakes/$METALAKE/owners/TAG/$TAG_P" \
  "{\"name\":\"$USER_BOB\",\"type\":\"USER\"}"
expect "B: $USER_BOB (former tag owner, cached) now rejected on $TAG_P" 403

api "$INSTANCE_B" "$USER_JACK" PUT "/api/metalakes/$METALAKE/owners/TAG/$TAG_P" \
  "{\"name\":\"$USER_JACK\",\"type\":\"USER\"}"
expect "B: $USER_JACK (new tag owner) accepted on $TAG_P" 200

# Metalake-level owner cache must NOT have been invalidated by the tag change.
# alice (still metalake owner) should still be able to do metalake-scoped ops.
api "$INSTANCE_B" "$USER_ALICE" POST "/api/metalakes/$METALAKE/tags" \
  "{\"name\":\"p_sanity_tag_${SUFFIX}\",\"comment\":\"\",\"properties\":{}}"
expect "B: metalake owner $USER_ALICE still has METALAKE-scope privileges (cache surgical)" 200

# ---- Q. several setOwner calls within one poll window — convergence --------

section "Q. Burst of setOwner calls on A — B converges to the final state"
# Sequence (no inter-step wait so the poller batches them in one cycle):
#   alice → bob → jack → alice
api "$INSTANCE_A" "$USER_ALICE" PUT "/api/metalakes/$METALAKE/owners/METALAKE/$METALAKE" \
  "{\"name\":\"$USER_BOB\",\"type\":\"USER\"}"
expect "A: setOwner(M) = bob (intermediate)" 200
api "$INSTANCE_A" "$USER_BOB" PUT "/api/metalakes/$METALAKE/owners/METALAKE/$METALAKE" \
  "{\"name\":\"$USER_JACK\",\"type\":\"USER\"}"
expect "A: setOwner(M) = jack (intermediate)" 200
api "$INSTANCE_A" "$USER_JACK" PUT "/api/metalakes/$METALAKE/owners/METALAKE/$METALAKE" \
  "{\"name\":\"$USER_ALICE\",\"type\":\"USER\"}"
expect "A: setOwner(M) = alice (final)" 200

echo "  ...sleeping ${POLL_WAIT_SECS}s for poller to process the burst"
sleep "$POLL_WAIT_SECS"

# Final state is alice. Confirm no intermediate state leaked.
api "$INSTANCE_B" "$USER_ALICE" PUT "/api/metalakes/$METALAKE/owners/METALAKE/$METALAKE" \
  "{\"name\":\"$USER_ALICE\",\"type\":\"USER\"}"
expect "B: $USER_ALICE (final owner of burst) re-asserts" 200

api "$INSTANCE_B" "$USER_BOB" PUT "/api/metalakes/$METALAKE/owners/METALAKE/$METALAKE" \
  "{\"name\":\"$USER_BOB\",\"type\":\"USER\"}"
expect "B: $USER_BOB (intermediate owner during burst) rejected" 403

api "$INSTANCE_B" "$USER_JACK" PUT "/api/metalakes/$METALAKE/owners/METALAKE/$METALAKE" \
  "{\"name\":\"$USER_JACK\",\"type\":\"USER\"}"
expect "B: $USER_JACK (intermediate owner during burst) rejected" 403

# ---- R. idempotent re-grant of an existing role assignment -----------------

section "R. Re-granting an already-held role is idempotent"
# bob still holds $ROLE_ALLOW from K (we revoked the other role in O).
api "$INSTANCE_A" "$USER_ALICE" PUT \
  "/api/metalakes/$METALAKE/permissions/users/$USER_BOB/grant/" \
  "{\"roleNames\":[\"$ROLE_ALLOW\"]}"
expect "A: re-grant $ROLE_ALLOW to $USER_BOB (already held)" 200

api "$INSTANCE_B" "$USER_BOB" POST "/api/metalakes/$METALAKE/users" \
  "{\"name\":\"r_user_${SUFFIX}\"}"
expect "B: $USER_BOB addUser still works after idempotent re-grant" 200

# Single revoke should fully strip the privilege (no duplicate g-row leftover).
api "$INSTANCE_A" "$USER_ALICE" PUT \
  "/api/metalakes/$METALAKE/permissions/users/$USER_BOB/revoke/" \
  "{\"roleNames\":[\"$ROLE_ALLOW\"]}"
expect "A: revoke $ROLE_ALLOW from $USER_BOB" 200

api "$INSTANCE_B" "$USER_BOB" POST "/api/metalakes/$METALAKE/users" \
  "{\"name\":\"r_after_revoke_${SUFFIX}\"}"
expect "B: $USER_BOB addUser denied after single revoke (no duplicate binding leftover)" 403

# ---- S. GROUP-type owner: ownerRelCache with GROUP principal ---------------

section "S. GROUP as owner type — tag owned by a group propagates to B (eventual, ~${POLL_WAIT_SECS}s)"
# ownerRelCache stores both USER and GROUP owners. Only USER-type has been
# tested in the phases above. This phase confirms GROUP-type entries are also
# propagated correctly by the JcasbinChangePoller.
# (Simple authenticator cannot enforce group-derived requests, so we verify the
# metadata read-back only — the owner is stored and visible on B as a GROUP.)
api "$INSTANCE_A" "$USER_ALICE" POST "/api/metalakes/$METALAKE/groups" \
  "{\"name\":\"$GROUP_S\"}"
expect "A: create group $GROUP_S for owner test" 200

api "$INSTANCE_A" "$USER_ALICE" POST "/api/metalakes/$METALAKE/tags" \
  "{\"name\":\"$TAG_S\",\"comment\":\"phase S\",\"properties\":{}}"
expect "A: $USER_ALICE creates tag $TAG_S" 200

api "$INSTANCE_A" "$USER_ALICE" PUT "/api/metalakes/$METALAKE/owners/TAG/$TAG_S" \
  "{\"name\":\"$GROUP_S\",\"type\":\"GROUP\"}"
expect "A: setOwner(TAG $TAG_S) = GROUP $GROUP_S" 200

echo "  ...sleeping ${POLL_WAIT_SECS}s for the change poller"
sleep "$POLL_WAIT_SECS"

api "$INSTANCE_B" "$USER_ALICE" GET "/api/metalakes/$METALAKE/owners/TAG/$TAG_S"
expect "B: GET owner(TAG $TAG_S) = GROUP $GROUP_S (type GROUP propagated)" 200 \
  "\"name\"[^}]*\"$GROUP_S\""

# alice is metalake owner and can delete the tag regardless of its object owner.
api "$INSTANCE_A" "$USER_ALICE" DELETE "/api/metalakes/$METALAKE/tags/$TAG_S"
expect "A: $USER_ALICE (metalake owner) deletes tag $TAG_S" 200

api "$INSTANCE_A" "$USER_ALICE" DELETE "/api/metalakes/$METALAKE/groups/$GROUP_S"
expect "A: delete group $GROUP_S" 200

# ---- T. metadataIdCache invalidation: tag delete + same-name re-create -----

section "T. Stale-owner safety on tag delete+re-create (eventual, ~${POLL_WAIT_SECS}s)"
# When a tag is deleted on A and re-created with the same name, B must not
# allow the old owner (bob) to exercise ownership on the new entity.
#
# Two separate paths are exercised:
#  1. GET /owners (OwnerManager.getOwner) does a live DB lookup — bypasses
#     the JcasbinAuthorizer caches — so it always returns the new owner (alice).
#  2. Enforcement (isOwner inside hasSetOwnerPermission) reads metadataIdCache
#     → ownerRelCache. Tags do not write to entity_change_log, so
#     metadataIdCache[name] may still hold the old entity ID. However, the
#     owner_meta soft-delete IS propagated via the owner_meta poll path, so
#     ownerRelCache[old_id] is invalidated → live DB returns no owner for the
#     deleted entity → bob is correctly denied.
api "$INSTANCE_A" "$USER_ALICE" POST "/api/metalakes/$METALAKE/tags" \
  "{\"name\":\"$TAG_T\",\"comment\":\"phase T\",\"properties\":{}}"
expect "A: create tag $TAG_T (owner = alice as creator)" 200

api "$INSTANCE_A" "$USER_ALICE" PUT "/api/metalakes/$METALAKE/owners/TAG/$TAG_T" \
  "{\"name\":\"$USER_BOB\",\"type\":\"USER\"}"
expect "A: setOwner(TAG $TAG_T) = $USER_BOB" 200

echo "  ...sleeping ${POLL_WAIT_SECS}s so B caches bob as owner of $TAG_T"
sleep "$POLL_WAIT_SECS"

api "$INSTANCE_B" "$USER_ALICE" GET "/api/metalakes/$METALAKE/owners/TAG/$TAG_T"
expect "B: GET owner(TAG $TAG_T) = $USER_BOB (pre-delete baseline)" 200 \
  "\"name\"[^}]*\"$USER_BOB\""

# Delete the tag on A — should signal the poller to invalidate B's metadataIdCache.
api "$INSTANCE_A" "$USER_ALICE" DELETE "/api/metalakes/$METALAKE/tags/$TAG_T"
expect "A: $USER_ALICE deletes tag $TAG_T (triggers metadataIdCache invalidation)" 200

# Re-create a tag with the same name on A — new entity ID in the DB, owner = alice.
api "$INSTANCE_A" "$USER_ALICE" POST "/api/metalakes/$METALAKE/tags" \
  "{\"name\":\"$TAG_T\",\"comment\":\"phase T re-create\",\"properties\":{}}"
expect "A: re-create tag $TAG_T (new entity ID, new owner = alice)" 200

echo "  ...sleeping ${POLL_WAIT_SECS}s for poller to propagate delete+re-create"
sleep "$POLL_WAIT_SECS"

# B's ownerRelCache had bob for the OLD tag ID. After invalidation the new
# tag (different ID) must be resolved fresh from the DB → owner = alice.
api "$INSTANCE_B" "$USER_ALICE" GET "/api/metalakes/$METALAKE/owners/TAG/$TAG_T"
expect "B: GET owner(re-created TAG $TAG_T) = $USER_ALICE (no stale bob owner)" 200 \
  "\"name\"[^}]*\"$USER_ALICE\""

# Enforcement: bob was owner of the old tag; must be rejected for the new one.
api "$INSTANCE_B" "$USER_BOB" PUT "/api/metalakes/$METALAKE/owners/TAG/$TAG_T" \
  "{\"name\":\"$USER_BOB\",\"type\":\"USER\"}"
expect "B: $USER_BOB (old tag owner, stale) rejected for re-created $TAG_T" 403

api "$INSTANCE_A" "$USER_ALICE" DELETE "/api/metalakes/$METALAKE/tags/$TAG_T"
expect "A: delete re-created tag $TAG_T" 200

# ---- U. user role-list metadata read-back on B -----------------------------

section "U. User role-list read-back on B — GET /users/{user} reflects grant/revoke from A"
# Phases C/D verify enforcement (can the user DO X?). This phase verifies the
# metadata layer: GET /users/{user} on B returns the up-to-date role list.
# userRoleCache is per-request version-validated so no poll wait is needed.
api "$INSTANCE_A" "$USER_ALICE" POST "/api/metalakes/$METALAKE/roles" "$(cat <<EOF
{"name":"$ROLE_U","properties":{},"securableObjects":[
  {"fullName":"$METALAKE","type":"METALAKE",
   "privileges":[{"name":"MANAGE_USERS","condition":"ALLOW"}]}]}
EOF
)"
expect "A: create role $ROLE_U" 200

api "$INSTANCE_A" "$USER_ALICE" PUT \
  "/api/metalakes/$METALAKE/permissions/users/$USER_BOB/grant/" \
  "{\"roleNames\":[\"$ROLE_U\"]}"
expect "A: grant $ROLE_U to $USER_BOB" 200

api "$INSTANCE_B" "$USER_ALICE" GET "/api/metalakes/$METALAKE/users/$USER_BOB"
expect "B: GET user $USER_BOB — $ROLE_U appears in roles list after grant" 200 \
  "\"$ROLE_U\""

api "$INSTANCE_A" "$USER_ALICE" PUT \
  "/api/metalakes/$METALAKE/permissions/users/$USER_BOB/revoke/" \
  "{\"roleNames\":[\"$ROLE_U\"]}"
expect "A: revoke $ROLE_U from $USER_BOB" 200

api "$INSTANCE_B" "$USER_ALICE" GET "/api/metalakes/$METALAKE/users/$USER_BOB"
expect_absent "B: GET user $USER_BOB — $ROLE_U absent from roles list after revoke" 200 \
  "\"$ROLE_U\""

api "$INSTANCE_A" "$USER_ALICE" DELETE "/api/metalakes/$METALAKE/roles/$ROLE_U"
expect "A: delete role $ROLE_U" 200

# ---- V. role privilege metadata read-back on B -----------------------------

section "V. Role privilege read-back on B — GET /roles/{role} reflects privilege changes from A"
# Complements Phase E (enforcement) with a metadata-layer check: after the
# privilege set on a role changes on A, GET /roles/{role} on B must show the
# updated list. loadedRoles is per-request version-validated, so this is immediate.
api "$INSTANCE_A" "$USER_ALICE" POST "/api/metalakes/$METALAKE/roles" "$(cat <<EOF
{"name":"$ROLE_V","properties":{},"securableObjects":[
  {"fullName":"$METALAKE","type":"METALAKE",
   "privileges":[{"name":"MANAGE_USERS","condition":"ALLOW"}]}]}
EOF
)"
expect "A: create role $ROLE_V with MANAGE_USERS" 200

api "$INSTANCE_B" "$USER_ALICE" GET "/api/metalakes/$METALAKE/roles/$ROLE_V"
expect "B: GET role $ROLE_V shows manage_users (initial)" 200 '"manage_users"'

# Swap privilege set on A: remove MANAGE_USERS, add CREATE_TAG.
api "$INSTANCE_A" "$USER_ALICE" PUT \
  "/api/metalakes/$METALAKE/permissions/roles/$ROLE_V/METALAKE/$METALAKE/revoke/" \
  '{"privileges":[{"name":"MANAGE_USERS","condition":"ALLOW"}]}'
expect "A: revoke MANAGE_USERS from $ROLE_V" 200

api "$INSTANCE_A" "$USER_ALICE" PUT \
  "/api/metalakes/$METALAKE/permissions/roles/$ROLE_V/METALAKE/$METALAKE/grant/" \
  '{"privileges":[{"name":"CREATE_TAG","condition":"ALLOW"}]}'
expect "A: grant CREATE_TAG to $ROLE_V" 200

api "$INSTANCE_B" "$USER_ALICE" GET "/api/metalakes/$METALAKE/roles/$ROLE_V"
expect "B: GET role $ROLE_V now shows create_tag" 200 '"create_tag"'
expect_absent "B: GET role $ROLE_V no longer shows manage_users" 200 '"manage_users"'

api "$INSTANCE_A" "$USER_ALICE" DELETE "/api/metalakes/$METALAKE/roles/$ROLE_V"
expect "A: delete role $ROLE_V" 200

# ---- W. bidirectional propagation: change on B observed on A ---------------

section "W. Bidirectional — grant on B is immediately effective on A"
# All prior phases propagate A→B. This phase confirms the reverse direction:
# a role grant written to B propagates to A without a poll wait, because the
# shared MySQL is written directly and A's per-request version check reloads it.
api "$INSTANCE_B" "$USER_ALICE" POST "/api/metalakes/$METALAKE/roles" "$(cat <<EOF
{"name":"$ROLE_W","properties":{},"securableObjects":[
  {"fullName":"$METALAKE","type":"METALAKE",
   "privileges":[{"name":"MANAGE_USERS","condition":"ALLOW"}]}]}
EOF
)"
expect "B: $USER_ALICE creates role $ROLE_W on B" 200

api "$INSTANCE_B" "$USER_ALICE" PUT \
  "/api/metalakes/$METALAKE/permissions/users/$USER_BOB/grant/" \
  "{\"roleNames\":[\"$ROLE_W\"]}"
expect "B: $USER_ALICE grants $ROLE_W to $USER_BOB on B" 200

# No poll wait — userRoleCache is version-validated per request.
api "$INSTANCE_A" "$USER_BOB" POST "/api/metalakes/$METALAKE/users" \
  "{\"name\":\"w_bi_${SUFFIX}\"}"
expect "A: $USER_BOB addUser succeeds immediately after grant made on B" 200

api "$INSTANCE_B" "$USER_ALICE" PUT \
  "/api/metalakes/$METALAKE/permissions/users/$USER_BOB/revoke/" \
  "{\"roleNames\":[\"$ROLE_W\"]}"
expect "B: revoke $ROLE_W from $USER_BOB on B" 200

api "$INSTANCE_A" "$USER_BOB" POST "/api/metalakes/$METALAKE/users" \
  "{\"name\":\"w_bi_revoked_${SUFFIX}\"}"
expect "A: $USER_BOB denied addUser immediately after revoke made on B" 403

api "$INSTANCE_B" "$USER_ALICE" DELETE "/api/metalakes/$METALAKE/roles/$ROLE_W"
expect "B: delete role $ROLE_W" 200

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
