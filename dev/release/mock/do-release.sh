#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Mock version of do-release.sh for testing the gravitino-release skill.
#
# Accepts identical flags and env vars as the real do-release.sh but replaces
# all build/publish/git operations with printed simulations. State tracking
# (mark_stage_done / check_stage_guard) runs for real so the skill's
# orchestration logic is fully exercised.
#
# Extra env vars for testing:
#   MOCK_FAIL_STAGE=<stage>   Simulate a failure at the given stage name.
#                             Valid values: tag, build, docs, publish, finalize
#   MOCK_STAGE_DELAY=<secs>   Seconds to sleep per stage (default: 1).

set -euo pipefail

SELF=$(cd "$(dirname "$0")" && pwd)
REAL_SCRIPTS_DIR="$SELF/.."

# ---------------------------------------------------------------------------
# Parse identical flags as the real do-release.sh
# ---------------------------------------------------------------------------
while getopts ":b:s:p:t:r:nyh" opt; do
  case $opt in
    b) GIT_BRANCH=$OPTARG ;;
    n) DRY_RUN=1 ;;
    s) RELEASE_STEP=$OPTARG ;;
    p) GPG_PASSPHRASE=$OPTARG ;;
    t) ASF_PASSWORD=$OPTARG ;;
    r) RC_COUNT=$OPTARG ;;
    y) FORCE=1 ;;
    h)
      echo "Usage: $0 [options]  (MOCK — no real operations run)"
      echo ""
      echo "Accepts identical flags and env vars as do-release.sh."
      echo "All credentials are validated exactly as in the real script."
      echo ""
      echo "Extra env vars for testing:"
      echo "  MOCK_FAIL_STAGE=<stage>   Simulate failure at: tag, build, docs, publish, finalize"
      echo "  MOCK_STAGE_DELAY=<secs>   Sleep duration per stage (default: 1)"
      exit 0
      ;;
    :) echo "Option -$OPTARG requires an argument." >&2; exit 1 ;;
    \?) echo "Invalid option: -$OPTARG" >&2; exit 1 ;;
  esac
done

export RUNNING_IN_DOCKER=${RUNNING_IN_DOCKER:-0}
export DRY_RUN=${DRY_RUN:-0}
export FORCE=${FORCE:-0}
export RC_COUNT=${RC_COUNT:-0}
export RELEASE_STEP=${RELEASE_STEP:-}
export GIT_BRANCH=${GIT_BRANCH:-}
export RELEASE_VERSION=${RELEASE_VERSION:-}
export RELEASE_TAG=${RELEASE_TAG:-}
export ASF_PASSWORD=${ASF_PASSWORD:-}
export GPG_PASSPHRASE=${GPG_PASSPHRASE:-}

if [[ "${RC_COUNT}" != "0" ]] && ! [[ "${RC_COUNT}" =~ ^[1-9][0-9]*$ ]]; then
  echo "Error: RC number must be a positive integer, got: '${RC_COUNT}'" >&2
  exit 1
fi

if [ -n "${RELEASE_STEP}" ]; then
  case "${RELEASE_STEP}" in
    tag|build|docs|publish|finalize) ;;
    *) echo "Error: invalid release step '${RELEASE_STEP}'. Valid steps: tag, build, docs, publish, finalize" >&2; exit 1 ;;
  esac
fi

# Source real release-util.sh for get_release_info, is_dry_run, is_force, etc.
. "$REAL_SCRIPTS_DIR/release-util.sh"

# Validate PYPI_API_TOKEN — same check as the real script.
if ! is_dry_run; then
  if [[ -z "${PYPI_API_TOKEN:-}" ]]; then
    echo 'The environment variable PYPI_API_TOKEN is not set. Exiting.'
    exit 1
  fi
fi

# Collect release parameters the same way as the real script.
# In -y (force) mode this reads from env vars with no prompts.
# Credentials (ASF_PASSWORD, GPG_PASSPHRASE) are collected inside get_release_info.
SKIP_TAG=0
get_release_info

function should_build {
  local WHAT=$1
  [ -z "$RELEASE_STEP" ] || [ "$WHAT" = "$RELEASE_STEP" ]
}

# ---------------------------------------------------------------------------
# State tracking (identical to real do-release.sh)
# ---------------------------------------------------------------------------
STATE_DIR="${RELEASE_STATE_DIR:-$SELF/.release-state}/${RELEASE_TAG}"
mkdir -p "$STATE_DIR"

function stage_file { echo "$STATE_DIR/$1.done"; }

function is_stage_done { [ -f "$(stage_file "$1")" ]; }

function mark_stage_done {
  local STEP="$1" INFO="${2:-}"
  {
    echo "completed_at=$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    echo "release=$RELEASE_TAG"
    echo "mode=MOCK"
    [ -n "$INFO" ] && echo "info=$INFO"
  } > "$(stage_file "$STEP")"
  echo "Stage '$STEP' marked as done (MOCK). State file: $(stage_file "$STEP")"
}

function check_stage_guard {
  local STEP="$1"
  if is_stage_done "$STEP"; then
    echo ""
    echo "=== Stage '$STEP' is already complete ==="
    cat "$(stage_file "$STEP")"
    echo "To re-run this stage, delete: $(stage_file "$STEP")"
    echo ""
    return 1
  fi
  return 0
}

# ---------------------------------------------------------------------------
# Mock stage runner
# Prints what the real stage would do, sleeps briefly, then either
# writes a .done file (success) or exits non-zero (if MOCK_FAIL_STAGE matches).
# ---------------------------------------------------------------------------
function mock_stage {
  local STEP="$1"
  local LOG_FILE="$SELF/${STEP}.log"

  echo "========================"
  echo "= [MOCK] Stage: $STEP"
  echo "= Release: $RELEASE_TAG  Branch: $GIT_BRANCH  DryRun: $DRY_RUN"

  case "$STEP" in
    tag)
      echo "= Would: clone repo, update versions, commit, create tag $RELEASE_TAG, bump to next SNAPSHOT, push"
      ;;
    build)
      echo "= Would: clone from $RELEASE_TAG, build source+binary tarballs, sign, checksum,"
      echo "=        upload Python RC to PyPI, upload artifacts to ASF SVN staging"
      ;;
    docs)
      echo "= Would: build Javadoc and Python Sphinx docs locally"
      ;;
    publish)
      echo "= Would: create Nexus staging repo, build Maven artifacts (Scala 2.12 + 2.13),"
      echo "=        sign, upload to Nexus, close staging repo"
      ;;
    finalize)
      echo "= Would: create tag v$RELEASE_VERSION, upload final Python package to PyPI,"
      echo "=        move SVN dev → release, update KEYS"
      ;;
  esac

  sleep "${MOCK_STAGE_DELAY:-1}"

  if [ "${MOCK_FAIL_STAGE:-}" = "$STEP" ]; then
    echo "[MOCK] Simulated failure injected for stage '$STEP'" | tee "$LOG_FILE" >&2
    echo "Error: MOCK_FAIL_STAGE=$STEP triggered a non-zero exit." >> "$LOG_FILE"
    exit 1
  fi

  echo "[MOCK] Stage '$STEP' completed successfully." | tee "$LOG_FILE"
}

# ---------------------------------------------------------------------------
# Stage execution (mirrors real do-release.sh structure exactly)
# ---------------------------------------------------------------------------
if should_build "tag"; then
  if [ $SKIP_TAG = 1 ]; then
    echo "Tag $RELEASE_TAG already exists on remote. Skipping tag creation."
    is_stage_done "tag" || mark_stage_done "tag" "MOCK: tag $RELEASE_TAG already existed on remote"
  else
    check_stage_guard "tag" && {
      mock_stage "tag"
      mark_stage_done "tag" "MOCK: tag $RELEASE_TAG simulated (not pushed to remote)"
    }
  fi
else
  echo "Skipping tag stage."
fi

if should_build "build"; then
  check_stage_guard "build" && {
    mock_stage "build"
    mark_stage_done "build" "MOCK: artifacts build and SVN staging upload simulated"
  }
else
  echo "Skipping build step."
fi

if should_build "docs"; then
  check_stage_guard "docs" && {
    mock_stage "docs"
    mark_stage_done "docs" "MOCK: Javadoc and Python docs build simulated"
  }
else
  echo "Skipping docs step."
fi

if should_build "publish"; then
  check_stage_guard "publish" && {
    mock_stage "publish"
    mark_stage_done "publish" "MOCK: Maven artifacts upload to Nexus simulated"
  }
else
  echo "Skipping publish step."
fi

if [ "$RELEASE_STEP" = "finalize" ]; then
  check_stage_guard "finalize" && {
    mock_stage "finalize"
    mark_stage_done "finalize" "MOCK: PyPI upload, SVN promotion, KEYS update simulated"
  }
fi

echo "Mock release completed."
