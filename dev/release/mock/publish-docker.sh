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

# Mock version of publish-docker.sh for testing the gravitino-release skill.
#
# Accepts identical arguments and env vars as the real publish-docker.sh but
# prints simulated workflow dispatch commands instead of triggering them.
# State tracking runs for real so the skill's guard logic is exercised.
#
# Extra env vars for testing:
#   MOCK_FAIL_STAGE=docker    Simulate a failure during docker stage.

set -e

SELF=$(cd "$(dirname "$0")" && pwd)

# Parse arguments — identical to real publish-docker.sh
DRY_RUN=false
INPUT_TAG=""
DOCKER_VERSION=""
TRINO_VER="478"
STATE_KEY="docker"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    --state-key)
      if [[ -z "${2:-}" || "$2" == -* ]]; then
        echo "ERROR: --state-key requires a non-empty value." >&2
        exit 1
      fi
      STATE_KEY="$2"
      shift 2
      ;;
    --docker-version)
      if [[ -z "${2:-}" || "$2" == -* ]]; then
        echo "ERROR: --docker-version requires a non-empty value." >&2
        exit 1
      fi
      DOCKER_VERSION="$2"
      shift 2
      ;;
    --trino-version)
      if [[ -z "${2:-}" || "$2" == -* ]]; then
        echo "ERROR: --trino-version requires a non-empty value." >&2
        exit 1
      fi
      TRINO_VER="$2"
      shift 2
      ;;
    -h|--help)
      cat << 'EOF'
Usage: publish-docker.sh <tag|branch> --docker-version <version> [--trino-version <version>] [--dry-run]
(MOCK VERSION — no workflows are actually triggered)

Accepts identical arguments as the real publish-docker.sh.

Extra env vars for testing:
  MOCK_FAIL_STAGE=docker   Simulate a failure during the docker stage.
EOF
      exit 0
      ;;
    *)
      if [[ -z "$INPUT_TAG" ]]; then
        INPUT_TAG="$1"
      else
        echo "ERROR: Unknown argument: $1" >&2
        exit 1
      fi
      shift
      ;;
  esac
done

if [[ -z "$INPUT_TAG" ]]; then
  echo "ERROR: Missing tag/branch argument" >&2
  exit 1
fi

if [[ -z "$DOCKER_VERSION" ]]; then
  echo "ERROR: Missing --docker-version argument" >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# State tracking — identical convention as real publish-docker.sh
# ---------------------------------------------------------------------------
DOCKER_STATE_DIR="${RELEASE_STATE_DIR:-$SELF/.release-state}/${INPUT_TAG}"
DOCKER_STATE_FILE="${DOCKER_STATE_DIR}/${STATE_KEY}.done"

if [[ "$DRY_RUN" == "false" ]] && [[ -f "$DOCKER_STATE_FILE" ]]; then
  echo ""
  echo "=== Stage '${STATE_KEY}' is already complete ==="
  cat "$DOCKER_STATE_FILE"
  echo "To re-run, delete: $DOCKER_STATE_FILE"
  echo ""
  exit 0
fi

TRINO_VERSION="${TRINO_VER}-gravitino-${DOCKER_VERSION}"

echo "[MOCK] Skipping remote tag/branch verification for: $INPUT_TAG"

# Validate credentials — same checks as the real script.
if [[ "$DRY_RUN" == "false" ]]; then
  if [[ -z "${GH_TOKEN:-}" ]] && ! gh auth status > /dev/null 2>&1; then
    echo "ERROR: GH_TOKEN is not set and gh auth is not configured."
    echo "Either export GH_TOKEN or run: gh auth login"
    exit 1
  fi
  if [[ -z "${DOCKER_USERNAME:-}" ]]; then
    echo "ERROR: DOCKER_USERNAME environment variable not set"
    exit 1
  fi
  if [[ -z "${PUBLISH_DOCKER_TOKEN:-}" ]]; then
    echo "ERROR: PUBLISH_DOCKER_TOKEN environment variable not set"
    exit 1
  fi
  echo "Username: ${DOCKER_USERNAME}"
fi
echo ""

if [[ "$DRY_RUN" == "true" ]]; then
  echo "=== [MOCK DRY RUN] Preview Gravitino Docker Image Build ==="
else
  echo "=== [MOCK] Simulating Gravitino Docker Image Build ==="
fi
echo "Input:          ${INPUT_TAG}"
echo "Docker Version: ${DOCKER_VERSION}"
echo "Trino Version:  ${TRINO_VERSION}"
echo ""

declare -a images=(
  "gravitino"
  "gravitino-iceberg-rest-server"
  "gravitino-lance-rest-server"
  "gravitino-mcp-server"
)

echo "=== [MOCK] Simulated Workflow Dispatches ==="

for img in "${images[@]}"; do
  echo "[MOCK] gh workflow run docker-image.yml -R apache/gravitino --ref ${INPUT_TAG} -f image=${img} -f version=${DOCKER_VERSION}"
  sleep 0.2
done

echo "[MOCK] gh workflow run docker-image.yml -R apache/gravitino --ref ${INPUT_TAG} -f image=gravitino-playground:trino -f version=${TRINO_VERSION}"

echo ""

if [[ "$DRY_RUN" == "true" ]]; then
  echo "=== [MOCK DRY RUN] Preview Complete ==="
else
  if [ "${MOCK_FAIL_STAGE:-}" = "docker" ]; then
    echo "[MOCK] Simulated failure injected for stage 'docker'" >&2
    exit 1
  fi
  echo "=== [MOCK] All workflows simulated ==="
fi

# Mark stage as done regardless of dry-run so the skill can track completion.
mkdir -p "$DOCKER_STATE_DIR"
{
  echo "completed_at=$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  echo "release=$INPUT_TAG"
  echo "docker_version=$DOCKER_VERSION"
  echo "trino_version=$TRINO_VERSION"
  echo "mode=MOCK"
  [[ "$DRY_RUN" == "true" ]] && echo "dry_run=true"
  echo "info=MOCK: all Docker image workflow dispatches simulated"
} > "$DOCKER_STATE_FILE"
echo "Stage '${STATE_KEY}' marked as done (MOCK). State file: $DOCKER_STATE_FILE"
