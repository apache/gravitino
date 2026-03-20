#!/bin/bash

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

# Build and publish Gravitino Docker images via GitHub Actions
#
# Usage: ./publish-docker.sh <tag|branch> --docker-version <version> [--trino-version <version>] [--dry-run]
#
# Arguments:
#   <tag>                     A Git tag, typically a release candidate (e.g., v1.2.0-rc5).
#                             The tag must already exist on the remote apache/gravitino repository.
#   <branch>                  A Git branch name (e.g., main, branch-1.2). The workflow will
#                             build from the latest commit on that branch.
#   --docker-version <ver>    Docker image version tag to publish (e.g., 1.2.0-rc5). Required.
#                             This is used as the image tag, independent of the Git tag/branch.
#   --trino-version <ver>     Trino version used for the playground image (e.g., 478).
#                             Defaults to 478. The playground image tag will be:
#                             trino-<ver>-gravitino-<docker-version>
#   --dry-run                 Print the workflow commands that would be triggered without
#                             actually running them. Useful for previewing before publishing.
#
# Examples:
#   ./publish-docker.sh v1.2.0-rc5 --docker-version 1.2.0-rc5 --trino-version 478 --dry-run
#   ./publish-docker.sh v1.2.0-rc5 --docker-version 1.2.0-rc5 --trino-version 478
#
# Environment variables required (set in env file or shell profile):
#   DOCKER_USERNAME       - Docker Hub username
#   PUBLISH_DOCKER_TOKEN  - Workflow authorization token (compared against secrets.PUBLISH_DOCKER_TOKEN
#                           in the workflow to authorize the dispatch; not used for Docker Hub login)
#   GH_TOKEN              - GitHub personal access token with repo and workflow scopes
#
# This script triggers the docker-image.yml workflow for the following images:
#   - apache/gravitino:<docker-version>
#   - apache/gravitino-iceberg-rest:<docker-version>
#   - apache/gravitino-lance-rest:<docker-version>
#   - apache/gravitino-mcp-server:<docker-version>
#   - apache/gravitino-playground:trino-<trino-version>-gravitino-<docker-version>
#

set -e

# Check required commands
for cmd in git gh; do
  if ! command -v "$cmd" > /dev/null 2>&1; then
    echo "ERROR: Required command '$cmd' is not installed or not in PATH."
    exit 1
  fi
done

# Parse arguments
DRY_RUN=false
INPUT_TAG=""
DOCKER_VERSION=""
TRINO_VER="478"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)
      DRY_RUN=true
      shift
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

Arguments:
  <tag|branch>              Git tag (e.g., v1.2.0-rc5) or branch (e.g., main) to use as
                            workflow source (--ref). Must exist on the remote apache/gravitino repository.
  --docker-version <ver>    Docker image version tag to publish (e.g., 1.2.0-rc5). Required.
  --trino-version <ver>     Trino version for the playground image (default: 478).
  --dry-run                 Preview mode, print commands without triggering workflows.

Environment variables:
  DOCKER_USERNAME        Docker Hub username (required for actual run)
  PUBLISH_DOCKER_TOKEN   Workflow authorization token (required for actual run)
  GH_TOKEN               GitHub token with repo/workflow permissions

Examples:
  publish-docker.sh v1.2.0-rc5 --docker-version 1.2.0-rc5 --trino-version 478 --dry-run
  publish-docker.sh v1.2.0-rc5 --docker-version 1.2.0-rc5 --trino-version 478

Images built:
  apache/gravitino:<docker-version>
  apache/gravitino-iceberg-rest:<docker-version>
  apache/gravitino-lance-rest:<docker-version>
  apache/gravitino-mcp-server:<docker-version>
  apache/gravitino-playground:trino-<trino-version>-gravitino-<docker-version>

EOF
      exit 0
      ;;
    *)
      if [[ -z "$INPUT_TAG" ]]; then
        INPUT_TAG="$1"
      else
        echo "ERROR: Unknown argument: $1"
        exit 1
      fi
      shift
      ;;
  esac
done

# Check required arguments
if [[ -z "$INPUT_TAG" ]]; then
  echo "ERROR: Missing tag/branch argument"
  echo "Usage: $0 <tag|branch> --docker-version <version> [--trino-version <version>] [--dry-run]"
  exit 1
fi

if [[ -z "$DOCKER_VERSION" ]]; then
  echo "ERROR: Missing --docker-version argument"
  echo "Usage: $0 <tag|branch> --docker-version <version> [--trino-version <version>] [--dry-run]"
  exit 1
fi

# Verify tag or branch exists on the remote
if ! git ls-remote --exit-code https://github.com/apache/gravitino.git "$INPUT_TAG" > /dev/null 2>&1; then
  echo "ERROR: Tag or branch '$INPUT_TAG' does not exist on remote 'apache/gravitino'"
  exit 1
fi

echo "Verified: $INPUT_TAG exists"

# Trino special version
TRINO_VERSION="${TRINO_VER}-gravitino-${DOCKER_VERSION}"

if [[ "$DRY_RUN" == "true" ]]; then
  echo "=== [DRY RUN] Preview Gravitino Docker Image Build ==="
else
  echo "=== Building Gravitino Docker Images ==="
fi
echo "Input: ${INPUT_TAG}"
echo "Docker Version: ${DOCKER_VERSION}"
echo "Trino Version: ${TRINO_VERSION}"

if [[ "$DRY_RUN" == "false" ]]; then
  if [[ -z "$GH_TOKEN" ]] && ! gh auth status > /dev/null 2>&1; then
    echo "ERROR: GH_TOKEN is not set and gh auth is not configured."
    echo "Either export GH_TOKEN or run: gh auth login"
    exit 1
  fi
  if [[ -z "$DOCKER_USERNAME" ]]; then
    echo "ERROR: DOCKER_USERNAME environment variable not set"
    exit 1
  fi
  if [[ -z "$PUBLISH_DOCKER_TOKEN" ]]; then
    echo "ERROR: PUBLISH_DOCKER_TOKEN environment variable not set"
    exit 1
  fi
  # NOTE: PUBLISH_DOCKER_TOKEN is passed as a plaintext workflow input (-f token=...).
  # GitHub Actions workflow_dispatch string inputs are not masked in the UI or API,
  # so this value may be visible to anyone with read access to the repository.
  echo "Username: ${DOCKER_USERNAME}"
fi
echo ""

# Image list
declare -a images=(
  "gravitino"
  "gravitino-iceberg-rest-server"
  "gravitino-lance-rest-server"
  "gravitino-mcp-server"
)

echo "=== Triggering Workflows ==="

# Build main images
for img in "${images[@]}"; do
  if [[ "$DRY_RUN" == "true" ]]; then
    echo "gh workflow run docker-image.yml -R apache/gravitino --ref ${INPUT_TAG} -f image=${img} -f version=${DOCKER_VERSION}"
  else
    echo ">>> Triggering ${img}:${DOCKER_VERSION}"
    gh workflow run docker-image.yml -R apache/gravitino \
      --ref "${INPUT_TAG}" \
      -f image="${img}" \
      -f docker_repo_name=apache \
      -f version="${DOCKER_VERSION}" \
      -f username="${DOCKER_USERNAME}" \
      -f token="${PUBLISH_DOCKER_TOKEN}"
  fi
done

# Build Trino playground
if [[ "$DRY_RUN" == "true" ]]; then
  echo "gh workflow run docker-image.yml -R apache/gravitino --ref ${INPUT_TAG} -f image=gravitino-playground:trino -f version=${TRINO_VERSION}"
else
  echo ">>> Triggering gravitino-playground:${TRINO_VERSION}"
  gh workflow run docker-image.yml -R apache/gravitino \
    --ref "${INPUT_TAG}" \
    -f image="gravitino-playground:trino" \
    -f docker_repo_name=apache \
    -f version="${TRINO_VERSION}" \
    -f username="${DOCKER_USERNAME}" \
    -f token="${PUBLISH_DOCKER_TOKEN}"
fi

echo ""
if [[ "$DRY_RUN" == "true" ]]; then
  echo "=== [DRY RUN] Preview Complete ==="
else
  echo "=== All workflows triggered ==="
  echo "View progress: https://github.com/apache/gravitino/actions/workflows/docker-image.yml"
fi
