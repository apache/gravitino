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
# Usage: ./publish_docker.sh <tag|branch> --docker-version <version> [--trino-version <version>] [--dry-run]
#
# Arguments:
#   <tag>                     A Git tag, typically a release candidate (e.g., v1.2.0-rc5).
#                             The tag must already exist locally or be fetched via:
#                             git fetch --tags origin
#   <branch>                  A Git branch name (e.g., main, branch-1.2). The workflow will
#                             build from the latest commit on that branch.
#   --docker-version <ver>    Docker image version tag to publish (e.g., 1.2.0-rc5). Required.
#                             This is used as the image tag, independent of the Git tag/branch.
#   --trino-version <ver>     Trino version used for the playground image (e.g., 478).
#                             Defaults to 478. The playground image tag will be:
#                             <ver>-gravitino-<docker-version>
#   --dry-run                 Print the workflow commands that would be triggered without
#                             actually running them. Useful for previewing before publishing.
#
# Examples:
#   ./publish_docker.sh v1.2.0-rc5 --docker-version 1.2.0-rc5                              # Publish with default Trino 478
#   ./publish_docker.sh v1.2.0-rc5 --docker-version 1.2.0-rc5 --trino-version 478         # Publish with Trino 478
#   ./publish_docker.sh v1.2.0-rc5 --docker-version 1.2.0-rc5 --trino-version 478 --dry-run  # Preview only
#   ./publish_docker.sh main --docker-version 1.2.0-SNAPSHOT                               # Publish from main branch
#
# Environment variables required (set in env file or shell profile):
#   DOCKER_USERNAME       - Docker Hub username
#   PUBLISH_DOCKER_TOKEN  - Docker Hub access token with push permission
#   GH_TOKEN              - GitHub personal access token with repo and workflow scopes
#
# This script triggers the docker-image.yml workflow for the following images:
#   - apache/gravitino:<tag>
#   - apache/gravitino-iceberg-rest-server:<tag>
#   - apache/gravitino-lance-rest-server:<tag>
#   - apache/gravitino-mcp-server:<tag>
#   - apache/gravitino-playground:<trino-version>-gravitino-<tag>
#

set -e

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
      DOCKER_VERSION="$2"
      shift 2
      ;;
    --trino-version)
      TRINO_VER="$2"
      shift 2
      ;;
    -h|--help)
      cat << EOF
Usage: $0 <tag|branch> [--dry-run]

Arguments:
  <tag|branch>   Git tag or branch name to build (e.g., v1.2.0-rc5, branch-1.2.0-rc5)
  --dry-run      Preview mode, print commands without triggering workflows

Environment variables:
  DOCKER_USERNAME        Docker Hub username (required for actual run)
  PUBLISH_DOCKER_TOKEN   Docker Hub access token (required for actual run)
  GH_TOKEN               GitHub token with repo/workflow permissions

Examples:
  $0 v1.2.0-rc5                  # Build version 1.2.0-rc5
  $0 branch-1.2.0-rc5            # Build from branch
  $0 v1.2.0-rc5 --dry-run        # Preview commands only

Images built:
  apache/gravitino:${VERSION}
  apache/gravitino-iceberg-rest-server:${VERSION}
  apache/gravitino-lance-rest-server:${VERSION}
  apache/gravitino-mcp-server:${VERSION}
  apache/gravitino-playground:trino-478-gravitino-${VERSION}

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

# Verify tag or branch exists
if ! git rev-parse "$INPUT_TAG" >/dev/null 2>&1; then
  echo "ERROR: Tag or branch '$INPUT_TAG' does not exist"
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
  if [[ -z "$DOCKER_USERNAME" ]]; then
    echo "ERROR: DOCKER_USERNAME environment variable not set"
    exit 1
  fi
  if [[ -z "$PUBLISH_DOCKER_TOKEN" ]]; then
    echo "ERROR: PUBLISH_DOCKER_TOKEN environment variable not set"
    exit 1
  fi
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
