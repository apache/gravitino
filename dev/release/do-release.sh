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

# Referred from Apache Spark's release script
# dev/create-release/do-release.sh

set -euo pipefail

SELF=$(cd "$(dirname "$0")" && pwd)

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
      echo "Usage: $0 [options]"
      echo ""
      echo "Options:"
      echo "  -b <branch>   Git branch to release (e.g., branch-1.2)"
      echo "  -s <step>     Release step to execute: tag, build, docs, publish, finalize"
      echo "  -r <num>      Release candidate number (e.g., 6 for rc6)"
      echo "  -n            Dry run mode (skip publishing)"
      echo "  -p <pass>     GPG passphrase (insecure; prefer GPG_PASSPHRASE env var)"
      echo "  -t <pass>     ASF password (insecure; prefer ASF_PASSWORD env var)"
      echo "  -y            Non-interactive mode: skip all confirmation prompts"
      echo "  -h            Show this help message"
      echo ""
      echo "Examples:"
      echo "  # Interactive mode (prompted for all inputs):"
      echo "  $0"
      echo ""
      echo "  # Non-interactive full release (use env vars for secrets):"
      echo "  export GPG_PASSPHRASE='my-gpg-pass'"
      echo "  export ASF_PASSWORD='my-asf-pass'"
      echo "  export PYPI_API_TOKEN='my-pypi-token'"
      echo "  export ASF_USERNAME='myuser'"
      echo "  $0 -b branch-1.2 -r 1 -y"
      echo ""
      echo "  # Run a single step only (e.g., build):"
      echo "  $0 -b branch-1.2 -r 1 -s build -y"
      echo ""
      echo "  # Dry run to test without publishing:"
      echo "  $0 -n -b branch-1.2"
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
export ASF_PASSWORD=${ASF_PASSWORD:-}
export GPG_PASSPHRASE=${GPG_PASSPHRASE:-}

cmds=("git" "gpg" "svn" "twine" "shasum" "sha1sum" "jq" "make")
for cmd in "${cmds[@]}"; do
  if ! command -v "$cmd" &> /dev/null; then
    echo "$cmd is required to run this script."
    exit 1
  fi
done

if ! command -v md5 &> /dev/null && ! command -v md5sum &> /dev/null; then
  echo "md5 or md5sum is required to run this script."
  exit 1
fi

. "$SELF/release-util.sh"

if ! is_dry_run; then
  if [[ -z "${PYPI_API_TOKEN:-}" ]]; then
    echo 'The environment variable PYPI_API_TOKEN is not set. Exiting.'
    exit 1
  fi
fi

if [ "$RUNNING_IN_DOCKER" = "1" ]; then
  # Inside docker, need to import the GPG key stored in the current directory.
  echo "${GPG_PASSPHRASE:-}" | $GPG --passphrase-fd 0 --import "$SELF/gpg.key"

  # We may need to adjust the path since JAVA_HOME may be overridden by the driver script.
  if [ -n "${JAVA_HOME:-}" ]; then
    export PATH="$JAVA_HOME/bin:$PATH"
  else
    # JAVA_HOME for the openjdk package.
    export JAVA_HOME=/usr
  fi
else
  # Outside docker, collect release information.
  # In force/non-interactive mode (-y), read_config uses env vars and skips prompts.
  get_release_info
fi

function should_build {
  local WHAT=$1
  [ -z "$RELEASE_STEP" ] || [ "$WHAT" = "$RELEASE_STEP" ]
}

if should_build "tag" && [ $SKIP_TAG = 0 ]; then
  run_silent "Creating release tag $RELEASE_TAG..." "tag.log" \
    "$SELF/release-tag.sh"
  echo "It may take some time for the tag to be synchronized to github."
  if is_force; then
    echo "Force mode: skipping wait."
  else
    echo "Press enter when you've verified that the new tag ($RELEASE_TAG) is available."
    read
  fi
else
  echo "Skipping tag creation for $RELEASE_TAG."
fi

if should_build "build"; then
  run_silent "Building Gravitino..." "build.log" \
    "$SELF/release-build.sh" package
else
  echo "Skipping build step."
fi

if should_build "docs"; then
  run_silent "Building documentation..." "docs.log" \
    "$SELF/release-build.sh" docs
else
  echo "Skipping docs step."
fi

if should_build "publish"; then
  run_silent "Publishing release" "publish.log" \
    "$SELF/release-build.sh" publish-release
else
  echo "Skipping publish step."
fi

if [ "$RELEASE_STEP" = "finalize" ]; then
  run_silent "Finalizing release" "finalize.log" \
    "$SELF/release-build.sh" finalize
fi

echo "Release build and publish completed"
