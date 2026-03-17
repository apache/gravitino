#!/bin/bash
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

DIR=$(cd "$(dirname "$0")" && pwd)/../../../
export GRAVITINO_ROOT_DIR=$(cd "$DIR" && pwd)
export GRAVITINO_HOME=$GRAVITINO_ROOT_DIR
export GRAVITINO_TEST=true
export HADOOP_USER_NAME=anonymous

echo $GRAVITINO_ROOT_DIR
cd $GRAVITINO_ROOT_DIR

# Parse --auto_patch and --trino_version from arguments.
# --auto_patch is consumed here and not forwarded to Gradle.
# --trino_version is forwarded to Gradle and also used for patch selection.
auto_patch=false
trino_version=""
args=""
for arg in "$@"; do
    case "$arg" in
        --auto_patch)
            auto_patch=true
            ;;
        --trino_version=*)
            trino_version="${arg#*=}"
            args="$args $arg"
            ;;
        *)
            args="$args $arg"
            ;;
    esac
done
args="${args# }"

TESTSETS_DIR="$GRAVITINO_ROOT_DIR/trino-connector/integration-test/src/test/resources/trino-ci-testset/testsets"

# Apply version-specific patches cumulatively based on the target Trino version.
# Patches are applied in descending order (newest first):
#   version <= 473: apply trino-478-473.patch
#   version <= 452: also apply trino-473-452.patch
#   version <= 446: also apply trino-452-446.patch
apply_version_patches() {
    local version=$1
    # Each entry is "max_version:patch_file"; patches are applied in order.
    local patches=(
        "473:trino-478-473.patch"
        "452:trino-473-452.patch"
        "446:trino-452-446.patch"
    )
    for entry in "${patches[@]}"; do
        local max_ver="${entry%%:*}"
        local patch="${entry##*:}"
        if [[ $version -le $max_ver ]]; then
            echo "Applying patch: $patch"
            git -C "$GRAVITINO_ROOT_DIR" apply "$TESTSETS_DIR/$patch" \
                || { echo "ERROR: Failed to apply $patch"; exit 1; }
        fi
    done
}

# Restore test resources to their original state by reverting any patch changes.
restore_test_files() {
    echo "Restoring test files..."
    git -C "$GRAVITINO_ROOT_DIR" checkout -- \
        trino-connector/integration-test/src/test/resources/ \
        integration-test-common/
    git -C "$GRAVITINO_ROOT_DIR" clean -f \
        trino-connector/integration-test/src/test/resources/trino-ci-testset/testsets/lakehouse-iceberg/
}

if [ "$auto_patch" = true ]; then
    # Abort if the testsets directory has uncommitted changes, to avoid
    # patch conflicts or accidental loss of in-progress work.
    TESTSETS_REL="trino-connector/integration-test/src/test/resources/trino-ci-testset/testsets"
    if ! git -C "$GRAVITINO_ROOT_DIR" diff --quiet -- "$TESTSETS_REL" || \
       ! git -C "$GRAVITINO_ROOT_DIR" diff --cached --quiet -- "$TESTSETS_REL"; then
        echo "ERROR: Uncommitted changes detected in $TESTSETS_REL."
        echo "Please commit or stash your changes before running with --auto_patch."
        exit 1
    fi
    # Register trap to ensure test files are always restored on exit,
    # even if the script is interrupted or exits abnormally.
    trap 'restore_test_files' EXIT
    if [ -n "$trino_version" ]; then
        apply_version_patches "$trino_version"
    fi
fi

./gradlew :trino-connector:integration-test:TrinoTest -PappArgs="\"$args\""
