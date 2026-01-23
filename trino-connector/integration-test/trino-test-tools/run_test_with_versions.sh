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

# This script is used to execute tests for the Trino connector across different Trino versions.
# It wraps the `trino_test.sh` script and iterates through a configured map of Trino versions
# to connector modules.
#
# Arguments:
# --trino_versions_map: Space-separated list of "version:module" pairs.
#                       Defaults to "435:trino-connector" if not provided.
# --trino_test_args:    Arguments passed directly to the underlying test script.
#
# Example:
# ./run_test_with_versions.sh \
#   --trino_versions_map="435:trino-connector 478:trino-connector-470-478" \
#   --trino_test_args="--auto=all"
#
# This configuration will run tests for:
# - Trino 435 using the connector in 'trino-connector' directory
# - Trino 478 using the connector in 'trino-connector-470-478' directory
# And pass "--auto=all" to the underlying test script for each version.

cd "$(dirname "$0")"

# Set the Gravitino server directories
GRAVITINO_HOME_DIR=../../../
GRAVITINO_HOME_DIR=`realpath $GRAVITINO_HOME_DIR`

# Parse arguments
trino_versions_map=""
trino_test_args=""

while [[ $# -gt 0 ]]; do
  case $1 in
    --trino_versions_map=*)
      trino_versions_map="${1#*=}"
      shift
      ;;
    --trino_test_args=*)
      trino_test_args="${1#*=}"
      shift
      ;;
    *)
      echo "Unknown argument: $1"
      exit 1
      ;;
  esac
done

if [ -z "$trino_versions_map" ]; then
  trino_versions_map="435:trino-connector"
fi

for entry in $trino_versions_map; do
    trino_version=${entry%%:*}
    trino_module_name=${entry##*:}
    
    trino_connector_dir="$GRAVITINO_HOME_DIR/trino-connector/$trino_module_name/build/libs"
    
    # Combine test arguments with version-specific arguments
    args="$trino_test_args --trino_version=${trino_version} --trino_connector_dir=${trino_connector_dir}"
    
    # execute test
    echo "Running test for Trino version: $trino_version with connector: $trino_module_name"
    echo "The args: $args"
    
    sleep 5
    $GRAVITINO_HOME_DIR/trino-connector/integration-test/trino-test-tools/trino_test.sh $args
    
    if [ $? -ne 0 ]; then
        echo "Test failed for Trino version $trino_version"
        exit 1
    fi
done

echo "Test success"
