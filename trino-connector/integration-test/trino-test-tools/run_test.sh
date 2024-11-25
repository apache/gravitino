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

# This script is used to execute tests for the Trino connector. Environment variables allow you to set the Trino connector
# test set, test environment, and other test parameters. By default, the script uses the test set in the
# `$GRAVITINO_HOME_DIR/trino-connector/integration-test/src/test/resources/trino-ci-testset/trino-cascading-testsets` directory.
# and the trino-cascading-query test environment in the `trino-cascading-env` directory. However, you can also pass in
# a test set and test environment directory as parameters.

cd "$(dirname "$0")"

# Set the Gravitino server and Trino connector directories
GRAVITINO_HOME_DIR=../../../
GRAVITINO_SERVER_DIR=
GRAVITINO_TRINO_CONNECTOR_DIR=

GRAVITINO_HOME_DIR=`realpath $GRAVITINO_HOME_DIR`

# Set the test set and test environment directories
# Set the test arguments and parameters for the test TrinoQueryTestTools
TRINO_TEST_DOCKER_HOME=trino-cascading-env
TRINO_TEST_SETS_DIR=$GRAVITINO_HOME_DIR/trino-connector/integration-test/src/test/resources/trino-ci-testset/trino-cascading-testsets
TRINO_TEST_ARGS=
TRINO_TEST_PARAMS=


source check_env.sh
GRAVITINO_SERVER_DIR=$(gravitino_server_dir)
GRAVITINO_TRINO_CONNECTOR_DIR=$(gravitino_trino_connector_dir)
check_environment


if [ ! -d "$TRINO_TEST_DOCKER_HOME" ]; then
  echo "Error: Trino test env setup directory '$TRINO_TEST_DOCKER_HOME' does not exist."
  exit 1
fi

if [ ! -d "$TRINO_TEST_SETS_DIR" ]; then
  echo "Error: Trino test sets directory '$TRINO_TEST_SETS_DIR' does not exist."
  exit 1
fi


TRINO_TEST_SETS_DIR=`realpath $TRINO_TEST_SETS_DIR`
GRAVITINO_SERVER_DIR=`realpath $GRAVITINO_SERVER_DIR`
GRAVITINO_TRINO_CONNECTOR_DIR=`realpath $GRAVITINO_TRINO_CONNECTOR_DIR`

set +e
# start test dockers
$TRINO_TEST_DOCKER_HOME/launch.sh $GRAVITINO_SERVER_DIR $GRAVITINO_TRINO_CONNECTOR_DIR

# resolve docker ip address
uris=$($TRINO_TEST_DOCKER_HOME/inspect_ip.sh)

args="--auto=none --test_sets_dir=$TRINO_TEST_SETS_DIR $uris $TRINO_TEST_ARGS"

if [[ -n $TRINO_TEST_PARAMS ]]; then
    if [[ "$args" == *"--params="* ]]; then
        args=$(echo "$args" | sed "s/--params=[^ ]*/--params=$TRINO_TEST_PARAMS,/")
    else 
        args="$args --params=$TRINO_TEST_PARAMS"
    fi
fi

# execute test
echo "The args: $args"

sleep 5
$GRAVITINO_HOME_DIR/trino-connector/integration-test/trino-test-tools/trino_test.sh $args

if [ $? -ne 0 ]; then
    echo "Test failed"
    # clean up
    $TRINO_TEST_DOCKER_HOME/shutdown.sh
    exit 1
fi

echo "Test success"
# clean up
$TRINO_TEST_DOCKER_HOME/shutdown.sh
