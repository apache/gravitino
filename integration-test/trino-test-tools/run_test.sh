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

set -e

cd "$(dirname "$0")"

export GRAVITINO_SERVER_HOME=../../distribution/package
export TRINO_TEST_DOCKER_HOME=../trino-distribution-it
export TRINO_TEST_SETS_DIR=../src/test/resources/trino-ci-testset/testsets
export TRINO_TEST_ARGS="--test_set=jdbc-mysql"
export TRINO_TEST_PARAMS=

if [ -f .env ]; then  
    source .env
fi

if [ ! -z "$1" ]; then
    GRAVITINO_SERVER_HOME=$1
fi

if [ ! -d "$GRAVITINO_SERVER_HOME" ]; then
  echo "Error: Gravitino server directory '$GRAVITINO_SERVER_HOME' does not exist."
  exit 1
fi

TRINO_TEST_SETS_DIR=`realpath $TRINO_TEST_SETS_DIR`

# Download mysql driver
MYSQL_VERSION="8.0.26"
MYSQL_DIVER_FILE_NAME="mysql-connector-java-$MYSQL_VERSION.jar"
MYSQL_DIVER_DOWNLOAD_URL="https://repo1.maven.org/maven2/mysql/mysql-connector-java/$MYSQL_VERSION/$MYSQL_DIVER_FILE_NAME"

if [ ! -f "$GRAVITINO_SERVER_HOME/catalogs/jdbc-mysql/libs/$MYSQL_DIVER_FILE_NAME" ]; then
  curl -L -o "$GRAVITINO_SERVER_HOME/catalogs/jdbc-mysql/libs/$MYSQL_DIVER_FILE_NAME" $MYSQL_DIVER_DOWNLOAD_URL
  if [ $? -ne 0 ]; then
      echo "download faild"
      exit 1
  fi
fi

# Download postgresql driver
POSTGRESQL_VERSION="42.7.0"
POSTGRESQL_DIVER_FILE_NAME="postgresql-$POSTGRESQL_VERSION.jar"
POSTGRESQL_DIVER_DOWNLOAD_URL="https://jdbc.postgresql.org/download/$POSTGRESQL_DIVER_FILE_NAME"

if [ ! -f "$GRAVITINO_SERVER_HOME/catalogs/jdbc-postgresql/libs/$POSTGRESQL_DIVER_FILE_NAME" ]; then
  curl -L -o "$GRAVITINO_SERVER_HOME/catalogs/jdbc-postgresql/libs/$POSTGRESQL_DIVER_FILE_NAME" $POSTGRESQL_DIVER_DOWNLOAD_URL
  if [ $? -ne 0 ]; then
      echo "download faild"
      exit 1
  fi
fi

#clean the Gravitino server data
rm -rf $GRAVITINO_SERVER_HOME/data/*

# start test dockers
$TRINO_TEST_DOCKER_HOME/launch.sh

# resolve docker ip address
uris=$($TRINO_TEST_DOCKER_HOME/inspect_ip.sh)

args="--auto=none --test_sets_dir=$(realpath $TRINO_TEST_SETS_DIR) $uris $TRINO_TEST_ARGS --params=$TRINO_TEST_PARAMS"

# execute test
echo "The args: $args"
./trino_test.sh $args

  if [ $? -ne 0 ]; then
      echo "Test failed"
      # clean up
      $TRINO_TEST_DOCKER_HOME/shutdown.sh
      exit 1
  fi

echo "Test success"
# clean up
$TRINO_TEST_DOCKER_HOME/shutdown.sh
