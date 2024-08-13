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

cd "$(dirname "$0")"

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
  curl -L -o "$GRAVITINO_SERVER_HOME/catalogs/jdbc-mysql/libs/$POSTGRESQL_DIVER_FILE_NAME" $POSTGRESQL_DIVER_DOWNLOAD_URL
  if [ $? -ne 0 ]; then
      echo "download faild"
      exit 1
  fi
fi

rm -rf ../../distribution/package/data/*


# start test dockers
../trino-distribution-it/launch.sh

# resolve docker ip address
uris=$(../trino-distribution-it/inspect_ip.sh)

args="--auto=none --test_sets_dir=$(realpath ../../integration-test/src/test/resources/trino-ci-testset/testsets) $uris --params=trino_remote_jdbc_uri,jdbc:trino://trino-remote:8080"

# execute test
echo "The args: $args"
../../integration-test/trino-test-tools/trino_test.sh $args

  if [ $? -ne 0 ]; then
      echo "Test failed"
      # clean up
      ../trino-distribution-it/shutdown.sh
      exit 1
  fi

echo "Test success"
# clean up
../trino-distribution-it/shutdown.sh
