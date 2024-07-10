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
#
set -ex
ranger_dir="$(dirname "${BASH_SOURCE-$0}")"
ranger_dir="$(cd "${ranger_dir}">/dev/null; pwd)"

# Environment variables definition
RANGER_VERSION=2.4.0
RANGER_PACKAGE_NAME="ranger-${RANGER_VERSION}-admin.tar.gz" # Must export this variable for Dockerfile
RANGER_DOWNLOAD_URL=https://github.com/datastrato/apache-ranger/releases/download/release-ranger-${RANGER_VERSION}/ranger-${RANGER_VERSION}-admin.tar.gz

MYSQL_CONNECTOR_VERSION=8.0.28
MYSQL_CONNECTOR_PACKAGE_NAME="mysql-connector-java-${MYSQL_CONNECTOR_VERSION}.jar"
MYSQL_CONNECTOR_DOWNLOAD_URL=https://search.maven.org/remotecontent?filepath=mysql/mysql-connector-java/${MYSQL_CONNECTOR_VERSION}/mysql-connector-java-${MYSQL_CONNECTOR_VERSION}.jar

# Prepare download packages
if [[ ! -d "${ranger_dir}/packages" ]]; then
  mkdir -p "${ranger_dir}/packages"
fi

if [ ! -f "${ranger_dir}/packages/${RANGER_PACKAGE_NAME}" ]; then
  curl -L -s -o "${ranger_dir}/packages/${RANGER_PACKAGE_NAME}" ${RANGER_DOWNLOAD_URL}
fi

if [ ! -f "${ranger_dir}/packages/${MYSQL_CONNECTOR_PACKAGE_NAME}" ]; then
  curl -L -s -o "${ranger_dir}/packages/${MYSQL_CONNECTOR_PACKAGE_NAME}" ${MYSQL_CONNECTOR_DOWNLOAD_URL}
fi
