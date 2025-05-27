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

MYSQL_CONNECTOR_VERSION=8.0.28
MYSQL_CONNECTOR_PACKAGE_NAME="mysql-connector-java-${MYSQL_CONNECTOR_VERSION}.jar"
MYSQL_CONNECTOR_DOWNLOAD_URL="https://search.maven.org/remotecontent?filepath=mysql/mysql-connector-java/${MYSQL_CONNECTOR_VERSION}/mysql-connector-java-${MYSQL_CONNECTOR_VERSION}.jar"

# Prepare download packages
if [[ ! -d "${ranger_dir}/packages" ]]; then
  mkdir -p "${ranger_dir}/packages"
fi

if [ ! -f "${ranger_dir}/packages/${RANGER_PACKAGE_NAME}" ]; then
  # Package not exist, we need to build them from source
  if [ ! -d "${ranger_dir}/packages/apache-ranger" ]; then
    git clone https://github.com/apache/ranger --branch master --single-branch ${ranger_dir}/packages/apache-ranger
    # set the commit to RANGER-5146: 500 API Error When Deleting TagDef with a Linked Tag
    # https://github.com/apache/ranger/commit/ff36aabe36169b94862c51a5b403f59c9d728b94
    cd ${ranger_dir}/packages/apache-ranger
    git reset --hard ff36aabe36169b94862c51a5b403f59c9d728b94
  fi

  cp ${ranger_dir}/.env ${ranger_dir}/packages/apache-ranger/dev-support/ranger-docker
  # overwrite docker-compose.ranger-base.yml in apache-ranger, add :Z option to volume to solve
  # SELinux problem
  cp ${ranger_dir}/docker-compose.ranger-build.yml ${ranger_dir}/packages/apache-ranger/dev-support/ranger-docker
  cd ${ranger_dir}/packages/apache-ranger/dev-support/ranger-docker

  mkdir -p ${ranger_dir}/packages/apache-ranger/dev-support/ranger-docker/dist

  # run command in subshell to avoid environment variable pollution
  (
    export DOCKER_BUILDKIT=1
    export COMPOSE_DOCKER_CLI_BUILD=1
    export RANGER_DB_TYPE=mysql
    # Prevent builder to pull remote image
    # https://github.com/moby/buildkit/issues/2343#issuecomment-1311890308
    export BUILDX_BUILDER=default

    docker compose -f docker-compose.ranger-base.yml -f docker-compose.ranger-build.yml up --pull=never
    docker compose -f docker-compose.ranger-base.yml -f docker-compose.ranger-build.yml cp ranger-build:/home/ranger/dist .
  )

  cp ${ranger_dir}/packages/apache-ranger/dev-support/ranger-docker/dist/* ${ranger_dir}/packages
fi

if [ ! -f "${ranger_dir}/packages/${MYSQL_CONNECTOR_PACKAGE_NAME}" ]; then
  curl -L -s -o "${ranger_dir}/packages/${MYSQL_CONNECTOR_PACKAGE_NAME}" ${MYSQL_CONNECTOR_DOWNLOAD_URL}
fi
