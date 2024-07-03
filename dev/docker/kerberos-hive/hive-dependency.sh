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
hive_dir="$(dirname "${BASH_SOURCE-$0}")"
hive_dir="$(cd "${hive_dir}">/dev/null; pwd)"

# Environment variables definition
HADOOP_VERSION="2.7.3"
HIVE_VERSION="2.3.9"
MYSQL_JDBC_DRIVER_VERSION="8.0.15"

HADOOP_PACKAGE_NAME="hadoop-${HADOOP_VERSION}.tar.gz" # Must export this variable for Dockerfile
HADOOP_DOWNLOAD_URL="https://archive.apache.org/dist/hadoop/core/hadoop-${HADOOP_VERSION}/${HADOOP_PACKAGE_NAME}"

HIVE_PACKAGE_NAME="apache-hive-${HIVE_VERSION}-bin.tar.gz" # Must export this variable for Dockerfile
HIVE_DOWNLOAD_URL="https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/${HIVE_PACKAGE_NAME}"

JDBC_DIVER_PACKAGE_NAME="mysql-connector-java-${MYSQL_JDBC_DRIVER_VERSION}.tar.gz" # Must export this variable for Dockerfile
JDBC_DIVER_DOWNLOAD_URL="https://downloads.mysql.com/archives/get/p/3/file/${JDBC_DIVER_PACKAGE_NAME}"

# Prepare download packages
if [[ ! -d "${hive_dir}/packages" ]]; then
  mkdir -p "${hive_dir}/packages"
fi

if [ ! -f "${hive_dir}/packages/${HADOOP_PACKAGE_NAME}" ]; then
  curl -L -s -o "${hive_dir}/packages/${HADOOP_PACKAGE_NAME}" ${HADOOP_DOWNLOAD_URL}
fi

if [ ! -f "${hive_dir}/packages/${HIVE_PACKAGE_NAME}" ]; then
  curl -L -s -o "${hive_dir}/packages/${HIVE_PACKAGE_NAME}" ${HIVE_DOWNLOAD_URL}
fi

if [ ! -f "${hive_dir}/packages/${JDBC_DIVER_PACKAGE_NAME}" ]; then
  curl -L -s -o "${hive_dir}/packages/${JDBC_DIVER_PACKAGE_NAME}" ${JDBC_DIVER_DOWNLOAD_URL}
fi
