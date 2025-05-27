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
ranger_dir="${hive_dir}/../ranger"

# Environment variables definition
HADOOP2_VERSION="2.7.3"
HADOOP3_VERSION="3.3.0"

HIVE2_VERSION="2.3.9"
HIVE3_VERSION="3.1.3"
MYSQL_JDBC_DRIVER_VERSION=${MYSQL_VERSION:-"8.0.15"}
ZOOKEEPER_VERSION=${ZOOKEEPER_VERSION:-"3.4.13"}
RANGER_VERSION="2.4.0" # Notice: Currently only tested Ranger plugin 2.4.0 in the Hadoop 3.1.0 and Hive 3.1.3

HADOOP2_PACKAGE_NAME="hadoop-${HADOOP2_VERSION}.tar.gz"
HADOOP2_DOWNLOAD_URL="https://archive.apache.org/dist/hadoop/core/hadoop-${HADOOP2_VERSION}/${HADOOP2_PACKAGE_NAME}"
HADOOP2_GCS_PACKAGE_NAME="gcs-connector-hadoop2-2.2.23-shaded.jar"
HADOOP2_GCS_DOWNLOAD_URL="https://github.com/GoogleCloudDataproc/hadoop-connectors/releases/download/v2.2.23/gcs-connector-hadoop2-2.2.23-shaded.jar"

HADOOP3_PACKAGE_NAME="hadoop-${HADOOP3_VERSION}.tar.gz"
HADOOP3_DOWNLOAD_URL="https://archive.apache.org/dist/hadoop/core/hadoop-${HADOOP3_VERSION}/${HADOOP3_PACKAGE_NAME}"
HADOOP3_GCS_PACKAGE_NAME="gcs-connector-hadoop3-2.2.23-shaded.jar"
HADOOP3_GCS_DOWNLOAD_URL="https://github.com/GoogleCloudDataproc/hadoop-connectors/releases/download/v2.2.23/gcs-connector-hadoop3-2.2.23-shaded.jar"

HIVE2_PACKAGE_NAME="apache-hive-${HIVE2_VERSION}-bin.tar.gz"
HIVE2_DOWNLOAD_URL="https://archive.apache.org/dist/hive/hive-${HIVE2_VERSION}/${HIVE2_PACKAGE_NAME}"

HIVE3_PACKAGE_NAME="apache-hive-${HIVE3_VERSION}-bin.tar.gz"
HIVE3_DOWNLOAD_URL="https://archive.apache.org/dist/hive/hive-${HIVE3_VERSION}/${HIVE3_PACKAGE_NAME}"

JDBC_DIVER_PACKAGE_NAME="mysql-connector-java-${MYSQL_JDBC_DRIVER_VERSION}.tar.gz"
JDBC_DIVER_DOWNLOAD_URL="https://downloads.mysql.com/archives/get/p/3/file/${JDBC_DIVER_PACKAGE_NAME}"

ZOOKEEPER_PACKAGE_NAME="zookeeper-${ZOOKEEPER_VERSION}.tar.gz"
ZOOKEEPER_DOWNLOAD_URL="https://archive.apache.org/dist/zookeeper/zookeeper-${ZOOKEEPER_VERSION}/${ZOOKEEPER_PACKAGE_NAME}"

RANGER_HIVE_PACKAGE_NAME="ranger-${RANGER_VERSION}-hive-plugin.tar.gz"

RANGER_HDFS_PACKAGE_NAME="ranger-${RANGER_VERSION}-hdfs-plugin.tar.gz"

# Prepare download packages
if [[ ! -d "${hive_dir}/packages" ]]; then
  mkdir -p "${hive_dir}/packages"
fi

if [ ! -f "${hive_dir}/packages/${HADOOP2_PACKAGE_NAME}" ]; then
  curl -L -s -o "${hive_dir}/packages/${HADOOP2_PACKAGE_NAME}" ${HADOOP2_DOWNLOAD_URL}
fi

if [ ! -f "${hive_dir}/packages/${HADOOP3_PACKAGE_NAME}" ]; then
  curl -L -s -o "${hive_dir}/packages/${HADOOP3_PACKAGE_NAME}" ${HADOOP3_DOWNLOAD_URL}
fi

if [ ! -f "${hive_dir}/packages/${HIVE2_PACKAGE_NAME}" ]; then
  curl -L -s -o "${hive_dir}/packages/${HIVE2_PACKAGE_NAME}" ${HIVE2_DOWNLOAD_URL}
fi

if [ ! -f "${hive_dir}/packages/${HIVE3_PACKAGE_NAME}" ]; then
  curl -L -s -o "${hive_dir}/packages/${HIVE3_PACKAGE_NAME}" ${HIVE3_DOWNLOAD_URL}
fi

if [ ! -f "${hive_dir}/packages/${JDBC_DIVER_PACKAGE_NAME}" ]; then
  curl -L -s -o "${hive_dir}/packages/${JDBC_DIVER_PACKAGE_NAME}" ${JDBC_DIVER_DOWNLOAD_URL}
fi

if [ ! -f "${hive_dir}/packages/${ZOOKEEPER_PACKAGE_NAME}" ]; then
  curl -L -s -o "${hive_dir}/packages/${ZOOKEEPER_PACKAGE_NAME}" ${ZOOKEEPER_DOWNLOAD_URL}
fi

if [ ! -f "${hive_dir}/packages/${RANGER_HDFS_PACKAGE_NAME}" ]; then
  # ranger-hdfs plugin not exist, run ranger-dependency.sh to build from source
  . ${ranger_dir}/ranger-dependency.sh
  cp "${ranger_dir}/packages/${RANGER_HDFS_PACKAGE_NAME}" "${hive_dir}/packages"
fi

if [[ $? -ne 0 ]]; then
  echo "Failed to download Ranger HDFS plugin package"
  exit 1
fi

if [ ! -f "${hive_dir}/packages/${RANGER_HIVE_PACKAGE_NAME}" ]; then
  # ranger-hive plugin not exist, run ranger-dependency.sh to build from source
  . ${ranger_dir}/ranger-dependency.sh
  cp "${ranger_dir}/packages/${RANGER_HIVE_PACKAGE_NAME}" "${hive_dir}/packages"
fi

if [[ $? -ne 0 ]]; then
  echo "Failed to download Ranger Hive plugin package"
  exit 1
fi

if [ ! -f "${hive_dir}/packages/${HADOOP2_GCS_PACKAGE_NAME}" ]; then
  curl -L -s -o "${hive_dir}/packages/${HADOOP2_GCS_PACKAGE_NAME}" ${HADOOP2_GCS_DOWNLOAD_URL}
fi

if [ ! -f "${hive_dir}/packages/${HADOOP3_GCS_PACKAGE_NAME}" ]; then
  curl -L -s -o "${hive_dir}/packages/${HADOOP3_GCS_PACKAGE_NAME}" ${HADOOP3_GCS_DOWNLOAD_URL}
fi
