#!/bin/bash
#
# Copyright 2023 Datastrato.
# This software is licensed under the Apache License version 2.
#
set -ex
hive_dir="$(dirname "${BASH_SOURCE-$0}")"
hive_dir="$(cd "${hive_dir}">/dev/null; pwd)"

# Environment variables definition
HADOOP_VERSION="2.7.3"
HIVE_VERSION="2.3.9"

HADOOP_PACKAGE_NAME="hadoop-${HADOOP_VERSION}.tar.gz" # Must export this variable for Dockerfile
HADOOP_DOWNLOAD_URL="http://archive.apache.org/dist/hadoop/core/hadoop-${HADOOP_VERSION}/${HADOOP_PACKAGE_NAME}"

HIVE_PACKAGE_NAME="apache-hive-${HIVE_VERSION}-bin.tar.gz" # Must export this variable for Dockerfile
HIVE_DOWNLOAD_URL="https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/${HIVE_PACKAGE_NAME}"

# Prepare download packages
if [[ ! -d "${hive_dir}/packages" ]]; then
  mkdir -p "${hive_dir}/packages"
fi

if [ ! -f "${hive_dir}/packages/${HADOOP_PACKAGE_NAME}" ]; then
  curl -s -o "${hive_dir}/packages/${HADOOP_PACKAGE_NAME}" ${HADOOP_DOWNLOAD_URL}
fi

if [ ! -f "${hive_dir}/packages/${HIVE_PACKAGE_NAME}" ]; then
  curl -s -o "${hive_dir}/packages/${HIVE_PACKAGE_NAME}" ${HIVE_DOWNLOAD_URL}
fi
