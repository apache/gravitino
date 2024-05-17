#!/bin/bash
#
# Copyright 2024 Datastrato Pvt Ltd.
# This software is licensed under the Apache License version 2.
#
set -ex
hadoop_dir="$(dirname "${BASH_SOURCE-$0}")"
hadoop_dir="$(cd "${hadoop_dir}">/dev/null; pwd)"

# Environment variables definition
HADOOP_VERSION="3.1.0"

HADOOP_PACKAGE_NAME="hadoop-${HADOOP_VERSION}.tar.gz" # Must export this variable for Dockerfile
HADOOP_DOWNLOAD_URL="https://archive.apache.org/dist/hadoop/core/hadoop-${HADOOP_VERSION}/${HADOOP_PACKAGE_NAME}"

# Prepare download packages
if [[ ! -d "${hadoop_dir}/packages" ]]; then
  mkdir -p "${hadoop_dir}/packages"
fi

if [ ! -f "${hadoop_dir}/packages/${HADOOP_PACKAGE_NAME}" ]; then
  curl --progress-bar -L -s -o "${hadoop_dir}/packages/${HADOOP_PACKAGE_NAME}" ${HADOOP_DOWNLOAD_URL}
fi
