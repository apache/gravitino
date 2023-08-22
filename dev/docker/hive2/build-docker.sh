#!/bin/bash
#
# Copyright 2023 Datastrato.
# This software is licensed under the Apache License version 2.
#
set -ex
bin="$(dirname "${BASH_SOURCE-$0}")"
bin="$(cd "${bin}">/dev/null; pwd)"

# Prepare download packages
hadoop_package_name="hadoop-2.7.3.tar.gz"
hadoop_download_url="http://archive.apache.org/dist/hadoop/core/hadoop-2.7.3/${hadoop_package_name}"

hive_package_name="apache-hive-2.3.9-bin.tar.gz"
hive_download_url="https://archive.apache.org/dist/hive/hive-2.3.9/${hive_package_name}"

if [[ ! -d "${bin}/packages" ]]; then
  mkdir -p "${bin}/packages"
fi

if [ ! -f "${bin}/packages/${hadoop_package_name}" ]; then
  curl -s -o "${bin}/packages/${hadoop_package_name}" ${hadoop_download_url}
fi

if [ ! -f "${bin}/packages/${hive_package_name}" ]; then
  curl -s -o "${bin}/packages/${hive_package_name}" ${hive_download_url}
fi

# Create multi-arch builder
BUILDER_NAME="hive2"
builders=$(docker buildx ls)
if echo "${builders}" | grep -q "${BUILDER_NAME}"; then
  echo "BuildKit builder '${BUILDER_NAME}' already exists."
else
  echo "BuildKit builder '${BUILDER_NAME}' does not exist."
  docker buildx create --platform linux/amd64,linux/arm64 --use --name hive2
fi

# Option params --no-cache --push
docker buildx build --platform=linux/amd64,linux/arm64 --output type=docker --progress plain -t datastrato/hive2:0.1.1 .
