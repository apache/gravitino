#!/bin/bash
#
# Copyright 2023 Datastrato.
# This software is licensed under the Apache License version 2.
#
set -ex
bin="$(dirname "${BASH_SOURCE-$0}")"
bin="$(cd "${bin}">/dev/null; pwd)"

# Environment variables definition
IMAGE_NAME="datastrato/hive2:0.1.0"
HADOOP_VERSION="2.7.3"
hive_version="2.3.9"

HADOOP_PACKAGE_NAME="hadoop-${HADOOP_VERSION}.tar.gz"
HADOOP_DOWNLOAD_URL="http://archive.apache.org/dist/hadoop/core/hadoop-${HADOOP_VERSION}/${HADOOP_PACKAGE_NAME}"

HIVE_PACKAGE_NAME="apache-hive-${HIVE_VERSION}-bin.tar.gz"
HIVE_DOWNLOAD_URL="https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/${HIVE_PACKAGE_NAME}"

# Prepare download packages
if [[ ! -d "${bin}/packages" ]]; then
  mkdir -p "${bin}/packages"
fi

if [ ! -f "${bin}/packages/${HADOOP_PACKAGE_NAME}" ]; then
  curl -s -o "${bin}/packages/${HADOOP_PACKAGE_NAME}" ${HADOOP_DOWNLOAD_URL}
fi

if [ ! -f "${bin}/packages/${HIVE_PACKAGE_NAME}" ]; then
  curl -s -o "${bin}/packages/${HIVE_PACKAGE_NAME}" ${HIVE_DOWNLOAD_URL}
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
docker buildx build --platform=linux/amd64,linux/arm64 --build-arg HADOOP_PACKAGE_NAME=${HADOOP_PACKAGE_NAME} --build-arg HIVE_PACKAGE_NAME=${HIVE_PACKAGE_NAME} --output type=docker --progress plain -t ${IMAGE_NAME} .
