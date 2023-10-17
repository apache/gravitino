#!/bin/bash
#
# Copyright 2023 Datastrato.
# This software is licensed under the Apache License version 2.
#
set -ex
bin="$(dirname "${BASH_SOURCE-$0}")"
bin="$(cd "${bin}">/dev/null; pwd)"

# Environment variables definition
HADOOP_VERSION="2.7.3"
HIVE_VERSION="2.3.9"

HADOOP_PACKAGE_NAME="hadoop-${HADOOP_VERSION}.tar.gz"
HADOOP_DOWNLOAD_URL="http://archive.apache.org/dist/hadoop/core/hadoop-${HADOOP_VERSION}/${HADOOP_PACKAGE_NAME}"

HIVE_PACKAGE_NAME="apache-hive-${HIVE_VERSION}-bin.tar.gz"
HIVE_DOWNLOAD_URL="https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/${HIVE_PACKAGE_NAME}"

# Build docker image for multi-arch
USAGE="-e Usage: ./build-docker.sh --platform [all|linux/amd64|linux/arm64] --image {image_name} --tag {tag_name} --latest"

# Get platform type
if [[ "$1" == "--platform" ]]; then
  shift
  platform_type="$1"
  if [[ "${platform_type}" == "linux/amd64" || "${platform_type}" == "linux/arm64" || "${platform_type}" == "all" ]]; then
    echo "INFO : platform type is ${platform_type}"
  else
    echo "ERROR : ${platform_type} is not a valid platform type"
    echo ${USAGE}
    exit 1
  fi
  shift
else
  platform_type="all"
fi

# Get docker image name
if [[ "$1" == "--image" ]]; then
  shift
  image_name="$1"
  shift
else
  echo "ERROR : must specify image name"
  echo ${USAGE}
  exit 1
fi

# Get docker image tag
if [[ "$1" == "--tag" ]]; then
  shift
  tag_name="$1"
  shift
fi

# Get latest flag
buildLatest=0
if [[ "$1" == "--latest" ]]; then
  shift
  buildLatest=1
fi

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
BUILDER_NAME="hive-builder"
builders=$(docker buildx ls)
if echo "${builders}" | grep -q "${BUILDER_NAME}"; then
  echo "BuildKit builder '${BUILDER_NAME}' already exists."
else
  echo "BuildKit builder '${BUILDER_NAME}' does not exist."
  docker buildx create --platform linux/amd64,linux/arm64 --use --name ${BUILDER_NAME}
fi

cd ${bin}
if [[ "${platform_type}" == "all" ]]; then
  if [ ${buildLatest} -eq 1 ]; then
    docker buildx build --platform=linux/amd64,linux/arm64 --build-arg HADOOP_PACKAGE_NAME=${HADOOP_PACKAGE_NAME} --build-arg HIVE_PACKAGE_NAME=${HIVE_PACKAGE_NAME} --push --tag ${image_name}:latest --progress plain -f Dockerfile -t ${image_name}:${tag_name} .
  else
    docker buildx build --platform=linux/amd64,linux/arm64 --build-arg HADOOP_PACKAGE_NAME=${HADOOP_PACKAGE_NAME} --build-arg HIVE_PACKAGE_NAME=${HIVE_PACKAGE_NAME} --push --tag ${tag_name} --progress plain -f Dockerfile -t ${image_name} .
  fi
else
  if [ ${buildLatest} -eq 1 ]; then
    docker buildx build --platform=${platform_type} --build-arg HADOOP_PACKAGE_NAME=${HADOOP_PACKAGE_NAME} --build-arg HIVE_PACKAGE_NAME=${HIVE_PACKAGE_NAME} --output type=docker --tag ${image_name}:latest --progress plain -f Dockerfile -t ${image_name}:${tag_name} .
  else
    docker buildx build --platform=${platform_type} --build-arg HADOOP_PACKAGE_NAME=${HADOOP_PACKAGE_NAME} --build-arg HIVE_PACKAGE_NAME=${HIVE_PACKAGE_NAME} --output type=docker --progress plain -f Dockerfile -t ${image_name}:${tag_name} .
  fi
fi