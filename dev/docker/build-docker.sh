#!/bin/bash
#
# Copyright 2023 Datastrato Pvt Ltd.
# This software is licensed under the Apache License version 2.
#
#set -ex
script_dir="$(dirname "${BASH_SOURCE-$0}")"
script_dir="$(cd "${script_dir}">/dev/null; pwd)"
# Build docker image for multi-arch
# shellcheck disable=SC2089

usage() {
  cat << EOF
Usage:

./build-docker.sh --platform [all|linux/amd64|linux/arm64] --type [gravitino|hive|trino|doris] --image {image_name} --tag {tag_name} --latest

Notice: You shouldn't use 'all' for the platform if you don't use the Github action to publish the Docker image.
EOF
}

# Get platform type
if [[ "$1" == "--platform" ]]; then
  shift
  platform_type="$1"
  if [[ "${platform_type}" == "linux/amd64" || "${platform_type}" == "linux/arm64" || "${platform_type}" == "all" ]]; then
    echo "INFO : platform type is ${platform_type}"
  else
    echo "ERROR : ${platform_type} is not a valid platform type"
    usage
    exit 1
  fi
  shift
else
  platform_type="all"
fi

# Get component type
if [[ "$1" == "--type" ]]; then
  shift
  component_type="$1"
  shift
else
  echo "ERROR : must specify component type"
  usage
  exit 1
fi

# Get docker image name
if [[ "$1" == "--image" ]]; then
  shift
  image_name="$1"
  shift
else
  echo "ERROR : must specify image name"
  usage
  exit 1
fi

# Get docker image tag
if [[ "$1" == "--tag" ]]; then
  shift
  tag_name="$1"
  shift
fi

# Get latest flag
build_latest=0
if [[ "$1" == "--latest" ]]; then
  shift
  build_latest=1
fi

if [[ "${component_type}" == "hive" ]]; then
  . ${script_dir}/hive/hive-dependency.sh
  build_args="--build-arg HADOOP_PACKAGE_NAME=${HADOOP_PACKAGE_NAME} --build-arg HIVE_PACKAGE_NAME=${HIVE_PACKAGE_NAME} --build-arg JDBC_DIVER_PACKAGE_NAME=${JDBC_DIVER_PACKAGE_NAME}"
elif [ "${component_type}" == "trino" ]; then
  . ${script_dir}/trino/trino-dependency.sh
elif [ "${component_type}" == "gravitino" ]; then
  . ${script_dir}/gravitino/gravitino-dependency.sh
elif [ "${component_type}" == "doris" ]; then
  . ${script_dir}/doris/doris-dependency.sh --platform ${platform_type}
  build_args="--build-arg DORIS_VERSION=${DORIS_VERSION}"
else
  echo "ERROR : ${component_type} is not a valid component type"
  usage
  exit 1
fi

build_args="${build_args} --build-arg IMAGE_NAME=${image_name}"

# Create multi-arch builder
BUILDER_NAME="gravitino-builder"
builders=$(docker buildx ls)
if echo "${builders}" | grep -q "${BUILDER_NAME}"; then
  echo "BuildKit builder '${BUILDER_NAME}' already exists."
else
  echo "BuildKit builder '${BUILDER_NAME}' does not exist."
  docker buildx create --platform linux/amd64,linux/arm64 --use --name ${BUILDER_NAME}
fi

cd ${script_dir}/${component_type}
if [[ "${platform_type}" == "all" ]]; then
  if [ ${build_latest} -eq 1 ]; then
    docker buildx build --no-cache --pull --platform=linux/amd64,linux/arm64 ${build_args} --push --progress plain -f Dockerfile -t ${image_name}:latest -t ${image_name}:${tag_name} .
  else
    docker buildx build --no-cache --pull --platform=linux/amd64,linux/arm64 ${build_args} --push --progress plain -f Dockerfile -t ${image_name}:${tag_name} .
  fi
else
  if [ ${build_latest} -eq 1 ]; then
    docker buildx build --no-cache --pull --platform=${platform_type} ${build_args} --output type=docker --progress plain -f Dockerfile -t ${image_name}:latest -t ${image_name}:${tag_name} .
  else
    docker buildx build --no-cache --pull --platform=${platform_type} ${build_args} --output type=docker --progress plain -f Dockerfile -t ${image_name}:${tag_name} .
  fi
fi
