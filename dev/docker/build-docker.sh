#!/bin/bash
#
# Copyright 2023 Datastrato.
# This software is licensed under the Apache License version 2.
#
#set -ex
script_dir="$(dirname "${BASH_SOURCE-$0}")"
script_dir="$(cd "${script_dir}">/dev/null; pwd)"
# Build docker image for multi-arch
# shellcheck disable=SC2089

# Create multi-arch builder
BUILDER_NAME="gravitino-builder"
builders=$(docker buildx ls)
if echo "${builders}" | grep -q "${BUILDER_NAME}"; then
  echo "BuildKit builder '${BUILDER_NAME}' already exists."
else
  echo "BuildKit builder '${BUILDER_NAME}' does not exist."
  docker buildx create --platform linux/amd64,linux/arm64 --use --name ${BUILDER_NAME}
fi

cd ${script_dir}/authorization-server/
docker buildx build --no-cache --pull --platform=linux/amd64,linux/arm64 --push --progress plain -f Dockerfile -t sample-authorization-server:0.3.0 .