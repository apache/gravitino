#!/bin/bash
#
# Copyright 2023 Datastrato.
# This software is licensed under the Apache License version 2.
#
#set -ex

bin="$(dirname "${BASH_SOURCE-$0}")"
bin="$(cd "${bin}">/dev/null; pwd)"

OS=$(uname -s)
if [ "${OS}" != "Darwin" ]; then
  echo "Only macOS needs to run mac-docker-connector."
  exit 1
fi

if pgrep -xq "docker-connector"; then
  echo "docker-connector is running."
  exit 1
fi

# Download docker-connector
DOCKER_CONNECTOR_PACKAGE_NAME="docker-connector-darwin.tar.gz"
DOCKER_CONNECTOR_DOWNLOAD_URL="https://github.com/wenjunxiao/mac-docker-connector/releases/download/v3.2/${DOCKER_CONNECTOR_PACKAGE_NAME}"
if [ ! -f "${bin}/docker-connector" ]; then
  wget -q -P "${bin}" ${DOCKER_CONNECTOR_DOWNLOAD_URL}
  tar -xzf "${bin}/${DOCKER_CONNECTOR_PACKAGE_NAME}" -C "${bin}"
  rm -rf "${bin}/${DOCKER_CONNECTOR_PACKAGE_NAME}"
fi

# Create a docker-connector.conf file with the routes to the docker networks
if [ ! -f "${bin}/docker-connector.conf" ]; then
  docker network ls --filter driver=bridge --format "{{.ID}}" | xargs docker network inspect --format "route {{range .IPAM.Config}}{{.Subnet}}{{end}}" > ${bin}/docker-connector.conf
fi

echo "Start docker-connector requires root privileges, Please enter the root password."
sudo ${bin}/docker-connector -config ${bin}/docker-connector.conf