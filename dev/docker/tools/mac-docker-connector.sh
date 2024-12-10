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
  curl -s -L -o "${bin}/${DOCKER_CONNECTOR_PACKAGE_NAME}" ${DOCKER_CONNECTOR_DOWNLOAD_URL}
  tar -xzf "${bin}/${DOCKER_CONNECTOR_PACKAGE_NAME}" -C "${bin}"
  rm -rf "${bin}/${DOCKER_CONNECTOR_PACKAGE_NAME}"
fi

# Start the Docker image on which the docker-connector server depends.
docker stop desktop-connector 2>/dev/null
docker rm desktop-connector 2>/dev/null
docker run -it -d --restart always --net host --cap-add NET_ADMIN --name desktop-connector wenjunxiao/desktop-docker-connector

# Start docker-connector server
echo "Start docker-connector requires root privileges, Please enter the root password."
sudo ${bin}/docker-connector -config ${bin}/docker-connector.conf
