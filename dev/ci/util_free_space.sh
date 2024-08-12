#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Copied from Apache Arrow: ci/scripts/util_free_space.sh

set -eux

if [ "${GITHUB_ACTIONS}" = "true" ]; then
  df -h
  # ~1GB (From 1.2GB to 214MB)
  sudo rm -rf \
    /usr/local/bin/aliyun \
    /usr/local/bin/azcopy \
    /usr/local/bin/bicep \
    /usr/local/bin/cmake-gui \
    /usr/local/bin/cpack \
    /usr/local/bin/helm \
    /usr/local/bin/hub \
    /usr/local/bin/kubectl \
    /usr/local/bin/minikube \
    /usr/local/bin/node \
    /usr/local/bin/packer \
    /usr/local/bin/pulumi* \
    /usr/local/bin/stack \
    /usr/local/bin/terraform || :
  # 1.3GB
  sudo rm -rf /usr/local/share/powershell || :
  # 5.3GB
  sudo rm -rf /opt/hostedtoolcache/CodeQL || :
  # 1.4GB
  sudo rm -rf /opt/hostedtoolcache/go || :
  # 489MB
  sudo rm -rf /opt/hostedtoolcache/PyPy || :
  # 376MB
  sudo rm -rf /opt/hostedtoolcache/node || :
  # Free at least 10G, android is useless for Gravitino CI.
  sudo rm -rf /usr/local/lib/android || :
  # Free at lease 5G, .ghcup is installed by Haskell CI and useless for Gravitino CI.
  sudo rm -rf /usr/local/.ghcup || :
  # Remove Web browser packages
  if dpkg-query -l firefox;then
    sudo apt purge -y firefox
  fi
  if dpkg-query -l google-chrome-stable;then
    sudo apt purge -y google-chrome-stable
  fi
  if dpkg-query -l microsoft-edge-stable;then
    sudo apt purge -y microsoft-edge-stable
  fi
  df -h
fi