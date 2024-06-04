#!/bin/bash
#
# Copyright 2024 Datastrato Pvt Ltd.
# This software is licensed under the Apache License version 2.
#
set -ex
ranger_dir="$(dirname "${BASH_SOURCE-$0}")"
ranger_dir="$(cd "${ranger_dir}">/dev/null; pwd)"

# Environment variables definition
RANGER_VERSION=2.4.0
RANGER_PACKAGE_NAME="ranger-distro-${RANGER_VERSION}-admin.tar.gz" # Must export this variable for Dockerfile
RANGER_DOWNLOAD_URL="https://repo.maven.apache.org/maven2/org/apache/ranger/ranger-distro/${RANGER_VERSION}/ranger-distro-${RANGER_VERSION}-admin.tar.gz"

# Prepare download packages
if [[ ! -d "${ranger_dir}/packages" ]]; then
  mkdir -p "${ranger_dir}/packages"
fi

if [ ! -f "${ranger_dir}/packages/${RANGER_PACKAGE_NAME}" ]; then
  curl -L -s -o "${ranger_dir}/packages/${RANGER_PACKAGE_NAME}" ${RANGER_DOWNLOAD_URL}
fi
