#!/bin/bash
#
# Copyright 2024 Datastrato Pvt Ltd.
# This software is licensed under the Apache License version 2.
#

doris_dir="$(dirname "${BASH_SOURCE-$0}")"
doris_dir="$(cd "${doris_dir}">/dev/null; pwd)"

TARGET_ARCH=""

# Get platform type
if [[ "$1" == "--platform" ]]; then
  shift
  platform_type="$1"
  if [[ "${platform_type}" == "linux/amd64" ]]; then
    echo "INFO : doris build platform type is ${platform_type}"
    TARGET_ARCH="x64"
  elif [[ "${platform_type}" == "linux/arm64" ]]; then
    echo "INFO : doris build platform type is ${platform_type}"
    TARGET_ARCH="arm64"
  elif [[ "${platform_type}" == "all" ]]; then
    echo "INFO : doris build platform type is ${platform_type}"
    TARGET_ARCH="all"
  else
    echo "ERROR : ${platform_type} is not a valid platform type for doris"
    usage
    exit 1
  fi
  shift
else
  TARGET_ARCH="all"
fi

# Environment variables definition
DORIS_VERSION="1.2.7.1"

download_and_check() {
  local arch="${1}"
  # Download doris package
  DORIS_PACKAGE_NAME="apache-doris-${DORIS_VERSION}-bin-${arch}"
  DORIS_FILE_NAME="${DORIS_PACKAGE_NAME}.tar.xz"
  DORIS_DOWNLOAD_URL="https://apache-doris-releases.oss-accelerate.aliyuncs.com/${DORIS_FILE_NAME}"
  SHA512SUMS_URL="${DORIS_DOWNLOAD_URL}.sha512"

  # Prepare download packages
  if [[ ! -d "${doris_dir}/packages" ]]; then
    mkdir -p "${doris_dir}/packages"
  fi

  if [[ ! -f "${doris_dir}/packages/${DORIS_FILE_NAME}" ]]; then
    echo "INFO : Downloading doris package ${DORIS_FILE_NAME}"
    curl -s -o "${doris_dir}/packages/${DORIS_FILE_NAME}" ${DORIS_DOWNLOAD_URL}
    echo "INFO : Downloading doris package done"
  fi

  # download sha512sum file
  if [[ ! -f "${doris_dir}/packages/${DORIS_FILE_NAME}.sha512" ]]; then
    curl -s -o "${doris_dir}/packages/${DORIS_FILE_NAME}.sha512" ${SHA512SUMS_URL}
  fi


  cd "${doris_dir}/packages" || exit 1

  # check sha512sum, if check file failed, exit 1
  echo "INFO : Checking sha512sum for ${DORIS_FILE_NAME}"
  if command -v shasum &>/dev/null; then
    shasum -c "${DORIS_FILE_NAME}.sha512"
  elif command -v sha512sum &>/dev/null; then
    sha512sum -c "${DORIS_FILE_NAME}.sha512"
  else
    cat << EOF
    WARN: cannot find shasum or sha512sum command, skip sha512sum check, please check the sha512sum by yourself.
    if build Docker image failed, maybe the package is broken.
EOF
  fi

  if [ $? -ne 0 ]; then
    echo "ERROR: check doris package sha512sum failed"
    exit 1
  fi
}

if [[ "${TARGET_ARCH}" == "all" ]]; then
  download_and_check "x64"
  download_and_check "arm64"
else
  download_and_check "${TARGET_ARCH}"
fi