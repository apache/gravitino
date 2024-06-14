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
RANGER_PACKAGE_NAME="ranger-${RANGER_VERSION}-admin.tar.gz" # Must export this variable for Dockerfile
RANGER_DOWNLOAD_URL=https://github.com/unknowntpo/apache-ranger/releases/download/release-ranger-${RANGER_VERSION}/ranger-${RANGER_VERSION}-admin.tar.gz

MYSQL_CONNECTOR_VERSION=8.0.28
MYSQL_CONNECTOR_PACKAGE_NAME="mysql-connector-java-${MYSQL_CONNECTOR_VERSION}.jar"
MYSQL_CONNECTOR_DOWNLOAD_URL=https://search.maven.org/remotecontent?filepath=mysql/mysql-connector-java/${MYSQL_CONNECTOR_VERSION}/mysql-connector-java-${MYSQL_CONNECTOR_VERSION}.jar

LOG4JDBC_VERSION=8.0.28
LOG4JDBC_PACKAGE_NAME="log4jdbc-${LOG4JDBC_VERSION}.jar"
LOG4JDBC_DOWNLOAD_URL=https://repo1.maven.org/maven2/com/googlecode/log4jdbc/log4jdbc/${LOG4JDBC_VERSION}/log4jdbc-${LOG4JDBC_VERSION}.jar

# Prepare download packages
if [[ ! -d "${ranger_dir}/packages" ]]; then
  mkdir -p "${ranger_dir}/packages"
fi

if [ ! -f "${ranger_dir}/packages/${RANGER_PACKAGE_NAME}" ]; then
  curl -L -s -o "${ranger_dir}/packages/${RANGER_PACKAGE_NAME}" ${RANGER_DOWNLOAD_URL}
fi

if [ ! -f "${ranger_dir}/packages/${MYSQL_CONNECTOR_PACKAGE_NAME}" ]; then
  curl -L -s -o "${ranger_dir}/packages/${MYSQL_CONNECTOR_PACKAGE_NAME}" ${MYSQL_CONNECTOR_DOWNLOAD_URL}
fi

if [ ! -f "${ranger_dir}/packages/${LOG4JDBC_PACKAGE_NAME}" ]; then
  curl -L -s -o "${ranger_dir}/packages/${LOG4JDBC_PACKAGE_NAME}" ${LOG4JDBC_DOWNLOAD_URL}
fi
