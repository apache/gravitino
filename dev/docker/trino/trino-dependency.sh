#!/bin/bash
#
# Copyright 2023 DATASTRATO Pvt Ltd.
# This software is licensed under the Apache License version 2.
#
set -ex

trino_dir="$(dirname "${BASH_SOURCE-$0}")"
trino_dir="$(cd "${trino_dir}">/dev/null; pwd)"
gravitino_home="$(cd "${trino_dir}/../../..">/dev/null; pwd)"

# Clean packages
rm -rf "${trino_dir}/packages"
mkdir -p "${trino_dir}/packages"

cd ${gravitino_home}
${gravitino_home}/gradlew clean assembleTrinoConnector -x test
cp -r "${gravitino_home}/distribution/gravitino-trino-connector" "${trino_dir}/packages/gravitino-trino-connector"

MYSQL_VERSION="8.0.27"
PG_VERSION="42.7.0"
MYSQL_JAVA_CONNECTOR_URL="https://repo1.maven.org/maven2/mysql/mysql-connector-java/${MYSQL_VERSION}/mysql-connector-java-${MYSQL_VERSION}.jar"
PG_JAVA_CONNECTOR_URL="https://jdbc.postgresql.org/download/postgresql-${PG_VERSION}.jar"

# Download MySQL jdbc driver if it does not exist.
if [ ! -f "${trino_dir}/packages/gravitino-trino-connector/mysql-connector-java-${MYSQL_VERSION}.jar" ]; then
  cd "${trino_dir}/packages/gravitino-trino-connector/" && curl -O "${MYSQL_JAVA_CONNECTOR_URL}" && cd -
fi

# Download PostgreSQL jdbc driver if it does not exist.
if [ ! -f "${trino_dir}/packages/gravitino-trino-connector/postgresql-{PG_VERSION}.jar" ]; then
  cd "${trino_dir}/packages/gravitino-trino-connector/" && curl -O "$PG_JAVA_CONNECTOR_URL" && cd -
fi

mkdir -p "${trino_dir}/packages/trino"
cp -r -p "${trino_dir}/conf" "${trino_dir}/packages/trino/conf"
