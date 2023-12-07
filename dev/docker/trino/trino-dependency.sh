#!/bin/bash
#
# Copyright 2023 Datastrato.
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

# Download MySQL jdbc driver
cd "${trino_dir}/packages/gravitino-trino-connector/" && curl -O "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar" && cd -

# Download PostgreSQL jdbc driver
cd "${trino_dir}/packages/gravitino-trino-connector/" && curl -O "https://jdbc.postgresql.org/download/postgresql-42.7.0.jar" && cd -

mkdir -p "${trino_dir}/packages/trino"
cp -r -p "${trino_dir}/conf" "${trino_dir}/packages/trino/conf"
