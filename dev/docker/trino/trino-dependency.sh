#!/bin/bash
#
# Copyright 2023 Datastrato.
# This software is licensed under the Apache License version 2.
#
set -ex

trino_dir="$(dirname "${BASH_SOURCE-$0}")"
trino_dir="$(cd "${trino_dir}">/dev/null; pwd)"
gravitino_home="$(cd "${trino_dir}/../../..">/dev/null; pwd)"

if [[ ! -d "${trino_dir}/packages/trino" ]]; then
  mkdir -p "${trino_dir}/packages/trino"
fi

cd ${gravitino_home}
${gravitino_home}/gradlew clean assembleTrinoConnector -x test
rm -rf "${trino_dir}/packages/gravitino-trino-connector"
cp -r "${gravitino_home}/distribution/gravitino-trino-connector" "${trino_dir}/packages/gravitino-trino-connector"

rm -rf "${trino_dir}/packages/trino/conf"
cp -r -p "${gravitino_home}/dev/docker/trino/conf" "${trino_dir}/packages/trino/conf"

# Set default gravitino metelake `gravitino.metalake = metalake_demo` in the `conf/catalog/gravitino.properties`
# gravitino.uri = http://GRAVITINO_HOST_IP:GRAVITINO_HOST_PORT
sed 's/GRAVITINO_HOST_IP:GRAVITINO_HOST_PORT/gravitino:8090/g' "${trino_dir}/packages/trino/conf/catalog/gravitino.properties.template" > "${trino_dir}/packages/trino/conf/catalog/gravitino.properties.1"
# Set default hive docker container hostname and port `hive.metastore.uri = thrift://hive:9083` in the `trino/conf/catalog/hive.properties`
# gravitino.metalake = GRAVITINO_METALAKE_NAME
sed 's/GRAVITINO_METALAKE_NAME/metalake_demo/g' "${trino_dir}/packages/trino/conf/catalog/gravitino.properties.1" > "${trino_dir}/packages/trino/conf/catalog/gravitino.properties"
rm "${trino_dir}/packages/trino/conf/catalog/gravitino.properties.1"

# Set default hive docker container hostname and port `hive.metastore.uri = thrift://hive:9083` in the `trino/conf/catalog/hive.properties`
# hive.metastore.uri = thrift://HIVE_HOST_IP:9083
sed 's/HIVE_HOST_IP/hive/g' "${trino_dir}/packages/trino/conf/catalog/hive.properties.template" > "${trino_dir}/packages/trino/conf/catalog/hive.properties"
