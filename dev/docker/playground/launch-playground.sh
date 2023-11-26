#!/bin/bash
#
# Copyright 2023 Datastrato.
# This software is licensed under the Apache License version 2.
#
set -ex

playground_dir="$(dirname "${BASH_SOURCE-$0}")"
playground_dir="$(cd "${playground_dir}">/dev/null; pwd)"
gravitino_home="$(cd "${playground_dir}/../../..">/dev/null; pwd)"

if [[ ! -d "${playground_dir}/packages/trino" ]]; then
  mkdir -p "${playground_dir}/packages/trino"
fi

if [[ ! -d "${gravitino_home}/distribution/gravitino-trino-connector" ]]; then
  . "${gravitino_home}/gradlew assembleTrinoConnector -x test"
fi
rm -rf "${playground_dir}/packages/gravitino-trino-connector"
cp -r "${gravitino_home}/distribution/gravitino-trino-connector" "${playground_dir}/packages/gravitino-trino-connector"

rm -rf "${playground_dir}/packages/trino/conf"
cp -r -p "${gravitino_home}/dev/docker/trino/conf" "${playground_dir}/packages/trino/conf"

# gravitino.uri = http://GRAVITINO_HOST_IP:GRAVITINO_HOST_PORT
# gravitino.metalake = GRAVITINO_METALAKE_NAME
sed 's/GRAVITINO_HOST_IP:GRAVITINO_HOST_PORT/gravitino:8090/g' "${playground_dir}/packages/trino/conf/catalog/gravitino.properties.template" > "${playground_dir}/packages/trino/conf/catalog/gravitino.properties"
sed -i '' 's/GRAVITINO_METALAKE_NAME/playground_metalake/g' "${playground_dir}/packages/trino/conf/catalog/gravitino.properties"

# hive.metastore.uri = thrift://HIVE_HOST_IP:9083
sed 's/HIVE_HOST_IP/hive/g' "${playground_dir}/packages/trino/conf/catalog/hive.properties.template" > "${playground_dir}/packages/trino/conf/catalog/hive.properties"

docker-compose up