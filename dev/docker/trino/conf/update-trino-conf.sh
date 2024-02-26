#!/bin/bash
#
# Copyright 2023 Datastrato Pvt Ltd.
# This software is licensed under the Apache License version 2.
#
# This script runs in the Trino docker container and updates the Trino configuration files
set -ex
trino_conf_dir="$(dirname "${BASH_SOURCE-$0}")"
trino_conf_dir="$(cd "${trino_conf_dir}">/dev/null; pwd)"

# Update `gravitino.uri = http://GRAVITINO_HOST_IP:GRAVITINO_HOST_PORT` in the `conf/catalog/gravitino.properties`
sed "s/GRAVITINO_HOST_IP:GRAVITINO_HOST_PORT/${GRAVITINO_HOST_IP}:${GRAVITINO_HOST_PORT}/g" "${trino_conf_dir}/catalog/gravitino.properties.template" > "${trino_conf_dir}/catalog/gravitino.properties.tmp"
# Update `gravitino.metalake = GRAVITINO_METALAKE_NAME` in the `conf/catalog/gravitino.properties`
sed "s/GRAVITINO_METALAKE_NAME/${GRAVITINO_METALAKE_NAME}/g" "${trino_conf_dir}/catalog/gravitino.properties.tmp" > "${trino_conf_dir}/catalog/gravitino.properties"
rm "${trino_conf_dir}/catalog/gravitino.properties.tmp"

# Check the number of Gravitino connector plugins present in the Trino plugin directory
num_of_gravitino_connector=$(ls /usr/lib/trino/plugin/gravitino | grep gravitino-trino-connector-* | wc -l)
if [[ "${num_of_gravitino_connector}" -ne 1 ]]; then
  echo "Multiple versions of the Gravitino connector plugin found or none present."
  exit 1
fi