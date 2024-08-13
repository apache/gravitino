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

set -ex
trino_conf_dir="$(dirname "${BASH_SOURCE-$0}")"
trino_conf_dir="$(cd "${trino_conf_dir}">/dev/null; pwd)"

cp $trino_conf_dir/config/*.* /etc/trino
cp $trino_conf_dir/config/catalog/*.* /etc/trino/catalog

#
# Update `gravitino.uri = http://GRAVITINO_HOST_IP:GRAVITINO_HOST_PORT` in the `conf/catalog/gravitino.properties`
sed -i "s/GRAVITINO_HOST_IP:GRAVITINO_HOST_PORT/${GRAVITINO_HOST_IP}:${GRAVITINO_HOST_PORT}/g" /etc/trino/catalog/gravitino.properties
# Update `gravitino.metalake = GRAVITINO_METALAKE_NAME` in the `conf/catalog/gravitino.properties`
sed -i "s/GRAVITINO_METALAKE_NAME/${GRAVITINO_METALAKE_NAME}/g" /etc/trino/catalog/gravitino.properties


ls /usr/lib/trino/plugin/gravitino
# Check the number of Gravitino connector plugins present in the Trino plugin directory
num_of_gravitino_connector=$(ls /usr/lib/trino/plugin/gravitino | grep gravitino-trino-connector-* | wc -l)
if [[ "${num_of_gravitino_connector}" -ne 1 ]]; then
  echo "Multiple versions of the Gravitino connector plugin found or none present."
  exit 1
fi

nohup /usr/lib/trino/bin/run-trino &

tail -f /dev/null
