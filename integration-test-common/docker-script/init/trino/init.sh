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

#

set -ex
trino_conf_dir="$(dirname "${BASH_SOURCE-$0}")"
trino_conf_dir="$(cd "${trino_conf_dir}">/dev/null; pwd)"

cp "$trino_conf_dir/config/jvm.config" /etc/trino/jvm.config
cp "$trino_conf_dir/config/log4j2.properties" /etc/trino/log4j2.properties
cp "$trino_conf_dir/config/node.properties" /etc/trino/node.properties
cp "$trino_conf_dir/config/catalog/gravitino.properties" /etc/trino/catalog/gravitino.properties

# Copy the MYSQL driver to iceberg connector
cp /usr/lib/trino/plugin/mysql/mysql-connector-j-8.2.0.jar /usr/lib/trino/plugin/iceberg/

# Update `gravitino.uri = http://GRAVITINO_HOST_IP:GRAVITINO_HOST_PORT` in the `conf/catalog/gravitino.properties`
sed -i "s/GRAVITINO_HOST_IP:GRAVITINO_HOST_PORT/${GRAVITINO_HOST_IP}:${GRAVITINO_HOST_PORT}/g" /etc/trino/catalog/gravitino.properties
# Update `gravitino.metalake = GRAVITINO_METALAKE_NAME` in the `conf/catalog/gravitino.properties`
sed -i "s/GRAVITINO_METALAKE_NAME/${GRAVITINO_METALAKE_NAME}/g" /etc/trino/catalog/gravitino.properties
# Update `node.id=NODE_ID` in the `/conf/node.properties`
sed -i "s/NODE_ID/${RANDOM}-${RANDOM}-${RANDOM}-${RANDOM}-${RANDOM}/g" /etc/trino/node.properties

# Update `/conf/config.properties`
if [[ "${TRINO_ROLE}" == "coordinator" ]]; then
  cp "$trino_conf_dir/config/config.properties" /etc/trino/config.properties
  if [[ "${TRINO_WORKER_NUM}" -gt 0 ]]; then
    # Deploy a distributed cluster, so update `node-scheduler.include-coordinator` to `false` in the`/conf/config.properties`
    sed -i "s/node-scheduler.include-coordinator=true/node-scheduler.include-coordinator=false/g" /etc/trino/config.properties
  fi
else
  cp "$trino_conf_dir/config/config-worker.properties" /etc/trino/config.properties
fi

# mkdir `node.data-dir` in the `/conf/node.properties`
mkdir -p /tmp/data

# Check the number of Gravitino connector plugins present in the Trino plugin directory
num_of_gravitino_connector=$(ls /usr/lib/trino/plugin/gravitino | grep gravitino-trino-connector-* | wc -l)
if [[ "${num_of_gravitino_connector}" -ne 1 ]]; then
  echo "Multiple versions of the Gravitino connector plugin found or none present."
  exit 1
fi

# Container start up
if [[ "${TRINO_ROLE}" == "coordinator" ]]; then
  nohup /usr/lib/trino/bin/run-trino &

  counter=0
  while [ $counter -le 300 ]; do
    counter=$((counter + 1))
    trino_ready=$(trino --execute  "SHOW CATALOGS LIKE 'gravitino'" | wc -l)
    if [ "$trino_ready" -eq 0 ];
    then
      echo "Wait for the initialization of services"
      sleep 1;
    else
      # persist the container
      tail -f /dev/null
    fi
  done
  exit 1
else
  /usr/lib/trino/bin/run-trino
fi
