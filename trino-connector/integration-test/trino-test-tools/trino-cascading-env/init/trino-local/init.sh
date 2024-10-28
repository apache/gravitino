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

cp "$trino_conf_dir/config/config.properties" /etc/trino/config.properties
cp "$trino_conf_dir/config/jvm.config" /etc/trino/jvm.config
cp "$trino_conf_dir/config/log4j2.properties" /etc/trino/log4j2.properties
cp "$trino_conf_dir/config/catalog/gravitino.properties" /etc/trino/catalog/gravitino.properties
cp "$trino_conf_dir/config/catalog/trino.properties" /etc/trino/catalog/trino.properties

#start the gravitino server
gravitino_server_dir=/tmp/gravitino-server
cp -r /opt/gravitino-server $gravitino_server_dir
rm -fr $gravitino_server_dir/logs
rm -fr $gravitino_server_dir/data

$gravitino_server_dir/bin/gravitino.sh start

#create test metalake
counter=0
while [ $counter -le 10 ]; do
  sleep 1
  counter=$((counter + 1))
  response=$(curl -s -o /dev/null -w "%{http_code}" -X POST -H "Content-Type: application/json" -d '{"name":"test","comment":"comment","properties":{}}' http://localhost:8090/api/metalakes) || true
  if [ "$response" -eq 200 ]; then
    break
  fi

  if [ "$counter" -eq 10 ]; then
    echo "Failed to create test metalake, the gravitino server is not running"
    jps
    for file in $gravitino_server_dir/logs/gravitino-server.*; do
        echo "====== Start of $file ======"
        cat "$file"
        echo "====== End of $file ======"
        echo ""
    done
    exit 1
  fi
done

echo "list gravitino connector jars"
ls /usr/lib/trino/plugin/gravitino

#
# Update `gravitino.uri = http://GRAVITINO_HOST_IP:GRAVITINO_HOST_PORT` in the `conf/catalog/gravitino.properties`
sed -i "s/GRAVITINO_HOST_IP:GRAVITINO_HOST_PORT/${GRAVITINO_HOST_IP}:${GRAVITINO_HOST_PORT}/g" /etc/trino/catalog/gravitino.properties
# Update `gravitino.metalake = GRAVITINO_METALAKE_NAME` in the `conf/catalog/gravitino.properties`
sed -i "s/GRAVITINO_METALAKE_NAME/${GRAVITINO_METALAKE_NAME}/g" /etc/trino/catalog/gravitino.properties


# Check the number of Gravitino connector plugins present in the Trino plugin directory
num_of_gravitino_connector=$(ls /usr/lib/trino/plugin/gravitino | grep gravitino-trino-connector-* | wc -l)
if [[ "${num_of_gravitino_connector}" -ne 1 ]]; then
  echo "Multiple versions of the Gravitino connector plugin found or none present."
  exit 1
fi

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
