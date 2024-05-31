#
# Copyright 2024 Datastrato Pvt Ltd.
# This software is licensed under the Apache License version 2.
#

set -ex
trino_conf_dir="$(dirname "${BASH_SOURCE-$0}")"
trino_conf_dir="$(cd "${trino_conf_dir}">/dev/null; pwd)"

cp "$trino_conf_dir/config/config.properties" /etc/trino/config.properties
cp "$trino_conf_dir/config/catalog/gravitino.properties" /etc/trino/catalog/gravitino.properties
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
