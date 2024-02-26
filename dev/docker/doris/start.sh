#!/bin/bash
#
# Copyright 2024 Datastrato Pvt Ltd.
# This software is licensed under the Apache License version 2.
#

DORIS_HOME="$(dirname "${BASH_SOURCE-$0}")"
DORIS_HOME="$(cd "${DORIS_HOME}">/dev/null; pwd)"
DORIS_FE_HOME="${DORIS_HOME}/fe/"
DORIS_BE_HOME="${DORIS_HOME}/be/"

# comment a code snippet about max_map_count, it's not necessary for IT environment

DORIS_BE_SCRIPT="${DORIS_BE_HOME}/bin/start_be.sh"

sed -i '/Please set vm.max_map_count/,/exit 1/{s/exit 1/#exit 1\n        echo "skip this"/}' ${DORIS_BE_SCRIPT}
sed -i '/Please set the maximum number of open file descriptors/,/exit 1/{s/exit 1/#exit 1\n        echo "skip this"/}' ${DORIS_BE_SCRIPT}

# update fe.conf & be.conf, set priority_networks
CONTAINER_IP=$(hostname -i)
PRIORITY_NETWORKS=$(echo "${CONTAINER_IP}" | awk -F '.' '{print$1"."$2"."$3".0/24"}')
echo "add priority_networks = ${PRIORITY_NETWORKS} to fe.conf & be.conf"
echo "priority_networks = ${PRIORITY_NETWORKS}" >> ${DORIS_FE_HOME}/conf/fe.conf
echo "priority_networks = ${PRIORITY_NETWORKS}" >> ${DORIS_BE_HOME}/conf/be.conf

# start doris fe and be
${DORIS_FE_HOME}/bin/start_fe.sh --daemon
${DORIS_BE_HOME}/bin/start_be.sh --daemon

# wait for the fe to start
for i in {1..10}; do
  sleep 5
  # check http://localhost:8030/api/bootstrap return contains content '"msg":"success"' to see if the fe is ready
  url="http://localhost:8030/api/bootstrap"
  response=$(curl --silent --request GET $url)

  if echo "$response" | grep -q "\"msg\":\"success\""; then
      break
  else
      echo "waiting for the fe to start"
  fi
done

# add the be to the fe
mysql -h127.0.0.1 -P9030 -uroot -e "ALTER SYSTEM ADD BACKEND '${CONTAINER_IP}:9050'"

# persist the container
tail -f /dev/null