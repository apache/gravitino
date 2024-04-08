#!/bin/bash
#
# Copyright 2024 Datastrato Pvt Ltd.
# This software is licensed under the Apache License version 2.
#

DORIS_HOME="$(dirname "${BASH_SOURCE-$0}")"
DORIS_HOME="$(cd "${DORIS_HOME}">/dev/null; pwd)"
DORIS_FE_HOME="${DORIS_HOME}/fe/"
DORIS_BE_HOME="${DORIS_HOME}/be/"

# print the disk usage
echo "Disk usage:"
df -h

# check free disk space, if little then 6GB, Doris FE (version < 2.1.0) can not start
# TODO: change the threshold to 2GB when Doris Version is up to 2.1.0, add bdbje_free_disk_bytes to fe.conf
THRESHOLD=6

DISK_FREE=`df -BG | grep '/$' | tr -s ' ' | cut -d ' ' -f4 | grep -o '[0-9]*'`
if [ "$DISK_FREE" -lt "$THRESHOLD" ]
then
    echo "ERROR: Doris FE (version < 2.1.0) can not start with less than ${THRESHOLD}G disk space."
    exit 1
fi

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
fe_started=false
for i in {1..10}; do
  sleep 5
  # check http://localhost:8030/api/bootstrap return contains content '"msg":"success"' to see if the fe is ready
  url="http://localhost:8030/api/bootstrap"
  response=$(curl --silent --request GET $url)

  if echo "$response" | grep -q "\"msg\":\"success\""; then
    fe_started=true
    break
  else
    echo "waiting for the fe to start"
  fi
done

if [ "$fe_started" = false ]; then
  echo "Doris fe failed to start"

  cat ${DORIS_FE_HOME}/log/fe.*
  exit 1
fi

# add the be to the fe
mysql -h127.0.0.1 -P9030 -uroot -e "ALTER SYSTEM ADD BACKEND '${CONTAINER_IP}:9050'"

if [ $? -ne 0 ]; then
  echo "Failed to add the BE to the FE"
  exit 1
fi

# persist the container
tail -f ${DORIS_FE_HOME}/log/fe.log ${DORIS_BE_HOME}/log/be.log
