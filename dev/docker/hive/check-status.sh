#!/bin/bash
#
# Copyright 2023 Datastrato.
# This software is licensed under the Apache License version 2.
#
set -ex
function check_hdfs_hive() {
  hdfs_ready=$(hdfs dfsadmin -report | grep "Live datanodes" | awk '{print $3}')
  if [[ ${hdfs_ready} == "(0)" || ${hdfs_ready} == "" ]]; then
    echo "HDFS is not ready"
    return 1
  else
    echo "HDFS is ready"
  fi

  hive_ready=$(hive -e "show databases;" 2>&1)
  if [[ ${hive_ready} == *"FAILED"* ]]; then
    echo "Hive is not ready"
    return 1
  else
    echo "Hive is ready"
  fi

  return 0
}

max_attempts=${1}
attempt=1

while [[ ${attempt} -le ${max_attempts} ]]
do
  check_hdfs_hive
  if [ $? -eq 0 ];then
    echo "HDFS and Hive are ready"
    break
  else
    echo "HDFS and Hive are not ready yet, attempt ${attempt} of ${max_attempts}"
    attempt=$((attempt+1))
  fi

  sleep 1
done

if [[ ${attempt} -gt ${max_attempts} ]]
then
  echo "HDFS and Hive are not ready after ${max_attempts} attempts."
fi