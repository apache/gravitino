#!/bin/bash
#
# Copyright 2024 Datastrato Pvt Ltd.
# This software is licensed under the Apache License version 2.
#
set -ex

hdfs_ready=$(hdfs dfsadmin -report | grep "Live datanodes" | awk '{print $3}')
if [[ ${hdfs_ready} == "(1):" ]]; then
  echo "HDFS is ready"
else
  echo "HDFS is not ready"
  exit 1
fi

hive_ready=$(hive -e "show databases;" 2>&1)
if [[ ${hive_ready} == *"FAILED"* ]]; then
  echo "Hive is not ready"
  exit 1
else
  echo "Hive is ready"
fi

exit 0