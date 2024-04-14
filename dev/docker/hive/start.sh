#!/bin/bash
#
# Copyright 2023 Datastrato Pvt Ltd.
# This software is licensed under the Apache License version 2.
#

# start hdfs
echo "Starting HDFS..."
echo "Starting NameNode..."
${HADOOP_HOME}/sbin/hadoop-daemon.sh start namenode

echo "Starting DataNode..."
${HADOOP_HOME}/sbin/hadoop-daemon.sh start datanode

# start mysql and create databases/users for hive
echo "Starting MySQL..."
chown -R mysql:mysql /var/lib/mysql
usermod -d /var/lib/mysql/ mysql
service mysql start

echo """
  CREATE USER 'hive'@'localhost' IDENTIFIED BY 'hive';
  GRANT ALL PRIVILEGES on *.* to 'hive'@'localhost' WITH GRANT OPTION;
  GRANT ALL on hive.* to 'hive'@'localhost' IDENTIFIED BY 'hive';
  CREATE USER 'iceberg'@'*' IDENTIFIED BY 'iceberg';
  GRANT ALL PRIVILEGES on *.* to 'iceberg'@'%' identified by 'iceberg' with grant option;
  FLUSH PRIVILEGES;
  CREATE DATABASE hive;
""" | mysql --user=root --password=${MYSQL_PWD}

# start hive
echo "Starting Hive..."
${HIVE_HOME}/bin/schematool -initSchema -dbType mysql
${HIVE_HOME}/bin/hive --service hiveserver2 > /dev/null 2>&1 &
${HIVE_HOME}/bin/hive --service metastore > /dev/null 2>&1 &

# persist the container
tail -f /dev/null
