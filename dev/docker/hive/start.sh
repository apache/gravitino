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

echo "Hive started successfully."

# persist the container
tail -f /dev/null
