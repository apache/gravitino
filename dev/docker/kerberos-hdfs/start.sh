#!/bin/bash
#
# Copyright 2024 Datastrato Pvt Ltd.
# This software is licensed under the Apache License version 2.
#

# start KDC
nohup /usr/sbin/krb5kdc -P /var/run/krb5kdc.pid -n &
sleep 1

echo "addprinc -randkey hdfs/localhost@EXAMPLE.COM" | kadmin.local
echo "ktadd -k /tmp/server.keytab hdfs/localhost@EXAMPLE.COM" | kadmin.local

echo "addprinc -randkey HTTP/localhost@EXAMPLE.COM" | kadmin.local
echo "ktadd -k /tmp/http.keytab HTTP/localhost@EXAMPLE.COM" | kadmin.local

echo "addprinc -randkey client@EXAMPLE.COM" | kadmin.local
echo "ktadd -k /tmp/client.keytab client@EXAMPLE.COM" | kadmin.local

kinit -kt /tmp/server.keytab hdfs/localhost@EXAMPLE.COM

# format namenode
${HADOOP_HOME}/bin/hdfs namenode -format -nonInteractive

# start hdfs
echo "Starting HDFS..."
echo "Starting NameNode..."
${HADOOP_HOME}/sbin/hadoop-daemon.sh start namenode

echo "Starting DataNode..."
${HADOOP_HOME}/sbin/hadoop-daemon.sh start datanode

sleep 5

# mkdir /user/client for user client
hadoop fs -mkdir -p /user/client
hadoop fs -chown client:hdfs /user/client

# persist the container
tail -f /dev/null
