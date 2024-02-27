#!/bin/bash
#
# Copyright 2024 Datastrato Pvt Ltd.
# This software is licensed under the Apache License version 2.
#

# start ssh
HOSTNAME=`hostname`
service ssh start
ssh-keyscan localhost > /root/.ssh/known_hosts
ssh-keyscan 0.0.0.0 >> /root/.ssh/known_hosts

FQDN="HADOOPPKRB"
ADMIN="admin"
PASS="Admin12!"
KRB5_KTNAME=/etc/admin.keytab
echo -e "${PASS}\n${PASS}" | kdb5_util create -s
echo -e "${PASS}\n${PASS}" | kadmin.local -q "addprinc ${ADMIN}/admin"
echo -e "${PASS}\n${PASS}" | kadmin.local -q "addprinc cli"
echo -e "${PASS}\n${PASS}" | kadmin.local -q "addprinc  server/${HOSTNAME}.${FQDN}"
kadmin.local -q "ktadd -norandkey -k ${KRB5_KTNAME} cli"
kadmin.local -q "ktadd -norandkey -k ${KRB5_KTNAME} server/${HOSTNAME}.${FQDN}"
kadmin.local -q "xst -k /app/hadoop.keytab -norandkey server/${HOSTNAME}.${FQDN}"
kadmin.local -q "xst -k /app/cli.keytab -norandkey cli"

service krb5-kdc start
service krb5-admin-server start

# start hdfs
${HADOOP_HOME}/sbin/start-dfs.sh

# start mysql and create databases/users for hive
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
${HIVE_HOME}/bin/schematool -initSchema -dbType mysql
${HIVE_HOME}/bin/hive --service hiveserver2 > /dev/null 2>&1 &
${HIVE_HOME}/bin/hive --service metastore > /dev/null 2>&1 &

# persist the container
tail -f /dev/null
