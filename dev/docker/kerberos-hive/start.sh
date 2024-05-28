#!/bin/bash
#
# Copyright 2024 Datastrato Pvt Ltd.
# This software is licensed under the Apache License version 2.
#

# start ssh
HOSTNAME=`hostname`
service ssh start
ssh-keyscan ${HOSTNAME} >> /root/.ssh/known_hosts
ssh-keyscan localhost >> /root/.ssh/known_hosts
ssh-keyscan 0.0.0.0 >> /root/.ssh/known_hosts
ssh-keyscan 127.0.0.1 >> /root/.ssh/known_hosts

# init the Kerberos database
echo -e "${PASS}\n${PASS}" | kdb5_util create -s

# start Kerberos related service
service krb5-kdc start
service krb5-admin-server start

# create Kerberos principal and keytab files
FQDN="HADOOPKRB"
ADMIN="admin"
PASS="Admin12!"
KRB5_KTNAME=/etc/admin.keytab

echo -e "${PASS}\n${PASS}" | kadmin.local -q "addprinc ${ADMIN}/admin"
echo -e "${PASS}\n${PASS}" | kadmin.local -q "addprinc cli@${FQDN}"
echo -e "${PASS}\n${PASS}" | kadmin.local -q "addprinc hdfs/${HOSTNAME}@${FQDN}"
echo -e "${PASS}\n${PASS}" | kadmin.local -q "addprinc HTTP/${HOSTNAME}@${FQDN}"
echo -e "${PASS}\n${PASS}" | kadmin.local -q "addprinc hive/${HOSTNAME}@${FQDN}"
echo -e "${PASS}\n${PASS}" | kadmin.local -q "addprinc yarn/${HOSTNAME}@${FQDN}"
kadmin.local -q "ktadd -norandkey -k ${KRB5_KTNAME} cli@${FQDN}"
kadmin.local -q "ktadd -norandkey -k ${KRB5_KTNAME} hdfs/${HOSTNAME}@${FQDN}"
kadmin.local -q "ktadd -norandkey -k ${KRB5_KTNAME} HTTP/${HOSTNAME}@${FQDN}"
kadmin.local -q "ktadd -norandkey -k ${KRB5_KTNAME} yarn/${HOSTNAME}@${FQDN}"

kadmin.local -q "xst -k /hdfs.keytab -norandkey hdfs/${HOSTNAME}@${FQDN}"
kadmin.local -q "xst -k /hdfs.keytab -norandkey HTTP/${HOSTNAME}@${FQDN}"
kadmin.local -q "xst -k /yarn.keytab -norandkey yarn/${HOSTNAME}@${FQDN}"

kadmin.local -q "ktadd -norandkey -k ${KRB5_KTNAME} hive/${HOSTNAME}@${FQDN}"
kadmin.local -q "xst -k /hive.keytab -norandkey hive/${HOSTNAME}@${FQDN}"
kadmin.local -q "xst -k /cli.keytab -norandkey cli@${FQDN}"

# For Gravitino web server
echo -e "${PASS}\n${PASS}" | kadmin.local -q "addprinc gravitino_client@${FQDN}"
kadmin.local -q "ktadd -norandkey -k /gravitino_client.keytab gravitino_client@${FQDN}"

echo -e "${PASS}\n${PASS}" | kadmin.local -q "addprinc HTTP/localhost@${FQDN}"
kadmin.local -q "ktadd -norandkey -k /gravitino_server.keytab HTTP/localhost@${FQDN}"

echo -e "${PASS}\n" | kinit hive/${HOSTNAME}

# Update the configuration file
sed -i "s/mockhost/${HOSTNAME}/g" ${HADOOP_CONF_DIR}/hdfs-site.xml
sed -i "s/mockhost/${HOSTNAME}/g" ${HADOOP_CONF_DIR}/core-site.xml
sed -i "s/mockhost/${HOSTNAME}/g" ${HIVE_HOME}/conf/hive-site.xml

# format HDFS
${HADOOP_HOME}/bin/hdfs namenode -format -nonInteractive

echo "Starting HDFS..."
echo "Starting NameNode..."
${HADOOP_HOME}/sbin/hadoop-daemon.sh start namenode

echo "Starting DataNode..."
${HADOOP_HOME}/sbin/start-secure-dns.sh
sleep 5

# Check if the DataNode is running
ps -ef | grep DataNode | grep -v "color=auto"
if [[ $? -ne 0 ]]; then
  echo "DataNode failed to start, please check the logs"
  exit 1
fi

retry_times=0
ready=0
while [[ ${retry_times} -lt 10 ]]; do
  hdfs_ready=$(hdfs dfsadmin -report | grep "Live datanodes" | awk '{print $3}')
  if [[ ${hdfs_ready} == "(1):" ]]; then
    echo "HDFS is ready, retry_times = ${retry_times}"
    let "ready=0"
    break
  fi
  retry_times=$((retry_times+1))
done

if [[ ${ready} -ne 0 ]]; then
  echo "HDFS is not ready"
  cat ${HADOOP_HOME}/bin/logs/hadoop-root-datanode-*.log
  exit 1
fi


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
