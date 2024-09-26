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

# Special Hive runtime version environment variable to decide which version of Hive to install
if [[ "${HIVE_RUNTIME_VERSION}" == "hive3" ]]; then
  ln -s ${HIVE3_HOME} ${HIVE_HOME}
  ln -s ${HADOOP3_HOME} ${HADOOP_HOME}
else
  ln -s ${HIVE2_HOME} ${HIVE_HOME}
  ln -s ${HADOOP2_HOME} ${HADOOP_HOME}
fi

 cp ${HADOOP_HOME}/share/hadoop/tools/lib/*aws* ${HIVE_HOME}/lib

# Copy Hadoop and Hive configuration file and update hostname
cp -f ${HADOOP_TMP_CONF_DIR}/* ${HADOOP_CONF_DIR}
cp -f ${HIVE_TMP_CONF_DIR}/* ${HIVE_CONF_DIR}
sed -i "s/__REPLACE__HOST_NAME/$(hostname)/g" ${HADOOP_CONF_DIR}/core-site.xml
sed -i "s/__REPLACE__HOST_NAME/$(hostname)/g" ${HADOOP_CONF_DIR}/hdfs-site.xml
sed -i "s/__REPLACE__HOST_NAME/$(hostname)/g" ${HIVE_CONF_DIR}/hive-site.xml

sed -i "s|S3_ACCESS_KEY_ID|${S3_ACCESS_KEY}|g" ${HIVE_CONF_DIR}/hive-site.xml
sed -i "s|S3_SECRET_KEY_ID|${S3_SECRET_KEY}|g" ${HIVE_CONF_DIR}/hive-site.xml
sed -i "s|S3_ENDPOINT_ID|${S3_ENDPOINT}|g" ${HIVE_CONF_DIR}/hive-site.xml

# Link mysql-connector-java after deciding where HIVE_HOME symbolic link points to.
ln -s /opt/mysql-connector-java-${MYSQL_JDBC_DRIVER_VERSION}/mysql-connector-java-${MYSQL_JDBC_DRIVER_VERSION}.jar ${HIVE_HOME}/lib

# install Ranger hive plugin
if [[ -n "${RANGER_HIVE_REPOSITORY_NAME}" && -n "${RANGER_SERVER_URL}" ]]; then
  # If Hive enable Ranger plugin need requires zookeeper
  echo "Starting zookeeper..."
  mv ${ZK_HOME}/conf/zoo_sample.cfg ${ZK_HOME}/conf/zoo.cfg
  sed -i "s|/tmp/zookeeper|${ZK_HOME}/data|g" ${ZK_HOME}/conf/zoo.cfg
  ${ZK_HOME}/bin/zkServer.sh start-foreground > /dev/null 2>&1 &

  cd ${RANGER_HIVE_PLUGIN_HOME}
  sed -i "s|POLICY_MGR_URL=|POLICY_MGR_URL=${RANGER_SERVER_URL}|g" install.properties
  sed -i "s|REPOSITORY_NAME=|REPOSITORY_NAME=${RANGER_HIVE_REPOSITORY_NAME}|g" install.properties
  echo "XAAUDIT.SUMMARY.ENABLE=true" >> install.properties
  sed -i "s|COMPONENT_INSTALL_DIR_NAME=|COMPONENT_INSTALL_DIR_NAME=${HIVE_HOME}|g" install.properties
  ${RANGER_HIVE_PLUGIN_HOME}/enable-hive-plugin.sh

  # Reduce poll policy interval in the ranger plugin configuration
  sed -i '/<name>ranger.plugin.hive.policy.pollIntervalMs<\/name>/{n;s/<value>30000<\/value>/<value>500<\/value>/}' ${HIVE_HOME}/conf/ranger-hive-security.xml

  # Enable audit log in hive
  cp ${HIVE_HOME}/conf/hive-log4j2.properties.template ${HIVE_HOME}/conf/hive-log4j2.properties
  sed -i "s/appenders = console, DRFA/appenders = console, DRFA, RANGERAUDIT/g" ${HIVE_HOME}/conf/hive-log4j2.properties
  sed -i "s/loggers = NIOServerCnxn, ClientCnxnSocketNIO, DataNucleus, Datastore, JPOX, PerfLogger, AmazonAws, ApacheHttp/loggers = NIOServerCnxn, ClientCnxnSocketNIO, DataNucleus, Datastore, JPOX, PerfLogger, AmazonAws, ApacheHttp, Ranger/g" ${HIVE_HOME}/conf/hive-log4j2.properties

  # Enable Ranger audit log in hive
cat <<'EOF' >> ${HIVE_HOME}/conf/hive-log4j2.properties

# RANGERAUDIT appender
logger.Ranger.name = xaaudit
logger.Ranger.level = INFO
logger.Ranger.appenderRefs = RANGERAUDIT
logger.Ranger.appenderRef.RANGERAUDIT.ref = RANGERAUDIT
appender.RANGERAUDIT.type=file
appender.RANGERAUDIT.name=RANGERAUDIT
appender.RANGERAUDIT.fileName=${sys:hive.log.dir}/ranger-hive-audit.log
appender.RANGERAUDIT.filePermissions=rwxrwxrwx
appender.RANGERAUDIT.layout.type=PatternLayout
appender.RANGERAUDIT.layout.pattern=%d{ISO8601} %q %5p [%t] %c{2} (%F:%M(%L)) - %m%n
EOF
fi

# install Ranger hdfs plugin
if [[ -n "${RANGER_HDFS_REPOSITORY_NAME}" && -n "${RANGER_SERVER_URL}" ]]; then
  cd ${RANGER_HDFS_PLUGIN_HOME}
  sed -i "s|POLICY_MGR_URL=|POLICY_MGR_URL=${RANGER_SERVER_URL}|g" install.properties
  sed -i "s|REPOSITORY_NAME=|REPOSITORY_NAME=${RANGER_HDFS_REPOSITORY_NAME}|g" install.properties
  echo "XAAUDIT.SUMMARY.ENABLE=true" >> install.properties
  sed -i "s|COMPONENT_INSTALL_DIR_NAME=|COMPONENT_INSTALL_DIR_NAME=${HADOOP_HOME}|g" install.properties
  ${RANGER_HDFS_PLUGIN_HOME}/enable-hdfs-plugin.sh

  # Reduce poll policy interval in the ranger plugin configuration
  sed -i '/<name>ranger.plugin.hdfs.policy.pollIntervalMs<\/name>/{n;s/<value>30000<\/value>/<value>500<\/value>/}' ${HADOOP_HOME}/etc/hadoop/ranger-hdfs-security.xml

  # Enable Ranger audit log in hdfs
cat <<'EOF' >> ${HADOOP_HOME}/etc/hadoop/log4j.properties

# RANGERAUDIT appender
ranger.logger=INFO,console,RANGERAUDIT
log4j.logger.xaaudit=${ranger.logger}
log4j.appender.RANGERAUDIT=org.apache.log4j.DailyRollingFileAppender
log4j.appender.RANGERAUDIT.File=${hadoop.log.dir}/ranger-hdfs-audit.log
log4j.appender.RANGERAUDIT.layout=org.apache.log4j.PatternLayout
log4j.appender.RANGERAUDIT.layout.ConversionPattern=%d{ISO8601} %p %c{2}: %L %m%n
log4j.appender.RANGERAUDIT.DatePattern=.yyyy-MM-dd
EOF
fi

# start hdfs
echo "Starting HDFS..."
echo "Format NameNode..."
${HADOOP_HOME}/bin/hdfs namenode -format -nonInteractive

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
