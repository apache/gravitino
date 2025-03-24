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

echo "Start to download the jar package of JDBC"
wget https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar -O ${GRAVITINO_HOME}/catalogs/jdbc-mysql/libs/mysql-connector-java-8.0.27.jar
wget https://jdbc.postgresql.org/download/postgresql-42.7.0.jar -O ${GRAVITINO_HOME}/catalogs/jdbc-postgresql/libs/postgresql-42.7.0.jar

cp ${GRAVITINO_HOME}/catalogs/jdbc-postgresql/libs/postgresql-42.7.0.jar ${GRAVITINO_HOME}/catalogs/lakehouse-iceberg/libs
cp ${GRAVITINO_HOME}/catalogs/jdbc-mysql/libs/mysql-connector-java-8.0.27.jar ${GRAVITINO_HOME}/catalogs/lakehouse-iceberg/libs

cp ${GRAVITINO_HOME}/catalogs/jdbc-postgresql/libs/postgresql-42.7.0.jar ${GRAVITINO_HOME}/iceberg-rest-server/libs
cp ${GRAVITINO_HOME}/catalogs/jdbc-mysql/libs/mysql-connector-java-8.0.27.jar ${GRAVITINO_HOME}/iceberg-rest-server/libs

cp ${GRAVITINO_HOME}/catalogs/jdbc-mysql/libs/mysql-connector-java-8.0.27.jar ${GRAVITINO_HOME}/libs
echo "Finish downloading"

cp /tmp/conf/* ${GRAVITINO_HOME}/conf
cp /tmp/conf/log4j2.properties ${GRAVITINO_HOME}/conf

{{- if .Values.log.containerStdout }}
# Redirect log files to container stdout and stderr
ln -sf /dev/stdout ${GRAVITINO_HOME}/logs/gravitino_audit.log
ln -sf /dev/stdout ${GRAVITINO_HOME}/logs/gravitino-server.log
ln -sf /dev/stderr ${GRAVITINO_HOME}/logs/gravitino-server.out
{{- end }}
echo "Start the Gravitino Server"
/bin/bash ${GRAVITINO_HOME}/bin/gravitino.sh start
