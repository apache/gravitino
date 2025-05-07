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



## Should be refactor
iceberg_version="1.6.1"
iceberg_gcp_bundle="iceberg-gcp-bundle-${iceberg_version}.jar"
wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-gcp-bundle/${iceberg_version}/${iceberg_gcp_bundle} -O ${GRAVITINO_HOME}/iceberg-rest-server/libs/${iceberg_gcp_bundle}
wget https://repo1.maven.org/maven2/org/apache/gravitino/gravitino-gcp/0.8.0-incubating/gravitino-gcp-0.8.0-incubating.jar -O ${GRAVITINO_HOME}/iceberg-rest-server/libs/gravitino-gcp-0.8.0-incubating.jar
export GOOGLE_APPLICATION_CREDENTIALS=/tmp/conf/gcs-credentials.json

cp /tmp/conf/* ${GRAVITINO_HOME}/conf
cp /tmp/conf/log4j2.properties ${GRAVITINO_HOME}/conf

echo "Start the Gravitino Server"
/bin/bash ${GRAVITINO_HOME}/bin/gravitino.sh run
