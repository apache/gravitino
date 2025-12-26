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

IP=$(hostname -I | awk '{print $1}')
sed -i "s|<value>hdfs://__REPLACE__HOST_NAME:9000|<value>hdfs://${IP}:9000|g" ${HIVE_TMP_CONF_DIR}/hive-site.xml

if [[ "$TRINO_CONNECTOR_TEST" == "true" ]]
then
  echo "Trino connector test will create Hive ACID table."
  /bin/bash /usr/local/sbin/start.sh &
  # wait Hive metastore server start
  until (ps -ef | grep -q "HiveMetaStore") && \
        (ss -tuln | grep -q ':9083'); do
     sleep 3
  done
  # create Hive ACID table
  hive -f /tmp/hive/init.sql
  # persist the container
  tail -f /dev/null
else
  /bin/bash /usr/local/sbin/start.sh
fi

