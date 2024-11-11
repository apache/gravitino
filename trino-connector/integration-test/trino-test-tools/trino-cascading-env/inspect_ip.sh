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

output=$(docker inspect --format='{{.Name}}:{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $(docker ps -q) |grep "/trino-ci-" | sed 's/\/trino-ci-//g')

gravitino_uri=""
mysql_uri=""
trino_uri=""
trino_remote_jdbc_uri=""

while IFS= read -r line; do
  name=$(echo $line | cut -d':' -f1)
  ip=$(echo $line | cut -d':' -f2)

  case $name in
    trino-local)
      gravitino_uri="--gravitino_uri=http://$ip:8090"
      trino_uri="--trino_uri=http://$ip:8080"
      ;;
    trino-remote)
      trino_remote_jdbc_uri="jdbc:trino://$ip:8080"
      ;;
    mysql)
      mysql_uri="--mysql_uri=jdbc:mysql://$ip"
      ;;
  esac
done <<< "$output"

echo "$gravitino_uri $mysql_uri $trino_uri --params=trino_remote_jdbc_uri,$trino_remote_jdbc_uri"
