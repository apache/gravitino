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
cd "$(dirname "$0")"

container_urls=$(docker inspect --format='{{.Name}}:{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $(docker ps -q) | grep "/trino-ci-" | sed 's/\/trino-ci-//g')

while IFS=: read -r container_name address; do
  case "$container_name" in
    trino)
      echo "trino_uri=http://$address:8080"
      ;;
    hive)
      echo "hive_uri=thrift://$address:9083"
      echo "hdfs_uri=hdfs://$address:9000"
      ;;
    mysql)
      echo "mysql_uri=jdbc:mysql://$address:3306"
      ;;
    postgresql)
      echo "postgresql_uri=jdbc:postgresql://$address"
      ;;
  esac
done <<< "$container_urls"
