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

set -e

output=$(docker inspect --format='{{.Name}}:{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $(docker ps -q) | grep "/trino-ci-" | sed 's/\/trino-ci-//g')

TRINO=127.0.0.1
HIVE=127.0.0.1
MYSQL=127.0.0.1
PGSQL=127.0.0.1

while IFS= read -r line; do
    name=$(echo "$line" | cut -d':' -f1)
    ip=$(echo "$line" | cut -d':' -f2)
    case $name in
        trino-coordinator)
            TRINO=$ip
            ;;
        hive)
            HIVE=$ip
            ;;
        postgresql)
            PGSQL=$ip
            ;;
        mysql)
            MYSQL=$ip
            ;;
    esac
done <<< "$output"

echo --trino_uri=http://${TRINO}:8080 --hive_uri=thrift://${HIVE}:9083 \
    --mysql_uri=jdbc:mysql://${MYSQL}:3306 --hdfs_uri=hdfs://${HIVE}:9000 \
    --postgresql_uri=jdbc:postgresql://${PGSQL} --gravitino_uri=http://${TRINO}:8090

