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
#set -ex
cd "$(dirname "${BASH_SOURCE-$0}")"

GRAVITINO_HOME_DIR=../../../../
GRAVITINO_SERVER_DIR=
GRAVITINO_TRINO_CONNECTOR_DIR=
GRAVITINO_TRINO_CASCADING_CONNECTOR_DIR=

GRAVITINO_HOME_DIR=`realpath $GRAVITINO_HOME_DIR`

source ../check_env.sh
GRAVITINO_SERVER_DIR=$(gravitino_server_dir)
GRAVITINO_TRINO_CONNECTOR_DIR=$(gravitino_trino_connector_dir)
GRAVITINO_TRINO_CASCADING_CONNECTOR_DIR=$(gravitino_trino_cascading_connector_dir)

if [ ! -z "$1" ]; then
    GRAVITINO_SERVER_DIR=$1
fi

if [ ! -z "$2" ]; then
    GRAVITINO_TRINO_CONNECTOR_DIR=$2
fi

if [ ! -z "$3" ]; then
    GRAVITINO_TRINO_CASCADING_CONNECTOR_DIR=$3
fi

check_environment

echo "Check depend jars"
source ../download_jar.sh
download_mysql_jar
download_postgresql_jar
download_trino-cascading-connector

export GRAVITINO_SERVER_DIR=`realpath $GRAVITINO_SERVER_DIR`
export GRAVITINO_TRINO_CONNECTOR_DIR=`realpath $GRAVITINO_TRINO_CONNECTOR_DIR`
export GRAVITINO_TRINO_CASCADING_CONNECTOR_DIR=`realpath $GRAVITINO_TRINO_CASCADING_CONNECTOR_DIR`
echo "GRAVITINO_SERVER_DIR is '$GRAVITINO_SERVER_DIR'"
echo "GRAVITINO_TRINO_CONNECTOR_DIR is '$GRAVITINO_TRINO_CONNECTOR_DIR'"
echo "GRAVITINO_TRINO_CASCADING_CONNECTOR_DIR is '$GRAVITINO_TRINO_CASCADING_CONNECTOR_DIR'"

# create log dir
LOG_DIR=../../build/trino-cascading-env
rm -fr $LOG_DIR
mkdir -p $LOG_DIR
LOG_FILE=$(realpath $LOG_DIR)/docker-compose.log
echo "The docker compose log is: $LOG_FILE"

docker compose up -d
nohup docker compose logs -f  -t > $LOG_FILE 2>&1 &

max_attempts=120
attempts=1
container_name="trino-local"

while true; do
    if [ -z "$(docker compose ps -q "$container_name")" ]; then
        echo "ERROR: Docker container $container_name has stopped."
        exit 1
    fi

    docker compose exec -T $container_name trino --execute "SELECT 1" >/dev/null 2>&1 && {
        break;
    }

    echo "Check the Trino server startup with try $attempts"

    if [ "$attempts" -ge "$max_attempts" ]; then
        echo "ERROR: Trino service did not start within the $max_attempts time."
        exit 1
    fi

    ((attempts++))
    sleep 1
done

echo "All docker compose service is now available."

