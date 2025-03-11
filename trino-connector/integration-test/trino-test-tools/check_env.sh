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

gravitino_server_dir() {
    local dir=""
    if [ ! -z "${GRAVITINO_HOME_DIR}" ]; then
        if [ -z "${GRAVITINO_SERVER_DIR}" ]; then
            dir=$GRAVITINO_HOME_DIR/distribution/package
	else
            dir=$GRAVITINO_SERVER_DIR
        fi
    fi
    
    if [ ! -d "$dir" ]; then
        echo "Error: Gravitino server directory '$dir' does not exist." >&2
        exit 1
    fi
    echo $dir

}

gravitino_trino_connector_dir() {
    local dir=""
    if [ ! -z "${GRAVITINO_HOME_DIR}" ]; then
        if [ -z "${GRAVITINO_TRINO_CONNECTOR_DIR}" ]; then
            dir=$GRAVITINO_HOME_DIR/trino-connector/trino-connector/build/libs
	else
            dir=$GRAVITINO_TRINO_CONNECTOR_DIR
        fi
    fi
    
    if [ ! -d "$dir" ]; then
        echo "Error: Gravitino Trino connector directory '$dir' does not exist." >&2
        exit 1
    fi
    echo $dir
}

gravitino_trino_cascading_connector_dir() {
    local dir=""
    if [ ! -z "${GRAVITINO_HOME_DIR}" ]; then
        if [ -z "${GRAVITINO_TRINO_CASCADING_CONNECTOR_DIR}" ]; then
            dir=$GRAVITINO_HOME_DIR/trino-connector/integration-test/build/trino-trino
	else
            dir=$GRAVITINO_TRINO_CASCADING_CONNECTOR_DIR
        fi
    fi

    echo $dir
}

check_environment() {
    if ! command -v docker &>/dev/null; then
        echo "ERROR: No docker service environment found, please install Docker first." >&2
        exit 1
    fi
}
