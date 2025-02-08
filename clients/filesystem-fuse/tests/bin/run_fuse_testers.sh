#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

source ./env.sh
source ./gravitino_server.sh
source ./gvfs_fuse.sh
source ./localstatck.sh

TEST_CONFIG_FILE=$CLIENT_FUSE_DIR/target/debug/gvfs-fuse.toml
MOUNT_DIR=$CLIENT_FUSE_DIR/target/gvfs


start_servers() {
  start_localstack
  start_gravitino_server
  generate_test_config
  start_gvfs_fuse
}

stop_servers() {
  set +e
  stop_gvfs_fuse
  stop_gravitino_server
  stop_localstack
}

# Main logic based on parameters
if [ "$1" == "test" ]; then
  trap stop_servers EXIT
  start_servers
  # Run the integration test
  echo "Running tests..."
  cd $CLIENT_FUSE_DIR
  export RUN_TEST_WITH_FUSE=1
  cargo test --test fuse_test fuse_it_ -- weak_consistency

elif [ "$1" == "start" ]; then
  # Start the servers
  echo "Starting servers..."
  start_servers

elif [ "$1" == "restart" ]; then
  # Stop the servers
  echo "Stopping servers..."
  stop_servers

  # Start the servers
  echo "Starting servers..."
  start_servers

elif [ "$1" == "stop" ]; then
  # Stop the servers
  echo "Stopping servers..."
  stop_servers

else
  echo "Usage: $0 {test|start|stop}"
  exit 1
fi


