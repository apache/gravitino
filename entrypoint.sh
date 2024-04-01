#!/bin/bash
#
# Copyright © 2015-2021 the original authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
set -e

function found_gravitino_server_pid() {
  process_name='GravitinoServer';
  RUNNING_PIDS=$(ps x | grep ${process_name} | grep -v grep | awk '{print $1}');

  if [[ -z "${RUNNING_PIDS}" ]]; then
    return
  fi

  if ! kill -0 ${RUNNING_PIDS} > /dev/null 2>&1; then
    # process is dead, exit
    return
  fi

  echo "${RUNNING_PIDS}"
}

function stop_gravitino_server() {
  echo "Received SIGINT or SIGTERM. Shutting down the gravitino server."
  bash /opt/gravitino/bin/gravitino.sh stop
  sleep 5
}

sleep 5

trap stop_gravitino_server SIGINT SIGTERM

bash /opt/gravitino/bin/gravitino.sh start

pid=$(found_gravitino_server_pid)

if [[ -z "${pid}" ]]; then
  echo "Gravitino Server is not running"
else
  while true; do
      # Check the process status
      if ! kill -0 $pid > /dev/null 2>&1; then
          echo "The gravitino server process has been terminated"
          break
      fi
      sleep 5
  done
fi

echo "Finished the entrypoint script"