#!/bin/bash
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