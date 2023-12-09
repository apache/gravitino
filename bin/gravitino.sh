#!/bin/bash
#
# Copyright 2023 Datastrato Pvt Ltd.
# This software is licensed under the Apache License version 2.
#
#set -ex
USAGE="-e Usage: bin/gravitino.sh [--config <conf-dir>]\n\t
        {start|stop|restart|status}"

if [[ "$1" == "--config" ]]; then
  shift
  conf_dir="$1"
  if [[ ! -d "${conf_dir}" ]]; then
    echo "ERROR : ${conf_dir} is not a directory"
    echo ${USAGE}
    exit 1
  else
    export GRAVITINO_CONF_DIR="${conf_dir}"
  fi
  shift
fi

bin="$(dirname "${BASH_SOURCE-$0}")"
bin="$(cd "${bin}">/dev/null; pwd)"

. "${bin}/common.sh"

check_java_version

function check_process_status() {
  local pid=$(found_gravitino_server_pid)

  if [[ -z "${pid}" ]]; then
    echo "Gravitino Server is not running"
  else
    echo "Gravitino Server is running[PID:$pid]"
  fi
}

function found_gravitino_server_pid() {
  process_name='GravitinoServer';
  RUNNING_PIDS=$(ps x | grep ${process_name} | grep -v grep | awk '{print $1}');

  if [[ -z "${RUNNING_PIDS}" ]]; then
    return
  fi

  if ! kill -0 ${RUNNING_PIDS} > /dev/null 2>&1; then
    echo "Gravitino Server running but process is dead"
  fi

  echo "${RUNNING_PIDS}"
}

function wait_for_gravitino_server_to_die() {
  timeout=10
  timeoutTime=$(date "+%s")
  let "timeoutTime+=$timeout"
  currentTime=$(date "+%s")
  forceKill=1

  while [[ $currentTime -lt $timeoutTime ]]; do
    local pid=$(found_gravitino_server_pid)
    if [[ -z "${pid}" ]]; then
      forceKill=0
      break
    fi

    $(kill ${pid} > /dev/null 2> /dev/null)
    if kill -0 ${pid} > /dev/null 2>&1; then
      sleep 3
    else
      forceKill=0
      break
    fi
    currentTime=$(date "+%s")
  done

  if [[ forceKill -ne 0 ]]; then
    $(kill -9 ${pid} > /dev/null 2> /dev/null)
  fi
}

function start() {
  local pid=$(found_gravitino_server_pid)

  if [[ ! -z "${pid}" ]]; then
    if kill -0 ${pid} >/dev/null 2>&1; then
      echo "Gravitino Server is already running"
      return 0;
    fi
  fi

  if [[ ! -d "${GRAVITINO_LOG_DIR}" ]]; then
    echo "Log dir doesn't exist, create ${GRAVITINO_LOG_DIR}"
    mkdir -p "${GRAVITINO_LOG_DIR}"
  fi

  nohup ${JAVA_RUNNER} ${JAVA_OPTS} ${GRAVITINO_DEBUG_OPTS} -cp ${GRAVITINO_CLASSPATH} ${GRAVITINO_SERVER_NAME} >> "${GRAVITINO_OUTFILE}" 2>&1 &

  pid=$!
  if [[ -z "${pid}" ]]; then
    echo "Gravitino Server start error!"
    return 1;
  else
    echo "Gravitino Server start success!"
  fi

  sleep 2
  check_process_status
}

function stop() {
  local pid

  pid=$(found_gravitino_server_pid)

  if [[ -z "${pid}" ]]; then
    echo "Gravitino Server is not running"
  else
    wait_for_gravitino_server_to_die
    echo "Gravitino Server stop"
  fi
}

HOSTNAME=$(hostname)
GRAVITINO_OUTFILE="${GRAVITINO_LOG_DIR}/gravitino-server.out"
GRAVITINO_SERVER_NAME=com.datastrato.gravitino.server.GravitinoServer

JAVA_OPTS+=" -Dfile.encoding=UTF-8"
JAVA_OPTS+=" -Dlog4j2.configurationFile=file://${GRAVITINO_CONF_DIR}/log4j2.properties"
JAVA_OPTS+=" -Dgravitino.log.path=${GRAVITINO_LOG_DIR} ${GRAVITINO_MEM}"

addJarInDir "${GRAVITINO_HOME}/libs"

case "${1}" in
  start)
    start
    ;;
  stop)
    stop
    ;;
  restart)
    stop
    start
    ;;
  status)
    check_process_status
    ;;
  *)
    echo ${USAGE}
esac