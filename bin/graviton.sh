#!/bin/bash
#
# Copyright 2023 Datastrato.
# This software is licensed under the Apache License version 2.
#
set -ex
USAGE="-e Usage: bin/graviton.sh [--config <conf-dir>]\n\t
        {start|stop|upstart|status}\n\t
        [--version | -v]"

if [[ "$1" == "--config" ]]; then
  shift
  conf_dir="$1"
  if [[ ! -d "${conf_dir}" ]]; then
    echo "ERROR : ${conf_dir} is not a directory"
    echo ${USAGE}
    exit 1
  else
    export GRAVITON_CONF_DIR="${conf_dir}"
  fi
  shift
fi

bin="$(dirname "${BASH_SOURCE-$0}")"
bin="$(cd "${bin}">/dev/null; pwd)"

. "${bin}/common.sh"

check_java_version

HOSTNAME=$(hostname)

GRAVITON_OUTFILE="${GRAVITON_LOG_DIR}/graviton-${HOSTNAME}.out"

function wait_zeppelin_is_up_for_ci() {
  if [[ "${CI}" == "true" ]]; then
    local count=0;
    while [[ "${count}" -lt 30 ]]; do
      curl -v localhost:8090 2>&1 | grep '200 OK'
      if [[ $? -ne 0 ]]; then
        sleep 1
        continue
      else
        break
      fi
        let "count+=1"
    done
  fi
}

function check_if_process_is_alive() {
  local pid
  pid=$(cat ${GRAVITON_PID})
  if ! kill -0 ${pid} >/dev/null 2>&1; then
    echo "Graviton Server process died"
    return 1
  fi
}

function start() {
  local pid

  if [[ -f "${GRAVITON_PID}" ]]; then
    pid=$(cat ${GRAVITON_PID})
    if kill -0 ${pid} >/dev/null 2>&1; then
      echo "Graviton Server is already running"
      return 0;
    fi
  fi

  initialize_default_directories

  nohup ${JAVA_RUNNER} ${JAVA_OPTS} ${GRAVITON_DEBUG_OPTS} -cp ${GRAVITON_CLASSPATH} ${GRAVITON_SERVER_NAME} >> "${GRAVITON_OUTFILE}" 2>&1 < /dev/null &

  pid=$!
  if [[ -z "${pid}" ]]; then
    echo "Graviton Server start error!"
    return 1;
  else
    echo "Graviton Server start success!"
    echo ${pid} > ${GRAVITON_PID}
  fi

  wait_zeppelin_is_up_for_ci
  sleep 3
  check_if_process_is_alive
}

function stop() {
  echo "stop"
}

function status() {
  echo "status"
}

function version() {
  echo "version"
}

function getGravitonVersion() {
    echo "getGravitonVersion"
}

function find_graviton_process() {
  local pid

  if [[ -f "${GRAVITON_PID}" ]]; then
    pid=$(cat ${GRAVITON_PID})
    if ! kill -0 ${pid} > /dev/null 2>&1; then
      echo "Graviton Server running but process is dead!"
      return 1
    else
      echo "Graviton Server is running[$pid]"
    fi
  else
    echo "Graviton Server is not running"
    return 1
  fi
}

GRAVITON_SERVER_NAME=com.datastrato.graviton.server.GravitonServer

JAVA_OPTS+=" -Dfile.encoding=UTF-8"
JAVA_OPTS+=" -Dlog4j2.configurationFile=file://${GRAVITON_CONF_DIR}/log4j2.properties"
JAVA_OPTS+=" -Dgraviton.log.path=${GRAVITON_LOG_DIR} ${GRAVITON_MEM}"

# construct classpath
if [[ -d "${GRAVITON_HOME}/catalog-hive/target/classes" ]]; then
  GRAVITON_CLASSPATH+=":${GRAVITON_HOME}/catalog-hive/target/classes"
fi

if [[ -d "${GRAVITON_HOME}/graviton-server/target/classes" ]]; then
  GRAVITON_CLASSPATH+=":${GRAVITON_HOME}/graviton-server/target/classes"
fi

addJarInDir "${GRAVITON_HOME}/lib"
#addJarInDir "${GRAVITON_HOME}/catalog/catalog-hive/lib"

GRAVITON_CLASSPATH="${CLASSPATH}:${GRAVITON_CLASSPATH}"

function initialize_default_directories() {
  if [[ ! -d "${GRAVITON_LOG_DIR}" ]]; then
    echo "Log dir doesn't exist, create ${GRAVITON_LOG_DIR}"
    mkdir -p "${GRAVITON_LOG_DIR}"
  fi

  if [[ ! -d "${GRAVITON_PID_DIR}" ]]; then
    echo "Pid dir doesn't exist, create ${GRAVITON_PID_DIR}"
    mkdir -p "${GRAVITON_PID_DIR}"
  fi
}

#exec ${JAVA_RUNNER} ${JAVA_OPTS} ${GRAVITON_DEBUG_OPTS} -cp ${GRAVITON_CLASSPATH} ${GRAVITON_SERVER_NAME} "$@"

case "${1}" in
  start)
    start
    ;;
  stop)
    stop
    ;;
  restart)
#    echo "${ZEPPELIN_NAME} is restarting" >> "${ZEPPELIN_OUTFILE}"
    stop
    start
    ;;
  status)
    find_graviton_process
    ;;
  -v | --version)
    getGravitonVersion
    ;;
  *)
    echo ${USAGE}
esac