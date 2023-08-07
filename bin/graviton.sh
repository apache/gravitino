#!/bin/bash
#
# Copyright 2023 Datastrato.
# This software is licensed under the Apache License version 2.
#
set -ex
function usage() {
    echo "Usage: bin/graviton.sh [--config <conf-dir>]"
}

POSITIONAL=()
while [[ $# -gt 0 ]]
do
  key="$1"
  case $key in
    --config)
    export GRAVITON_CONF_DIR="$2"
    shift # past argument
    shift # past value
    ;;
    -h|--help)
        usage
        exit 0
        ;;
    *)
        echo "Unsupported argument."
        usage
        exit 1
        ;;
  esac
done
set -- "${POSITIONAL[@]}" # restore positional parameters

bin="$(dirname "${BASH_SOURCE-$0}")"
bin="$(cd "${bin}">/dev/null; pwd)"

. "${bin}/common.sh"

check_java_version

HOSTNAME=$(hostname)
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

if [[ ! -d "${GRAVITON_LOG_DIR}" ]]; then
  echo "Log dir doesn't exist, create ${GRAVITON_LOG_DIR}"
  mkdir -p "${GRAVITON_LOG_DIR}"
fi

if [[ ! -d "${GRAVITON_PID_DIR}" ]]; then
  echo "Pid dir doesn't exist, create ${GRAVITON_PID_DIR}"
  mkdir -p "${GRAVITON_PID_DIR}"
fi

exec ${JAVA_RUNNER} ${JAVA_OPTS} ${GRAVITON_DEBUG_OPTS} -cp ${GRAVITON_CLASSPATH} ${GRAVITON_SERVER_NAME} "$@"
