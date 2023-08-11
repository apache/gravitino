#!/bin/bash
#
# Copyright 2023 Datastrato.
# This software is licensed under the Apache License version 2.
#

if [ -L "${BASH_SOURCE-$0}" ]; then
  FWDIR=$(dirname "$(readlink "${BASH_SOURCE-$0}")")
else
  FWDIR=$(dirname "${BASH_SOURCE-$0}")
fi

echo ${GRAVITON_HOME}

if [[ -z "${GRAVITON_HOME}" ]]; then
  export GRAVITON_HOME="$(cd "${FWDIR}/.." || exit; pwd)"
fi

if [[ -z "${GRAVITON_CONF_DIR}" ]]; then
  export GRAVITON_CONF_DIR="${GRAVITON_HOME}/conf"
fi

if [[ -z "${GRAVITON_LOG_DIR}" ]]; then
  export GRAVITON_LOG_DIR="${GRAVITON_HOME}/logs"
fi

if [[ -f "${GRAVITON_CONF_DIR}/graviton-env.sh" ]]; then
  . "${GRAVITON_CONF_DIR}/graviton-env.sh"
fi

GRAVITON_CLASSPATH+=":${GRAVITON_CONF_DIR}"

function check_java_version() {
  if [[ -n "${JAVA_HOME+x}" ]]; then
    JAVA="$JAVA_HOME/bin/java"
  fi
  java_ver_output=$("${JAVA:-java}" -version 2>&1)
  jvmver=$(echo "$java_ver_output" | grep '[openjdk|java] version' | awk -F'"' 'NR==1 {print $2}' | cut -d\- -f1)
  JVM_VERSION=$(echo "$jvmver"|sed -e 's|^\([0-9][0-9]*\)\..*$|\1|')
  if [ "$JVM_VERSION" = "1" ]; then
    JVM_VERSION=$(echo "$jvmver"|sed -e 's|^1\.\([0-9][0-9]*\)\..*$|\1|')
  fi

  # JDK 8u151 version fixed a number of security vulnerabilities and issues to improve system stability and security.
  # https://www.oracle.com/java/technologies/javase/8u151-relnotes.html
  if [ "$JVM_VERSION" -lt 8 ] || { [ "$JVM_VERSION" -eq 8 ] && [ "${jvmver#*_}" -lt 151 ]; } ; then
    echo "Graviton requires either Java 8 update 151 or newer"
    exit 1;
  fi
}

function addEachJarInDir(){
  if [[ -d "${1}" ]]; then
    for jar in "${1}"/*.jar ; do
      GRAVITON_CLASSPATH="${jar}:${GRAVITON_CLASSPATH}"
    done
  fi
}

function addEachJarInDirRecursive(){
  if [[ -d "${1}" ]]; then
    for jar in "${1}"/**/*.jar ; do
      GRAVITON_CLASSPATH="${jar}:${GRAVITON_CLASSPATH}"
    done
  fi
}

function addJarInDir(){
  if [[ -d "${1}" ]]; then
    GRAVITON_CLASSPATH="${1}/*:${GRAVITON_CLASSPATH}"
  fi
}

if [[ -z "${GRAVITON_MEM}" ]]; then
  export GRAVITON_MEM="-Xmx1024m"
fi

if [[ -n "${JAVA_HOME}" ]]; then
  export JAVA_RUNNER="${JAVA_HOME}/bin/java"
else
  export JAVA_RUNNER=java
fi
