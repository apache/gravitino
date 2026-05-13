#!/usr/bin/env bash
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

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

SPARK_HOME="${SPARK_HOME:-/Users/fanng/deploy/demo/spark-3.5.3-bin-hadoop3}"
SPARK_SQL_BIN="${SPARK_SQL_BIN:-${SPARK_HOME}/bin/spark-sql}"
GRAVITINO_HOME="${GRAVITINO_HOME:-/Users/fanng/deploy/chinamobile/package}"

GRAVITINO_URI="${GRAVITINO_URI:-http://127.0.0.1:8090}"
GRAVITINO_METAKE="${GRAVITINO_METAKE:-test}"
ICEBERG_REST_URI="${ICEBERG_REST_URI:-http://127.0.0.1:9001/iceberg/}"
LANCE_REST_URI="${LANCE_REST_URI:-http://127.0.0.1:9101/lance}"
LANCE_PARENT_CATALOG="${LANCE_PARENT_CATALOG:-lance_catalog}"

GRAVITINO_SPARK_CONNECTOR_JAR="${GRAVITINO_SPARK_CONNECTOR_JAR:-/Users/fanng/deploy/demo/jars/gravitino-spark-connector-runtime-3.5_2.12-1.2.0.jar}"
LANCE_SPARK_BUNDLE_JAR="${LANCE_SPARK_BUNDLE_JAR:-/Users/fanng/deploy/chinamobile/jars/lance-spark-bundle-3.5_2.12-0.2.0.jar}"
ICEBERG_SPARK_RUNTIME_JAR="${ICEBERG_SPARK_RUNTIME_JAR:-/Users/fanng/deploy/demo/jars/iceberg-spark-runtime-3.5_2.12-1.9.2.jar}"
ICEBERG_AWS_BUNDLE_JAR="${ICEBERG_AWS_BUNDLE_JAR:-/Users/fanng/deploy/demo/jars/iceberg-aws-bundle-1.9.2.jar}"

GRAVITINO_FORCE_LOCAL_CONNECTOR_BUILD="${GRAVITINO_FORCE_LOCAL_CONNECTOR_BUILD:-false}"
LANCE_CATALOG_CLASS="${LANCE_CATALOG_CLASS:-}"
LANCE_SPARK_EXTENSION_CLASS="${LANCE_SPARK_EXTENSION_CLASS:-}"

log() {
  printf '[spark-demo] %s\n' "$*"
}

fail() {
  printf '[spark-demo] ERROR: %s\n' "$*" >&2
  exit 1
}

require_path() {
  local path="$1"
  [[ -e "${path}" ]] || fail "Required path not found: ${path}"
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "Required command not found: $1"
}

resolve_lance_catalog_class() {
  if [[ -n "${LANCE_CATALOG_CLASS}" ]]; then
    printf '%s\n' "${LANCE_CATALOG_CLASS}"
    return 0
  fi

  require_path "${LANCE_SPARK_BUNDLE_JAR}"

  if jar tf "${LANCE_SPARK_BUNDLE_JAR}" | rg -q '^org/lance/spark/LanceNamespaceSparkCatalog.class$'; then
    printf 'org.lance.spark.LanceNamespaceSparkCatalog\n'
    return 0
  fi

  if jar tf "${LANCE_SPARK_BUNDLE_JAR}" | rg -q '^com/lancedb/lance/spark/LanceNamespaceSparkCatalog.class$'; then
    printf 'com.lancedb.lance.spark.LanceNamespaceSparkCatalog\n'
    return 0
  fi

  fail "Unable to locate LanceNamespaceSparkCatalog in ${LANCE_SPARK_BUNDLE_JAR}"
}

resolve_lance_extension_class() {
  if [[ -n "${LANCE_SPARK_EXTENSION_CLASS}" ]]; then
    printf '%s\n' "${LANCE_SPARK_EXTENSION_CLASS}"
    return 0
  fi

  require_path "${LANCE_SPARK_BUNDLE_JAR}"

  if jar tf "${LANCE_SPARK_BUNDLE_JAR}" \
    | rg -q '^org/lance/spark/extensions/LanceSparkSessionExtensions.class$'; then
    printf 'org.lance.spark.extensions.LanceSparkSessionExtensions\n'
    return 0
  fi

  if jar tf "${LANCE_SPARK_BUNDLE_JAR}" \
    | rg -q '^com/lancedb/lance/spark/extensions/LanceSparkSessionExtensions.class$'; then
    printf 'com.lancedb.lance.spark.extensions.LanceSparkSessionExtensions\n'
    return 0
  fi

  fail "Unable to locate LanceSparkSessionExtensions in ${LANCE_SPARK_BUNDLE_JAR}"
}

build_local_gravitino_connector_jar() {
  local jar_path

  log "Building local Spark 3.5 connector runtime jar"
  (
    cd "${REPO_ROOT}"
    ./gradlew :spark-connector:spark-runtime-3.5:shadowJar -x test >/dev/null
  )

  jar_path="$(
    find "${REPO_ROOT}/spark-connector/v3.5/spark-runtime/build/libs" \
      -maxdepth 1 \
      -type f \
      -name 'gravitino-spark-connector-runtime-3.5_2.12-*.jar' \
      | head -n 1
  )"
  [[ -n "${jar_path}" ]] || fail "Local Spark connector runtime jar was not produced"
  printf '%s\n' "${jar_path}"
}

resolve_gravitino_connector_jar() {
  if [[ "${GRAVITINO_FORCE_LOCAL_CONNECTOR_BUILD}" == "true" ]]; then
    build_local_gravitino_connector_jar
    return 0
  fi

  if [[ -f "${GRAVITINO_SPARK_CONNECTOR_JAR}" ]]; then
    printf '%s\n' "${GRAVITINO_SPARK_CONNECTOR_JAR}"
    return 0
  fi

  build_local_gravitino_connector_jar
}

collect_jars() {
  local jars=("$@")
  local jar
  local joined=""

  for jar in "${jars[@]}"; do
    require_path "${jar}"
    if [[ -z "${joined}" ]]; then
      joined="${jar}"
    else
      joined="${joined},${jar}"
    fi
  done

  printf '%s\n' "${joined}"
}

main() {
  local connector_jar
  local lance_catalog_class
  local lance_extension_class
  local jars

  require_command jar
  require_command rg
  require_command find
  require_path "${SPARK_SQL_BIN}"
  require_path "${GRAVITINO_HOME}"

  connector_jar="$(resolve_gravitino_connector_jar)"
  lance_catalog_class="$(resolve_lance_catalog_class)"
  lance_extension_class="$(resolve_lance_extension_class)"
  jars="$(
    collect_jars \
      "${connector_jar}" \
      "${LANCE_SPARK_BUNDLE_JAR}" \
      "${ICEBERG_SPARK_RUNTIME_JAR}" \
      "${ICEBERG_AWS_BUNDLE_JAR}"
  )"

  log "Using Gravitino Spark connector jar: ${connector_jar}"
  log "Using Lance catalog class: ${lance_catalog_class}"
  log "Using Lance Spark extension class: ${lance_extension_class}"

  exec "${SPARK_SQL_BIN}" \
    --jars "${jars}" \
    --conf "spark.plugins=org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin" \
    --conf "spark.sql.extensions=${lance_extension_class}" \
    --conf "spark.sql.gravitino.uri=${GRAVITINO_URI}" \
    --conf "spark.sql.gravitino.metalake=${GRAVITINO_METAKE}" \
    --conf "spark.sql.gravitino.enableIcebergSupport=true" \
    --conf "spark.sql.catalog.iceberg_rest=org.apache.iceberg.spark.SparkCatalog" \
    --conf "spark.sql.catalog.iceberg_rest.type=rest" \
    --conf "spark.sql.catalog.iceberg_rest.uri=${ICEBERG_REST_URI}" \
    --conf "spark.sql.catalog.iceberg_rest.header.X-Iceberg-Access-Delegation=vended-credentials" \
    --conf "spark.sql.catalog.lance=${lance_catalog_class}" \
    --conf "spark.sql.catalog.lance.impl=rest" \
    --conf "spark.sql.catalog.lance.uri=${LANCE_REST_URI}" \
    --conf "spark.sql.catalog.lance.parent=${LANCE_PARENT_CATALOG}" \
    --conf "spark.driver.extraJavaOptions=--add-opens=java.base/sun.nio.ch=ALL-UNNAMED" \
    --conf "spark.executor.extraJavaOptions=--add-opens=java.base/sun.nio.ch=ALL-UNNAMED" \
    "$@"
}

main "$@"
