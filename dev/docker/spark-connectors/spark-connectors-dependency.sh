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

# Build every Apache Gravitino Spark connector runtime shadow jar present in
# the source tree and prepare the layout consumed by the Docker image.
#
# The set of Spark runtime modules is DISCOVERED from the Gradle project, and
# the Scala variants per module are determined by inspecting each module's
# build script, so this script does not hard-code the version matrix. Whatever
# the checked-out branch supports is built automatically:
#   - branch-1.3 : Spark 3.3 (2.12), 3.4 (2.12/2.13), 3.5 (2.12/2.13)
#   - main       : Spark 3.5 (2.12/2.13), 4.0 (2.13 only, Scala-locked)
#
# Scala variant rules (derived, not hard-coded per version):
#   - A module whose build script hard-codes Scala (e.g. Spark 4.0 pins 2.13)
#     is built once, with no -PscalaVersion; the produced jar carries its own
#     Scala suffix.
#   - Otherwise the module is built for Scala 2.12, and additionally for 2.13
#     when the Spark major is >= 3.4 (Spark 3.3 is Scala 2.12 only across the
#     project).
#
# Output layout:
#   packages/connectors/spark-<major>_<scala>/gravitino-spark-connector-runtime-<major>_<scala>-*.jar

set -euo pipefail

conn_dir="$(dirname "${BASH_SOURCE-$0}")"
conn_dir="$(cd "${conn_dir}" >/dev/null; pwd)"
gravitino_home="$(cd "${conn_dir}/../../.." >/dev/null; pwd)"

cd "${gravitino_home}"

# Discover all Spark runtime modules from the Gradle project,
# e.g. "spark-runtime-3.5" -> major "3.5". Keep stderr so a Gradle failure is
# visible instead of being misreported as "no modules found".
runtime_modules="$(./gradlew -q projects | grep -oE "spark-runtime-[0-9]+\.[0-9]+" | sort -u)"

if [ -z "${runtime_modules}" ]; then
  echo "ERROR: no spark-runtime modules found in the Gradle project." >&2
  exit 1
fi

majors="$(echo "${runtime_modules}" | sed 's/spark-runtime-//' | sort -u)"

echo "Discovered Spark runtime majors: $(echo "${majors}" | tr '\n' ' ')"

# Locate a module's build.gradle.kts to inspect Scala handling.
runtime_build_file() {
  local major="$1"
  echo "spark-connector/v${major}/spark-runtime/build.gradle.kts"
}

# Detect a Scala version hard-coded in the module build script, e.g.
#   val scalaVersion: String = "2.13"
# Returns the pinned Scala version, or empty if the module reads -PscalaVersion.
detect_locked_scala() {
  local build_file="$1"
  [ -f "${build_file}" ] || { echo ""; return 0; }
  grep -oE 'val[[:space:]]+scalaVersion[[:space:]]*:[[:space:]]*String[[:space:]]*=[[:space:]]*"[0-9]+\.[0-9]+"' "${build_file}" \
    | grep -oE '"[0-9]+\.[0-9]+"' | tr -d '"' | head -n1 || true
}

# Compare two dotted versions: returns 0 (true) if $1 >= $2.
version_ge() {
  [ "$(printf '%s\n%s\n' "$2" "$1" | sort -t. -k1,1n -k2,2n | tail -n1)" = "$1" ]
}

# Build the list of "major:scala:kind" build targets, deriving Scala variants.
targets=""
for major in ${majors}; do
  build_file="$(runtime_build_file "${major}")"
  locked_scala="$(detect_locked_scala "${build_file}")"
  if [ -n "${locked_scala}" ]; then
    # Scala pinned by the module (e.g. Spark 4.0 -> 2.13). Build once.
    targets="${targets} ${major}:${locked_scala}:locked"
  else
    # Flexible module: always Scala 2.12; add 2.13 for Spark major >= 3.4.
    targets="${targets} ${major}:2.12:flag"
    if version_ge "${major}" "3.4"; then
      targets="${targets} ${major}:2.13:flag"
    fi
  fi
done

echo "Planned Spark build targets (major:scala):"
for t in ${targets}; do echo "  - ${t%:*}"; done

# Run the gradle builds. Group flag-driven builds by Scala to minimise passes.
flag_212_modules=""
flag_213_modules=""
locked_majors=""
for t in ${targets}; do
  major="$(echo "$t" | cut -d: -f1)"
  scala="$(echo "$t" | cut -d: -f2)"
  kind="$(echo "$t" | cut -d: -f3)"
  case "${kind}" in
    locked) locked_majors="${locked_majors} ${major}" ;;
    flag)
      if [ "${scala}" = "2.12" ]; then
        flag_212_modules="${flag_212_modules} :spark-connector:spark-runtime-${major}:shadowJar"
      else
        flag_213_modules="${flag_213_modules} :spark-connector:spark-runtime-${major}:shadowJar"
      fi
      ;;
  esac
done

if [ -n "${flag_212_modules}" ]; then
  # shellcheck disable=SC2086
  ./gradlew ${flag_212_modules} -PscalaVersion=2.12 -x test
fi
if [ -n "${flag_213_modules}" ]; then
  # shellcheck disable=SC2086
  ./gradlew ${flag_213_modules} -PscalaVersion=2.13 -x test
fi
for major in ${locked_majors}; do
  # shellcheck disable=SC2086
  ./gradlew :spark-connector:spark-runtime-${major}:shadowJar -x test
done

# Clean old packages
rm -rf "${conn_dir}/packages"
mkdir -p "${conn_dir}/packages/connectors"

# Copy shadow jars into per-combination directories. The jar name embeds the
# Scala suffix (…-runtime-<major>_<scala>-<ver>.jar), so we match on it and
# never rely on a hard-coded matrix here.
copy_variant() {
  local major="$1" scala="$2"
  local libs_dir="spark-connector/v${major}/spark-runtime/build/libs"
  local dest="${conn_dir}/packages/connectors/spark-${major}_${scala}"
  [ -d "${libs_dir}" ] || return 1
  local found=0
  for jar in "${libs_dir}"/*_"${scala}"-*.jar; do
    [ -e "$jar" ] || continue
    case "$jar" in
      *-empty*) continue ;;
    esac
    mkdir -p "${dest}"
    cp "$jar" "${dest}/"
    found=1
  done
  [ "${found}" -eq 1 ]
}

copied=0
for t in ${targets}; do
  major="$(echo "$t" | cut -d: -f1)"
  scala="$(echo "$t" | cut -d: -f2)"
  if copy_variant "${major}" "${scala}"; then
    copied=$((copied + 1))
  else
    echo "ERROR: no runtime jar found for Spark ${major} Scala ${scala}" >&2
    exit 1
  fi
done

if [ "${copied}" -eq 0 ]; then
  echo "ERROR: no Spark runtime jars were staged." >&2
  exit 1
fi

# Stage the canonical Apache-2.0 LICENSE and NOTICE from the repository root so
# the image ships the real texts (not drifting copies committed in-tree).
cp "${gravitino_home}/LICENSE" "${conn_dir}/licenses/LICENSE"
cp "${gravitino_home}/NOTICE" "${conn_dir}/licenses/NOTICE"

echo ""
echo "=== Spark connectors prepared (${copied} combination(s)) ==="
echo "Output: ${conn_dir}/packages/connectors/"
find "${conn_dir}/packages/connectors/" -name "*.jar" | sort
