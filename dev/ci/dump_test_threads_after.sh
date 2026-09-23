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
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Waits for the given number of minutes, then prints a thread dump of every running Gradle test
# JVM. Start it in the background before a long Gradle build so that a hung test shows its stack
# in the CI log before the job times out:
#
#   dev/ci/dump_test_threads_after.sh 85 &
#
# Dumps are also written to build/reports/thread-dumps so the failure report upload keeps them.

set -uo pipefail

delay_minutes="${1:?usage: $0 <delay-minutes>}"
dump_dir="${2:-build/reports/thread-dumps}"

sleep "$((delay_minutes * 60))"

pids=$(pgrep -f 'Gradle Test Executor' || true)
if [ -z "${pids}" ]; then
  echo "[TEST-THREAD-DUMP] No Gradle test JVM is running after ${delay_minutes} minutes."
  exit 0
fi

mkdir -p "${dump_dir}"
for pid in ${pids}; do
  dump_file="${dump_dir}/test-jvm-${pid}.txt"
  echo "[TEST-THREAD-DUMP] Gradle test JVM ${pid} is still running after ${delay_minutes} minutes:"
  ps -o args= -p "${pid}" | cut -c1-500
  jcmd "${pid}" Thread.print > "${dump_file}" 2>&1 || jstack "${pid}" > "${dump_file}" 2>&1
  cat "${dump_file}"
done
