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
# Stages two Gravitino distribution instances (A on port 8090, B on 8190)
# backed by a shared MySQL entity store and starts both servers.
# Called by .github/workflows/multi-instance-consistency-test.yml.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"

# ---------------------------------------------------------------------------
# 1. MySQL JDBC driver
#    The entity store needs the connector in libs/, not only under
#    catalogs/jdbc-mysql/libs/ (which is for the JDBC catalog backend).
#    mysql-connector-java was renamed to mysql-connector-j starting with
#    8.0.31 (new Maven group: com.mysql).
# ---------------------------------------------------------------------------
JDBC_VERSION=8.0.33
JDBC_JAR=distribution/package/libs/mysql-connector-j-${JDBC_VERSION}.jar
JDBC_URL=https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/${JDBC_VERSION}/mysql-connector-j-${JDBC_VERSION}.jar

curl -fsSL "${JDBC_URL}" -o "${JDBC_JAR}"
EXPECTED_SHA1="$(curl -fsSL "${JDBC_URL}.sha1")"
echo "${EXPECTED_SHA1}  ${JDBC_JAR}" | sha1sum --check

# ---------------------------------------------------------------------------
# 2. Copy distribution to get two independent instances.
# ---------------------------------------------------------------------------
cp -a distribution/package distribution/package-b

# ---------------------------------------------------------------------------
# 3. Patch gravitino.conf for each instance.
#    Uses Python for reliable key-present/key-absent handling: if a key
#    already exists it is replaced in-place; if it is absent it is appended
#    so it is never silently skipped.
# ---------------------------------------------------------------------------
configure_instance() {
  local conf="$1" http="$2" iceberg="$3" lance="$4"
  python3 - "$conf" "$http" "$iceberg" "$lance" <<'PY'
import sys, re, pathlib
conf, http, iceberg, lance = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
p = pathlib.Path(conf)
src = p.read_text()
subs = [
  (r'^gravitino\.entity\.store\.relational\.jdbcUrl\s*=.*$',
   'gravitino.entity.store.relational.jdbcUrl = '
   'jdbc:mysql://127.0.0.1:3306/gravitino?useSSL=false&allowPublicKeyRetrieval=true'),
  (r'^gravitino\.entity\.store\.relational\.jdbcDriver\s*=.*$',
   'gravitino.entity.store.relational.jdbcDriver = com.mysql.cj.jdbc.Driver'),
  (r'^gravitino\.entity\.store\.relational\.jdbcUser\s*=.*$',
   'gravitino.entity.store.relational.jdbcUser = gravitino'),
  (r'^gravitino\.entity\.store\.relational\.jdbcPassword\s*=.*$',
   'gravitino.entity.store.relational.jdbcPassword = gravitino'),
  (r'^gravitino\.cache\.enabled\s*=.*$',
   'gravitino.cache.enabled = false'),
  (r'^gravitino\.authorization\.enable\s*=.*$',
   'gravitino.authorization.enable = true'),
  (r'^gravitino\.authorization\.serviceAdmins\s*=.*$',
   'gravitino.authorization.serviceAdmins = admin'),
  (r'^gravitino\.server\.webserver\.httpPort\s*=.*$',
   f'gravitino.server.webserver.httpPort = {http}'),
  (r'^gravitino\.iceberg-rest\.httpPort\s*=.*$',
   f'gravitino.iceberg-rest.httpPort = {iceberg}'),
  (r'^gravitino\.lance-rest\.httpPort\s*=.*$',
   f'gravitino.lance-rest.httpPort = {lance}'),
]
appended = []
for pat, repl in subs:
  new_src = re.sub(pat, repl, src, flags=re.MULTILINE)
  if new_src == src:
    appended.append(repl)
  src = new_src
if appended:
  src = src.rstrip('\n') + '\n' + '\n'.join(appended) + '\n'
p.write_text(src)
PY
}

configure_instance distribution/package/conf/gravitino.conf   8090 9001 9101
configure_instance distribution/package-b/conf/gravitino.conf 8190 9011 9111

# Instance B also needs the JDBC driver.
cp "${JDBC_JAR}" distribution/package-b/libs/

# ---------------------------------------------------------------------------
# 4. Patch bin/gravitino.sh PID detection so two installs on the same host
#    do not interfere.
#    bin/gravitino.sh's found_gravitino_server_pid() greps `ps` only by
#    the "GravitinoServer" process name. When two installs share a host the
#    second `start` invocation sees the first install's JVM and
#    short-circuits with "Gravitino Server is already running" — B never
#    actually starts. Scope the grep by GRAVITINO_HOME so each install only
#    matches its own JVM.
# ---------------------------------------------------------------------------
patch_pid_grep() {
  python3 - "$1" <<'PY'
import sys, pathlib
p = pathlib.Path(sys.argv[1])
src = p.read_text()
# If the script already scopes the PID lookup by GRAVITINO_HOME
# (the template was updated upstream), no patching is needed.
if '${GRAVITINO_HOME}' in src:
  sys.exit(0)
old = ("RUNNING_PIDS=$(ps x | grep ${process_name} | "
       "grep -v grep | awk '{print $1}');")
new = ("RUNNING_PIDS=$(ps x | grep ${process_name} | "
       'grep "${GRAVITINO_HOME}" | '
       "grep -v grep | awk '{print $1}');")
assert old in src, f"pid-grep line not found in {p}"
p.write_text(src.replace(old, new))
PY
}

patch_pid_grep distribution/package/bin/gravitino.sh
patch_pid_grep distribution/package-b/bin/gravitino.sh

# ---------------------------------------------------------------------------
# 5. Start both instances and wait until they are ready.
# ---------------------------------------------------------------------------
distribution/package/bin/gravitino.sh   start
distribution/package-b/bin/gravitino.sh start

wait_ready() {
  local url=$1
  for i in $(seq 1 60); do
    if curl -fsS --connect-timeout 2 --max-time 5 "$url/api/version" >/dev/null 2>&1; then
      echo "$url ready"
      return 0
    fi
    sleep 2
  done
  echo "$url did NOT become ready in time" >&2
  cat distribution/package/logs/*.log   2>/dev/null || true
  cat distribution/package-b/logs/*.log 2>/dev/null || true
  return 1
}

wait_ready http://localhost:8090
wait_ready http://localhost:8190
