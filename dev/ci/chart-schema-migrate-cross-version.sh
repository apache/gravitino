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
#

# Live kind coverage: seed an existing relational schema at FROM_VERSION, then
# helm upgrade with schema.migrate.enabled so the Job applies the upgrade chain
# to the image version.
#
# Required env:
#   KIND_REGISTRY, tag
# Optional env:
#   ENGINE (mysql|postgresql, default mysql)
#   FROM_VERSION (1.2.0|1.3.0, default 1.2.0)
#   NAMESPACE, RELEASE_NAME, CHART_DIR, VALUES_FILE, REPO_ROOT
set -euo pipefail

REPO_ROOT="${REPO_ROOT:-$(cd "$(dirname "$0")/../.." && pwd)}"
CHART_DIR="${CHART_DIR:-${REPO_ROOT}/dev/charts/gravitino}"
ENGINE="${ENGINE:-mysql}"
FROM_VERSION="${FROM_VERSION:-1.2.0}"
CHECK_SCRIPT="${REPO_ROOT}/dev/ci/check-schema-migrate.sh"

case "$ENGINE" in
  mysql)
    VALUES_FILE="${VALUES_FILE:-${REPO_ROOT}/dev/charts/gravitino/resources/scenarios/schema-migrate-mysql.yaml}"
    SCHEMA_SQL="${REPO_ROOT}/scripts/mysql/schema-${FROM_VERSION}-mysql.sql"
    WAIT_INIT=init-mysql
    DB_LABEL=mysql
    ;;
  postgresql)
    VALUES_FILE="${VALUES_FILE:-${REPO_ROOT}/dev/charts/gravitino/resources/scenarios/schema-migrate-postgresql.yaml}"
    SCHEMA_SQL="${REPO_ROOT}/scripts/postgresql/schema-${FROM_VERSION}-postgresql.sql"
    WAIT_INIT=init-postgresql
    DB_LABEL=postgresql
    ;;
  *)
    echo "ERROR: ENGINE must be mysql or postgresql (got ${ENGINE})" >&2
    exit 1
    ;;
esac

case "$FROM_VERSION" in
  1.2.0 | 1.3.0) ;;
  *)
    echo "ERROR: FROM_VERSION must be 1.2.0 or 1.3.0 (got ${FROM_VERSION})" >&2
    exit 1
    ;;
esac

FROM_SLUG=${FROM_VERSION//./}
NAMESPACE="${NAMESPACE:-schema-migrate-cross-${ENGINE}-${FROM_SLUG}}"
RELEASE_NAME="${RELEASE_NAME:-gravitino-sm-${ENGINE}-${FROM_SLUG}}"

: "${KIND_REGISTRY:?KIND_REGISTRY is required}"
: "${tag:?tag is required}"

if [ ! -f "$SCHEMA_SQL" ]; then
  echo "ERROR: missing baseline schema SQL: ${SCHEMA_SQL}" >&2
  exit 1
fi
if [ ! -f "$CHECK_SCRIPT" ]; then
  echo "ERROR: missing ${CHECK_SCRIPT}" >&2
  exit 1
fi

chmod +x "$CHECK_SCRIPT"
helm dependency update "$CHART_DIR"

cleanup() {
  helm uninstall "$RELEASE_NAME" -n "$NAMESPACE" --wait || true
  kubectl delete namespace "$NAMESPACE" --wait=false || true
}
trap cleanup EXIT

kubectl get namespace "$NAMESPACE" >/dev/null 2>&1 || kubectl create namespace "$NAMESPACE"

echo "==> Fresh install with schema.migrate.enabled (${ENGINE})"
helm upgrade --install "$RELEASE_NAME" "$CHART_DIR" -n "$NAMESPACE" --wait --timeout 10m \
  --values "$VALUES_FILE" \
  --set image.registry="" \
  --set image.repository="${KIND_REGISTRY}/gravitino" \
  --set image.tag="${tag}"
bash "$CHECK_SCRIPT" "$NAMESPACE" "$RELEASE_NAME" "$ENGINE"

DEPLOY=$(kubectl get deploy -n "$NAMESPACE" -l "release=${RELEASE_NAME}" -o jsonpath='{.items[0].metadata.name}')
DB_POD=$(kubectl get pods -n "$NAMESPACE" -l "app.kubernetes.io/name=${DB_LABEL}" \
  -o jsonpath='{.items[0].metadata.name}')
DB_NAME=$(kubectl get deploy -n "$NAMESPACE" "$DEPLOY" \
  -o jsonpath="{.spec.template.spec.initContainers[?(@.name==\"${WAIT_INIT}\")].env[?(@.name==\"GRAVITINO_DB\")].value}")

column_count() {
  local table="$1"
  local column="$2"
  if [ "$ENGINE" = mysql ]; then
    local root_pw
    root_pw=$(kubectl get secret -n "$NAMESPACE" "${RELEASE_NAME}-mysql" \
      -o jsonpath='{.data.mysql-root-password}' | base64 -d)
    kubectl exec -n "$NAMESPACE" "$DB_POD" -- \
      mysql -uroot -p"${root_pw}" -D "${DB_NAME}" -sNe \
      "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='${table}' AND column_name='${column}'" \
      | tr -d '[:space:]'
  else
    local pg_user pg_pw
    pg_user=$(kubectl get deploy -n "$NAMESPACE" "$DEPLOY" \
      -o jsonpath='{.spec.template.spec.initContainers[?(@.name=="init-postgresql")].env[?(@.name=="POSTGRES_USER")].value}')
    pg_pw=$(kubectl get secret -n "$NAMESPACE" "${RELEASE_NAME}-postgresql" \
      -o jsonpath='{.data.password}' | base64 -d)
    kubectl exec -n "$NAMESPACE" "$DB_POD" -- \
      env PGPASSWORD="${pg_pw}" psql -U "${pg_user}" -d "${DB_NAME}" -tAc \
      "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='${table}' AND column_name='${column}'" \
      | tr -d '[:space:]'
  fi
}

assert_column() {
  local table="$1"
  local column="$2"
  local expected="$3"
  local actual
  actual=$(column_count "$table" "$column")
  if [ "$actual" != "$expected" ]; then
    echo "ERROR: ${table}.${column} count=${actual}, expected ${expected}" >&2
    exit 1
  fi
}

echo "==> Scale Gravitino to zero before reseeding ${FROM_VERSION} ${ENGINE} schema"
kubectl scale deploy -n "$NAMESPACE" "$DEPLOY" --replicas=0
kubectl wait -n "$NAMESPACE" --for=delete pod -l "release=${RELEASE_NAME}" --timeout=180s || true

echo "==> Reset database to schema-${FROM_VERSION}-${ENGINE}.sql"
REMOTE_SQL="/tmp/schema-${FROM_VERSION}-${ENGINE}.sql"
kubectl cp "$SCHEMA_SQL" "${NAMESPACE}/${DB_POD}:${REMOTE_SQL}"
if [ "$ENGINE" = mysql ]; then
  ROOT_PW=$(kubectl get secret -n "$NAMESPACE" "${RELEASE_NAME}-mysql" \
    -o jsonpath='{.data.mysql-root-password}' | base64 -d)
  kubectl exec -n "$NAMESPACE" "$DB_POD" -- \
    mysql -uroot -p"${ROOT_PW}" -e "DROP DATABASE IF EXISTS \`${DB_NAME}\`; CREATE DATABASE \`${DB_NAME}\`;"
  kubectl exec -n "$NAMESPACE" "$DB_POD" -- \
    bash -c "mysql -uroot -p\"${ROOT_PW}\" -D '${DB_NAME}' < '${REMOTE_SQL}'"
else
  PG_USER=$(kubectl get deploy -n "$NAMESPACE" "$DEPLOY" \
    -o jsonpath='{.spec.template.spec.initContainers[?(@.name=="init-postgresql")].env[?(@.name=="POSTGRES_USER")].value}')
  PG_PW=$(kubectl get secret -n "$NAMESPACE" "${RELEASE_NAME}-postgresql" \
    -o jsonpath='{.data.password}' | base64 -d)
  # Drop schema objects so leftover gravitino_schema_migration cannot leak.
  # Prefer DROP SCHEMA over DROP DATABASE: the chart app user may not be a superuser.
  kubectl exec -n "$NAMESPACE" "$DB_POD" -- \
    env PGPASSWORD="${PG_PW}" psql -U "${PG_USER}" -d "${DB_NAME}" -v ON_ERROR_STOP=1 \
    -c "DROP SCHEMA IF EXISTS public CASCADE;" \
    -c "CREATE SCHEMA public AUTHORIZATION \"${PG_USER}\";" \
    -c "GRANT ALL ON SCHEMA public TO \"${PG_USER}\";"
  kubectl exec -n "$NAMESPACE" "$DB_POD" -- \
    env PGPASSWORD="${PG_PW}" psql -U "${PG_USER}" -d "${DB_NAME}" -v ON_ERROR_STOP=1 -f "${REMOTE_SQL}"
fi

case "$FROM_VERSION" in
  1.2.0)
    assert_column view_meta audit_info 0
    assert_column user_meta enabled 0
    echo "Seeded ${FROM_VERSION}: view_meta.audit_info and user_meta.enabled absent"
    ;;
  1.3.0)
    assert_column view_meta audit_info 1
    assert_column user_meta enabled 0
    echo "Seeded ${FROM_VERSION}: view_meta.audit_info present, user_meta.enabled absent"
    ;;
esac

echo "==> Helm upgrade with schema.baselineVersion=${FROM_VERSION} (${ENGINE})"
helm upgrade "$RELEASE_NAME" "$CHART_DIR" -n "$NAMESPACE" --wait --timeout 10m \
  --values "$VALUES_FILE" \
  --set image.registry="" \
  --set image.repository="${KIND_REGISTRY}/gravitino" \
  --set image.tag="${tag}" \
  --set "schema.baselineVersion=${FROM_VERSION}" \
  --set "podAnnotations.schema-migrate-cross=$(date +%s)"

bash "$CHECK_SCRIPT" "$NAMESPACE" "$RELEASE_NAME" "$ENGINE"

JOB=$(kubectl get jobs -n "$NAMESPACE" -l app.kubernetes.io/component=schema-migrate \
  --sort-by=.metadata.creationTimestamp -o jsonpath='{.items[-1:].metadata.name}')
LOG="/tmp/${JOB}-cross.log"
kubectl logs -n "$NAMESPACE" "job/${JOB}" --all-containers=true | tee "$LOG"
grep -q "schema.baselineVersion=${FROM_VERSION}" "$LOG"
grep -q "upgrade-1.3.0-to-2.0.0-${ENGINE}" "$LOG"

case "$FROM_VERSION" in
  1.2.0)
    grep -q "upgrade-1.2.0-to-1.3.0-${ENGINE}" "$LOG"
    assert_column view_meta audit_info 1
    assert_column user_meta enabled 1
    ;;
  1.3.0)
    if grep -q "upgrade-1.2.0-to-1.3.0-${ENGINE}" "$LOG"; then
      echo "ERROR: unexpected upgrade-1.2.0-to-1.3.0 when baseline is 1.3.0" >&2
      exit 1
    fi
    assert_column view_meta audit_info 1
    assert_column user_meta enabled 1
    ;;
esac

echo "Cross-version schema migrate (${ENGINE} ${FROM_VERSION} -> image) passed"
