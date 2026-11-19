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

# Assert schema-migrate Job completed and gravitino_schema_migration is ready.
# Usage: check-schema-migrate.sh <namespace> <release> <mysql|postgresql>
set -euo pipefail

NS="${1:?namespace required}"
RELEASE="${2:?release required}"
ENGINE="${3:?engine mysql|postgresql required}"

echo "Checking schema-migrate Jobs in namespace ${NS}..."
kubectl get jobs -n "$NS" -l app.kubernetes.io/component=schema-migrate
kubectl wait -n "$NS" --for=condition=complete \
  job -l app.kubernetes.io/component=schema-migrate --timeout=300s

JOB=$(kubectl get jobs -n "$NS" -l app.kubernetes.io/component=schema-migrate \
  --sort-by=.metadata.creationTimestamp -o jsonpath='{.items[-1:].metadata.name}')
echo "Latest schema-migrate Job: ${JOB}"

HOOK=$(kubectl get job -n "$NS" "$JOB" -o jsonpath='{.metadata.annotations.helm\.sh/hook}' || true)
if [ -n "${HOOK}" ]; then
  echo "ERROR: Job ${JOB} still has helm.sh/hook=${HOOK}" >&2
  exit 1
fi

kubectl logs -n "$NS" "job/${JOB}" --all-containers=true | tee "/tmp/${JOB}.log"
grep -q "Schema migration completed successfully" "/tmp/${JOB}.log"
VERSION=$(sed -n 's/.*Recorded schema version \([0-9][0-9.]*\).*/\1/p' "/tmp/${JOB}.log" | tail -n1)
if [ -z "$VERSION" ]; then
  echo "ERROR: could not find recorded schema version in Job logs" >&2
  exit 1
fi
echo "Recorded schema version from Job log: ${VERSION}"

DEPLOY=$(kubectl get deploy -n "$NS" -l "release=${RELEASE}" -o jsonpath='{.items[0].metadata.name}')
if [ -z "$DEPLOY" ]; then
  echo "ERROR: no Deployment with release=${RELEASE}" >&2
  exit 1
fi

WAIT_INIT="init-${ENGINE}"
CMD=$(kubectl get deploy -n "$NS" "$DEPLOY" \
  -o jsonpath="{.spec.template.spec.initContainers[?(@.name==\"${WAIT_INIT}\")].command[2]}")
echo "$CMD" | grep -q "Waiting for schema version"
if echo "$CMD" | grep -Eq "upgrade-.*-to-.*-${ENGINE}"; then
  echo "ERROR: ${WAIT_INIT} still applies upgrade SQL" >&2
  exit 1
fi

# Resolve auth Secret from the wait-init container (matches Bitnami fullname
# collapse and mysql.auth.existingSecret / postgresql.auth.existingSecret).
init_secret_name() {
  local init_name="$1"
  local env_name="$2"
  local secret
  secret=$(kubectl get deploy -n "$NS" "$DEPLOY" \
    -o jsonpath="{.spec.template.spec.initContainers[?(@.name==\"${init_name}\")].env[?(@.name==\"${env_name}\")].valueFrom.secretKeyRef.name}")
  if [ -z "$secret" ]; then
    echo "ERROR: could not resolve Secret from ${DEPLOY}/${init_name}/${env_name}" >&2
    return 1
  fi
  printf '%s\n' "$secret"
}

DB_POD=$(kubectl get pods -n "$NS" -l "app.kubernetes.io/name=${ENGINE}" \
  -o jsonpath='{.items[0].metadata.name}')
case "$ENGINE" in
  mysql)
    DB_NAME=$(kubectl get deploy -n "$NS" "$DEPLOY" \
      -o jsonpath='{.spec.template.spec.initContainers[?(@.name=="init-mysql")].env[?(@.name=="GRAVITINO_DB")].value}')
    ROOT_SECRET=$(init_secret_name init-mysql MYSQL_PWD)
    ROOT_PW=$(kubectl get secret -n "$NS" "$ROOT_SECRET" -o jsonpath='{.data.mysql-root-password}' | base64 -d)
    RECORDED=$(kubectl exec -n "$NS" "$DB_POD" -- \
      mysql -uroot -p"${ROOT_PW}" -D "${DB_NAME}" -sNe "SELECT schema_version FROM gravitino_schema_migration WHERE id=1")
    HAS_AUDIT=$(kubectl exec -n "$NS" "$DB_POD" -- \
      mysql -uroot -p"${ROOT_PW}" -D "${DB_NAME}" -sNe "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='view_meta' AND column_name='audit_info'")
    ;;
  postgresql)
    DB_NAME=$(kubectl get deploy -n "$NS" "$DEPLOY" \
      -o jsonpath='{.spec.template.spec.initContainers[?(@.name=="init-postgresql")].env[?(@.name=="GRAVITINO_DB")].value}')
    PGUSER=$(kubectl get deploy -n "$NS" "$DEPLOY" \
      -o jsonpath='{.spec.template.spec.initContainers[?(@.name=="init-postgresql")].env[?(@.name=="POSTGRES_USER")].value}')
    PG_SECRET=$(init_secret_name init-postgresql POSTGRES_PASSWORD)
    PG_PW=$(kubectl get secret -n "$NS" "$PG_SECRET" -o jsonpath='{.data.password}' | base64 -d)
    RECORDED=$(kubectl exec -n "$NS" "$DB_POD" -- \
      env PGPASSWORD="${PG_PW}" psql -U "${PGUSER}" -d "${DB_NAME}" -tAc "SELECT schema_version FROM gravitino_schema_migration WHERE id=1")
    HAS_AUDIT=$(kubectl exec -n "$NS" "$DB_POD" -- \
      env PGPASSWORD="${PG_PW}" psql -U "${PGUSER}" -d "${DB_NAME}" -tAc "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='view_meta' AND column_name='audit_info'")
    ;;
  *)
    echo "ERROR: unsupported engine ${ENGINE}" >&2
    exit 1
    ;;
esac

RECORDED=$(echo "$RECORDED" | tr -d '[:space:]')
HAS_AUDIT=$(echo "$HAS_AUDIT" | tr -d '[:space:]')
echo "DB recorded schema version: ${RECORDED}"
if [ "$RECORDED" != "$VERSION" ]; then
  echo "ERROR: DB schema version ${RECORDED} != Job recorded ${VERSION}" >&2
  exit 1
fi
if [ "$HAS_AUDIT" != "1" ]; then
  echo "ERROR: view_meta.audit_info is missing after migration" >&2
  exit 1
fi

kubectl rollout status -n "$NS" "deploy/${DEPLOY}" --timeout=300s
echo "Schema migrate checks passed for ${ENGINE} in ${NS}"
